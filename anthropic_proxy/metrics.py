"""In-memory rolling metrics (latency, tokens, cost) + pricing math.

Pure module: no FastAPI/httpx. `Metrics` keeps a 24h (configurable) deque of
completions and produces the windowed overall/per-model summaries the
dashboard shows; `compute_cost` is the single place pricing is applied.
"""
from __future__ import annotations

import time
from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .persistence import PersistentStats

METRIC_WINDOWS = [("1m", 60), ("10m", 600), ("1h", 3600), ("5h", 18000), ("24h", 86400)]


def _stats(durations: list[float]) -> dict[str, float | None]:
    n = len(durations)
    if n == 0:
        return {"avg_seconds": None, "p50_seconds": None, "p95_seconds": None}
    s = sorted(durations)
    return {
        "avg_seconds": sum(s) / n,
        "p50_seconds": s[n // 2],
        "p95_seconds": s[min(n - 1, int(n * 0.95))],
    }


def _empty_window_bucket() -> dict[str, Any]:
    return {
        "durations": [],
        # Slot-hold ("upstream") and queue/pacing ("wait") halves of each
        # duration, tracked separately: a request can wait far longer than it
        # runs, and averaging the two together hides both numbers.
        "upstream": [],
        "wait": [],
        "success": 0,
        "errors": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cost": 0.0,
        "priced_requests": 0,
    }


def compute_cost(pricing: dict[str, dict[str, float]], model: str,
                 in_t: int, out_t: int, cc_t: int, cr_t: int) -> float | None:
    """Cost (in the provider's billing unit) for one model's token totals.

    Returns None when the model has no pricing configured. cache_creation /
    cache_read default to the plain input rate when unset.
    """
    p = pricing.get(model)
    if not p:
        return None
    in_p  = float(p.get("input", 0) or 0)
    out_p = float(p.get("output", 0) or 0)
    cc_p  = float(p.get("cache_creation", in_p))
    cr_p  = float(p.get("cache_read", in_p))
    return (in_t * in_p + out_t * out_p + cc_t * cc_p + cr_t * cr_p) / 1_000_000


class Metrics:
    """Per-request timing + token tracker; 24h rolling window of completions."""

    def __init__(self, max_window_seconds: float = 86400,
                 pricing: dict[str, dict[str, float]] | None = None,
                 persist: "PersistentStats | None" = None):
        # (end, dur, upstream, model, status, in_tok, out_tok, cc_tok, cr_tok)
        self._completions: deque[
            tuple[float, float, float, str, int, int, int, int, int]
        ] = deque()
        self._active_per_model: dict[str, int] = {}
        self._max_age = max_window_seconds
        self._pricing: dict[str, dict[str, float]] = pricing or {}
        self._persist = persist
        # EWMAs updated per completion; None until the first one.
        # `_ewma_duration` is the wall-clock a client experienced (queueing,
        # pacing and 429 backoff included) — the honest "how long did my call
        # take" number.
        # `_ewma_upstream` is only the time a concurrency slot was actually
        # held. That is the request's service time, and the one AutoPacer must
        # use for its Little's-Law throughput cap: feeding the total in there
        # creates a feedback loop where pacing delay inflates the measured
        # duration, which lowers the cap, which delays pacing further.
        self._ewma_duration: float | None = None
        self._ewma_upstream: float | None = None

    def set_pricing(self, pricing: dict[str, dict[str, float]] | None) -> None:
        self._pricing = pricing or {}

    def request_started(self, model: str) -> float:
        self._active_per_model[model] = self._active_per_model.get(model, 0) + 1
        return time.time()

    def request_finished(self, model: str, started_at: float, status: int,
                         usage: dict | None = None,
                         upstream_seconds: float | None = None) -> None:
        """Record one completed request.

        `started_at` is from `request_started` (the client's clock: it starts
        before pacing and queueing). `upstream_seconds` is how long the request
        actually held a concurrency slot, summed over its attempts — the caller
        measures it because only it knows when the slot was acquired and
        released. None means "not measured", and the whole duration is counted
        as upstream so the split degrades to the old single number rather than
        reporting a bogus zero wait.
        """
        now = time.time()
        c = self._active_per_model.get(model, 0) - 1
        if c <= 0:
            self._active_per_model.pop(model, None)
        else:
            self._active_per_model[model] = c
        u = usage or {}
        dur = max(0.0, now - started_at)
        # Clamped into [0, dur]: the two clocks differ (wall vs monotonic) and
        # the lane-split invariant upstream + wait == dur must hold regardless.
        upstream = dur if upstream_seconds is None else min(max(0.0, upstream_seconds), dur)
        self._completions.append((
            now,
            dur,
            upstream,
            model,
            status,
            int(u.get("input_tokens", 0) or 0),
            int(u.get("output_tokens", 0) or 0),
            int(u.get("cache_creation_input_tokens", 0) or 0),
            int(u.get("cache_read_input_tokens", 0) or 0),
        ))
        cutoff = now - self._max_age
        while self._completions and self._completions[0][0] < cutoff:
            self._completions.popleft()
        self._ewma_duration = dur if self._ewma_duration is None \
            else 0.2 * dur + 0.8 * self._ewma_duration
        self._ewma_upstream = upstream if self._ewma_upstream is None \
            else 0.2 * upstream + 0.8 * self._ewma_upstream
        if self._persist is not None:
            self._persist.record(model, status, dur, u, upstream)

    def avg_duration(self, fallback: float) -> float:
        """EWMA of total client-visible duration, or `fallback` before any data.

        Includes pacing/queue/backoff wait — do NOT use this to size throughput
        (see `avg_upstream_time`).
        """
        return self._ewma_duration if self._ewma_duration is not None else fallback

    def avg_upstream_time(self, fallback: float) -> float:
        """EWMA of slot-hold (service) time, or `fallback` before any data.

        This is the throughput-relevant one: `concurrency / avg_upstream_time`
        is how many requests per second can actually complete.
        """
        return self._ewma_upstream if self._ewma_upstream is not None else fallback

    def set_max_age(self, seconds: float) -> None:
        self._max_age = seconds

    def _cost(self, model: str, in_t: int, out_t: int, cc_t: int, cr_t: int) -> float | None:
        return compute_cost(self._pricing, model, in_t, out_t, cc_t, cr_t)

    def cost_of(self, model: str, usage: dict | None) -> float | None:
        """Priced cost of one request's usage dict (None when model unpriced)."""
        u = usage or {}
        return self._cost(
            model,
            int(u.get("input_tokens", 0) or 0),
            int(u.get("output_tokens", 0) or 0),
            int(u.get("cache_creation_input_tokens", 0) or 0),
            int(u.get("cache_read_input_tokens", 0) or 0),
        )

    def summary(self) -> dict[str, Any]:
        now = time.time()
        cutoff = now - self._max_age
        while self._completions and self._completions[0][0] < cutoff:
            self._completions.popleft()

        overall = {label: _empty_window_bucket() for label, _ in METRIC_WINDOWS}
        per_model_buckets: dict[str, dict[str, dict[str, Any]]] = {}

        for end, dur, ups, model, status, in_t, out_t, cc_t, cr_t in self._completions:
            age = now - end
            is_success = 200 <= status < 400
            cost = self._cost(model, in_t, out_t, cc_t, cr_t)
            mb = per_model_buckets.get(model)
            if mb is None:
                mb = {label: _empty_window_bucket() for label, _ in METRIC_WINDOWS}
                per_model_buckets[model] = mb
            for label, w in METRIC_WINDOWS:
                if age > w:
                    continue
                for bucket in (overall[label], mb[label]):
                    bucket["durations"].append(dur)
                    bucket["upstream"].append(ups)
                    bucket["wait"].append(max(0.0, dur - ups))
                    if is_success:
                        bucket["success"] += 1
                    else:
                        bucket["errors"] += 1
                    bucket["input_tokens"] += in_t
                    bucket["output_tokens"] += out_t
                    bucket["cache_creation_input_tokens"] += cc_t
                    bucket["cache_read_input_tokens"] += cr_t
                    if cost is not None:
                        bucket["cost"] += cost
                        bucket["priced_requests"] += 1

        def finalize(bucket_set: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
            out: dict[str, dict[str, Any]] = {}
            for label, b in bucket_set.items():
                st = _stats(b["durations"])
                ups = _stats(b["upstream"])
                wait = _stats(b["wait"])
                out[label] = {
                    "count": b["success"] + b["errors"],
                    "success": b["success"],
                    "errors": b["errors"],
                    # avg/p50/p95_seconds stay the total (client-visible)
                    # latency; the upstream/wait pairs split it into service
                    # time and time spent queued, paced or backing off.
                    **st,
                    "avg_upstream_seconds": ups["avg_seconds"],
                    "p50_upstream_seconds": ups["p50_seconds"],
                    "p95_upstream_seconds": ups["p95_seconds"],
                    "avg_wait_seconds": wait["avg_seconds"],
                    "p95_wait_seconds": wait["p95_seconds"],
                    "input_tokens": b["input_tokens"],
                    "output_tokens": b["output_tokens"],
                    "cache_creation_input_tokens": b["cache_creation_input_tokens"],
                    "cache_read_input_tokens": b["cache_read_input_tokens"],
                    "cost": b["cost"] if b["priced_requests"] > 0 else None,
                    "priced_requests": b["priced_requests"],
                }
            return out

        per_model_out: dict[str, dict[str, Any]] = {}
        for model, buckets in per_model_buckets.items():
            per_model_out[model] = finalize(buckets)

        for model in self._active_per_model:
            per_model_out.setdefault(model, finalize(
                {label: _empty_window_bucket() for label, _ in METRIC_WINDOWS}
            ))

        for model in per_model_out:
            per_model_out[model]["active"] = self._active_per_model.get(model, 0)
            per_model_out[model]["has_pricing"] = model in self._pricing

        return {
            "overall": finalize(overall),
            "per_model": per_model_out,
            "total_active": sum(self._active_per_model.values()),
            "pricing_configured_models": sorted(self._pricing.keys()),
        }
