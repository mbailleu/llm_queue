"""Per-request timing/token metrics, cost, and the rolling-window summary."""
from __future__ import annotations

import time
from collections import deque
from typing import Any

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


_TOK_KEYS = ("input_tokens", "output_tokens",
             "cache_creation_input_tokens", "cache_read_input_tokens")


def _empty_window_bucket() -> dict[str, Any]:
    return {
        "durations": [],
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
        # (end, dur, model, status, in_tok, out_tok, cc_tok, cr_tok)
        self._completions: deque[
            tuple[float, float, str, int, int, int, int, int]
        ] = deque()
        self._active_per_model: dict[str, int] = {}
        self._max_age = max_window_seconds
        self._pricing: dict[str, dict[str, float]] = pricing or {}
        self._persist = persist
        # EWMA of request duration (seconds), updated per completion. Cheap
        # O(1) read for AutoPacer (which needs avg request time on every gate);
        # None until the first completion.
        self._ewma_duration: float | None = None

    def set_pricing(self, pricing: dict[str, dict[str, float]] | None) -> None:
        self._pricing = pricing or {}

    def request_started(self, model: str) -> float:
        self._active_per_model[model] = self._active_per_model.get(model, 0) + 1
        return time.time()

    def request_finished(self, model: str, started_at: float, status: int,
                         usage: dict | None = None) -> None:
        now = time.time()
        c = self._active_per_model.get(model, 0) - 1
        if c <= 0:
            self._active_per_model.pop(model, None)
        else:
            self._active_per_model[model] = c
        u = usage or {}
        self._completions.append((
            now,
            now - started_at,
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
        dur = now - started_at
        self._ewma_duration = dur if self._ewma_duration is None \
            else 0.2 * dur + 0.8 * self._ewma_duration
        if self._persist is not None:
            self._persist.record(model, status, dur, u)

    def avg_duration(self, fallback: float) -> float:
        """EWMA request duration in seconds, or `fallback` before any data."""
        return self._ewma_duration if self._ewma_duration is not None else fallback

    def set_max_age(self, seconds: float) -> None:
        self._max_age = seconds

    def _cost(self, model: str, in_t: int, out_t: int, cc_t: int, cr_t: int) -> float | None:
        return compute_cost(self._pricing, model, in_t, out_t, cc_t, cr_t)

    def summary(self) -> dict[str, Any]:
        now = time.time()
        cutoff = now - self._max_age
        while self._completions and self._completions[0][0] < cutoff:
            self._completions.popleft()

        overall = {label: _empty_window_bucket() for label, _ in METRIC_WINDOWS}
        per_model_buckets: dict[str, dict[str, dict[str, Any]]] = {}

        for end, dur, model, status, in_t, out_t, cc_t, cr_t in self._completions:
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
                out[label] = {
                    "count": b["success"] + b["errors"],
                    "success": b["success"],
                    "errors": b["errors"],
                    **st,
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
