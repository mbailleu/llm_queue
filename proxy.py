# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "fastapi>=0.110",
#     "httpx>=0.27",
#     "uvicorn>=0.29",
#     "pyyaml>=6.0",
# ]
# ///
"""
anthropic_proxy — queueing proxy for LLM APIs.

Forwards every path/header verbatim, so it fronts both the Anthropic Messages
API (/v1/messages) and the OpenAI-compatible API (/v1/chat/completions,
/v1/responses) through one shared queue. Token/cost metrics understand all
three usage shapes.

Two concurrency tiers, auto-detected by probing under load:
  - LOW:  max_concurrent=4
  - HIGH: max_concurrent=1000
There is no preemptive rate pacing; when upstream returns 429/503/529 the
request is retried with backoff (honoring Retry-After) and callers wait.

Web dashboard at  http://<host>:<port>/_proxy/
Raw metrics JSON  http://<host>:<port>/_proxy/metrics
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse


CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config.yaml")).resolve()

DEFAULT_CONFIG: dict[str, Any] = {
    "upstream_base_url": "https://api.anthropic.com",
    "listen_host": "127.0.0.1",
    "listen_port": 8787,
    "initial_tier": "low",
    "force_tier": None,
    "tiers": {
        "low":  {"max_concurrent": 4},
        "high": {"max_concurrent": 1000},
    },
    "promotion_cooldown_seconds": 300,
    "retry_max_attempts": 12,
    "retry_base_delay": 1.0,
    "retry_max_delay": 60.0,
    # How long a rate-limited (429/503/529) request may keep waiting+retrying
    # before the proxy gives up and returns the error to the client. Defaults
    # to a bit over the 5h quota window so a queued request can outlast a full
    # window and run once the quota resets. (retry_max_attempts still bounds
    # connection-error retries, which a long wait won't fix.)
    "retry_max_elapsed_seconds": 18900,
    # Rolling request-quota window, for the dashboard "X / N this window"
    # indicator. Tracked, not enforced.
    "rate_window_seconds": 18000,   # 5h
    "rate_window_limit": 600,
    "upstream_timeout": 600,
    "log_level": "INFO",
    "config_poll_seconds": 2.0,
    "metrics_window_seconds": 86400,
    "model_pricing": {},
    # Long-horizon persisted stats (weekly/monthly/lifetime + graphs).
    "stats_persist_path": "stats.json",
    "stats_flush_seconds": 60.0,
    "stats_retention_days": 120,
}

RATE_LIMIT_STATUSES = {429, 503, 529}
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
    "content-encoding",  # stripped because we forward already-decoded chunks
}
METRIC_WINDOWS = [("1m", 60), ("10m", 600), ("1h", 3600), ("5h", 18000), ("24h", 86400)]

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("proxy")


# ---------- Tier + Limiter ----------

class Tier:
    __slots__ = ("name", "max_concurrent")

    def __init__(self, name: str, max_concurrent: int):
        self.name = name
        self.max_concurrent = max_concurrent


class Limiter:
    """Concurrency cap (with a queue) plus auto-tier-detection.

    There is no preemptive per-window pacing: requests are admitted as fast as
    the concurrency cap allows. When the upstream rate limit is hit, the 429/
    503/529 retry+backoff in the proxy handler is what makes callers wait.
    """

    def __init__(self, low: Tier, high: Tier, initial_tier: str,
                 promotion_cooldown: float, forced: str | None,
                 window_seconds: float = 18000.0, window_limit: int = 600):
        self._cond = asyncio.Condition()
        self._low = low
        self._high = high
        self._active = high if initial_tier == "high" else low
        self._forced: str | None = forced if forced in ("low", "high") else None
        if self._forced:
            self._active = self._low if self._forced == "low" else self._high
        self._promotion_cooldown = promotion_cooldown
        self._last_demotion = 0.0
        self._in_flight = 0
        self._waiters = 0
        self._probe_in_flight = False
        # Rolling request-quota window (the upstream "N requests per W seconds"
        # budget). Anchored at the first request after the previous window
        # expired, matching "the 5h starts when the first request is sent".
        # Tracked for display; not enforced (upstream 429s + retry do that).
        self._window_seconds = float(window_seconds)
        self._window_limit = int(window_limit)
        self._window_start: float | None = None
        self._window_count = 0
        self._n_requests = 0
        self._n_rate_limited = 0
        self._n_other_errors = 0
        self._n_concurrency_waits = 0
        self._n_promotions = 0
        self._n_demotions = 0
        self._n_probes_sent = 0

    def note_request(self) -> None:
        """Count one client request against the rolling quota window.

        Called once per client request (not per retry). Starts a fresh window
        when there is none active or the current one has elapsed. Synchronous
        and await-free, so it's atomic under the single-threaded event loop.
        """
        now = time.time()
        if self._window_start is None or now - self._window_start >= self._window_seconds:
            self._window_start = now
            self._window_count = 0
        self._window_count += 1

    def _window_snapshot(self) -> dict[str, Any]:
        now = time.time()
        ws = self._window_start
        active = ws is not None and now - ws < self._window_seconds
        if not active:
            return {
                "active": False,
                "limit": self._window_limit,
                "window_seconds": self._window_seconds,
                "count": 0,
                "started_at": None,
                "elapsed_seconds": None,
                "remaining_seconds": None,
            }
        elapsed = now - ws
        return {
            "active": True,
            "limit": self._window_limit,
            "window_seconds": self._window_seconds,
            "count": self._window_count,
            "started_at": ws,
            "elapsed_seconds": elapsed,
            "remaining_seconds": max(0.0, self._window_seconds - elapsed),
        }

    async def acquire(self) -> bool:
        async with self._cond:
            self._waiters += 1
            try:
                while True:
                    now = time.monotonic()

                    if self._in_flight < self._active.max_concurrent:
                        self._in_flight += 1
                        self._n_requests += 1
                        return False

                    can_probe = (
                        self._forced is None
                        and self._active is self._low
                        and not self._probe_in_flight
                        and self._waiters > 0
                        and now - self._last_demotion >= self._promotion_cooldown
                    )
                    if can_probe:
                        self._in_flight += 1
                        self._n_requests += 1
                        self._probe_in_flight = True
                        self._n_probes_sent += 1
                        log.info(
                            f"probing HIGH tier (in_flight={self._in_flight}, "
                            f"waiters={self._waiters})"
                        )
                        return True

                    self._n_concurrency_waits += 1
                    await self._cond.wait()
            finally:
                self._waiters -= 1

    async def release_success(self, was_probe: bool) -> None:
        async with self._cond:
            self._in_flight -= 1
            if was_probe:
                self._probe_in_flight = False
                if self._active is self._low and self._forced is None:
                    self._active = self._high
                    self._n_promotions += 1
                    log.warning(
                        f"tier promoted LOW -> HIGH (max_concurrent={self._high.max_concurrent})"
                    )
            self._cond.notify_all()

    async def release_rate_limited(self, was_probe: bool) -> None:
        async with self._cond:
            self._in_flight -= 1
            self._n_rate_limited += 1
            now = time.monotonic()
            if was_probe:
                self._probe_in_flight = False
                self._last_demotion = now
                if self._active is self._low:
                    log.info("probe failed; staying LOW, cooldown reset")
            if self._active is self._high and self._forced is None:
                self._active = self._low
                self._n_demotions += 1
                self._last_demotion = now
                log.warning(
                    f"tier demoted HIGH -> LOW (max_concurrent={self._low.max_concurrent})"
                )
            self._cond.notify_all()

    async def release_other_error(self, was_probe: bool) -> None:
        async with self._cond:
            self._in_flight -= 1
            self._n_other_errors += 1
            if was_probe:
                self._probe_in_flight = False
            self._cond.notify_all()

    async def boost_high(self) -> bool:
        """Temporarily jump to HIGH without pinning it.

        Unlike force_tier="high", auto-demotion stays enabled: the first
        rate-limited response (429/503/529) drops back to LOW. Refused while a
        force_tier is set, since that pins the tier explicitly.
        """
        async with self._cond:
            if self._forced is not None:
                return False
            if self._active is not self._high:
                self._active = self._high
                self._n_promotions += 1
                log.warning(
                    "tier boosted LOW -> HIGH (temporary; auto-demotes on "
                    f"rate-limit; max_concurrent={self._high.max_concurrent})"
                )
            self._cond.notify_all()
            return True

    async def update_tiers(self, low: Tier, high: Tier,
                           promotion_cooldown: float, forced: str | None,
                           window_seconds: float | None = None,
                           window_limit: int | None = None) -> None:
        async with self._cond:
            self._low = low
            self._high = high
            self._promotion_cooldown = promotion_cooldown
            self._forced = forced if forced in ("low", "high") else None
            if window_seconds is not None:
                self._window_seconds = float(window_seconds)
            if window_limit is not None:
                self._window_limit = int(window_limit)
            if self._forced == "low":
                self._active = self._low
            elif self._forced == "high":
                self._active = self._high
            else:
                self._active = self._low if self._active.name == "low" else self._high
            self._cond.notify_all()

    def snapshot(self) -> dict[str, Any]:
        return {
            "active_tier": self._active.name,
            "forced_tier": self._forced,
            "max_concurrent": self._active.max_concurrent,
            "in_flight": self._in_flight,
            "queued": self._waiters,
            "probe_in_flight": self._probe_in_flight,
            "window": self._window_snapshot(),
            "totals": {
                "requests": self._n_requests,
                "rate_limited": self._n_rate_limited,
                "other_errors": self._n_other_errors,
                "concurrency_waits": self._n_concurrency_waits,
                "promotions": self._n_promotions,
                "demotions": self._n_demotions,
                "probes_sent": self._n_probes_sent,
            },
        }


# ---------- Metrics ----------

def normalize_usage(usage: Any) -> dict | None:
    """Map a provider `usage` block to our canonical 4-field shape.

    Handles three wire formats:
      - Anthropic Messages:   input_tokens / output_tokens /
                              cache_creation_input_tokens / cache_read_input_tokens
      - OpenAI Responses:     input_tokens / output_tokens, with the cached
                              subset in input_tokens_details.cached_tokens
                              (input_tokens is inclusive of cached)
      - OpenAI Chat/Completions: prompt_tokens / completion_tokens, with the
                              cached subset in prompt_tokens_details.cached_tokens
                              (prompt_tokens is inclusive of cached)

    For the OpenAI shapes we split the cached tokens out of the prompt so
    `input_tokens` and `cache_read_input_tokens` stay disjoint (matching how
    Anthropic reports them, and how the per-token pricing is applied).
    OpenAI has no separate cache-write charge, so cache_creation stays 0.
    """
    if not isinstance(usage, dict):
        return None

    if "input_tokens" in usage or "output_tokens" in usage:
        inp = int(usage.get("input_tokens", 0) or 0)
        out = int(usage.get("output_tokens", 0) or 0)
        cc = int(usage.get("cache_creation_input_tokens", 0) or 0)
        cr = int(usage.get("cache_read_input_tokens", 0) or 0)
        details = usage.get("input_tokens_details")
        if isinstance(details, dict) and "cached_tokens" in details:
            # OpenAI Responses API: input_tokens is inclusive of cached.
            cr = int(details.get("cached_tokens", 0) or 0)
            inp = max(0, inp - cr)
        return {
            "input_tokens": inp,
            "output_tokens": out,
            "cache_creation_input_tokens": cc,
            "cache_read_input_tokens": cr,
        }

    if "prompt_tokens" in usage or "completion_tokens" in usage:
        prompt = int(usage.get("prompt_tokens", 0) or 0)
        completion = int(usage.get("completion_tokens", 0) or 0)
        details = usage.get("prompt_tokens_details")
        cached = 0
        if isinstance(details, dict):
            cached = int(details.get("cached_tokens", 0) or 0)
        return {
            "input_tokens": max(0, prompt - cached),
            "output_tokens": completion,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": cached,
        }

    return None


class SSEUsageExtractor:
    """Watches an SSE byte stream for usage info (Anthropic + OpenAI).

    Anthropic emits a `message_start` event whose data contains
    `message.usage` (input_tokens, cache_*), and a `message_delta` event whose
    data contains `usage.output_tokens`. OpenAI Chat Completions emit a final
    chunk carrying a top-level `usage` (only when the client sets
    `stream_options.include_usage`); the OpenAI Responses API nests usage under
    `response.usage` on `response.completed`. We accumulate field-wise maxima
    across whatever arrives and return them once the stream ends.
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self._input = 0
        self._output = 0
        self._cache_creation = 0
        self._cache_read = 0
        self._got_any = False

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buf.extend(chunk)
        while True:
            sep = self._buf.find(b"\n\n")
            if sep < 0:
                if len(self._buf) > 65536:
                    # Drop everything but the last 64KB to bound memory.
                    del self._buf[: len(self._buf) - 65536]
                return
            event = bytes(self._buf[:sep])
            del self._buf[: sep + 2]
            self._parse_event(event)

    def _parse_event(self, block: bytes) -> None:
        data_parts: list[bytes] = []
        for line in block.split(b"\n"):
            if line.startswith(b"data:"):
                data_parts.append(line[5:].lstrip())
        if not data_parts:
            return
        try:
            obj = json.loads(b"\n".join(data_parts))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        if not isinstance(obj, dict):
            return
        et = obj.get("type")
        if et == "message_start":
            self._merge((obj.get("message") or {}).get("usage"))
            return
        if et == "message_delta":
            self._merge(obj.get("usage"))
            return
        # OpenAI Responses API: usage rides on response.completed/.incomplete.
        resp = obj.get("response")
        if isinstance(resp, dict) and isinstance(resp.get("usage"), dict):
            self._merge(resp["usage"])
            return
        # OpenAI Chat Completions: final chunk carries a top-level usage.
        if isinstance(obj.get("usage"), dict):
            self._merge(obj["usage"])

    def _merge(self, usage: Any) -> None:
        norm = normalize_usage(usage)
        if norm is None:
            return
        self._got_any = True
        self._input = max(self._input, norm["input_tokens"])
        self._output = max(self._output, norm["output_tokens"])
        self._cache_creation = max(self._cache_creation, norm["cache_creation_input_tokens"])
        self._cache_read = max(self._cache_read, norm["cache_read_input_tokens"])

    def final_usage(self) -> dict | None:
        if not self._got_any:
            return None
        return {
            "input_tokens": self._input,
            "output_tokens": self._output,
            "cache_creation_input_tokens": self._cache_creation,
            "cache_read_input_tokens": self._cache_read,
        }


class JSONUsageExtractor:
    """Buffers a non-streaming JSON response body and extracts top-level usage.

    Works for Anthropic Messages and OpenAI Chat Completions / Responses, all of
    which put a `usage` object at the top level of the response body.
    """

    def __init__(self, max_bytes: int = 8 * 1024 * 1024) -> None:
        self._buf = bytearray()
        self._max = max_bytes
        self._oversize = False

    def feed(self, chunk: bytes) -> None:
        if self._oversize or not chunk:
            return
        if len(self._buf) + len(chunk) > self._max:
            self._oversize = True
            self._buf.clear()
            return
        self._buf.extend(chunk)

    def final_usage(self) -> dict | None:
        if self._oversize or not self._buf:
            return None
        try:
            obj = json.loads(bytes(self._buf))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        if not isinstance(obj, dict):
            return None
        return normalize_usage(obj.get("usage"))


def make_extractor(content_type: str):
    if "text/event-stream" in content_type.lower():
        return SSEUsageExtractor()
    return JSONUsageExtractor()


def extract_model(method: str, body: bytes) -> str:
    """Extract `model` from a JSON request body. Falls back to '(unknown)'."""
    if method == "GET" or not body:
        return "(no-body)"
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        return "(unknown)"
    if isinstance(data, dict):
        m = data.get("model")
        if isinstance(m, str) and m:
            return m
    return "(unknown)"


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
        if self._persist is not None:
            self._persist.record(model, status, now - started_at, u)

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


# ---------- Persistent long-horizon stats ----------

PERSIST_VERSION = 1
HOUR_SECONDS = 3600
WEEK_SECONDS = 7 * 86400
MONTH_SECONDS = 30 * 86400

# Aggregated per-bucket counters. Percentiles can't be merged across buckets, so
# long-term we keep duration_sum (-> average) only; cost is derived at read time
# from current pricing rather than stored, so re-pricing applies retroactively.
_COUNTER_KEYS = (
    "count", "success", "errors",
    "input_tokens", "output_tokens",
    "cache_creation_input_tokens", "cache_read_input_tokens",
    "duration_sum",
)


def _empty_counter() -> dict[str, float]:
    return {k: 0 for k in _COUNTER_KEYS}


def _add_counter(dst: dict[str, float], src: dict[str, Any]) -> None:
    for k in _COUNTER_KEYS:
        dst[k] += src.get(k, 0) or 0


class PersistentStats:
    """Disk-backed aggregated stats for weekly/monthly/lifetime + time series.

    Rather than recording every request, completions are folded into hourly
    per-model counter buckets (plus running lifetime totals) and flushed to a
    JSON file on a fixed interval. Hourly buckets older than the retention
    window are pruned; lifetime totals are kept forever.
    """

    def __init__(self, path: str, flush_seconds: float = 60.0,
                 retention_days: float = 120,
                 pricing: dict[str, dict[str, float]] | None = None):
        self._path = Path(path).resolve()
        self._flush_seconds = float(flush_seconds)
        self._retention = float(retention_days) * 86400
        self._pricing: dict[str, dict[str, float]] = pricing or {}
        self._hours: dict[int, dict[str, dict[str, float]]] = {}
        self._lifetime: dict[str, dict[str, float]] = {}
        self._started_at = time.time()
        self._dirty = False
        self._last_flush = 0.0
        self._load()

    # -- config wiring --

    def set_pricing(self, pricing: dict[str, dict[str, float]] | None) -> None:
        self._pricing = pricing or {}

    def configure(self, flush_seconds: float, retention_days: float) -> None:
        self._flush_seconds = float(flush_seconds)
        self._retention = float(retention_days) * 86400

    # -- persistence --

    def _load(self) -> None:
        try:
            if not self._path.exists():
                return
            with open(self._path) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError, ValueError) as e:
            log.warning(f"stats: could not load {self._path}: {e!r}")
            return
        if not isinstance(data, dict):
            return
        self._started_at = float(data.get("started_at") or self._started_at)
        for model, c in (data.get("lifetime") or {}).items():
            if isinstance(c, dict):
                m = _empty_counter(); _add_counter(m, c); self._lifetime[model] = m
        for hk, models in (data.get("hours") or {}).items():
            try:
                hour = int(hk)
            except (TypeError, ValueError):
                continue
            if not isinstance(models, dict):
                continue
            bucket: dict[str, dict[str, float]] = {}
            for model, c in models.items():
                if isinstance(c, dict):
                    m = _empty_counter(); _add_counter(m, c); bucket[model] = m
            if bucket:
                self._hours[hour] = bucket
        self._prune()
        log.info(
            f"stats: loaded {len(self._hours)} hourly buckets, "
            f"{len(self._lifetime)} lifetime models from {self._path}"
        )

    def _prune(self) -> None:
        if not self._hours:
            return
        cutoff = time.time() - self._retention
        for h in [h for h in self._hours if h + HOUR_SECONDS <= cutoff]:
            del self._hours[h]

    def _serialize(self) -> dict[str, Any]:
        return {
            "version": PERSIST_VERSION,
            "started_at": self._started_at,
            "saved_at": time.time(),
            "lifetime": self._lifetime,
            "hours": {str(h): m for h, m in self._hours.items()},
        }

    def _write(self, data: dict[str, Any]) -> None:
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        os.replace(tmp, self._path)

    async def maybe_flush(self, force: bool = False) -> None:
        now = time.time()
        if not force:
            if not self._dirty or now - self._last_flush < self._flush_seconds:
                return
        if not self._dirty and not force:
            return
        self._prune()
        data = self._serialize()
        self._last_flush = now
        self._dirty = False
        try:
            await asyncio.to_thread(self._write, data)
        except OSError as e:
            self._dirty = True
            log.warning(f"stats: flush to {self._path} failed: {e!r}")

    # -- recording --

    def record(self, model: str, status: int, duration: float,
               usage: dict | None) -> None:
        now = time.time()
        hour = int(now // HOUR_SECONDS) * HOUR_SECONDS
        u = usage or {}
        in_t = int(u.get("input_tokens", 0) or 0)
        out_t = int(u.get("output_tokens", 0) or 0)
        cc_t = int(u.get("cache_creation_input_tokens", 0) or 0)
        cr_t = int(u.get("cache_read_input_tokens", 0) or 0)
        success = 1 if 200 <= status < 400 else 0
        bucket = self._hours.setdefault(hour, {})
        for store in (bucket.setdefault(model, _empty_counter()),
                      self._lifetime.setdefault(model, _empty_counter())):
            store["count"] += 1
            store["success"] += success
            store["errors"] += 1 - success
            store["input_tokens"] += in_t
            store["output_tokens"] += out_t
            store["cache_creation_input_tokens"] += cc_t
            store["cache_read_input_tokens"] += cr_t
            store["duration_sum"] += duration
        self._dirty = True

    # -- read models --

    def _window_models(self, window_seconds: float | None) -> dict[str, dict[str, float]]:
        """Per-model counters aggregated over a trailing window (None=lifetime)."""
        if window_seconds is None:
            return {m: dict(c) for m, c in self._lifetime.items()}
        cutoff = time.time() - window_seconds
        agg: dict[str, dict[str, float]] = {}
        for hour, models in self._hours.items():
            if hour + HOUR_SECONDS <= cutoff:
                continue
            for model, c in models.items():
                _add_counter(agg.setdefault(model, _empty_counter()), c)
        return agg

    def _format_model(self, model: str, c: dict[str, float]) -> dict[str, Any]:
        cnt = c["count"]
        return {
            "count": int(cnt),
            "success": int(c["success"]),
            "errors": int(c["errors"]),
            "avg_seconds": (c["duration_sum"] / cnt) if cnt else None,
            "input_tokens": int(c["input_tokens"]),
            "output_tokens": int(c["output_tokens"]),
            "cache_creation_input_tokens": int(c["cache_creation_input_tokens"]),
            "cache_read_input_tokens": int(c["cache_read_input_tokens"]),
            "cost": compute_cost(
                self._pricing, model,
                int(c["input_tokens"]), int(c["output_tokens"]),
                int(c["cache_creation_input_tokens"]),
                int(c["cache_read_input_tokens"]),
            ),
            "has_pricing": model in self._pricing,
        }

    def _format_overall(self, models: dict[str, dict[str, float]]) -> dict[str, Any]:
        total = _empty_counter()
        cost = 0.0
        priced = 0
        for model, c in models.items():
            _add_counter(total, c)
            mc = compute_cost(
                self._pricing, model,
                int(c["input_tokens"]), int(c["output_tokens"]),
                int(c["cache_creation_input_tokens"]),
                int(c["cache_read_input_tokens"]),
            )
            if mc is not None:
                cost += mc
                priced += int(c["count"])
        cnt = total["count"]
        return {
            "count": int(cnt),
            "success": int(total["success"]),
            "errors": int(total["errors"]),
            "avg_seconds": (total["duration_sum"] / cnt) if cnt else None,
            "input_tokens": int(total["input_tokens"]),
            "output_tokens": int(total["output_tokens"]),
            "cache_creation_input_tokens": int(total["cache_creation_input_tokens"]),
            "cache_read_input_tokens": int(total["cache_read_input_tokens"]),
            "cost": cost if priced > 0 else None,
            "priced_requests": priced,
        }

    def summary(self) -> dict[str, Any]:
        windows: list[tuple[str, float | None]] = [
            ("24h", 86400.0),
            ("7d", float(WEEK_SECONDS)),
            ("30d", float(MONTH_SECONDS)),
            ("lifetime", None),
        ]
        out: dict[str, Any] = {"since": self._started_at}
        for label, secs in windows:
            models = self._window_models(secs)
            out[label] = {
                "overall": self._format_overall(models),
                "per_model": {m: self._format_model(m, c) for m, c in models.items()},
            }
        return out

    def series(self, window: str) -> dict[str, Any]:
        """Bucketed time series for graphing requests + tokens.

        24h / 7d use hourly buckets; 30d / lifetime roll up into daily buckets.
        Empty buckets are emitted so the x-axis is evenly spaced.
        """
        now = time.time()
        if window == "24h":
            span, step = 86400, HOUR_SECONDS
        elif window == "7d":
            span, step = WEEK_SECONDS, HOUR_SECONDS
        elif window == "30d":
            span, step = MONTH_SECONDS, 86400
        else:
            window = "lifetime"
            span, step = int(self._retention), 86400
        floor = int(now // step) * step
        start = floor - span + step
        bins: dict[int, dict[str, float]] = {}
        earliest = now - span
        for hour, models in self._hours.items():
            if hour + HOUR_SECONDS <= earliest:
                continue
            b = bins.setdefault((hour // step) * step, _empty_counter())
            for c in models.values():
                _add_counter(b, c)
        points = []
        t = start
        while t <= floor:
            c = bins.get(t)
            points.append({
                "t": t,
                "requests": int(c["count"]) if c else 0,
                "errors": int(c["errors"]) if c else 0,
                "input_tokens": int(c["input_tokens"]) if c else 0,
                "output_tokens": int(c["output_tokens"]) if c else 0,
                "cache_creation_input_tokens": int(c["cache_creation_input_tokens"]) if c else 0,
                "cache_read_input_tokens": int(c["cache_read_input_tokens"]) if c else 0,
            })
            t += step
        return {"window": window, "step": step, "points": points}


# ---------- Config loading & hot-reload ----------

config: dict[str, Any] = {}
config_mtime: float = 0.0
limiter: Limiter | None = None
metrics: Metrics | None = None
pstats: PersistentStats | None = None
client: httpx.AsyncClient | None = None


def load_config_file() -> dict[str, Any]:
    merged: dict[str, Any] = {**DEFAULT_CONFIG, "tiers": dict(DEFAULT_CONFIG["tiers"])}
    if not CONFIG_PATH.exists():
        log.warning(f"config file {CONFIG_PATH} not found; using defaults")
        return merged
    with open(CONFIG_PATH) as f:
        loaded = yaml.safe_load(f) or {}
    for k, v in loaded.items():
        if k == "tiers" and isinstance(v, dict):
            merged["tiers"] = {**merged["tiers"], **v}
        else:
            merged[k] = v
    return merged


def make_tier(cfg: dict[str, Any], name: str) -> Tier:
    t = cfg["tiers"][name]
    return Tier(name=name, max_concurrent=int(t["max_concurrent"]))


def init_from_config(cfg: dict[str, Any]) -> tuple[Limiter, Metrics, PersistentStats]:
    forced = cfg.get("force_tier") if cfg.get("force_tier") in ("low", "high") else None
    lim = Limiter(
        low=make_tier(cfg, "low"),
        high=make_tier(cfg, "high"),
        initial_tier=cfg.get("initial_tier", "low"),
        promotion_cooldown=float(cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_seconds=float(cfg.get("rate_window_seconds", 18000)),
        window_limit=int(cfg.get("rate_window_limit", 600)),
    )
    pricing = cfg.get("model_pricing") or {}
    if not isinstance(pricing, dict):
        pricing = {}
    ps = PersistentStats(
        path=str(cfg.get("stats_persist_path", "stats.json")),
        flush_seconds=float(cfg.get("stats_flush_seconds", 60.0)),
        retention_days=float(cfg.get("stats_retention_days", 120)),
        pricing=pricing,
    )
    met = Metrics(
        max_window_seconds=float(cfg["metrics_window_seconds"]),
        pricing=pricing,
        persist=ps,
    )
    return lim, met, ps


async def apply_config_change(new_cfg: dict[str, Any]) -> None:
    global config
    if new_cfg.get("upstream_base_url") != config.get("upstream_base_url"):
        log.warning("config: upstream_base_url changed -> restart required")
    log_level = str(new_cfg.get("log_level", "INFO")).upper()
    logging.getLogger().setLevel(log_level)
    forced = new_cfg.get("force_tier") if new_cfg.get("force_tier") in ("low", "high") else None
    await limiter.update_tiers(
        low=make_tier(new_cfg, "low"),
        high=make_tier(new_cfg, "high"),
        promotion_cooldown=float(new_cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_seconds=float(new_cfg.get("rate_window_seconds", 18000)),
        window_limit=int(new_cfg.get("rate_window_limit", 600)),
    )
    metrics.set_max_age(float(new_cfg["metrics_window_seconds"]))
    new_pricing = new_cfg.get("model_pricing") or {}
    new_pricing = new_pricing if isinstance(new_pricing, dict) else {}
    metrics.set_pricing(new_pricing)
    pstats.set_pricing(new_pricing)
    pstats.configure(
        flush_seconds=float(new_cfg.get("stats_flush_seconds", 60.0)),
        retention_days=float(new_cfg.get("stats_retention_days", 120)),
    )
    config = new_cfg
    log.info(f"config reloaded from {CONFIG_PATH} (force_tier={forced})")


async def config_watch_loop() -> None:
    global config_mtime
    while True:
        try:
            if CONFIG_PATH.exists():
                mt = CONFIG_PATH.stat().st_mtime
                if mt != config_mtime:
                    new_cfg = load_config_file()
                    await apply_config_change(new_cfg)
                    config_mtime = mt
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"config reload failed: {e!r}")
        await asyncio.sleep(float(config.get("config_poll_seconds", 2.0)))


async def persist_loop() -> None:
    """Flush aggregated stats to disk on the configured interval (when dirty)."""
    while True:
        try:
            await pstats.maybe_flush()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"stats: periodic flush error: {e!r}")
        await asyncio.sleep(5.0)


# Bootstrap
config = load_config_file()
try:
    config_mtime = CONFIG_PATH.stat().st_mtime
except FileNotFoundError:
    config_mtime = 0.0
logging.getLogger().setLevel(str(config.get("log_level", "INFO")).upper())
limiter, metrics, pstats = init_from_config(config)


# ---------- HTTP app ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = httpx.AsyncClient(
        base_url=str(config["upstream_base_url"]).rstrip("/"),
        timeout=httpx.Timeout(float(config["upstream_timeout"]), connect=15.0),
        limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
    )
    watcher = asyncio.create_task(config_watch_loop())
    persister = asyncio.create_task(persist_loop())
    log.info(
        f"anthropic_proxy on http://{config['listen_host']}:{config['listen_port']} "
        f"-> {config['upstream_base_url']} | tier={limiter._active.name} "
        f"forced={limiter._forced} | dashboard: /_proxy/"
    )
    try:
        yield
    finally:
        watcher.cancel()
        persister.cancel()
        for task in (watcher, persister):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await pstats.maybe_flush(force=True)
        await client.aclose()


app = FastAPI(lifespan=lifespan)


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def compute_backoff(attempt: int, retry_after: float | None,
                    retry_after_cap: float | None = None) -> float:
    """Backoff before the next retry.

    A server-provided Retry-After is honored up to `retry_after_cap` (the
    remaining retry budget) so we can sleep out a long quota reset in one wait
    instead of hammering upstream every `retry_max_delay`. Without Retry-After
    we fall back to capped exponential backoff.
    """
    if retry_after is not None and retry_after > 0:
        cap = retry_after_cap if retry_after_cap is not None else float(config["retry_max_delay"])
        return min(retry_after, cap)
    delay = float(config["retry_base_delay"]) * (2 ** (attempt - 1))
    return min(delay, float(config["retry_max_delay"]))


# ---------- Dashboard ----------

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>anthropic_proxy</title>
<style>
  :root {
    --bg: #0d1117; --panel: #161b22; --border: #30363d;
    --text: #c9d1d9; --muted: #8b949e; --accent: #58a6ff;
    --green: #3fb950; --yellow: #d29922; --red: #f85149;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    margin: 0; padding: 20px; font-size: 14px;
  }
  .container { max-width: 1100px; margin: 0 auto; }
  h1 { margin: 0 0 4px 0; font-size: 20px; font-weight: 600; }
  .header {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 20px;
  }
  .header .sub { color: var(--muted); font-size: 12px; }
  #live { font-size: 12px; color: var(--green); font-variant-numeric: tabular-nums; }
  #live.error { color: var(--red); }
  .panel {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px; margin-bottom: 16px;
  }
  .panel h2 {
    margin: 0 0 12px 0; font-size: 11px; font-weight: 600;
    color: var(--muted); text-transform: uppercase; letter-spacing: 0.8px;
  }
  .grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 10px;
  }
  .stat {
    background: rgba(0,0,0,0.25); padding: 12px; border-radius: 6px;
    border: 1px solid rgba(255,255,255,0.03);
  }
  .stat .label {
    color: var(--muted); font-size: 10px; text-transform: uppercase;
    letter-spacing: 0.7px; margin-bottom: 6px;
  }
  .stat .value {
    font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums;
  }
  .stat .value.tier-low { color: var(--yellow); }
  .stat .value.tier-high { color: var(--green); }
  .stat .value.warn { color: var(--yellow); }
  .stat .value.crit { color: var(--red); }
  .stat .sub { color: var(--muted); font-size: 11px; margin-top: 4px; }
  table {
    width: 100%; border-collapse: collapse; font-size: 13px;
    font-variant-numeric: tabular-nums;
  }
  th, td { text-align: right; padding: 6px 10px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  th:first-child, td:first-child { text-align: left; }
  th {
    color: var(--muted); font-weight: 600; text-transform: uppercase;
    font-size: 10px; letter-spacing: 0.7px;
  }
  .model-card {
    margin-bottom: 12px; padding: 12px;
    background: rgba(0,0,0,0.25); border-radius: 6px;
    border: 1px solid rgba(255,255,255,0.03);
  }
  .model-card:last-child { margin-bottom: 0; }
  .model-head {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 8px;
  }
  .model-name {
    font-weight: 600; color: var(--accent);
    font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 13px;
  }
  .model-active { color: var(--muted); font-size: 11px; }
  .model-active strong { color: var(--text); }
  .footer { color: var(--muted); font-size: 11px; margin-top: 20px; text-align: center; }
  .footer a { color: var(--muted); }
  .empty { color: var(--muted); font-style: italic; text-align: center; padding: 12px; }
  .err { color: var(--red); }
  .ok { color: var(--green); }
  #boost-btn {
    background: var(--panel); color: var(--text);
    border: 1px solid var(--border); border-radius: 6px;
    padding: 6px 12px; font-size: 12px; font-weight: 600; cursor: pointer;
  }
  #boost-btn:hover:not(:disabled) { border-color: var(--green); color: var(--green); }
  #boost-btn:disabled { opacity: 0.5; cursor: default; }
  .panel-head {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 12px;
  }
  .panel-head h2 {
    font-size: 11px; font-weight: 600; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.8px;
  }
  .seg { display: inline-flex; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  .seg button {
    background: var(--panel); color: var(--muted); border: none;
    padding: 4px 10px; font-size: 11px; cursor: pointer; font-weight: 600;
    border-left: 1px solid var(--border);
  }
  .seg button:first-child { border-left: none; }
  .seg button:hover { color: var(--text); }
  .seg button.active { background: rgba(88,166,255,0.15); color: var(--accent); }
  .chart-block { margin-bottom: 14px; }
  .chart-block:last-child { margin-bottom: 0; }
  .chart-title {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.7px; margin-bottom: 6px; display: flex;
    align-items: center; gap: 12px;
  }
  .legend { display: inline-flex; gap: 10px; text-transform: none; letter-spacing: 0; }
  .legend span { display: inline-flex; align-items: center; gap: 4px; color: var(--muted); }
  .legend i { width: 9px; height: 9px; border-radius: 2px; display: inline-block; }
  svg.chart {
    width: 100%; height: 150px; display: block;
    background: rgba(0,0,0,0.2); border-radius: 6px;
  }
  svg.chart text { fill: var(--muted); font-size: 9px; font-variant-numeric: tabular-nums; }
  .bar {
    height: 6px; border-radius: 3px; background: rgba(255,255,255,0.08);
    overflow: hidden; margin-top: 8px;
  }
  .bar > i { display: block; height: 100%; background: var(--accent); border-radius: 3px; }
  .bar > i.warn { background: var(--yellow); }
  .bar > i.crit { background: var(--red); }
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <div>
      <h1>anthropic_proxy</h1>
      <div class="sub" id="upstream"></div>
    </div>
    <div style="display:flex; align-items:baseline; gap:12px;">
      <button id="boost-btn" title="Temporarily switch to HIGH; auto-demotes to LOW on the first rate-limit">⚡ Boost HIGH</button>
      <div id="live">connecting…</div>
    </div>
  </div>

  <div class="panel">
    <h2>Current State</h2>
    <div id="state-grid" class="grid"></div>
  </div>

  <div class="panel">
    <h2>Throughput · requests processed</h2>
    <div id="throughput-grid" class="grid"></div>
  </div>

  <div class="panel">
    <h2>Totals · persisted (weekly / monthly / lifetime)</h2>
    <div id="totals-grid" class="grid"></div>
  </div>

  <div class="panel">
    <div class="panel-head">
      <h2 style="margin:0">Graphs</h2>
      <div id="series-controls" class="seg">
        <button data-w="24h" class="active">24h</button>
        <button data-w="7d">7d</button>
        <button data-w="30d">30d</button>
        <button data-w="lifetime">lifetime</button>
      </div>
    </div>
    <div class="chart-block">
      <div class="chart-title">Requests <span id="chart-requests-legend" class="legend"></span></div>
      <svg id="chart-requests" class="chart" preserveAspectRatio="none"></svg>
    </div>
    <div class="chart-block">
      <div class="chart-title">Tokens <span id="chart-tokens-legend" class="legend"></span></div>
      <svg id="chart-tokens" class="chart" preserveAspectRatio="none"></svg>
    </div>
  </div>

  <div class="panel">
    <h2>Overall Latency</h2>
    <table id="overall-table"></table>
  </div>

  <div class="panel">
    <h2>Overall Tokens &amp; Cost</h2>
    <table id="overall-tokens-table"></table>
  </div>

  <div class="panel">
    <h2>Per Model</h2>
    <div id="per-model"></div>
  </div>

  <div class="footer">
    metrics refresh every 2s · graphs every 15s ·
    <a href="/_proxy/metrics">metrics json</a> ·
    <a href="/_proxy/series?window=7d">series json</a> ·
    <a href="/_proxy/config">config json</a> ·
    <a href="/_proxy/status">limiter status</a>
  </div>
</div>
<script>
const WINDOWS = ["1m", "10m", "1h", "5h", "24h"];
function fmtDur(s) {
  if (s === null || s === undefined) return "—";
  if (s < 1)   return (s * 1000).toFixed(0) + "ms";
  if (s < 60)  return s.toFixed(2) + "s";
  if (s < 3600) return (s / 60).toFixed(1) + "m";
  return (s / 3600).toFixed(2) + "h";
}
function fmtSpan(s) {
  // Coarse "2h14m" / "47m" / "12s" for window timers.
  if (s === null || s === undefined) return "—";
  s = Math.max(0, Math.round(s));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  if (h > 0) return h + "h" + String(m).padStart(2, "0") + "m";
  if (m > 0) return m + "m";
  return s + "s";
}
function fmtNum(n) {
  if (n === null || n === undefined) return "—";
  if (n === 0) return "0";
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (abs >= 1e3) return (n / 1e3).toFixed(1) + "k";
  return n.toString();
}
function fmtCost(c) {
  if (c === null || c === undefined) return "—";
  if (c === 0) return "$0.00";
  if (c < 0.01) return "<$0.01";
  if (c < 1) return "$" + c.toFixed(3);
  if (c < 100) return "$" + c.toFixed(2);
  return "$" + c.toFixed(0);
}
const COLORS = {
  ok: "#3fb950", err: "#f85149",
  input: "#58a6ff", output: "#a371f7",
  cache: "#d29922",
};
function fmtTime(t, step) {
  const d = new Date(t * 1000);
  const p = (n) => String(n).padStart(2, "0");
  if (step < 86400) return p(d.getHours()) + ":" + p(d.getMinutes());
  return (d.getMonth() + 1) + "/" + d.getDate();
}
function setLegend(id, items) {
  document.getElementById(id).innerHTML = items
    .map((it) => `<span><i style="background:${it.c}"></i>${it.l}</span>`).join("");
}
// Stacked-bar chart drawn as raw SVG. `series` is a list of
// {key|fn, color}; each point's segments stack bottom-up.
function drawChart(svgId, data, series, fmtVal) {
  const svg = document.getElementById(svgId);
  const pts = data.points || [];
  const W = svg.clientWidth || svg.parentElement.clientWidth || 800;
  const H = 150, padL = 46, padR = 8, padT = 8, padB = 16;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const val = (p, s) => s.fn ? s.fn(p) : (p[s.key] || 0);
  let max = 0;
  for (const p of pts) {
    let tot = 0;
    for (const s of series) tot += val(p, s);
    if (tot > max) max = tot;
  }
  max = max || 1;
  const n = pts.length || 1;
  const bw = plotW / n;
  const x = (i) => padL + i * bw;
  const y = (v) => padT + plotH - (v / max) * plotH;
  let svgParts = [];
  // gridlines + y labels (0, max/2, max)
  for (const frac of [0, 0.5, 1]) {
    const yy = padT + plotH - frac * plotH;
    svgParts.push(`<line x1="${padL}" y1="${yy}" x2="${W - padR}" y2="${yy}" stroke="#30363d" stroke-width="1"/>`);
    svgParts.push(`<text x="${padL - 4}" y="${yy + 3}" text-anchor="end">${fmtVal(max * frac)}</text>`);
  }
  const gap = bw > 4 ? Math.min(2, bw * 0.2) : 0;
  for (let i = 0; i < pts.length; i++) {
    const p = pts[i];
    let base = 0;
    for (const s of series) {
      const v = val(p, s);
      if (v <= 0) continue;
      const yTop = y(base + v), yBot = y(base);
      svgParts.push(
        `<rect x="${(x(i) + gap / 2).toFixed(1)}" y="${yTop.toFixed(1)}" ` +
        `width="${Math.max(0.5, bw - gap).toFixed(1)}" height="${Math.max(0, yBot - yTop).toFixed(1)}" ` +
        `fill="${s.color}"><title>${new Date(p.t * 1000).toLocaleString()}: ${fmtVal(v)}</title></rect>`
      );
      base += v;
    }
  }
  // x labels: first, middle, last
  if (pts.length) {
    const idxs = [0, Math.floor(pts.length / 2), pts.length - 1];
    const anchors = ["start", "middle", "end"];
    idxs.forEach((i, k) => {
      const xx = padL + (i + 0.5) * bw;
      svgParts.push(`<text x="${xx.toFixed(1)}" y="${H - 4}" text-anchor="${anchors[k]}">${fmtTime(pts[i].t, data.step)}</text>`);
    });
  }
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.innerHTML = svgParts.join("");
}
function row(stats) {
  const errClass = stats.errors > 0 ? "err" : "";
  return `
    <td>${stats.count}</td>
    <td class="ok">${stats.success ?? 0}</td>
    <td class="${errClass}">${stats.errors ?? 0}</td>
    <td>${fmtDur(stats.avg_seconds)}</td>
    <td>${fmtDur(stats.p50_seconds)}</td>
    <td>${fmtDur(stats.p95_seconds)}</td>
  `;
}
function tokenRow(stats) {
  return `
    <td>${fmtNum(stats.input_tokens)}</td>
    <td>${fmtNum(stats.output_tokens)}</td>
    <td>${fmtNum(stats.cache_creation_input_tokens)}</td>
    <td>${fmtNum(stats.cache_read_input_tokens)}</td>
    <td>${fmtCost(stats.cost)}</td>
  `;
}
function renderWindowTable(data) {
  let html = "<tr><th>Window</th><th>Count</th><th>OK</th><th>Err</th><th>Avg</th><th>p50</th><th>p95</th></tr>";
  for (const w of WINDOWS) {
    const s = data[w] || {count:0, success:0, errors:0, avg_seconds:null, p50_seconds:null, p95_seconds:null};
    html += `<tr><td>${w}</td>${row(s)}</tr>`;
  }
  return html;
}
function renderTokenTable(data) {
  let html = "<tr><th>Window</th><th>Input</th><th>Output</th><th>Cache Write</th><th>Cache Read</th><th>Cost</th></tr>";
  for (const w of WINDOWS) {
    const s = data[w] || {input_tokens:0, output_tokens:0, cache_creation_input_tokens:0, cache_read_input_tokens:0, cost:null};
    html += `<tr><td>${w}</td>${tokenRow(s)}</tr>`;
  }
  return html;
}
function pct(used, max) {
  if (!max) return 0;
  return Math.min(100, (used / max) * 100);
}
async function tick() {
  try {
    const r = await fetch("/_proxy/metrics");
    if (!r.ok) throw new Error("HTTP " + r.status);
    const m = await r.json();
    const liveEl = document.getElementById("live");
    liveEl.textContent = "● live · " + new Date().toLocaleTimeString();
    liveEl.classList.remove("error");

    document.getElementById("upstream").textContent = "→ " + (m.upstream || "");

    const L = m.limiter;
    const boostBtn = document.getElementById("boost-btn");
    if (L.forced_tier) {
      boostBtn.disabled = true;
      boostBtn.textContent = "forced: " + L.forced_tier;
    } else if (L.active_tier === "high") {
      boostBtn.disabled = true;
      boostBtn.textContent = "⚡ on HIGH";
    } else {
      boostBtn.disabled = false;
      boostBtn.textContent = "⚡ Boost HIGH";
    }
    const tierClass = L.active_tier === "high" ? "tier-high" : "tier-low";
    const concPct = pct(L.in_flight, L.max_concurrent);
    const concClass = concPct >= 95 ? "crit" : concPct >= 75 ? "warn" : "";

    const W = L.window || {active:false};
    const winHrs = (W.window_seconds || 18000) / 3600;
    const winLabel = (Number.isInteger(winHrs) ? winHrs : winHrs.toFixed(1)) + "h Window";
    let winCard;
    if (!W.active) {
      winCard = `
        <div class="stat">
          <div class="label">${winLabel}</div>
          <div class="value">idle</div>
          <div class="sub">starts on next request · ${W.limit} max</div>
          <div class="bar"><i style="width:0%"></i></div>
        </div>`;
    } else {
      const timePct = pct(W.elapsed_seconds, W.window_seconds);
      const usePct = pct(W.count, W.limit);
      const useClass = usePct >= 95 ? "crit" : usePct >= 80 ? "warn" : "";
      winCard = `
        <div class="stat">
          <div class="label">${winLabel}</div>
          <div class="value ${useClass}">${W.count} / ${W.limit}</div>
          <div class="sub">${fmtSpan(W.elapsed_seconds)} in · ${fmtSpan(W.remaining_seconds)} left</div>
          <div class="bar"><i class="${useClass}" style="width:${timePct.toFixed(1)}%"></i></div>
        </div>`;
    }

    document.getElementById("state-grid").innerHTML = `
      <div class="stat">
        <div class="label">Active Tier</div>
        <div class="value ${tierClass}">${L.active_tier.toUpperCase()}</div>
        <div class="sub">${L.forced_tier ? "forced" : "auto"}${L.probe_in_flight ? " · probing" : ""}</div>
      </div>
      <div class="stat">
        <div class="label">In Flight</div>
        <div class="value ${concClass}">${L.in_flight} / ${L.max_concurrent}</div>
        <div class="sub">${concPct.toFixed(0)}%</div>
      </div>
      <div class="stat">
        <div class="label">Queued</div>
        <div class="value">${L.queued}</div>
        <div class="sub">${L.queued > 0 ? "waiting for slot" : "idle"}</div>
      </div>
      ${winCard}
      <div class="stat">
        <div class="label">Lifetime</div>
        <div class="value">${L.totals.requests}</div>
        <div class="sub">
          ${L.totals.rate_limited} 429 · ${L.totals.promotions}↑ ${L.totals.demotions}↓ · ${L.totals.probes_sent} probes
        </div>
      </div>
    `;

    let tpHtml = "";
    for (const w of WINDOWS) {
      const s = m.overall[w] || {count:0, errors:0, avg_seconds:null, cost:null, input_tokens:0, output_tokens:0};
      const errBadge = s.errors > 0 ? ` <span class="err">(${s.errors} err)</span>` : "";
      const costBadge = s.cost !== null && s.cost !== undefined ? ` · ${fmtCost(s.cost)}` : "";
      const tokSub = `${fmtNum(s.input_tokens + (s.cache_creation_input_tokens||0) + (s.cache_read_input_tokens||0))} in / ${fmtNum(s.output_tokens)} out`;
      tpHtml += `
        <div class="stat">
          <div class="label">Last ${w}</div>
          <div class="value">${s.count}</div>
          <div class="sub">${fmtDur(s.avg_seconds)} avg${errBadge}${costBadge}</div>
          <div class="sub">${tokSub}</div>
        </div>
      `;
    }
    document.getElementById("throughput-grid").innerHTML = tpHtml;

    if (m.persistent) {
      const P = m.persistent;
      const totalCards = [
        ["24h", "24h"], ["Weekly", "7d"], ["Monthly", "30d"], ["Lifetime", "lifetime"],
      ];
      let totHtml = "";
      for (const [label, key] of totalCards) {
        const o = (P[key] && P[key].overall) || {count:0, errors:0, input_tokens:0, output_tokens:0, cache_creation_input_tokens:0, cache_read_input_tokens:0, cost:null};
        const inTot = o.input_tokens + (o.cache_creation_input_tokens||0) + (o.cache_read_input_tokens||0);
        const errBadge = o.errors > 0 ? ` <span class="err">(${o.errors} err)</span>` : "";
        const costBadge = o.cost !== null && o.cost !== undefined ? ` · ${fmtCost(o.cost)}` : "";
        totHtml += `
          <div class="stat">
            <div class="label">${label}</div>
            <div class="value">${fmtNum(o.count)}</div>
            <div class="sub">${fmtNum(inTot)} in / ${fmtNum(o.output_tokens)} out</div>
            <div class="sub">${fmtDur(o.avg_seconds)} avg${errBadge}${costBadge}</div>
          </div>
        `;
      }
      document.getElementById("totals-grid").innerHTML = totHtml;
    }

    document.getElementById("overall-table").innerHTML = renderWindowTable(m.overall);
    document.getElementById("overall-tokens-table").innerHTML = renderTokenTable(m.overall);

    const models = Object.keys(m.per_model).sort();
    if (models.length === 0) {
      document.getElementById("per-model").innerHTML = `<div class="empty">No model traffic yet.</div>`;
    } else {
      let html = "";
      for (const model of models) {
        const d = m.per_model[model];
        const active = d.active || 0;
        const priced = d.has_pricing ? "" : ' <span class="model-active">(no pricing)</span>';
        html += `
          <div class="model-card">
            <div class="model-head">
              <div class="model-name">${model}${priced}</div>
              <div class="model-active">active: <strong>${active}</strong></div>
            </div>
            <table>${renderWindowTable(d)}</table>
            <div style="height:8px"></div>
            <table>${renderTokenTable(d)}</table>
          </div>
        `;
      }
      document.getElementById("per-model").innerHTML = html;
    }
  } catch (e) {
    const el = document.getElementById("live");
    el.textContent = "✗ disconnected: " + e.message;
    el.classList.add("error");
  }
}
let seriesWindow = "24h";
async function drawSeries() {
  try {
    const r = await fetch("/_proxy/series?window=" + seriesWindow);
    if (!r.ok) throw new Error("HTTP " + r.status);
    const data = await r.json();
    drawChart("chart-requests", data, [
      { fn: (p) => Math.max(0, p.requests - p.errors), color: COLORS.ok },
      { key: "errors", color: COLORS.err },
    ], fmtNum);
    setLegend("chart-requests-legend", [
      { c: COLORS.ok, l: "ok" }, { c: COLORS.err, l: "errors" },
    ]);
    drawChart("chart-tokens", data, [
      { key: "input_tokens", color: COLORS.input },
      { fn: (p) => (p.cache_creation_input_tokens || 0) + (p.cache_read_input_tokens || 0), color: COLORS.cache },
      { key: "output_tokens", color: COLORS.output },
    ], fmtNum);
    setLegend("chart-tokens-legend", [
      { c: COLORS.input, l: "input" }, { c: COLORS.cache, l: "cache" }, { c: COLORS.output, l: "output" },
    ]);
  } catch (e) { /* leave previous chart in place */ }
}
document.querySelectorAll("#series-controls button").forEach((b) => {
  b.addEventListener("click", () => {
    seriesWindow = b.dataset.w;
    document.querySelectorAll("#series-controls button").forEach((x) => x.classList.remove("active"));
    b.classList.add("active");
    drawSeries();
  });
});
window.addEventListener("resize", drawSeries);
document.getElementById("boost-btn").addEventListener("click", async () => {
  const btn = document.getElementById("boost-btn");
  btn.disabled = true;
  try {
    const r = await fetch("/_proxy/boost", { method: "POST" });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      alert(e.error || ("boost failed: HTTP " + r.status));
    }
  } catch (e) {
    alert("boost failed: " + e.message);
  }
  tick();
});
tick();
setInterval(tick, 2000);
drawSeries();
setInterval(drawSeries, 15000);
</script>
</body>
</html>
"""


@app.get("/_proxy/", response_class=HTMLResponse)
@app.get("/_proxy", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/_proxy/metrics")
async def metrics_endpoint():
    return {
        "upstream": config["upstream_base_url"],
        "limiter": limiter.snapshot(),
        **metrics.summary(),
        "persistent": pstats.summary(),
    }


@app.get("/_proxy/series")
async def series_endpoint(req: Request):
    """Bucketed time series for graphs. ?window=24h|7d|30d|lifetime."""
    window = req.query_params.get("window", "24h")
    if window not in ("24h", "7d", "30d", "lifetime"):
        window = "24h"
    return pstats.series(window)


@app.get("/_proxy/status")
async def status_endpoint():
    return limiter.snapshot()


def _fmt_dur_short(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


@app.get("/_proxy/statusline", response_class=PlainTextResponse)
async def statusline_endpoint(req: Request):
    """Compact one-line status for tmux / Claude Code status bars.

    Query params:
      fmt=plain | tmux | ansi   (default: plain)
      window=1m | 10m | 1h | 5h | 24h   (which throughput window to show; default 1m)
    """
    fmt = req.query_params.get("fmt", "plain")
    window = req.query_params.get("window", "1m")
    if window not in {w for w, _ in METRIC_WINDOWS}:
        window = "1m"

    snap = limiter.snapshot()
    summ = metrics.summary()
    tier = snap["active_tier"].upper()
    w = summ["overall"].get(window, {"count": 0, "errors": 0, "avg_seconds": None})

    def color(s: str, name: str) -> str:
        if fmt == "tmux":
            mapping = {
                "green":  "#[fg=green]",
                "yellow": "#[fg=yellow]",
                "red":    "#[fg=red]",
                "cyan":   "#[fg=cyan]",
                "reset":  "#[default]",
            }
        elif fmt == "ansi":
            mapping = {
                "green":  "\x1b[32m",
                "yellow": "\x1b[33m",
                "red":    "\x1b[31m",
                "cyan":   "\x1b[36m",
                "reset":  "\x1b[0m",
            }
        else:
            return s
        return f"{mapping[name]}{s}{mapping['reset']}"

    tier_color = "green" if tier == "HIGH" else "yellow"
    queued_color = "red" if snap["queued"] > 0 else "reset"
    err_color = "red" if w["errors"] > 0 else "reset"

    parts = [
        color(tier, tier_color),
        f"{snap['in_flight']}/{snap['max_concurrent']}",
        color(f"q{snap['queued']}", queued_color) if snap['queued'] > 0 else f"q{snap['queued']}",
        f"{window}:{w['count']}",
        _fmt_dur_short(w["avg_seconds"]),
    ]
    if req.query_params.get("cost") in ("1", "true", "yes") and w.get("cost") is not None:
        c = w["cost"]
        if c < 0.01:
            cs = "<$0.01"
        elif c < 1:
            cs = f"${c:.3f}"
        elif c < 100:
            cs = f"${c:.2f}"
        else:
            cs = f"${c:.0f}"
        parts.append(cs)
    if w["errors"] > 0:
        parts.append(color(f"!{w['errors']}err", err_color))
    if snap.get("probe_in_flight"):
        parts.append(color("probe", "cyan"))

    return " ".join(parts)


@app.get("/_proxy/config")
async def config_endpoint():
    return {"path": str(CONFIG_PATH), "loaded_mtime": config_mtime, "values": config}


@app.post("/_proxy/force_tier")
async def force_tier_endpoint(req: Request):
    body = await req.json()
    tier = body.get("tier")
    if tier not in (None, "low", "high"):
        return JSONResponse({"error": "tier must be 'low', 'high', or null"}, status_code=400)
    cfg = {**config, "force_tier": tier}
    await apply_config_change(cfg)
    return limiter.snapshot()


@app.post("/_proxy/boost")
async def boost_endpoint():
    """Temporarily switch to HIGH, keeping auto-demotion enabled.

    The first rate-limited response (429/503/529) drops back to LOW on its own.
    Use force_tier="high" instead if you want HIGH pinned permanently.
    """
    ok = await limiter.boost_high()
    if not ok:
        return JSONResponse(
            {"error": "cannot boost while force_tier is set; clear force_tier first"},
            status_code=409,
        )
    return limiter.snapshot()


# ---------- Proxy handler ----------

@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def proxy(full_path: str, request: Request):
    body = await request.body()
    target = "/" + full_path
    if request.url.query:
        target = f"{target}?{request.url.query}"
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
    }
    model = extract_model(request.method, body)
    started_at = metrics.request_started(model)
    limiter.note_request()

    finished = False
    handed_off = False

    def finalize(status: int, usage: dict | None = None) -> None:
        nonlocal finished
        if not finished:
            finished = True
            metrics.request_finished(model, started_at, status, usage)

    try:
        # Connection errors give up after retry_max_attempts (a down upstream
        # won't be fixed by waiting). Rate-limit (429/503/529) retries instead
        # run against a wall-clock budget (retry_max_elapsed_seconds, > the 5h
        # quota window) so a queued request can outlast a full window and run
        # once the quota resets, rather than being purged after a few minutes.
        max_conn_attempts = int(config["retry_max_attempts"])
        deadline = time.monotonic() + float(config.get("retry_max_elapsed_seconds", 18900))
        attempt = 0
        conn_errors = 0
        while True:
            attempt += 1
            was_probe = await limiter.acquire()
            try:
                outbound = client.build_request(
                    method=request.method, url=target,
                    content=body, headers=headers,
                )
                response = await client.send(outbound, stream=True)
            except httpx.HTTPError as e:
                await limiter.release_other_error(was_probe)
                conn_errors += 1
                log.warning(
                    f"upstream error attempt={attempt} "
                    f"conn_errors={conn_errors}/{max_conn_attempts}: {e!r}"
                )
                if conn_errors >= max_conn_attempts:
                    finalize(502)
                    return JSONResponse(
                        {"error": "upstream_unreachable", "detail": str(e)},
                        status_code=502,
                    )
                await asyncio.sleep(compute_backoff(conn_errors, None))
                continue

            if response.status_code in RATE_LIMIT_STATUSES:
                conn_errors = 0
                retry_after = parse_retry_after(response.headers.get("retry-after"))
                try:
                    rl_body = await response.aread()
                finally:
                    await response.aclose()
                await limiter.release_rate_limited(was_probe)
                remaining = deadline - time.monotonic()
                backoff = compute_backoff(
                    attempt, retry_after, retry_after_cap=max(0.0, remaining)
                )
                log.info(
                    f"upstream {response.status_code} attempt={attempt} "
                    f"retry_after={retry_after} backoff={backoff:.1f}s "
                    f"budget_left={remaining:.0f}s probe={was_probe}"
                )
                if remaining - backoff <= 0:
                    finalize(response.status_code)
                    return Response(
                        content=rl_body,
                        status_code=response.status_code,
                        headers={
                            k: v for k, v in response.headers.items()
                            if k.lower() not in HOP_BY_HOP
                        },
                    )
                await asyncio.sleep(backoff)
                continue

            is_success = 200 <= response.status_code < 400
            status_code = response.status_code
            out_headers = {
                k: v for k, v in response.headers.items()
                if k.lower() not in HOP_BY_HOP
            }
            extractor = make_extractor(response.headers.get("content-type", ""))

            async def body_stream():
                try:
                    async for chunk in response.aiter_bytes():
                        extractor.feed(chunk)
                        yield chunk
                finally:
                    try:
                        await response.aclose()
                    except Exception:
                        pass
                    if is_success:
                        await limiter.release_success(was_probe)
                    else:
                        await limiter.release_other_error(was_probe)
                    usage = extractor.final_usage() if is_success else None
                    finalize(status_code, usage)

            handed_off = True
            return StreamingResponse(
                body_stream(),
                status_code=status_code,
                headers=out_headers,
            )
    finally:
        if not handed_off and not finished:
            metrics.request_finished(model, started_at, 0)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=config["listen_host"],
        port=int(config["listen_port"]),
        log_level=str(config.get("log_level", "info")).lower(),
        access_log=False,
    )
