"""Disk persistence: long-horizon stats (stats.json) + window state (window.json).

Pure module: no FastAPI. `PersistentStats` folds completions into hourly
per-model buckets and lifetime totals; the window helpers save/restore the
limiter's current rolling-window state across restarts.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from .limiter import Limiter
from .metrics import compute_cost

log = logging.getLogger("proxy.persistence")

PERSIST_VERSION = 1
HOUR_SECONDS = 3600
WEEK_SECONDS = 7 * 86400
MONTH_SECONDS = 30 * 86400

# Aggregated per-bucket counters. Percentiles can't be merged across buckets, so
# long-term we keep duration_sum (-> average) only; cost is derived at read time
# from current pricing rather than stored, so re-pricing applies retroactively.
# `upstream_sum` is the slot-hold half of duration_sum (see Metrics): buckets
# written before it existed simply have 0, which reads back as "not measured"
# rather than as a real zero.
_COUNTER_KEYS = (
    "count", "success", "errors",
    "input_tokens", "output_tokens",
    "cache_creation_input_tokens", "cache_read_input_tokens",
    "duration_sum", "upstream_sum",
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
               usage: dict | None, upstream: float | None = None) -> None:
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
            store["upstream_sum"] += duration if upstream is None else upstream
        self._dirty = True

    # -- read models --

    def lifetime_tokens(self) -> dict[str, dict[str, float]]:
        """Cumulative per-model token counters by class, for price calibration.

        Monotone across restarts (lifetime totals are never pruned), which is
        exactly what the calibrator's snapshot deltas need. Class names match
        the model_pricing schema, not the usage wire fields.
        """
        return {
            m: {
                "input": float(c["input_tokens"]),
                "cache_creation": float(c["cache_creation_input_tokens"]),
                "cache_read": float(c["cache_read_input_tokens"]),
                "output": float(c["output_tokens"]),
            }
            for m, c in self._lifetime.items()
        }

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

    @staticmethod
    def _avg_split(c: dict[str, float]) -> dict[str, float | None]:
        """Average upstream / wait seconds, or None when nothing measured them.

        Buckets written before `upstream_sum` existed have a 0 sum; reporting
        that as "0s upstream, all wait" would be a lie, so it comes back as
        unknown instead.
        """
        cnt = c["count"]
        ups = c.get("upstream_sum", 0) or 0
        if not cnt or ups <= 0:
            return {"avg_upstream_seconds": None, "avg_wait_seconds": None}
        return {
            "avg_upstream_seconds": ups / cnt,
            "avg_wait_seconds": max(0.0, c["duration_sum"] - ups) / cnt,
        }

    def _format_model(self, model: str, c: dict[str, float]) -> dict[str, Any]:
        cnt = c["count"]
        return {
            "count": int(cnt),
            "success": int(c["success"]),
            "errors": int(c["errors"]),
            "avg_seconds": (c["duration_sum"] / cnt) if cnt else None,
            **self._avg_split(c),
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
            **self._avg_split(total),
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

    def series(self, window: str, model_limit: int = 8) -> dict[str, Any]:
        """Bucketed time series for graphing requests + tokens + latency.

        24h / 7d use hourly buckets; 30d / lifetime roll up into daily buckets.
        Empty buckets are emitted so the x-axis is evenly spaced.

        `models` adds a per-model latency series over the same buckets: average
        total and average upstream seconds, `null` in buckets where that model
        ran nothing (a gap in the line, not a zero). Only the `model_limit`
        busiest models in the window are included, so a long tail of one-off
        models can't turn the chart into spaghetti.
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
        # Same bucketing, kept per model, for the latency-per-model chart.
        model_bins: dict[str, dict[int, dict[str, float]]] = {}
        earliest = now - span
        for hour, models in self._hours.items():
            if hour + HOUR_SECONDS <= earliest:
                continue
            bucket_t = (hour // step) * step
            b = bins.setdefault(bucket_t, _empty_counter())
            for model, c in models.items():
                _add_counter(b, c)
                _add_counter(model_bins.setdefault(model, {})
                             .setdefault(bucket_t, _empty_counter()), c)
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
        times = [p["t"] for p in points]
        busiest = sorted(
            model_bins,
            key=lambda m: sum(b["count"] for b in model_bins[m].values()),
            reverse=True,
        )[:max(0, int(model_limit))]
        models_out: dict[str, Any] = {}
        for model in busiest:
            buckets = model_bins[model]
            series_points = []
            for tt in times:
                c = buckets.get(tt)
                cnt = c["count"] if c else 0
                ups = (c.get("upstream_sum", 0) or 0) if c else 0
                series_points.append({
                    "t": tt,
                    "count": int(cnt),
                    # null (not 0) where the model didn't run: the chart draws a
                    # gap rather than a dive to zero.
                    "avg_seconds": (c["duration_sum"] / cnt) if cnt else None,
                    # Also null when the bucket predates upstream_sum, which is
                    # indistinguishable from a real zero.
                    "avg_upstream_seconds": (ups / cnt) if (cnt and ups > 0) else None,
                })
            models_out[model] = series_points
        return {"window": window, "step": step, "points": points,
                "models": models_out}


# ---------- window.json (current rolling-window state) ----------

def load_window_file(path: Path) -> dict[str, Any] | None:
    """Read the persisted rolling-window state (count + start), if any."""
    try:
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, ValueError) as e:
        log.warning(f"window: could not load {path}: {e!r}")
        return None


def save_window_file(limiter: Limiter, path: Path) -> None:
    """Persist the current rolling-window state for the next restart.

    Skips writing when no window is active (nothing worth restoring). Written
    atomically via a temp file + os.replace.
    """
    state = limiter.window_state()
    if state.get("started_at") is None:
        # No active window — clear any stale file so a restart starts fresh.
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(state, f, separators=(",", ":"))
        os.replace(tmp, path)
    except OSError as e:
        log.warning(f"window: save to {path} failed: {e!r}")
