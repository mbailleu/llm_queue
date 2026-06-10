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
    "listen_port": 8787,            # human lane (unthrottled)
    # Automation lane (paced). A second listening port for scripts / tight loops;
    # set to null to disable the second lane entirely. Both ports share the same
    # upstream, queue, tier auto-detection, and quota window — they differ only
    # in admission policy: the human port is never throttled and has concurrency
    # priority; the auto port is paced by AutoPacer to spend only the leftover
    # budget without starving the human (statistically).
    "throttle_listen_port": 8788,
    "auto_pacing_enabled": True,
    # Predicted human demand = human_demand_safety * observed_human_rate *
    # time_left. Higher safety leaves more headroom for humans (slower auto).
    "human_demand_safety": 1.5,
    # Trailing horizon (seconds) over which the human request rate is measured.
    "human_demand_horizon_seconds": 3600,
    # How far ahead (seconds) to project that measured human rate when reserving
    # quota: the reservation term is human_demand_safety * human_rate *
    # min(time_left, this). Capping the projection stops a long (e.g. 5h LOW)
    # window from reserving almost the entire quota off a small human rate.
    "human_demand_lookahead_seconds": 3600,
    # Optional hard floor of requests always kept free for humans (0 = purely
    # statistical, the default the user asked for).
    "human_quota_floor": 0,
    # Concurrency slots reserved for humans (auto in-flight is capped at
    # max_concurrent - this). 0 relies on human queue-priority alone.
    "auto_concurrency_reserve": 0,
    # Assumed request seconds before any latency has been measured (pacer uses
    # the live EWMA once traffic exists).
    "auto_assumed_request_seconds": 30.0,
    # How often a parked/over-pace auto request re-checks whether it may go.
    "auto_poll_seconds": 1.0,
    "initial_tier": "low",
    "force_tier": None,
    # Optional RECURRING DAILY automatic LOW->HIGH switch. Accepts unix epoch
    # seconds, "HH:MM"/"HH:MM:SS", or an ISO-8601 datetime — only the local
    # time-of-day is used, and the switch fires at that time every day. The tier
    # is promoted to HIGH (auto-demotes on the next rate-limit, like a boost — use
    # force_tier to pin instead). Before each fire, the pacer treats the current
    # LOW window as ending at the switch time, so background traffic drains the
    # leftover over the shorter horizon. null = no daily switch. Hot-reloadable.
    # A separate ONE-SHOT switch is available via
    # POST /_proxy/schedule_high {"at": ...}; the two are independent.
    "scheduled_high_at": None,
    # Optional RECURRING DAILY automatic HIGH->LOW switch — the mirror of
    # scheduled_high_at. Same value formats (unix seconds, "HH:MM[:SS]", ISO-8601;
    # only the local time-of-day is used). At that time the tier drops to LOW
    # every day. A separate ONE-SHOT switch is available via
    # POST /_proxy/schedule_low {"at": ...}. null = no daily switch. Hot-reloadable.
    "scheduled_low_at": None,
    # Each tier has a max concurrency cap and its own rolling request-quota
    # window (window_limit requests per window_seconds). The active tier's window
    # drives the dashboard indicator and restarts whenever the tier switches.
    # window_seconds/window_limit fall back to the top-level rate_window_* below
    # when a tier omits them.
    "tiers": {
        "low":  {"max_concurrent": 4,    "window_seconds": 18000, "window_limit": 600},
        "high": {"max_concurrent": 1000, "window_seconds": 3600,  "window_limit": 99999},
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
    # Fallback rolling request-quota window for the dashboard "X / N this window"
    # indicator, used only when a tier omits its own window_seconds/window_limit.
    # Tracked, not enforced.
    "rate_window_seconds": 18000,   # 5h
    "rate_window_limit": 600,
    # Per-model weighting for the window count: a request for a listed model
    # counts as `factor` requests toward the window (only the window — metrics
    # and per-model stats still count each request once). Models not listed use
    # `default_window_weight`. Keys match by exact model name first, then by
    # substring (so "opus" matches "claude-opus-4-20250514").
    "model_window_weights": {},
    "default_window_weight": 1,
    "upstream_timeout": 600,
    "log_level": "INFO",
    "config_poll_seconds": 2.0,
    "metrics_window_seconds": 86400,
    "model_pricing": {},
    # Long-horizon persisted stats (weekly/monthly/lifetime + graphs).
    "stats_persist_path": "stats.json",
    "stats_flush_seconds": 60.0,
    "stats_retention_days": 120,
    # Current rolling-window state (count + start) persisted across restarts.
    # Restored on boot unless it has already elapsed past its window.
    "window_persist_path": "window.json",
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
    __slots__ = ("name", "max_concurrent", "window_seconds", "window_limit")

    def __init__(self, name: str, max_concurrent: int,
                 window_seconds: float = 18000.0, window_limit: int = 600):
        self.name = name
        self.max_concurrent = max_concurrent
        # Each tier carries its own rolling request-quota window (the upstream
        # "N requests per W seconds" budget for that tier). LOW is typically the
        # small per-window quota; HIGH a large per-hour ceiling. The active
        # tier's window is what the dashboard "X / N this window" indicator uses,
        # and the counter restarts whenever the active tier changes.
        self.window_seconds = float(window_seconds)
        self.window_limit = int(window_limit)


class Limiter:
    """Concurrency cap (with a queue) plus auto-tier-detection.

    There is no preemptive per-window pacing: requests are admitted as fast as
    the concurrency cap allows. When the upstream rate limit is hit, the 429/
    503/529 retry+backoff in the proxy handler is what makes callers wait.
    """

    def __init__(self, low: Tier, high: Tier, initial_tier: str,
                 promotion_cooldown: float, forced: str | None,
                 window_weights: dict[str, float] | None = None,
                 default_window_weight: float = 1.0):
        self._cond = asyncio.Condition()
        self._low = low
        self._high = high
        self._active = high if initial_tier == "high" else low
        self._forced: str | None = forced if forced in ("low", "high") else None
        if self._forced:
            self._active = self._low if self._forced == "low" else self._high
        self._promotion_cooldown = promotion_cooldown
        self._last_demotion = 0.0
        # Optional scheduled automatic tier switches. Each direction (LOW->HIGH
        # and HIGH->LOW) has two independent slots:
        #  - a recurring DAILY switch from config (`scheduled_high_at` /
        #    `scheduled_low_at`): fires at a local time-of-day every day, re-arming
        #    itself after each fire.
        #  - a ONE-SHOT switch from the API (`POST /_proxy/schedule_high` /
        #    `POST /_proxy/schedule_low`): fires once at an absolute time, then
        #    clears.
        # For each direction the effective next switch is whichever comes first.
        # Before a LOW->HIGH switch fires the pacer treats the current LOW window
        # as ending then, so auto drains the leftover over the shorter horizon.
        # None/None = no scheduled switch.
        self._switch_daily_tod: float | None = None    # seconds since local midnight
        self._switch_daily_next: float | None = None   # absolute unix of next daily fire
        self._switch_once_at: float | None = None       # absolute unix of one-shot fire
        # HIGH->LOW counterparts.
        self._switch_low_daily_tod: float | None = None
        self._switch_low_daily_next: float | None = None
        self._switch_low_once_at: float | None = None
        self._in_flight = 0
        self._waiters = 0
        self._probe_in_flight = False
        # Lane accounting. The proxy fronts two ingress ports: a "human" lane
        # (unthrottled, concurrency-priority) and an "auto" lane (paced by
        # AutoPacer). The limiter tracks per-lane in-flight + waiters so human
        # callers are admitted ahead of automation, and so automation can be
        # capped below the concurrency limit when a reserve is configured.
        self._human_in_flight = 0
        self._auto_in_flight = 0
        self._human_waiters = 0
        self._auto_waiters = 0
        self._auto_concurrency_reserve = 0
        # Requests currently parked in retry backoff after upstream pushback
        # (429/503/529 — e.g. the backend's own quota limit, which the proxy does
        # not enforce but does wait out). These hold no concurrency slot and are
        # not `waiters`, so without this gauge they'd be invisible on the
        # dashboard while the client is, in fact, waiting. Per-lane split + the
        # unix time of the most recent rate-limit, for an "upstream limiting us"
        # indicator.
        self._rl_waiting = 0
        self._rl_waiting_human = 0
        self._rl_waiting_auto = 0
        self._last_rate_limited_at: float | None = None
        # Recent human-request arrival times (monotonic), used to estimate
        # future human demand for the pacer. Trimmed to _human_horizon.
        self._human_times: deque[float] = deque()
        self._human_horizon = 3600.0
        self._started_at = time.monotonic()
        # Rolling request-quota window. The limit/duration come from the ACTIVE
        # tier (LOW and HIGH each have their own), so they switch when the tier
        # does. Anchored at the first request after the previous window expired,
        # matching "the window starts when the first request is sent". Tracked
        # for display; not enforced (upstream 429s + retry do that). The counter
        # is restarted on every tier change (see _restart_window).
        self._window_start: float | None = None
        self._window_count = 0.0
        # The same window count split by ingress lane, so the dashboard can show
        # how much of the window human vs background (auto) traffic has spent.
        # Invariant: _window_human_count + _window_auto_count == _window_count
        # (both move together with note_request / discount_request / restart).
        self._window_human_count = 0.0
        self._window_auto_count = 0.0
        # Weight of requests that have been noted against the window but not yet
        # finalized ("in flight" for accounting). Used to re-seed the count when
        # the window restarts mid-flight (e.g. a probe-driven tier switch), so
        # requests already running aren't erased from the indicator and silently
        # uncounted for the rest of their life. note_request adds, note_done
        # subtracts; the lane split mirrors the window split. Not persisted (no
        # request survives a process restart).
        self._inflight_weight = 0.0
        self._inflight_weight_human = 0.0
        self._inflight_weight_auto = 0.0
        # Per-model window weighting. Each entry maps a model (matched exactly,
        # then by substring) to how many "requests" it costs the window count.
        self._window_weights: dict[str, float] = dict(window_weights or {})
        self._default_window_weight = float(default_window_weight)
        self._n_requests = 0
        self._n_rate_limited = 0
        self._n_other_errors = 0
        self._n_concurrency_waits = 0
        self._n_promotions = 0
        self._n_demotions = 0
        self._n_probes_sent = 0

    def _window_weight_for(self, model: str) -> float:
        """How many window-units a request for `model` costs.

        Exact match wins; otherwise the first configured key that is a substring
        of the model name (so "opus" matches "claude-opus-4-20250514"); else the
        default weight. Only affects the rolling window count, not statistics.
        """
        if model in self._window_weights:
            return float(self._window_weights[model])
        for key, w in self._window_weights.items():
            if key and key in model:
                return float(w)
        return self._default_window_weight

    def _restart_window(self) -> None:
        """Restart the rolling quota window for the (new) active tier.

        Called on every tier change (promotion, demotion, boost, or a forced
        switch) so the timer resets and the window re-anchors under the new
        tier's limit/duration. Requests still in flight are carried forward (they
        keep consuming quota under the new tier, so they must stay counted) — only
        their contribution survives; the window otherwise starts empty. Anchors a
        fresh start now when anything is in flight, else leaves the window dormant
        for the next request to anchor. Synchronous and await-free; callers
        already hold `self._cond`.
        """
        if self._inflight_weight > 0:
            self._window_start = time.time()
            self._window_count = self._inflight_weight
            self._window_human_count = self._inflight_weight_human
            self._window_auto_count = self._inflight_weight_auto
        else:
            self._window_start = None
            self._window_count = 0.0
            self._window_human_count = 0.0
            self._window_auto_count = 0.0

    def note_request(self, model: str = "", lane: str = "human") -> tuple[float, float]:
        """Count one client request against the rolling quota window.

        Called once per client request (not per retry). The request counts as
        `_window_weight_for(model)` units toward the window (a per-model factor);
        this affects only the window indicator, never the per-request metrics or
        per-model stats. The window's duration/limit come from the active tier,
        so they follow tier changes. Starts a fresh window when there is none
        active or the current one has elapsed. Synchronous and await-free, so
        it's atomic under the single-threaded event loop. Returns
        `(weight, window_token)`; pass both back to `discount_request` to undo
        the count if the request ultimately fails (the token identifies the
        window so a since-rolled window is never wrongly decremented).
        """
        now = time.time()
        window_seconds = float(self._active.window_seconds)
        weight = self._window_weight_for(model)
        if self._window_start is None or now - self._window_start >= window_seconds:
            # Fresh window: carry forward requests already in flight (they spill
            # into this window and keep consuming its quota). This request's own
            # weight is added below, after it joins the in-flight set.
            self._window_start = now
            self._window_count = self._inflight_weight
            self._window_human_count = self._inflight_weight_human
            self._window_auto_count = self._inflight_weight_auto
        self._window_count += weight
        self._inflight_weight += weight
        if lane == "auto":
            self._window_auto_count += weight
            self._inflight_weight_auto += weight
        else:
            self._window_human_count += weight
            self._inflight_weight_human += weight
        return weight, self._window_start

    def note_done(self, weight: float, lane: str = "human") -> None:
        """Drop a noted request from the in-flight set once it has finalized.

        Called exactly once per request in `note_request`'s wake (on any outcome),
        so the in-flight accumulator that `_restart_window` carries forward only
        reflects requests still running. Independent of `discount_request` (which
        reverses the *window* count for failed requests); this only touches the
        in-flight tally and never the window. Synchronous and await-free.
        """
        weight = max(0.0, float(weight))
        self._inflight_weight = max(0.0, self._inflight_weight - weight)
        if lane == "auto":
            self._inflight_weight_auto = max(0.0, self._inflight_weight_auto - weight)
        else:
            self._inflight_weight_human = max(0.0, self._inflight_weight_human - weight)

    def discount_request(self, weight: float, window_token: float | None,
                         lane: str = "human") -> None:
        """Reverse a previously-noted request that never consumed quota.

        Upstream rate-limit quota only counts requests that actually went
        through; a request that ultimately failed (rate-limited out, connection
        error, client abort) should not stay on the window count. No-op when the
        window has since rolled (token mismatch) so a fresh window is never
        wrongly reduced. Synchronous and await-free like `note_request`.
        """
        if window_token is None or self._window_start != window_token:
            return
        weight = max(0.0, float(weight))
        self._window_count = max(0.0, self._window_count - weight)
        if lane == "auto":
            self._window_auto_count = max(0.0, self._window_auto_count - weight)
        else:
            self._window_human_count = max(0.0, self._window_human_count - weight)

    def _window_snapshot(self) -> dict[str, Any]:
        now = time.time()
        window_seconds = float(self._active.window_seconds)
        window_limit = int(self._active.window_limit)
        ws = self._window_start
        active = ws is not None and now - ws < window_seconds

        def _n(x: float) -> float:
            return int(x) if float(x).is_integer() else round(x, 2)

        if not active:
            return {
                "active": False,
                "tier": self._active.name,
                "limit": window_limit,
                "window_seconds": window_seconds,
                "count": 0,
                "count_human": 0,
                "count_auto": 0,
                "projected_human": 0,
                "started_at": None,
                "elapsed_seconds": None,
                "remaining_seconds": None,
                "effective_remaining_seconds": None,
                "switch_at": None,
            }
        elapsed = now - ws
        remaining = max(0.0, window_seconds - elapsed)
        count = self._window_count
        # A pending LOW->HIGH switch ends this window early: at the switch the
        # window restarts under HIGH, so everything (the pacer's drain rate, the
        # human/background projections, the dashboard countdown) should treat the
        # switch time as the effective window end. Only relevant while LOW — a
        # HIGH->LOW switch doesn't shorten anything to drain. `effective_remaining`
        # is what every consumer should use; `remaining_seconds` stays the true
        # window remaining for reference.
        effective_remaining = remaining
        switch_at = self.scheduled_switch_at() if self._active is self._low else None
        if switch_at is not None:
            until = switch_at - now
            if until > 0:
                effective_remaining = min(effective_remaining, until)
            else:
                switch_at = None  # already due; the switch itself will roll the window
        # Estimated human total by window end: what humans have spent so far plus
        # their measured arrival rate projected over the (effective) time left —
        # raw, no safety factor; this is a display estimate, not the pacer's
        # reservation. The background (auto) projection is added by
        # AutoPacer.status() since it owns the leftover-budget calculation.
        projected_human = self._window_human_count + self.human_rate() * effective_remaining
        return {
            "active": True,
            "tier": self._active.name,
            "limit": window_limit,
            "window_seconds": window_seconds,
            "count": _n(count),
            "count_human": _n(self._window_human_count),
            "count_auto": _n(self._window_auto_count),
            "projected_human": _n(min(projected_human, float(window_limit))),
            "started_at": ws,
            "elapsed_seconds": elapsed,
            "remaining_seconds": remaining,
            "effective_remaining_seconds": effective_remaining,
            "switch_at": switch_at,
        }

    # -- manual window overrides + persistence (sync, await-free: atomic under
    #    the single-threaded loop, like note_request) --

    def set_window_count(self, count: float) -> dict[str, Any]:
        """Force the rolling window's current request count.

        If no window is currently active (none started, or the last one expired)
        a fresh one is anchored at now so the count is visible. Returns the new
        window snapshot.
        """
        now = time.time()
        if self._window_start is None or now - self._window_start >= float(self._active.window_seconds):
            self._window_start = now
        self._window_count = max(0.0, float(count))
        # Keep the lane split consistent: preserve the auto attribution (clamped
        # to the new total) and assign the remainder to humans.
        self._window_auto_count = min(self._window_auto_count, self._window_count)
        self._window_human_count = self._window_count - self._window_auto_count
        return self._window_snapshot()

    def set_window_start(self, started_at: float | None) -> dict[str, Any]:
        """Force the rolling window's start timestamp (unix seconds).

        `None` clears it, so the next request re-anchors a fresh window. Returns
        the new window snapshot.
        """
        if started_at is None:
            self._restart_window()
        else:
            self._window_start = float(started_at)
        return self._window_snapshot()

    def window_state(self) -> dict[str, Any]:
        """Serializable window state for persistence across restarts."""
        return {
            "started_at": self._window_start,
            "count": self._window_count,
            "count_human": self._window_human_count,
            "count_auto": self._window_auto_count,
            "tier": self._active.name,
            "window_seconds": float(self._active.window_seconds),
        }

    def load_window_state(self, state: dict[str, Any] | None) -> bool:
        """Restore a persisted window, discarding it if it has already elapsed.

        The saved window is dropped when `now - started_at` is past the window
        duration (using the saved window's own duration), so a stale count from a
        long-ago run never resurfaces. Returns True if a window was restored.
        """
        if not isinstance(state, dict):
            return False
        start = state.get("started_at")
        if start is None:
            return False
        try:
            start = float(start)
            count = max(0.0, float(state.get("count", 0) or 0))
            window_seconds = float(state.get("window_seconds")
                                   or self._active.window_seconds)
            auto = max(0.0, float(state.get("count_auto", 0) or 0))
        except (TypeError, ValueError):
            return False
        if time.time() - start >= window_seconds:
            log.info("window: persisted state already elapsed; discarded")
            return False
        self._window_start = start
        self._window_count = count
        # The human share is derived from count - auto rather than read from the
        # saved count_human (which is still written, for inspectability), so the
        # lane-split invariant human + auto == count holds even if the file was
        # hand-edited into an inconsistent state.
        self._window_auto_count = min(auto, count)
        self._window_human_count = count - self._window_auto_count
        log.info(f"window: restored count={count} "
                 f"(human={self._window_human_count} auto={self._window_auto_count}) "
                 f"started_at={start}")
        return True

    async def acquire(self, lane: str = "human") -> bool:
        """Acquire a concurrency slot for `lane` ("human" | "auto").

        Human callers take priority: an auto request is never admitted while a
        human is waiting for a slot, and auto in-flight is capped at
        `max_concurrent - auto_concurrency_reserve` so a reserve (if configured)
        is always free for humans. Only human requests trigger HIGH-tier probes.
        Returns True if this call was admitted as a speculative probe.
        """
        is_auto = lane == "auto"
        async with self._cond:
            self._waiters += 1
            if is_auto:
                self._auto_waiters += 1
            else:
                self._human_waiters += 1
                self._note_human()
            try:
                while True:
                    now = time.monotonic()
                    free = self._in_flight < self._active.max_concurrent

                    if is_auto:
                        auto_cap = max(0, self._active.max_concurrent - self._auto_concurrency_reserve)
                        if free and self._human_waiters == 0 and self._auto_in_flight < auto_cap:
                            self._in_flight += 1
                            self._auto_in_flight += 1
                            self._n_requests += 1
                            return False
                        # Auto never probes — let human traffic drive promotion.
                        self._n_concurrency_waits += 1
                        await self._cond.wait()
                        continue

                    if free:
                        self._in_flight += 1
                        self._human_in_flight += 1
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
                        self._human_in_flight += 1
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
                if is_auto:
                    self._auto_waiters -= 1
                else:
                    self._human_waiters -= 1

    def _release_slot(self, lane: str) -> None:
        self._in_flight -= 1
        if lane == "auto":
            self._auto_in_flight = max(0, self._auto_in_flight - 1)
        else:
            self._human_in_flight = max(0, self._human_in_flight - 1)

    # -- human-demand tracking (for AutoPacer) --

    def _note_human(self) -> None:
        """Record a human arrival and trim history to the demand horizon."""
        now = time.monotonic()
        self._human_times.append(now)
        cutoff = now - self._human_horizon
        while self._human_times and self._human_times[0] < cutoff:
            self._human_times.popleft()

    def human_rate(self) -> float:
        """Smoothed human requests/second, averaged over the demand horizon.

        Deliberately divides by the *horizon* (not the span to the oldest
        sample) so a short burst of human requests is amortized rather than
        extrapolated as a sustained high rate — otherwise a quick flurry of human
        calls would make the auto pacer predict enormous future demand and park
        for the rest of the window. Real-time protection against sudden human
        spikes is handled separately by concurrency priority + the HIGH probe;
        this average only feeds the slower *quota* prediction.

        Until the proxy has been up for a full horizon, the elapsed uptime is
        used as the denominator (capped at the horizon) so early estimates
        aren't artificially diluted toward zero.
        """
        now = time.monotonic()
        cutoff = now - self._human_horizon
        while self._human_times and self._human_times[0] < cutoff:
            self._human_times.popleft()
        if not self._human_times:
            return 0.0
        elapsed = now - self._started_at
        denom = min(self._human_horizon, max(1.0, elapsed))
        return len(self._human_times) / denom

    def set_auto_params(self, concurrency_reserve: int, human_horizon: float) -> None:
        self._auto_concurrency_reserve = max(0, int(concurrency_reserve))
        self._human_horizon = max(1.0, float(human_horizon))

    # -- rate-limit retry-backoff gauge (a request waiting out upstream
    #    pushback holds no slot and is not a waiter, so track it explicitly).
    #    Sync + await-free: atomic under the single-threaded loop. --

    def enter_rl_wait(self, lane: str = "human") -> None:
        """Mark a request as parked in retry backoff after upstream rate-limited it."""
        self._rl_waiting += 1
        if lane == "auto":
            self._rl_waiting_auto += 1
        else:
            self._rl_waiting_human += 1

    def leave_rl_wait(self, lane: str = "human") -> None:
        """Clear the backoff mark once the request retries or gives up."""
        self._rl_waiting = max(0, self._rl_waiting - 1)
        if lane == "auto":
            self._rl_waiting_auto = max(0, self._rl_waiting_auto - 1)
        else:
            self._rl_waiting_human = max(0, self._rl_waiting_human - 1)

    async def release_success(self, was_probe: bool, lane: str = "human") -> None:
        async with self._cond:
            self._release_slot(lane)
            if was_probe:
                self._probe_in_flight = False
                if self._active is self._low and self._forced is None:
                    self._active = self._high
                    self._n_promotions += 1
                    self._restart_window()
                    log.warning(
                        f"tier promoted LOW -> HIGH (max_concurrent={self._high.max_concurrent}, "
                        f"window={self._high.window_limit}/{self._high.window_seconds:.0f}s)"
                    )
            self._cond.notify_all()

    async def release_rate_limited(self, was_probe: bool, lane: str = "human") -> None:
        async with self._cond:
            self._release_slot(lane)
            self._n_rate_limited += 1
            self._last_rate_limited_at = time.time()
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
                self._restart_window()
                log.warning(
                    f"tier demoted HIGH -> LOW (max_concurrent={self._low.max_concurrent}, "
                    f"window={self._low.window_limit}/{self._low.window_seconds:.0f}s)"
                )
            self._cond.notify_all()

    async def release_other_error(self, was_probe: bool, lane: str = "human") -> None:
        async with self._cond:
            self._release_slot(lane)
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
                self._restart_window()
                log.warning(
                    "tier boosted LOW -> HIGH (temporary; auto-demotes on "
                    f"rate-limit; max_concurrent={self._high.max_concurrent}, "
                    f"window={self._high.window_limit}/{self._high.window_seconds:.0f}s)"
                )
            self._cond.notify_all()
            return True

    # -- scheduled tier switches (daily from config + one-shot from API) --
    #    LOW->HIGH (set_daily_switch/set_oneshot_switch) and the HIGH->LOW
    #    counterparts (set_daily_low_switch/set_oneshot_low_switch).

    def set_daily_switch(self, ts: float | None) -> dict[str, Any]:
        """Arm (or clear) the recurring DAILY LOW->HIGH switch from `ts`.

        Only `ts`'s local time-of-day matters; the switch then fires at that time
        every day (re-arming after each fire). `None` clears it. Sync +
        await-free. Returns the schedule snapshot.
        """
        if ts is None:
            self._switch_daily_tod = None
            self._switch_daily_next = None
        else:
            self._switch_daily_tod = _local_tod_seconds(float(ts))
            self._switch_daily_next = _next_time_of_day(self._switch_daily_tod, time.time())
        return self.schedule_snapshot()

    def set_oneshot_switch(self, ts: float | None) -> dict[str, Any]:
        """Arm (or clear) the ONE-SHOT LOW->HIGH switch at absolute unix `ts`.

        Independent of the daily switch; fires once then clears. Sync +
        await-free. Returns the schedule snapshot.
        """
        self._switch_once_at = float(ts) if ts is not None else None
        return self.schedule_snapshot()

    def set_daily_low_switch(self, ts: float | None) -> dict[str, Any]:
        """Arm (or clear) the recurring DAILY HIGH->LOW switch from `ts`.

        Mirror of `set_daily_switch` for the demotion direction.
        """
        if ts is None:
            self._switch_low_daily_tod = None
            self._switch_low_daily_next = None
        else:
            self._switch_low_daily_tod = _local_tod_seconds(float(ts))
            self._switch_low_daily_next = _next_time_of_day(self._switch_low_daily_tod, time.time())
        return self.schedule_snapshot()

    def set_oneshot_low_switch(self, ts: float | None) -> dict[str, Any]:
        """Arm (or clear) the ONE-SHOT HIGH->LOW switch at absolute unix `ts`.

        Mirror of `set_oneshot_switch` for the demotion direction.
        """
        self._switch_low_once_at = float(ts) if ts is not None else None
        return self.schedule_snapshot()

    def scheduled_switch_at(self) -> float | None:
        """Absolute unix time of the next pending LOW->HIGH switch (the earlier of
        the daily and one-shot slots), or None. Read by the pacer to shorten the
        effective window when the switch ends it early."""
        cands = [t for t in (self._switch_daily_next, self._switch_once_at) if t is not None]
        return min(cands) if cands else None

    def scheduled_low_switch_at(self) -> float | None:
        """Absolute unix time of the next pending HIGH->LOW switch, or None."""
        cands = [t for t in (self._switch_low_daily_next, self._switch_low_once_at) if t is not None]
        return min(cands) if cands else None

    @staticmethod
    def _fmt_tod(tod: float | None) -> str | None:
        if tod is None:
            return None
        tod = int(tod)
        h, m, s = tod // 3600, (tod % 3600) // 60, tod % 60
        return f"{h:02d}:{m:02d}" + (f":{s:02d}" if s else "")

    def schedule_snapshot(self) -> dict[str, Any]:
        now = time.time()
        eff_h = self.scheduled_switch_at()
        eff_l = self.scheduled_low_switch_at()
        recurring_h = (
            self._switch_daily_next is not None
            and (self._switch_once_at is None or self._switch_daily_next <= self._switch_once_at)
        )
        recurring_l = (
            self._switch_low_daily_next is not None
            and (self._switch_low_once_at is None or self._switch_low_daily_next <= self._switch_low_once_at)
        )
        return {
            "switch_high_at": eff_h,
            "seconds_until": (eff_h - now) if eff_h is not None else None,
            "pending": eff_h is not None and eff_h > now,
            "recurring": recurring_h,
            "daily_at": self._fmt_tod(self._switch_daily_tod),
            "oneshot_at": self._switch_once_at,
            "switch_low_at": eff_l,
            "low_seconds_until": (eff_l - now) if eff_l is not None else None,
            "low_pending": eff_l is not None and eff_l > now,
            "low_recurring": recurring_l,
            "low_daily_at": self._fmt_tod(self._switch_low_daily_tod),
            "low_oneshot_at": self._switch_low_once_at,
        }

    async def apply_scheduled_switch(self) -> bool:
        """Apply any scheduled tier switch (LOW->HIGH or HIGH->LOW) now due.

        One-shot slots clear once reached; daily slots re-arm to the next day.
        Skipped (slots left pending) while a `force_tier` pins the tier. Restarts
        the quota window under the new tier like any other switch. If both
        directions are due in the same tick (unusual), the one scheduled later
        wins. Returns True if any slot fired.
        """
        async with self._cond:
            if self._forced is not None:
                return False  # pinned tier wins; re-check once force is cleared
            now = time.time()
            high_at: float | None = None  # fire time of a due LOW->HIGH switch
            low_at: float | None = None   # fire time of a due HIGH->LOW switch
            if self._switch_once_at is not None and now >= self._switch_once_at:
                high_at = self._switch_once_at
                self._switch_once_at = None
            if self._switch_daily_next is not None and now >= self._switch_daily_next:
                high_at = self._switch_daily_next if high_at is None else max(high_at, self._switch_daily_next)
                self._switch_daily_next = _next_time_of_day(self._switch_daily_tod, now)
            if self._switch_low_once_at is not None and now >= self._switch_low_once_at:
                low_at = self._switch_low_once_at
                self._switch_low_once_at = None
            if self._switch_low_daily_next is not None and now >= self._switch_low_daily_next:
                low_at = self._switch_low_daily_next if low_at is None else max(low_at, self._switch_low_daily_next)
                self._switch_low_daily_next = _next_time_of_day(self._switch_low_daily_tod, now)
            if high_at is None and low_at is None:
                return False
            # If both directions are due, honor whichever was scheduled later.
            if high_at is not None and low_at is not None:
                go_high = high_at >= low_at
            else:
                go_high = high_at is not None
            target = self._high if go_high else self._low
            if self._active is not target:
                self._active = target
                if go_high:
                    self._n_promotions += 1
                else:
                    self._n_demotions += 1
                self._restart_window()
                arrow = "LOW -> HIGH" if go_high else "HIGH -> LOW"
                tail = ("auto-demotes on rate-limit; " if go_high else "")
                log.warning(
                    f"tier switched {arrow} on schedule ({tail}"
                    f"max_concurrent={target.max_concurrent}, "
                    f"window={target.window_limit}/{target.window_seconds:.0f}s)"
                )
                self._cond.notify_all()
            return True

    async def update_tiers(self, low: Tier, high: Tier,
                           promotion_cooldown: float, forced: str | None,
                           window_weights: dict[str, float] | None = None,
                           default_window_weight: float | None = None) -> None:
        async with self._cond:
            old_active_name = self._active.name
            self._low = low
            self._high = high
            self._promotion_cooldown = promotion_cooldown
            self._forced = forced if forced in ("low", "high") else None
            if window_weights is not None:
                self._window_weights = dict(window_weights)
            if default_window_weight is not None:
                self._default_window_weight = float(default_window_weight)
            if self._forced == "low":
                self._active = self._low
            elif self._forced == "high":
                self._active = self._high
            else:
                self._active = self._low if self._active.name == "low" else self._high
            # A config change that forces a different tier restarts the window
            # under the new tier's limit/duration, same as an auto switch. (When
            # the tier is unchanged the new per-tier limit/duration are still
            # picked up live from self._active on the next snapshot.)
            if self._active.name != old_active_name:
                self._restart_window()
            self._cond.notify_all()

    def window_snapshot(self) -> dict[str, Any]:
        """Public read of the current rolling-window state (used by AutoPacer)."""
        return self._window_snapshot()

    def snapshot(self) -> dict[str, Any]:
        return {
            "active_tier": self._active.name,
            "forced_tier": self._forced,
            "max_concurrent": self._active.max_concurrent,
            "in_flight": self._in_flight,
            "queued": self._waiters,
            "rate_limited_waiting": self._rl_waiting,
            "last_rate_limited_at": self._last_rate_limited_at,
            "probe_in_flight": self._probe_in_flight,
            "schedule": self.schedule_snapshot(),
            "lanes": {
                "human": {"in_flight": self._human_in_flight, "queued": self._human_waiters,
                          "rate_limited_waiting": self._rl_waiting_human},
                "auto": {"in_flight": self._auto_in_flight, "queued": self._auto_waiters,
                         "concurrency_reserve": self._auto_concurrency_reserve,
                         "rate_limited_waiting": self._rl_waiting_auto},
                "human_rate_per_min": round(self.human_rate() * 60.0, 2),
            },
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


# ---------- Automation-lane pacing ----------

class AutoPacer:
    """Paces the automation lane to spend only the *leftover* quota window.

    The human lane is never paced. Automation is admitted at a rate that spreads
    its share of the window evenly, but the share is computed against *predicted
    future human demand* rather than a fixed reserve:

        usable   = window_limit - used - safety * human_rate * remaining - floor
        rate     = usable / remaining            (requests/sec to even-spread)
        rate     = min(rate, free_slots / avg)   (never schedule faster than the
                                                   pipe can drain — uses tracked
                                                   avg request time + tier slots)

    As the window nears its end, `remaining -> 0`, the predicted-human term
    vanishes, and automation is free to drain whatever is left (up to ~100%).
    Early on, it holds back exactly what humans are statistically expected to
    still need. If humans have already consumed the window down to that
    prediction, `usable <= 0` and automation parks until the window advances or
    resets. Gating is serialized so the average admission rate is honored;
    concurrency is still bounded separately by the Limiter (with human priority).
    """

    def __init__(self, limiter: Limiter, metrics: "Metrics", cfg: dict[str, Any]):
        self._limiter = limiter
        self._metrics = metrics
        self._lock = asyncio.Lock()
        self._next = 0.0  # loop.time() at which the next auto request may go
        # Number of automation requests currently blocked in gate() — either
        # holding the lock and re-checking, or queued behind it. Surfaced so the
        # dashboard/statusline can show traffic held back by pacing (it holds no
        # concurrency slot, so it is otherwise invisible to the limiter).
        self._parked = 0
        self.configure(cfg)

    def configure(self, cfg: dict[str, Any]) -> None:
        self._enabled = bool(cfg.get("auto_pacing_enabled", True))
        self._safety = max(0.0, float(cfg.get("human_demand_safety", 1.5)))
        self._floor = max(0.0, float(cfg.get("human_quota_floor", 0)))
        self._assumed = max(0.1, float(cfg.get("auto_assumed_request_seconds", 30.0)))
        self._poll = max(0.05, float(cfg.get("auto_poll_seconds", 1.0)))
        # Cap on how far ahead the measured human rate is projected when
        # reserving quota (see _usable_and_rate). Falls back to the measurement
        # horizon so the two default together.
        self._lookahead = max(
            1.0,
            float(cfg.get("human_demand_lookahead_seconds",
                          cfg.get("human_demand_horizon_seconds", 3600))),
        )

    def _usable_and_rate(self) -> tuple[float, float]:
        """Return (usable_requests, target_rate_per_sec) for automation now."""
        snap = self._limiter.window_snapshot()
        if not snap["active"]:
            # No window open yet — the first request anchors it; let it through.
            return 1.0, float("inf")
        # `effective_remaining_seconds` already folds in a pending LOW->HIGH
        # switch (the window snapshot caps it at the switch time), so the leftover
        # drains over the shorter horizon and the predicted-human term shrinks
        # with it. Fall back to the true remaining if the field is absent.
        remaining = float(
            snap.get("effective_remaining_seconds")
            if snap.get("effective_remaining_seconds") is not None
            else snap.get("remaining_seconds") or 0.0
        )
        if remaining <= 0:
            return 1.0, float("inf")  # window about to roll; drain freely
        limit = float(snap["limit"])
        used = float(snap["count"])
        # Project the measured human rate forward only up to _lookahead, not
        # across the whole (possibly multi-hour) remaining window — otherwise a
        # small human rate reserves nearly the entire quota on a long window.
        horizon = min(remaining, self._lookahead)
        expected_human = self._safety * self._limiter.human_rate() * horizon
        usable = limit - used - expected_human - self._floor
        if usable <= 0:
            return usable, 0.0
        rate = usable / remaining
        # Cap by physical throughput: free slots / avg request time.
        avg = self._metrics.avg_duration(self._assumed)
        capacity = max(1, self._limiter._active.max_concurrent) / max(0.1, avg)
        return usable, min(rate, capacity)

    async def gate(self) -> None:
        """Block until the calling automation request may proceed."""
        if not self._enabled:
            return
        self._parked += 1
        try:
            async with self._lock:
                while True:
                    usable, rate = self._usable_and_rate()
                    if usable <= 0 or rate <= 0:
                        # Would eat into predicted human demand — park and
                        # re-check. Drop any stale schedule so a later rate jump
                        # isn't held behind it.
                        self._next = 0.0
                        await asyncio.sleep(self._poll)
                        continue
                    if rate == float("inf"):
                        self._next = 0.0
                        return
                    now = asyncio.get_event_loop().time()
                    interval = 1.0 / rate
                    if self._next < now:
                        self._next = now
                    # Never sit more than one *current* interval ahead. Without
                    # this, a slow rate (e.g. a 5h LOW window) pushes _next far
                    # into the future; when the window/tier then changes and the
                    # rate jumps, parked requests would still be stuck behind the
                    # old schedule. Clamping re-anchors them to the new rate.
                    self._next = min(self._next, now + interval)
                    wait = self._next - now
                    if wait > 0:
                        await asyncio.sleep(min(wait, self._poll))
                        continue
                    self._next += interval
                    return
        finally:
            self._parked -= 1

    def status(self) -> dict[str, Any]:
        """Current pacing state for the dashboard / statusline.

        `parked` is how many automation requests are held in gate() right now.
        `next_seconds` is the wait until the next one may go (None when parked on
        human-reserved quota — that releases when the window advances/resets, not
        on a fixed timer). `reason` is one of disabled/open/paced/reserved.
        """
        snap = self._limiter.window_snapshot()
        count_auto = float(snap.get("count_auto", 0) or 0)
        if not self._enabled:
            return {"enabled": False, "parked": self._parked, "usable": None,
                    "rate_per_min": None, "next_seconds": None, "reason": "disabled",
                    "count_auto": round(count_auto, 2), "projected_auto": round(count_auto, 2)}
        usable, rate = self._usable_and_rate()
        inf = rate == float("inf")
        if usable <= 0 or rate <= 0:
            reason, next_s, rpm = "reserved", None, 0.0
        elif inf:
            reason, next_s, rpm = "open", 0.0, None
        else:
            now = asyncio.get_event_loop().time()
            reason = "paced"
            next_s = max(0.0, self._next - now) if self._parked else 0.0
            rpm = round(rate * 60.0, 2)
        # Estimated background total by window end = spent so far + remaining
        # usable budget (only when there's a real active window to project into).
        if snap.get("active") and reason in ("paced", "reserved"):
            projected_auto = count_auto + max(0.0, usable)
        else:
            projected_auto = count_auto
        return {
            "enabled": True,
            "parked": self._parked,
            "usable": round(usable, 2),
            "rate_per_min": rpm,
            "next_seconds": round(next_s, 1) if next_s is not None else None,
            "reason": reason,
            "count_auto": round(count_auto, 2),
            "projected_auto": round(projected_auto, 2),
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
pacer: AutoPacer | None = None
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
    """Build a Tier from config.

    Each tier may carry its own `window_seconds` / `window_limit` (its rolling
    request-quota budget). When omitted they fall back to the top-level
    `rate_window_seconds` / `rate_window_limit` so older single-window configs
    keep working.
    """
    t = cfg["tiers"][name]
    return Tier(
        name=name,
        max_concurrent=int(t["max_concurrent"]),
        window_seconds=float(t.get("window_seconds", cfg.get("rate_window_seconds", 18000))),
        window_limit=int(t.get("window_limit", cfg.get("rate_window_limit", 600))),
    )


def parse_window_weights(cfg: dict[str, Any]) -> tuple[dict[str, float], float]:
    """Read per-model window weights + default weight from config, validated.

    Non-positive or non-numeric weights are dropped/ignored (falling back to the
    default) so a bad config line can't silently zero out the window count.
    """
    raw = cfg.get("model_window_weights") or {}
    weights: dict[str, float] = {}
    if isinstance(raw, dict):
        for model, w in raw.items():
            try:
                wf = float(w)
            except (TypeError, ValueError):
                log.warning(f"model_window_weights[{model!r}]={w!r} not numeric; ignored")
                continue
            if wf <= 0:
                log.warning(f"model_window_weights[{model!r}]={w!r} must be > 0; ignored")
                continue
            weights[str(model)] = wf
    try:
        default = float(cfg.get("default_window_weight", 1))
    except (TypeError, ValueError):
        default = 1.0
    if default <= 0:
        default = 1.0
    return weights, default


def _local_tod_seconds(ts: float) -> float:
    """Seconds since local midnight for an absolute unix timestamp."""
    import datetime as _dt
    ln = _dt.datetime.fromtimestamp(ts)
    return ln.hour * 3600 + ln.minute * 60 + ln.second


def _next_time_of_day(tod_seconds: float, now: float) -> float:
    """Absolute unix of the next local occurrence of `tod_seconds` strictly after
    `now` (today if still ahead, else tomorrow). DST-safe via the local date."""
    import datetime as _dt
    ln = _dt.datetime.fromtimestamp(now)
    midnight = ln.replace(hour=0, minute=0, second=0, microsecond=0)
    cand = midnight + _dt.timedelta(seconds=float(tod_seconds))
    if cand.timestamp() <= now:
        cand += _dt.timedelta(days=1)
    return cand.timestamp()


def parse_switch_time(value: Any, now: float | None = None) -> float | None:
    """Resolve a scheduled-switch time (config key or API body) to unix seconds.

    Accepts:
      - None / "" / 0           -> None (no scheduled switch)
      - a number (or numeric    -> taken as unix epoch seconds
        string)
      - "HH:MM" / "HH:MM:SS"     -> the next future occurrence in local time
      - an ISO-8601 datetime     -> parsed in local time if it has no offset
        ("YYYY-MM-DDTHH:MM[:SS]")

    Returns None (with a warning) for anything unparseable, so a bad value never
    arms a switch at a surprising time.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        pass
    import datetime as _dt
    now = time.time() if now is None else now
    local_now = _dt.datetime.fromtimestamp(now)
    parts = s.split(":")
    if len(parts) in (2, 3) and all(p.isdigit() for p in parts):
        hh, mm = int(parts[0]), int(parts[1])
        ss = int(parts[2]) if len(parts) == 3 else 0
        if 0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60:
            cand = local_now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
            if cand.timestamp() <= now:
                cand += _dt.timedelta(days=1)  # already passed today -> tomorrow
            return cand.timestamp()
    try:
        return _dt.datetime.fromisoformat(s).timestamp()
    except ValueError:
        log.warning(f"scheduled switch time: could not parse {value!r}; ignoring")
        return None


def init_from_config(cfg: dict[str, Any]) -> tuple[Limiter, Metrics, PersistentStats, AutoPacer]:
    forced = cfg.get("force_tier") if cfg.get("force_tier") in ("low", "high") else None
    window_weights, default_window_weight = parse_window_weights(cfg)
    lim = Limiter(
        low=make_tier(cfg, "low"),
        high=make_tier(cfg, "high"),
        initial_tier=cfg.get("initial_tier", "low"),
        promotion_cooldown=float(cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_weights=window_weights,
        default_window_weight=default_window_weight,
    )
    lim.set_auto_params(
        concurrency_reserve=int(cfg.get("auto_concurrency_reserve", 0)),
        human_horizon=float(cfg.get("human_demand_horizon_seconds", 3600)),
    )
    lim.set_daily_switch(parse_switch_time(cfg.get("scheduled_high_at")))
    lim.set_daily_low_switch(parse_switch_time(cfg.get("scheduled_low_at")))
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
    pc = AutoPacer(lim, met, cfg)
    return lim, met, ps, pc


async def apply_config_change(new_cfg: dict[str, Any]) -> None:
    global config
    if new_cfg.get("upstream_base_url") != config.get("upstream_base_url"):
        log.warning("config: upstream_base_url changed -> restart required")
    if client is not None and new_cfg.get("upstream_timeout") != config.get("upstream_timeout"):
        client.timeout = httpx.Timeout(float(new_cfg["upstream_timeout"]), connect=15.0)
        log.info(f"config: upstream_timeout -> {new_cfg['upstream_timeout']}s")
    log_level = str(new_cfg.get("log_level", "INFO")).upper()
    logging.getLogger().setLevel(log_level)
    forced = new_cfg.get("force_tier") if new_cfg.get("force_tier") in ("low", "high") else None
    window_weights, default_window_weight = parse_window_weights(new_cfg)
    await limiter.update_tiers(
        low=make_tier(new_cfg, "low"),
        high=make_tier(new_cfg, "high"),
        promotion_cooldown=float(new_cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_weights=window_weights,
        default_window_weight=default_window_weight,
    )
    limiter.set_auto_params(
        concurrency_reserve=int(new_cfg.get("auto_concurrency_reserve", 0)),
        human_horizon=float(new_cfg.get("human_demand_horizon_seconds", 3600)),
    )
    limiter.set_daily_switch(parse_switch_time(new_cfg.get("scheduled_high_at")))
    limiter.set_daily_low_switch(parse_switch_time(new_cfg.get("scheduled_low_at")))
    pacer.configure(new_cfg)
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
            # Apply a due scheduled LOW->HIGH switch (one-shot; cheap no-op when
            # nothing is armed). Polled here so the switch lands within
            # config_poll_seconds of its target time.
            if await limiter.apply_scheduled_switch():
                await asyncio.to_thread(save_window_file)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"config reload failed: {e!r}")
        await asyncio.sleep(float(config.get("config_poll_seconds", 2.0)))


def _window_persist_path() -> Path:
    return Path(config.get("window_persist_path", "window.json")).resolve()


def load_window_file() -> dict[str, Any] | None:
    """Read the persisted rolling-window state (count + start), if any."""
    p = _window_persist_path()
    try:
        if not p.exists():
            return None
        with open(p) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, ValueError) as e:
        log.warning(f"window: could not load {p}: {e!r}")
        return None


def save_window_file() -> None:
    """Persist the current rolling-window state for the next restart.

    Skips writing when no window is active (nothing worth restoring). Written
    atomically via a temp file + os.replace.
    """
    state = limiter.window_state()
    p = _window_persist_path()
    if state.get("started_at") is None:
        # No active window — clear any stale file so a restart starts fresh.
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass
        return
    tmp = p.with_name(p.name + ".tmp")
    try:
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(state, f, separators=(",", ":"))
        os.replace(tmp, p)
    except OSError as e:
        log.warning(f"window: save to {p} failed: {e!r}")


async def persist_loop() -> None:
    """Flush aggregated stats + window state to disk on a fixed interval."""
    while True:
        try:
            await pstats.maybe_flush()
            await asyncio.to_thread(save_window_file)
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
limiter, metrics, pstats, pacer = init_from_config(config)
limiter.load_window_state(load_window_file())


# ---------- HTTP app ----------

# Background tasks live here so startup()/shutdown() can be called either from
# the FastAPI lifespan (single-server `uvicorn proxy:app`) or directly from
# serve() (the dual-port `python proxy.py` path), without double-initializing.
_bg_tasks: list[asyncio.Task] = []


async def startup() -> None:
    global client
    if client is not None:
        return
    client = httpx.AsyncClient(
        base_url=str(config["upstream_base_url"]).rstrip("/"),
        timeout=httpx.Timeout(float(config["upstream_timeout"]), connect=15.0),
        limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
    )
    _bg_tasks.append(asyncio.create_task(config_watch_loop()))
    _bg_tasks.append(asyncio.create_task(persist_loop()))
    human = f"http://{config['listen_host']}:{config['listen_port']}"
    auto_port = config.get("throttle_listen_port")
    auto = f" | auto-lane http://{config['listen_host']}:{auto_port}" if auto_port else ""
    log.info(
        f"anthropic_proxy human-lane {human}{auto} -> {config['upstream_base_url']} "
        f"| tier={limiter._active.name} forced={limiter._forced} | dashboard: /_proxy/"
    )


async def shutdown() -> None:
    global client
    for task in _bg_tasks:
        task.cancel()
    for task in _bg_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    _bg_tasks.clear()
    await pstats.maybe_flush(force=True)
    await asyncio.to_thread(save_window_file)
    if client is not None:
        await client.aclose()
        client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    await startup()
    try:
        yield
    finally:
        await shutdown()


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
<script>
  /* Apply a saved theme before first paint to avoid a flash. "auto" (or no
     value) leaves data-theme unset so the prefers-color-scheme media query
     governs; "light"/"dark" pin it explicitly. */
  (function () {
    try {
      var t = localStorage.getItem("theme");
      if (t === "light" || t === "dark") {
        document.documentElement.setAttribute("data-theme", t);
      }
    } catch (e) {}
  })();
</script>
<style>
  /* Dark theme (default). Light values live in the two blocks below: one for
     the system "prefers light" setting (auto), one for the explicit
     [data-theme="light"] toggle override. */
  :root {
    --bg: #0d1117; --panel: #161b22; --border: #30363d;
    --text: #c9d1d9; --muted: #8b949e; --accent: #58a6ff;
    --green: #3fb950; --yellow: #d29922; --red: #f85149;
    --card-bg: rgba(0,0,0,0.25); --card-border: rgba(255,255,255,0.03);
    --chart-bg: rgba(0,0,0,0.2); --bar-track: rgba(255,255,255,0.08);
    --grid: #30363d; --accent-tint: rgba(88,166,255,0.15);
  }
  /* Auto: follow the OS setting when the user hasn't picked a theme (no
     data-theme attribute). Reacts live to system light/dark changes. */
  @media (prefers-color-scheme: light) {
    :root:not([data-theme]) {
      --bg: #ffffff; --panel: #f6f8fa; --border: #d0d7de;
      --text: #1f2328; --muted: #656d76; --accent: #0969da;
      --green: #1a7f37; --yellow: #9a6700; --red: #cf222e;
      --card-bg: rgba(0,0,0,0.03); --card-border: rgba(0,0,0,0.06);
      --chart-bg: rgba(0,0,0,0.03); --bar-track: rgba(0,0,0,0.08);
      --grid: #d0d7de; --accent-tint: rgba(9,105,218,0.12);
    }
  }
  /* Manual override: the toggle sets data-theme="light"|"dark" on <html>. */
  :root[data-theme="light"] {
    --bg: #ffffff; --panel: #f6f8fa; --border: #d0d7de;
    --text: #1f2328; --muted: #656d76; --accent: #0969da;
    --green: #1a7f37; --yellow: #9a6700; --red: #cf222e;
    --card-bg: rgba(0,0,0,0.03); --card-border: rgba(0,0,0,0.06);
    --chart-bg: rgba(0,0,0,0.03); --bar-track: rgba(0,0,0,0.08);
    --grid: #d0d7de; --accent-tint: rgba(9,105,218,0.12);
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    background: var(--bg); color: var(--text);
    margin: 0; padding: 20px; font-size: 14px;
  }
  .container { max-width: 1600px; margin: 0 auto; }
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
  /* Wrapping flexbox (not CSS grid): every card is at least as wide as its
     own content (the nowrap .sub lines below), so text never wraps mid-line;
     cards flow onto extra rows only when the row truly runs out of space. */
  .grid { display: flex; flex-wrap: wrap; gap: 10px; }
  .stat {
    flex: 1 1 150px;
    background: var(--card-bg); padding: 12px; border-radius: 6px;
    border: 1px solid var(--card-border);
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
  .stat .sub { color: var(--muted); font-size: 11px; margin-top: 4px; white-space: nowrap; }
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
    background: var(--card-bg); border-radius: 6px;
    border: 1px solid var(--card-border);
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
  #boost-btn, #theme-btn {
    background: var(--panel); color: var(--text);
    border: 1px solid var(--border); border-radius: 6px;
    padding: 6px 12px; font-size: 12px; font-weight: 600; cursor: pointer;
  }
  #boost-btn:hover:not(:disabled) { border-color: var(--green); color: var(--green); }
  #boost-btn:disabled { opacity: 0.5; cursor: default; }
  #theme-btn:hover { border-color: var(--accent); color: var(--accent); }
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
  .seg button.active { background: var(--accent-tint); color: var(--accent); }
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
    background: var(--chart-bg); border-radius: 6px;
  }
  svg.chart text { fill: var(--muted); font-size: 9px; font-variant-numeric: tabular-nums; }
  .bar {
    height: 6px; border-radius: 3px; background: var(--bar-track);
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
      <button id="theme-btn" title="Theme: Auto follows your system; click to cycle Auto → Light → Dark">🖥 Auto</button>
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
  const gridColor = getComputedStyle(document.documentElement)
    .getPropertyValue("--grid").trim() || "#30363d";
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
    svgParts.push(`<line x1="${padL}" y1="${yy}" x2="${W - padR}" y2="${yy}" stroke="${gridColor}" stroke-width="1"/>`);
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
    const rlWait = L.rate_limited_waiting || 0;
    const rlAgo = L.last_rate_limited_at ? ((Date.now()/1000) - L.last_rate_limited_at) : null;
    const rlSub = rlWait > 0
      ? "backing off upstream limit"
      : (rlAgo !== null ? "last 429 " + fmtSpan(rlAgo) + " ago" : "none");

    const W = L.window || {active:false};
    const winHrs = (W.window_seconds || 18000) / 3600;
    const winLabel = (Number.isInteger(winHrs) ? winHrs : winHrs.toFixed(1)) + "h Window";
    const lanes = L.lanes || {human:{in_flight:0,queued:0}, auto:{in_flight:0,queued:0,concurrency_reserve:0}, human_rate_per_min:0};
    const autoQ = lanes.auto.queued || 0;
    const laneCard = `
      <div class="stat">
        <div class="label">Lanes · human / auto</div>
        <div class="value">${lanes.human.in_flight} / ${lanes.auto.in_flight}</div>
        <div class="sub">${autoQ > 0 ? autoQ + " awaiting slot · " : ""}human ~${lanes.human_rate_per_min}/min${lanes.auto.concurrency_reserve ? " · reserve " + lanes.auto.concurrency_reserve : ""}</div>
      </div>`;

    // Automation pacing: requests held in the pacer (no slot yet) + when the
    // next one is due. "reserved" = parked because the rest of the window is
    // statistically kept for humans (releases when the window advances/resets).
    const P = m.pacer || {enabled:true, parked:0, reason:"open", next_seconds:null, rate_per_min:null};
    const paceVal = P.parked > 0 ? P.parked + " held" : (P.enabled ? "clear" : "off");
    const paceClass = P.parked > 0 ? (P.reason === "reserved" ? "warn" : "") : "";
    let paceSub;
    if (!P.enabled) paceSub = "pacing disabled";
    else if (P.reason === "reserved") paceSub = "reserved for humans · waits for window";
    else if (P.next_seconds != null && P.next_seconds >= 0.05) paceSub = "next in " + fmtSpan(P.next_seconds);
    else paceSub = P.parked > 0 ? "releasing now" : "idle";
    if (P.rate_per_min != null) paceSub += " · " + P.rate_per_min + "/min budget";
    const paceCard = `
      <div class="stat">
        <div class="label">Auto Pacing</div>
        <div class="value ${paceClass}">${paceVal}</div>
        <div class="sub">${paceSub}</div>
      </div>`;

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
      const usePct = pct(W.count, W.limit);
      const useClass = usePct >= 95 ? "crit" : usePct >= 80 ? "warn" : "";
      // A pending LOW→HIGH switch ends the window early: count down to (and fill
      // the time bar toward) the switch, matching the shortened pacing horizon.
      const effRem = W.effective_remaining_seconds ?? W.remaining_seconds;
      const shortened = W.switch_at != null && effRem < (W.remaining_seconds - 1);
      const timePct = shortened
        ? pct(W.elapsed_seconds, W.elapsed_seconds + effRem)
        : pct(W.elapsed_seconds, W.window_seconds);
      const leftSub = shortened
        ? `${fmtSpan(effRem)} to ⏰HIGH (${fmtSpan(W.remaining_seconds)} window)`
        : `${fmtSpan(W.remaining_seconds)} left`;
      // Split + end-of-window estimate: human "so far → projected", background
      // (auto) "so far → projected" (projected_auto comes from the pacer).
      const ch = W.count_human ?? 0, ca = W.count_auto ?? 0;
      // Projections are estimates; whole requests read better than decimals.
      const ph = Math.round(W.projected_human ?? ch), pa = Math.round(P.projected_auto ?? ca);
      winCard = `
        <div class="stat">
          <div class="label">${winLabel}</div>
          <div class="value ${useClass}">${W.count} / ${W.limit}</div>
          <div class="sub">${fmtSpan(W.elapsed_seconds)} in · ${leftSub}</div>
          <div class="sub">human ${ch}→~${ph} · auto ${ca}→~${pa}</div>
          <div class="bar"><i class="${useClass}" style="width:${timePct.toFixed(1)}%"></i></div>
        </div>`;
    }

    // Pending scheduled switches get one sub-line each (they're too long to
    // share the tier mode line without forcing the card extremely wide).
    const sched = L.schedule || {};
    const schedSubs = [];
    if (sched.pending) {
      schedSubs.push("⏰HIGH " + (sched.recurring
        ? "daily @" + sched.daily_at + " (in " + fmtSpan(sched.seconds_until) + ")"
        : "in " + fmtSpan(sched.seconds_until)));
    }
    if (sched.low_pending) {
      schedSubs.push("⏰LOW " + (sched.low_recurring
        ? "daily @" + sched.low_daily_at + " (in " + fmtSpan(sched.low_seconds_until) + ")"
        : "in " + fmtSpan(sched.low_seconds_until)));
    }
    document.getElementById("state-grid").innerHTML = `
      <div class="stat">
        <div class="label">Active Tier</div>
        <div class="value ${tierClass}">${L.active_tier.toUpperCase()}</div>
        <div class="sub">${L.forced_tier ? "forced" : "auto"}${L.probe_in_flight ? " · probing" : ""}</div>
        ${schedSubs.map((s) => `<div class="sub">${s}</div>`).join("")}
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
      <div class="stat">
        <div class="label">Waiting on 429</div>
        <div class="value ${rlWait > 0 ? "crit" : ""}">${rlWait}</div>
        <div class="sub">${rlSub}</div>
      </div>
      ${laneCard}
      ${paceCard}
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
      const PS = m.persistent;  // don't shadow P (the pacer) above
      const totalCards = [
        ["24h", "24h"], ["Weekly", "7d"], ["Monthly", "30d"], ["Lifetime", "lifetime"],
      ];
      let totHtml = "";
      for (const [label, key] of totalCards) {
        const o = (PS[key] && PS[key].overall) || {count:0, errors:0, input_tokens:0, output_tokens:0, cache_creation_input_tokens:0, cache_read_input_tokens:0, cost:null};
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
// ---- Theme toggle: cycles Auto -> Light -> Dark, persisted in localStorage.
// "auto" clears data-theme so the prefers-color-scheme media query governs.
const THEMES = ["auto", "light", "dark"];
const THEME_LABEL = { auto: "🖥 Auto", light: "☀ Light", dark: "🌙 Dark" };
function currentTheme() {
  const t = localStorage.getItem("theme");
  return THEMES.includes(t) ? t : "auto";
}
function applyTheme(t) {
  if (t === "auto") document.documentElement.removeAttribute("data-theme");
  else document.documentElement.setAttribute("data-theme", t);
  const btn = document.getElementById("theme-btn");
  if (btn) btn.textContent = THEME_LABEL[t];
}
document.getElementById("theme-btn").addEventListener("click", () => {
  const next = THEMES[(THEMES.indexOf(currentTheme()) + 1) % THEMES.length];
  localStorage.setItem("theme", next);
  applyTheme(next);
  drawSeries();   // repaint SVG gridlines with the new theme's --grid color
});
applyTheme(currentTheme());
// Repaint charts when the OS theme flips while in Auto mode.
if (window.matchMedia) {
  window.matchMedia("(prefers-color-scheme: light)").addEventListener("change", () => {
    if (currentTheme() === "auto") drawSeries();
  });
}
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
        "pacer": pacer.status(),
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
    pace = pacer.status()
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
    parked = pace.get("parked", 0)
    if parked > 0:
        nxt = pace.get("next_seconds")
        if pace.get("reason") == "reserved":
            held = f"⏸{parked}"  # paused: quota reserved for humans
        elif nxt is not None and nxt >= 0.05:
            held = f"⏳{parked}→{_fmt_dur_short(nxt)}"
        else:
            held = f"⏳{parked}"
        parts.append(color(held, "yellow"))
    rl_wait = snap.get("rate_limited_waiting", 0)
    if rl_wait > 0:
        # Requests waiting out upstream rate-limiting (e.g. the backend's quota).
        parts.append(color(f"⏳429×{rl_wait}", "red"))
    if w["errors"] > 0:
        parts.append(color(f"!{w['errors']}err", err_color))
    if snap.get("probe_in_flight"):
        parts.append(color("probe", "cyan"))
    sched = snap.get("schedule") or {}
    if sched.get("pending"):
        parts.append(color(f"⏰HIGH→{_fmt_dur_short(sched.get('seconds_until'))}", "cyan"))
    if sched.get("low_pending"):
        parts.append(color(f"⏰LOW→{_fmt_dur_short(sched.get('low_seconds_until'))}", "cyan"))

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


@app.post("/_proxy/schedule_high")
async def schedule_high_endpoint(req: Request):
    """Arm (or clear) a ONE-SHOT automatic LOW->HIGH switch.

    Body: {"at": <value>} where value is unix epoch seconds, "HH:MM"/"HH:MM:SS"
    (next future local occurrence), an ISO-8601 datetime, or null to clear. At
    that time the tier promotes to HIGH once (auto-demotes on the next
    rate-limit; use force_tier to pin). Before it fires, the pacer drains the
    current LOW window's leftover over the shorter horizon ending at the switch.

    This is independent of the recurring DAILY switch configured via
    `scheduled_high_at` (which keeps firing every day); the proxy acts on
    whichever comes first. Example:
      curl -X POST localhost:8787/_proxy/schedule_high -d '{"at": "14:30"}'
    """
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "at" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'at'"}, status_code=400)
    raw = body["at"]
    ts = parse_switch_time(raw)
    if raw is not None and ts is None:
        return JSONResponse(
            {"error": "'at' must be unix seconds, 'HH:MM[:SS]', an ISO-8601 "
                      "datetime, or null"},
            status_code=400,
        )
    return limiter.set_oneshot_switch(ts)


@app.post("/_proxy/schedule_low")
async def schedule_low_endpoint(req: Request):
    """Arm (or clear) a ONE-SHOT automatic HIGH->LOW switch.

    Body: {"at": <value>} where value is unix epoch seconds, "HH:MM"/"HH:MM:SS"
    (next future local occurrence), an ISO-8601 datetime, or null to clear. At
    that time the tier drops to LOW once.

    Mirror of POST /_proxy/schedule_high, and independent of the recurring DAILY
    switch configured via `scheduled_low_at`; the proxy acts on whichever comes
    first. Example:
      curl -X POST localhost:8787/_proxy/schedule_low -d '{"at": "09:00"}'
    """
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "at" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'at'"}, status_code=400)
    raw = body["at"]
    ts = parse_switch_time(raw)
    if raw is not None and ts is None:
        return JSONResponse(
            {"error": "'at' must be unix seconds, 'HH:MM[:SS]', an ISO-8601 "
                      "datetime, or null"},
            status_code=400,
        )
    return limiter.set_oneshot_low_switch(ts)


@app.post("/_proxy/window/count")
async def set_window_count_endpoint(req: Request):
    """Set the current rolling-window request count for the active session.

    Body: {"count": <number >= 0>}. Anchors a fresh window at now if none is
    active. Example:
      curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'
    """
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "count" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'count'"}, status_code=400)
    try:
        count = float(body["count"])
    except (TypeError, ValueError):
        return JSONResponse({"error": "'count' must be a number"}, status_code=400)
    if count < 0:
        return JSONResponse({"error": "'count' must be >= 0"}, status_code=400)
    snap = limiter.set_window_count(count)
    await asyncio.to_thread(save_window_file)
    return {"window": snap}


@app.post("/_proxy/window/start")
async def set_window_start_endpoint(req: Request):
    """Set the current rolling-window start time (unix seconds).

    Body: {"started_at": <unix seconds>} to anchor the window, or
    {"started_at": null} to clear it (the next request re-anchors). Example:
      curl -X POST localhost:8787/_proxy/window/start -d '{"started_at": 1733250000}'
    """
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "started_at" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'started_at'"}, status_code=400)
    started_at = body["started_at"]
    if started_at is not None:
        try:
            started_at = float(started_at)
        except (TypeError, ValueError):
            return JSONResponse(
                {"error": "'started_at' must be a unix timestamp (seconds) or null"},
                status_code=400,
            )
    snap = limiter.set_window_start(started_at)
    await asyncio.to_thread(save_window_file)
    return {"window": snap}


# ---------- Proxy handler ----------

def request_lane(request: Request) -> str:
    """Which ingress lane a request arrived on, decided by listening port.

    The automation port (`throttle_listen_port`) is the paced lane; the human
    `listen_port` (and anything else) is unthrottled. Reads the server port from
    the ASGI scope, falling back to the URL port.
    """
    auto_port = config.get("throttle_listen_port")
    if auto_port is None:
        return "human"
    server = request.scope.get("server")
    port = server[1] if server and len(server) >= 2 else request.url.port
    return "auto" if port == int(auto_port) else "human"


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
    lane = request_lane(request)
    started_at = metrics.request_started(model)
    # Automation lane: wait for the pacer before committing the request to the
    # quota window. The human lane is never paced. (gate() can park for a while
    # when the window is nearly spent; it holds no concurrency slot meanwhile.)
    if lane == "auto":
        await pacer.gate()
    noted_weight, noted_token = limiter.note_request(model, lane)

    finished = False
    handed_off = False

    def finalize(status: int, usage: dict | None = None) -> None:
        nonlocal finished
        if not finished:
            finished = True
            # Window counts requests that actually consumed upstream quota: a
            # request that ultimately failed (rate-limited out / connection
            # error / client abort) is taken back off the window count.
            if not (200 <= status < 400):
                limiter.discount_request(noted_weight, noted_token, lane)
            # Always drop it from the in-flight tally (any outcome ends its life).
            limiter.note_done(noted_weight, lane)
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
            was_probe = await limiter.acquire(lane)
            try:
                outbound = client.build_request(
                    method=request.method, url=target,
                    content=body, headers=headers,
                )
                response = await client.send(outbound, stream=True)
            except httpx.HTTPError as e:
                await limiter.release_other_error(was_probe, lane)
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
                await limiter.release_rate_limited(was_probe, lane)
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
                # Parked waiting out upstream pushback — surface it so the
                # dashboard shows the client is waiting (the request holds no
                # slot here and isn't a concurrency waiter).
                limiter.enter_rl_wait(lane)
                try:
                    await asyncio.sleep(backoff)
                finally:
                    limiter.leave_rl_wait(lane)
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
                        await limiter.release_success(was_probe, lane)
                    else:
                        await limiter.release_other_error(was_probe, lane)
                    usage = extractor.final_usage() if is_success else None
                    finalize(status_code, usage)

            handed_off = True
            return StreamingResponse(
                body_stream(),
                status_code=status_code,
                headers=out_headers,
            )
    finally:
        # Reached without handing off a response (e.g. cancellation/unexpected
        # error escaping the loop). Run finalize so the request is dropped from
        # the in-flight tally and its window count reversed, not just recorded.
        if not handed_off and not finished:
            finalize(0)


async def serve() -> None:
    """Run the human lane and (if configured) the automation lane together.

    Both ports serve the same app + shared state; startup()/shutdown() run once
    around them, so there is a single upstream client, queue, and set of
    background tasks regardless of how many ports are listening.
    """
    import uvicorn

    host = str(config["listen_host"])
    log_level = str(config.get("log_level", "info")).lower()
    ports = [int(config["listen_port"])]
    auto_port = config.get("throttle_listen_port")
    if auto_port is not None and int(auto_port) not in ports:
        ports.append(int(auto_port))

    await startup()
    servers = [
        uvicorn.Server(uvicorn.Config(
            app, host=host, port=p, lifespan="off",
            log_level=log_level, access_log=False,
        ))
        for p in ports
    ]
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(serve())
