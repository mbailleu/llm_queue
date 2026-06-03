"""Concurrency limiter: tiers, auto-detection, per-tier quota window, and the
human/automation lane policy with human-demand tracking."""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from ._log import log


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
        switch) so the count and timer reset and the next request re-anchors a
        fresh window under the new tier's limit/duration. Synchronous and
        await-free; callers already hold `self._cond`.
        """
        self._window_start = None
        self._window_count = 0.0

    def note_request(self, model: str = "") -> float:
        """Count one client request against the rolling quota window.

        Called once per client request (not per retry). The request counts as
        `_window_weight_for(model)` units toward the window (a per-model factor);
        this affects only the window indicator, never the per-request metrics or
        per-model stats. The window's duration/limit come from the active tier,
        so they follow tier changes. Starts a fresh window when there is none
        active or the current one has elapsed. Synchronous and await-free, so
        it's atomic under the single-threaded event loop. Returns the weight
        applied.
        """
        now = time.time()
        window_seconds = float(self._active.window_seconds)
        if self._window_start is None or now - self._window_start >= window_seconds:
            self._window_start = now
            self._window_count = 0.0
        weight = self._window_weight_for(model)
        self._window_count += weight
        return weight

    def _window_snapshot(self) -> dict[str, Any]:
        now = time.time()
        window_seconds = float(self._active.window_seconds)
        window_limit = int(self._active.window_limit)
        ws = self._window_start
        active = ws is not None and now - ws < window_seconds
        if not active:
            return {
                "active": False,
                "tier": self._active.name,
                "limit": window_limit,
                "window_seconds": window_seconds,
                "count": 0,
                "started_at": None,
                "elapsed_seconds": None,
                "remaining_seconds": None,
            }
        elapsed = now - ws
        count = self._window_count
        return {
            "active": True,
            "tier": self._active.name,
            "limit": window_limit,
            "window_seconds": window_seconds,
            "count": int(count) if float(count).is_integer() else round(count, 2),
            "started_at": ws,
            "elapsed_seconds": elapsed,
            "remaining_seconds": max(0.0, window_seconds - elapsed),
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
        except (TypeError, ValueError):
            return False
        if time.time() - start >= window_seconds:
            log.info("window: persisted state already elapsed; discarded")
            return False
        self._window_start = start
        self._window_count = count
        log.info(f"window: restored count={count} started_at={start}")
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
            "probe_in_flight": self._probe_in_flight,
            "lanes": {
                "human": {"in_flight": self._human_in_flight, "queued": self._human_waiters},
                "auto": {"in_flight": self._auto_in_flight, "queued": self._auto_waiters,
                         "concurrency_reserve": self._auto_concurrency_reserve},
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
