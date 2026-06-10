"""Automation-lane pacing: spend only the leftover of the quota window.

Pure module: depends on `limiter` (window snapshot, human rate, active tier)
and `metrics` (EWMA latency) — keep that dependency direction acyclic. The
human lane never touches the pacer.
"""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from .limiter import Limiter

if TYPE_CHECKING:
    from .metrics import Metrics


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
        self._next = 0.0  # time.monotonic() at which the next auto request may go
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
        capacity = max(1, self._limiter.active.max_concurrent) / max(0.1, avg)
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
                    now = time.monotonic()
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
            now = time.monotonic()
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
