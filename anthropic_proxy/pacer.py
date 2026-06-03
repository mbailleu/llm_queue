"""AutoPacer: paces the automation lane to spend only the leftover quota."""
from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .limiter import Limiter
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
        self._next = 0.0  # loop.time() at which the next auto request may go
        self.configure(cfg)

    def configure(self, cfg: dict[str, Any]) -> None:
        self._enabled = bool(cfg.get("auto_pacing_enabled", True))
        self._safety = max(0.0, float(cfg.get("human_demand_safety", 1.5)))
        self._floor = max(0.0, float(cfg.get("human_quota_floor", 0)))
        self._assumed = max(0.1, float(cfg.get("auto_assumed_request_seconds", 30.0)))
        self._poll = max(0.05, float(cfg.get("auto_poll_seconds", 1.0)))

    def _usable_and_rate(self) -> tuple[float, float]:
        """Return (usable_requests, target_rate_per_sec) for automation now."""
        snap = self._limiter.window_snapshot()
        if not snap["active"]:
            # No window open yet — the first request anchors it; let it through.
            return 1.0, float("inf")
        remaining = float(snap["remaining_seconds"] or 0.0)
        if remaining <= 0:
            return 1.0, float("inf")  # window about to roll; drain freely
        limit = float(snap["limit"])
        used = float(snap["count"])
        expected_human = self._safety * self._limiter.human_rate() * remaining
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
        async with self._lock:
            while True:
                usable, rate = self._usable_and_rate()
                if usable <= 0 or rate <= 0:
                    # Would eat into predicted human demand — park and re-check.
                    await asyncio.sleep(self._poll)
                    continue
                if rate == float("inf"):
                    return
                now = asyncio.get_event_loop().time()
                if self._next < now:
                    self._next = now
                wait = self._next - now
                if wait > 0:
                    await asyncio.sleep(min(wait, self._poll))
                    continue
                self._next += 1.0 / rate
                return
