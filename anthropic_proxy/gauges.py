"""Short-horizon history of the live queue gauges (requests in the system).

Pure module, no dependencies: a caller samples the counters at a fixed
interval and this keeps the trailing window of them. The persisted stats in
`persistence.py` answer "how much traffic ran", which is a *completion*
history bucketed hourly; this answers "how much is in the proxy right now, and
how did that develop over the last hour", which needs sampling because a gauge
has no completion event to record.

In-memory only: a restart starts a fresh history (an hour of 2-second samples
isn't worth persisting, and stale gauge data would be misleading).
"""
from __future__ import annotations

import time
from collections import deque
from typing import Any


class GaugeHistory:
    """Trailing window of sampled queue gauges.

    Each sample splits the requests the proxy is holding into where they are:

      upstream — holding a concurrency slot, i.e. actually at the API
      queued   — admitted but waiting for a slot
      backoff  — sleeping out upstream 429/503/529 pushback
      parked   — held by the auto-lane pacer, no slot yet

    `total` (the sum) is every request in the system; `upstream` is the part
    the API is working on. The gap between the two lines is exactly what the
    proxy is holding back, which is the point of graphing them together.
    """

    __slots__ = ("_samples", "_sample_seconds", "_history_seconds")

    def __init__(self, sample_seconds: float = 2.0, history_seconds: float = 3600.0):
        # (t, upstream, queued, backoff, parked)
        self._samples: deque[tuple[float, int, int, int, int]] = deque()
        self._sample_seconds = 1.0
        self._history_seconds = 3600.0
        self.configure(sample_seconds, history_seconds)

    def configure(self, sample_seconds: float, history_seconds: float) -> None:
        self._sample_seconds = max(0.2, float(sample_seconds))
        self._history_seconds = max(60.0, float(history_seconds))
        self._trim(time.time())

    @property
    def sample_seconds(self) -> float:
        return self._sample_seconds

    def _trim(self, now: float) -> None:
        cutoff = now - self._history_seconds
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def sample(self, upstream: int, queued: int, backoff: int, parked: int) -> None:
        """Record one observation. Sync and await-free, like the counters it reads."""
        now = time.time()
        self._samples.append((now, max(0, int(upstream)), max(0, int(queued)),
                              max(0, int(backoff)), max(0, int(parked))))
        self._trim(now)

    def series(self) -> dict[str, Any]:
        """The trailing window as chart-ready points (oldest first)."""
        self._trim(time.time())
        points = [
            {
                "t": t,
                "upstream": up,
                "queued": q,
                "backoff": b,
                "parked": p,
                "total": up + q + b + p,
            }
            for t, up, q, b, p in self._samples
        ]
        return {
            "step": self._sample_seconds,
            "span": self._history_seconds,
            "points": points,
        }
