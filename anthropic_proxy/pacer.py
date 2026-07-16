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
    """Paces the automation lane to spend only the *leftover* quota.

    The human lane is never paced. Automation is admitted at a rate that spreads
    its share of the window evenly, but the share is computed against *predicted
    future human demand* rather than a fixed reserve. With multiple budgets per
    tier (requests / tokens / cost, each its own window) every budget is
    evaluated and the **binding** (most constraining) one sets the pace. Per
    budget, in that budget's units:

        usable_units = limit - used - safety * human_rate * human_units_per_req
                                              * min(remaining, lookahead)
                       - floor            (floor only for requests budgets)
        usable_reqs  = usable_units / auto_units_per_req
        rate         = usable_reqs / remaining   (requests/sec to even-spread)

    `units_per_req` converts between requests and the budget's units (1 for
    requests; a live EWMA of tokens/cost per request otherwise, with configured
    assumed values before any data). It is per-lane: predicted human demand is
    sized with the human lane's EWMA, while the leftover is converted into auto
    requests with the auto lane's — the two kinds of traffic can differ in size
    by orders of magnitude. Windows shorter than
    `auto_pace_min_window_seconds` are exempt from pacing entirely: they never
    bind the rate and never park auto, even when fully spent. A per-minute
    limit self-heals within seconds, so throttling there is left to the lane's
    own mechanisms — the concurrency queue (with human priority) plus the
    upstream 429 retry/backoff loop — while pacing lives on the long (e.g. 5h
    cost) budgets. Exempt windows are still evaluated so the dashboard can show
    their leftover/projection. Independently, windows shorter than
    `human_reserve_min_window_seconds` skip the predicted-human term: they
    recover within seconds and the human lane is protected there by queue
    priority + upstream 429 retries, so reserving on them only starves auto —
    the reservation belongs to the long budgets. The final rate is the minimum
    across pacing budgets, additionally capped by physical throughput (free
    slots / avg request time).

    As a window nears its end, `remaining -> 0`, the predicted-human term
    vanishes, and automation is free to drain whatever is left (up to ~100%).
    Early on, it holds back exactly what humans are statistically expected to
    still need. If humans have already consumed any budget down to that
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
        # Windows shorter than this carry no predicted-human reservation (see
        # class docstring); 0 reserves on every window.
        self._reserve_min_window = max(
            0.0, float(cfg.get("human_reserve_min_window_seconds", 300)))
        # Windows shorter than this don't pace at all — they can neither bind
        # the rate nor park auto; the queue + upstream 429 backoff throttle
        # them instead (see class docstring). 0 paces on every window.
        self._pace_min_window = max(
            0.0, float(cfg.get("auto_pace_min_window_seconds", 300)))

    def _evaluate(self) -> dict[str, Any]:
        """Evaluate every budget window and pick the binding (slowest) one.

        Returns {usable, rate, binding, windows} where `usable` is in REQUESTS
        (the binding budget's leftover converted via units_per_request),
        `rate` is the final requests/sec (inf = open), `binding` is the index
        of the binding window in the limiter snapshot (None when open), and
        `windows` has one entry per snapshot window (None for windows that
        can't constrain: inactive, about to roll, or unpriced-cost) with
        {metric, window_seconds, usable_units, usable_reqs, rate, paces,
        count_auto}. Entries with paces=False (windows shorter than
        auto_pace_min_window_seconds) are evaluated for display only and are
        never picked as binding.
        """
        snap = self._limiter.window_snapshot()
        wins = snap.get("windows") or []
        human_rate = self._limiter.human_rate()
        entries: list[dict[str, Any] | None] = []
        binding: int | None = None
        for i, w in enumerate(wins):
            if not w.get("active"):
                entries.append(None)  # dormant — the first request anchors it
                continue
            metric = str(w.get("metric", "requests"))
            # Convert the leftover into requests with the AUTO lane's own
            # per-request estimate — that's what an auto request will actually
            # consume; the blended EWMA is dominated by whichever lane runs
            # more traffic (often big-context human requests).
            units_per_req = self._limiter.units_per_request(metric, "auto")
            if units_per_req <= 0:
                # Can't convert this budget into requests — e.g. a cost budget
                # with no model_pricing configured (its count never rises
                # either). It can never bind.
                entries.append(None)
                continue
            # `effective_remaining_seconds` already folds in a pending LOW->HIGH
            # switch (the window snapshot caps it at the switch time), so the
            # leftover drains over the shorter horizon and the predicted-human
            # term shrinks with it.
            remaining = float(
                w.get("effective_remaining_seconds")
                if w.get("effective_remaining_seconds") is not None
                else w.get("remaining_seconds") or 0.0
            )
            if remaining <= 0:
                entries.append(None)  # about to roll; drains freely
                continue
            limit = float(w["limit"])
            used = float(w["count"])
            window_seconds = float(w.get("window_seconds") or 0.0)
            # Short windows are exempt from pacing: their entry is computed
            # for the dashboard, but they never bind and never park auto —
            # the queue + upstream 429 backoff throttle overruns there.
            paces = window_seconds >= self._pace_min_window
            if window_seconds < self._reserve_min_window:
                # Short windows self-heal within seconds and the human lane is
                # protected there by queue priority + upstream 429 retries —
                # reserving predicted demand on them only starves auto. The
                # reservation lives on the long (e.g. 5h cost) budgets.
                expected_human = 0.0
            else:
                # Project the measured human rate forward only up to
                # _lookahead, not across the whole (possibly multi-hour)
                # remaining window — otherwise a small human rate reserves
                # nearly the entire quota on a long window. Sized with the
                # HUMAN lane's per-request estimate.
                horizon = min(remaining, self._lookahead)
                expected_human = (self._safety * human_rate * horizon
                                  * self._limiter.units_per_request(metric, "human"))
            # human_quota_floor is a request count; it applies only to requests
            # budgets rather than being converted into token/cost floors.
            floor_units = self._floor if metric == "requests" else 0.0
            usable_units = limit - used - expected_human - floor_units
            usable_reqs = usable_units / units_per_req
            rate = usable_reqs / remaining if usable_reqs > 0 else 0.0
            entries.append({
                "metric": metric,
                "window_seconds": window_seconds,
                "usable_units": usable_units,
                "usable_reqs": usable_reqs,
                "rate": rate,
                "paces": paces,
                "count_auto": float(w.get("count_auto", 0) or 0),
            })
            if paces and (binding is None or rate < entries[binding]["rate"]):
                binding = i
        if binding is None:
            # No budget can constrain right now — let the request through (it
            # anchors the windows).
            return {"usable": 1.0, "rate": float("inf"), "binding": None,
                    "windows": entries}
        b = entries[binding]
        if b["usable_reqs"] <= 0:
            return {"usable": b["usable_reqs"], "rate": 0.0, "binding": binding,
                    "windows": entries}
        # Cap by physical throughput: free slots / avg request time.
        avg = self._metrics.avg_duration(self._assumed)
        capacity = max(1, self._limiter.active.max_concurrent) / max(0.1, avg)
        return {"usable": b["usable_reqs"], "rate": min(b["rate"], capacity),
                "binding": binding, "windows": entries}

    def _usable_and_rate(self) -> tuple[float, float]:
        """Return (usable_requests, target_rate_per_sec) for automation now."""
        ev = self._evaluate()
        return ev["usable"], ev["rate"]

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
        `usable` is in requests (the binding budget's leftover converted via
        units_per_request); `windows` mirrors the limiter snapshot's windows by
        index with each budget's leftover + auto projection in its OWN units, so
        the dashboard can annotate every quota meter; `binding_metric` /
        `binding_window_seconds` identify the budget that currently sets the
        pace. Top-level count_auto/projected_auto stay in the snapshot's
        binding-window units to match the mirror fields there.
        """
        snap = self._limiter.window_snapshot()
        count_auto = float(snap.get("count_auto", 0) or 0)
        if not self._enabled:
            return {"enabled": False, "parked": self._parked, "usable": None,
                    "rate_per_min": None, "next_seconds": None, "reason": "disabled",
                    "count_auto": round(count_auto, 2), "projected_auto": round(count_auto, 2),
                    "binding_metric": None, "binding_window_seconds": None,
                    "windows": []}
        ev = self._evaluate()
        usable, rate = ev["usable"], ev["rate"]
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
        # Per-budget view, aligned by index with the limiter snapshot's windows.
        # projected_auto per window = spent so far + remaining usable budget in
        # that window's units (only while there's a real budget to project into).
        projecting = reason in ("paced", "reserved")
        windows_status: list[dict[str, Any]] = []
        for i, w in enumerate(snap.get("windows") or []):
            e = ev["windows"][i] if i < len(ev["windows"]) else None
            ca = float(w.get("count_auto", 0) or 0)
            if e is not None and w.get("active") and projecting:
                pa = ca + max(0.0, e["usable_units"])
            else:
                pa = ca
            windows_status.append({
                "metric": w.get("metric"),
                "window_seconds": w.get("window_seconds"),
                "usable_units": round(e["usable_units"], 4) if e is not None else None,
                "projected_auto": round(pa, 4),
            })
        pace_binding = ev["binding"]
        binding_metric = binding_ws = None
        if pace_binding is not None and pace_binding < len(windows_status):
            binding_metric = windows_status[pace_binding]["metric"]
            binding_ws = windows_status[pace_binding]["window_seconds"]
        # Top-level projection follows the snapshot's binding (mirror) window.
        mirror_i = snap.get("binding", 0) or 0
        if 0 <= mirror_i < len(windows_status):
            projected_auto = windows_status[mirror_i]["projected_auto"]
        else:
            projected_auto = count_auto + max(0.0, usable) if (snap.get("active") and projecting) else count_auto
        return {
            "enabled": True,
            "parked": self._parked,
            "usable": round(usable, 2),
            "rate_per_min": rpm,
            "next_seconds": round(next_s, 1) if next_s is not None else None,
            "reason": reason,
            "count_auto": round(count_auto, 2),
            "projected_auto": round(projected_auto, 2),
            "binding_metric": binding_metric,
            "binding_window_seconds": binding_ws,
            "windows": windows_status,
        }
