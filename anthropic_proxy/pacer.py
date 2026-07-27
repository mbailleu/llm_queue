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


# -- formatting helpers for the human-readable pacing explanation --

def _fmt_count(v: float) -> str:
    a = abs(v)
    if a >= 1e6:
        return f"{v / 1e6:.2f}M"
    if a >= 1e3:
        return f"{v / 1e3:.1f}k"
    if a >= 100:
        return f"{v:.0f}"
    if a >= 1:
        return f"{v:.1f}".rstrip("0").rstrip(".")
    return f"{v:.3g}"


def _fmt_units(metric: str, v: float) -> str:
    """A quota quantity in its budget's units: $12.34 / $0.0132 / 312.5k / 140."""
    if metric == "cost":
        return f"${v:,.2f}" if abs(v) >= 1 else f"${v:.4f}"
    return _fmt_count(v)


def _fmt_secs(s: float | None) -> str:
    if s is None:
        return "—"
    s = max(0.0, float(s))
    if s < 60:
        return f"{s:.0f}s"
    if s < 3600:
        return f"{s / 60:.0f}m"
    h, m = int(s // 3600), int((s % 3600) // 60)
    return f"{h}h{m:02d}m"


def _fmt_win_len(s: float | None) -> str:
    """Budget window length, as the dashboard labels it: 1min / 5h / 90s."""
    if not s:
        return "?"
    if s % 3600 == 0:
        return f"{int(s // 3600)}h"
    if s % 60 == 0:
        return f"{int(s // 60)}min"
    return f"{s:.0f}s"


def _fmt_rate(per_sec: float) -> str:
    """A request rate as requests/min (what the dashboard and config talk in)."""
    if per_sec == float("inf"):
        return "unlimited"
    rpm = per_sec * 60.0
    if rpm >= 100:
        return f"{rpm:.0f} req/min"
    if rpm >= 1:
        return f"{rpm:.1f} req/min"
    return f"{rpm:.2f} req/min"


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
    across pacing budgets, additionally capped by physical throughput (the
    limiter's *effective* concurrency — the adaptive estimate while HIGH —
    divided by the measured avg **upstream** time, i.e. how long a request
    holds a concurrency slot; never the client-visible duration, which contains
    time spent parked in this very gate).

    Every intermediate term of that arithmetic is kept, and `explain()` renders
    it as sentences (which budget binds, what each term contributed, why the
    others don't count). `status()` returns it under `explain`, plus per-window
    `explain` lines, so the dashboard can show exactly how the current rate was
    derived instead of just its value.

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
        # One-shot manual override (`release_all`): N free passes through the
        # gate, expiring shortly after they were granted so leftovers can't be
        # picked up by unrelated traffic minutes later.
        self._free_passes = 0
        self._free_passes_until = 0.0
        self.configure(cfg)

    def release_all(self) -> int:
        """Let every currently parked automation request through, once.

        A manual override for "I know the budget is fine, go now": it grants one
        free pass per request parked in `gate()` right now, without changing the
        computed rate — the next arrivals are paced normally again. Sync and
        await-free; parked requests pick their pass up on their next poll (they
        are queued on `_lock`, so they drain one after another). Returns how many
        passes were granted.
        """
        self._free_passes = self._parked
        self._free_passes_until = time.monotonic() + max(10.0, self._poll * 5)
        self._next = 0.0  # drop any pending schedule so they aren't held again
        return self._free_passes

    def _take_free_pass(self) -> bool:
        """Consume one unexpired free pass, if any (see `release_all`)."""
        if self._free_passes <= 0:
            return False
        if time.monotonic() >= self._free_passes_until:
            self._free_passes = 0
            return False
        self._free_passes -= 1
        return True

    def set_enabled(self, enabled: bool) -> None:
        """Turn pacing on/off at runtime (the dashboard's throttle switch).

        Disabling makes `gate()` a no-op, so the automation lane runs at the
        same admission policy as the human one (still behind the concurrency
        queue, still yielding to humans). Callers that want this to survive a
        config reload should go through `apply_config_change` instead.
        """
        self._enabled = bool(enabled)

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

    def _evaluate_window(self, w: dict[str, Any], human_rate: float) -> dict[str, Any]:
        """Score one budget window, keeping every intermediate term.

        The returned entry always carries `metric` / `window_seconds` / `group`
        and a `skip` reason (None when the math below ran). `skip` marks a
        window that cannot constrain the pace at all:
          "inactive"      — dormant; the next request anchors it
          "rolling"       — no time left; it drains freely
          "unconvertible" — no per-request estimate for its metric (e.g. a cost
                            budget with no pricing), so it can neither fill nor
                            be converted into a request rate
        Computable entries also carry the terms of

            usable_units = limit − used − expected_human − floor
            usable_reqs  = usable_units / auto_units_per_req
            rate         = usable_reqs / remaining

        so `explain()` can show the arithmetic that produced the current pace.
        `paces` is False for windows shorter than auto_pace_min_window_seconds:
        they are evaluated for the dashboard but never bind and never park auto.
        """
        metric = str(w.get("metric", "requests"))
        window_seconds = float(w.get("window_seconds") or 0.0)
        e: dict[str, Any] = {
            "metric": metric,
            "window_seconds": window_seconds,
            "group": w.get("group"),
            "count_auto": float(w.get("count_auto", 0) or 0),
            "paces": window_seconds >= self._pace_min_window,
            "skip": None,
            "binds": False,
        }
        if not w.get("active"):
            e["skip"] = "inactive"
            return e
        # Convert the leftover into requests with the AUTO lane's own
        # per-request estimate — that's what an auto request will actually
        # consume; the blended EWMA is dominated by whichever lane runs more
        # traffic (often big-context human requests).
        units_per_req = self._limiter.units_per_request(metric, "auto")
        if units_per_req <= 0:
            # Can't convert this budget into requests — e.g. a cost budget with
            # no model_pricing configured (its count never rises either).
            e["skip"] = "unconvertible"
            return e
        # `effective_remaining_seconds` already folds in a pending LOW->HIGH
        # switch (the window snapshot caps it at the switch time), so the
        # leftover drains over the shorter horizon and the predicted-human term
        # shrinks with it.
        remaining = float(
            w.get("effective_remaining_seconds")
            if w.get("effective_remaining_seconds") is not None
            else w.get("remaining_seconds") or 0.0
        )
        if remaining <= 0:
            e["skip"] = "rolling"
            return e
        reserve_applies = window_seconds >= self._reserve_min_window
        if reserve_applies:
            # Project the measured human rate forward only up to _lookahead,
            # not across the whole (possibly multi-hour) remaining window —
            # otherwise a small human rate reserves nearly the entire quota on a
            # long window. Sized with the HUMAN lane's per-request estimate.
            horizon = min(remaining, self._lookahead)
            human_units_per_req = self._limiter.units_per_request(metric, "human")
            expected_human = self._safety * human_rate * horizon * human_units_per_req
        else:
            # Short windows self-heal within seconds and the human lane is
            # protected there by queue priority + upstream 429 retries —
            # reserving predicted demand on them only starves auto. The
            # reservation lives on the long (e.g. 5h cost) budgets.
            horizon = 0.0
            human_units_per_req = 0.0
            expected_human = 0.0
        # human_quota_floor is a request count; it applies only to requests
        # budgets rather than being converted into token/cost floors.
        floor_units = self._floor if metric == "requests" else 0.0
        limit_v = float(w["limit"])
        used = float(w["count"])
        usable_units = limit_v - used - expected_human - floor_units
        usable_reqs = usable_units / units_per_req
        e.update({
            "limit": limit_v,
            "used": used,
            "remaining": remaining,
            "reserve_applies": reserve_applies,
            "human_rate": human_rate,
            "human_units_per_req": human_units_per_req,
            "auto_units_per_req": units_per_req,
            "lookahead": horizon,
            "expected_human": expected_human,
            "floor_units": floor_units,
            "usable_units": usable_units,
            "usable_reqs": usable_reqs,
            "rate": usable_reqs / remaining if usable_reqs > 0 else 0.0,
        })
        e["binds"] = e["paces"]
        return e

    def _evaluate(self) -> dict[str, Any]:
        """Evaluate every budget window and pick the binding (slowest) one.

        Returns {usable, rate, binding, windows, capacity, avg_seconds} where
        `usable` is in REQUESTS (the binding budget's leftover converted via
        units_per_request), `rate` is the final requests/sec (inf = open),
        `binding` is the index of the binding window in the limiter snapshot
        (None when no budget can constrain), `windows` has one entry per
        snapshot window (see `_evaluate_window`), and `capacity` is the
        physical-throughput cap (concurrency slots / avg request time).
        """
        snap = self._limiter.window_snapshot()
        wins = snap.get("windows") or []
        human_rate = self._limiter.human_rate()
        entries: list[dict[str, Any]] = []
        binding: int | None = None
        for w in wins:
            e = self._evaluate_window(w, human_rate)
            entries.append(e)
            if e["binds"] and (binding is None or e["rate"] < entries[binding]["rate"]):
                binding = len(entries) - 1
        # Physical throughput ceiling (Little's Law): concurrency slots / avg
        # SERVICE time. It must be the slot-hold time, never the client-visible
        # duration: that one includes time parked in this very gate, so using it
        # would make pacing delay feed back into a lower cap, which delays
        # pacing further — a spiral that a single request waiting out a 429
        # backoff is enough to start. Kept in the result even when it isn't the
        # minimum, so the explanation can show why it didn't bind.
        avg = self._metrics.avg_upstream_time(self._assumed)
        capacity = max(1, self._limiter.effective_max_concurrent) / max(0.1, avg)
        out = {"windows": entries, "capacity": capacity, "avg_seconds": avg,
               "concurrency": max(1, self._limiter.effective_max_concurrent)}
        if binding is None:
            # No budget can constrain right now — let the request through (it
            # anchors the windows).
            return {**out, "usable": 1.0, "rate": float("inf"), "binding": None}
        b = entries[binding]
        if b["usable_reqs"] <= 0:
            return {**out, "usable": b["usable_reqs"], "rate": 0.0, "binding": binding}
        return {**out, "usable": b["usable_reqs"],
                "rate": min(b["rate"], capacity), "binding": binding}

    def _usable_and_rate(self) -> tuple[float, float]:
        """Return (usable_requests, target_rate_per_sec) for automation now."""
        ev = self._evaluate()
        return ev["usable"], ev["rate"]

    # -- explanation: the arithmetic behind the current rate, in words --

    @staticmethod
    def _budget_name(e: dict[str, Any]) -> str:
        name = f"{e['metric']}/{_fmt_win_len(e['window_seconds'])}"
        return f"{name} ({e['group']})" if e.get("group") else name

    def _explain_window(self, e: dict[str, Any]) -> list[str]:
        """Why this budget allows (or doesn't allow) what it does, in sentences.

        Written as prose rather than an aligned table because it is shown in a
        native tooltip, which renders in a proportional font.
        """
        name = self._budget_name(e)
        skip = e.get("skip")
        if skip == "inactive":
            return [f"{name}: window idle — the next request starts it. No pacing from here."]
        if skip == "rolling":
            return [f"{name}: window is rolling over — it drains freely right now."]
        if skip == "unconvertible":
            return [f"{name}: no per-request {e['metric']} estimate (unpriced models?), "
                    f"so this budget can neither fill nor set a rate."]
        m = e["metric"]

        def u(v: float) -> str:
            return _fmt_units(m, v)

        lines = [
            f"{name}: limit {u(e['limit'])} − spent {u(e['used'])}"
            + (f" − human reserve {u(e['expected_human'])}" if e["reserve_applies"] else "")
            + (f" − floor {_fmt_count(e['floor_units'])} req" if e["floor_units"] else "")
            + f" = usable {u(e['usable_units'])}."
        ]
        if e["reserve_applies"]:
            lines.append(
                f"Human reserve = {self._safety:g} safety × "
                f"{e['human_rate'] * 60:.2f} human req/min × "
                f"{u(e['human_units_per_req'])} per human request × "
                f"{_fmt_secs(e['lookahead'])} lookahead."
            )
        else:
            lines.append(
                f"No human reserve here: the window is shorter than "
                f"human_reserve_min_window_seconds ({_fmt_secs(self._reserve_min_window)}) — "
                f"humans are protected on it by queue priority and 429 retries instead."
            )
        if e["usable_reqs"] > 0:
            lines.append(
                f"{u(e['usable_units'])} ÷ {u(e['auto_units_per_req'])} per auto request "
                f"= {_fmt_count(e['usable_reqs'])} requests, spread over the "
                f"{_fmt_secs(e['remaining'])} left = {_fmt_rate(e['rate'])}."
            )
        else:
            lines.append(
                f"Nothing usable left ({u(e['usable_units'])}), so this budget would "
                f"park the auto lane until the window advances or resets."
            )
            if e["expected_human"] > 0 and e["limit"] - e["used"] > 0:
                # The budget itself isn't spent — the predicted-human reserve is
                # what parks automation. That is the knob to turn, so name it.
                lines.append(
                    f"{u(e['limit'] - e['used'])} of the budget is actually unspent: "
                    f"the human reserve alone parks automation. Lower "
                    f"human_demand_safety ({self._safety:g}) or "
                    f"human_demand_lookahead_seconds "
                    f"({_fmt_secs(self._lookahead)}) to loosen it."
                )
        if not e["paces"]:
            lines.append(
                f"Not pacing: the window is shorter than auto_pace_min_window_seconds "
                f"({_fmt_secs(self._pace_min_window)}), so it never sets the rate and "
                f"never parks auto — the queue and the upstream 429 backoff throttle it."
            )
        return lines

    def explain(self, ev: dict[str, Any] | None = None) -> list[str]:
        """The full derivation of the current auto-lane rate, line by line.

        Shown verbatim in the dashboard's Auto Pacing tooltip: headline, the
        binding budget's arithmetic, the throughput cap, then a one-line
        roll-up of every other budget.
        """
        if not self._enabled:
            return ["Auto pacing is disabled (auto_pacing_enabled: false) — "
                    "the automation lane is only bounded by the concurrency queue."]
        ev = self._evaluate() if ev is None else ev
        entries: list[dict[str, Any]] = ev["windows"]
        binding = ev["binding"]
        lines: list[str] = []
        if binding is None:
            lines.append("Auto lane open: no budget can pace right now, so requests "
                         "pass straight through.")
        else:
            b = entries[binding]
            if ev["rate"] <= 0:
                lines.append(f"Auto lane parked — {self._budget_name(b)} is the binding "
                             f"budget and has nothing left for automation.")
            else:
                capped = ev["capacity"] < b["rate"]
                lines.append(
                    f"Auto pace {_fmt_rate(ev['rate'])}, set by "
                    f"{'the throughput cap' if capped else self._budget_name(b) + ' (the binding budget)'}."
                )
            lines += self._explain_window(b)
        lines.append(
            f"Throughput cap: {ev['concurrency']} concurrency slots ÷ "
            f"{ev['avg_seconds']:.1f}s avg upstream time (slot-hold only, "
            f"excludes queueing and backoff) = {_fmt_rate(ev['capacity'])}"
            + ("." if binding is None or ev["capacity"] < entries[binding]["rate"]
               else " (not binding).")
        )
        others = [self._explain_others(e) for i, e in enumerate(entries) if i != binding]
        if others:
            lines.append("Other budgets: " + "; ".join(others) + ".")
        return lines

    def _explain_others(self, e: dict[str, Any]) -> str:
        """One clause per non-binding budget for the roll-up line."""
        name = self._budget_name(e)
        skip = e.get("skip")
        if skip:
            return f"{name} {skip}"
        if not e["paces"]:
            return (f"{name} not paced (window < "
                    f"{_fmt_secs(self._pace_min_window)}), "
                    f"{_fmt_units(e['metric'], e['usable_units'])} left")
        if e["usable_reqs"] <= 0:
            return f"{name} spent"
        return f"{name} would allow {_fmt_rate(e['rate'])}"

    async def gate(self) -> None:
        """Block until the calling automation request may proceed."""
        if not self._enabled:
            return
        self._parked += 1
        try:
            async with self._lock:
                while True:
                    if self._take_free_pass():
                        return          # manual one-shot release
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
        index with each budget's leftover + auto projection in its OWN units
        plus whether it can pace at all (`paces`) and its own `explain` lines,
        so the dashboard can annotate every quota meter; `binding_index` /
        `binding_metric` / `binding_window_seconds` identify the budget that
        currently sets the pace — that is the one the dashboard highlights, NOT
        the merely most-utilized window. `explain` is the full derivation of the
        current rate (see `explain()`). Top-level count_auto/projected_auto stay
        in the snapshot's binding-window units to match the mirror fields there.
        """
        snap = self._limiter.window_snapshot()
        count_auto = float(snap.get("count_auto", 0) or 0)
        if not self._enabled:
            return {"enabled": False, "parked": self._parked, "usable": None,
                    "rate_per_min": None, "next_seconds": None, "reason": "disabled",
                    "count_auto": round(count_auto, 2), "projected_auto": round(count_auto, 2),
                    "binding_index": None, "binding_metric": None,
                    "binding_window_seconds": None, "explain": self.explain(),
                    "free_passes": 0, "windows": []}
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
            computed = e is not None and e.get("skip") is None
            ca = float(w.get("count_auto", 0) or 0)
            if computed and w.get("active") and projecting:
                pa = ca + max(0.0, e["usable_units"])
            else:
                pa = ca
            windows_status.append({
                "metric": w.get("metric"),
                "window_seconds": w.get("window_seconds"),
                "usable_units": round(e["usable_units"], 4) if computed else None,
                "projected_auto": round(pa, 4),
                # False = this window can never set the pace or park auto (too
                # short to pace, or it can't constrain at all). The dashboard
                # uses it to avoid flagging such a budget as binding.
                "paces": bool(e["paces"]) if e is not None else False,
                "binds": bool(e["binds"]) if e is not None else False,
                "rate_per_min": round(e["rate"] * 60.0, 2) if computed else None,
                "explain": self._explain_window(e) if e is not None else [],
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
            "binding_index": pace_binding,
            "free_passes": self._free_passes,
            "binding_metric": binding_metric,
            "binding_window_seconds": binding_ws,
            "explain": self.explain(ev),
            "windows": windows_status,
        }
