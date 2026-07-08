"""Concurrency tiers, the shared queue, lanes, and the rolling quota window.

Pure module: no FastAPI/httpx. The single-threaded event loop is what makes
the await-free counter mutations here atomic — keep methods marked "sync and
await-free" synchronous when editing.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import time
from collections import deque
from typing import Any

log = logging.getLogger("proxy.limiter")


def local_tod_seconds(ts: float) -> float:
    """Seconds since local midnight for an absolute unix timestamp."""
    ln = datetime.datetime.fromtimestamp(ts)
    return ln.hour * 3600 + ln.minute * 60 + ln.second


def next_time_of_day(tod_seconds: float, now: float) -> float:
    """Absolute unix of the next local occurrence of `tod_seconds` strictly after
    `now` (today if still ahead, else tomorrow). DST-safe via the local date."""
    ln = datetime.datetime.fromtimestamp(now)
    midnight = ln.replace(hour=0, minute=0, second=0, microsecond=0)
    cand = midnight + datetime.timedelta(seconds=float(tod_seconds))
    if cand.timestamp() <= now:
        cand += datetime.timedelta(days=1)
    return cand.timestamp()


BUDGET_METRICS = ("requests", "tokens", "cost")
_METRIC_UNIT = {"requests": "req", "tokens": "tok", "cost": "$"}


class Budget:
    """One rolling quota dimension: `limit` units of `metric` per `window_seconds`.

    metric is "requests" (weighted request count), "tokens" (all four usage
    token fields summed), or "cost" (priced USD, so cost budgets only count
    models with `model_pricing` entries).
    """
    __slots__ = ("metric", "limit", "window_seconds")

    def __init__(self, metric: str, limit: float, window_seconds: float):
        if metric not in BUDGET_METRICS:
            raise ValueError(f"unknown budget metric {metric!r}")
        self.metric = metric
        self.limit = float(limit)
        self.window_seconds = float(window_seconds)

    def __str__(self) -> str:
        lim = int(self.limit) if self.limit.is_integer() else self.limit
        return f"{lim}{_METRIC_UNIT[self.metric]}/{self.window_seconds:.0f}s"


class BudgetWindow:
    """The live rolling-window state for one Budget of the active tier.

    `start` is None while dormant (nothing has anchored it yet, or the state
    was reset); `human + auto == count` is the lane-split invariant.
    """
    __slots__ = ("budget", "start", "count", "human", "auto")

    def __init__(self, budget: Budget):
        self.budget = budget
        self.start: float | None = None
        self.count = 0.0
        self.human = 0.0
        self.auto = 0.0


class Tier:
    __slots__ = ("name", "max_concurrent", "budgets")

    def __init__(self, name: str, max_concurrent: int,
                 window_seconds: float = 18000.0, window_limit: int = 600,
                 budgets: list[Budget] | None = None):
        self.name = name
        self.max_concurrent = max_concurrent
        # Each tier carries its own list of rolling quota budgets (the upstream
        # "N units per W seconds" limits for that tier) — one or more of
        # requests / tokens / cost, each with its own window length. The active
        # tier's windows drive the dashboard quota indicators, and the counters
        # restart whenever the active tier changes. The legacy single
        # request-window style (window_seconds/window_limit) compiles to one
        # requests budget, so older configs and call sites keep working.
        if budgets:
            self.budgets = list(budgets)
        else:
            self.budgets = [Budget("requests", float(window_limit), float(window_seconds))]

    def budgets_str(self) -> str:
        return "[" + ", ".join(str(b) for b in self.budgets) + "]"


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
        # Wakes requests sleeping out a 429 Retry-After when the tier switches
        # to HIGH (fresh quota window): the Retry-After they were honoring was
        # computed against the old LOW window and is stale the moment the
        # switch lands. Replaced (not just .clear()ed) on each wake so a sleeper
        # that grabbed the old event can't miss a set() racing its wait().
        self._rl_wake = asyncio.Event()
        # Recent human-request arrival times (monotonic), used to estimate
        # future human demand for the pacer. Trimmed to _human_horizon.
        self._human_times: deque[float] = deque()
        self._human_horizon = 3600.0
        self._started_at = time.monotonic()
        # Rolling quota windows — one BudgetWindow per Budget of the ACTIVE
        # tier (LOW and HIGH each have their own list), so they switch when the
        # tier does. Every window is anchored at the first request after it
        # expired, matching "the window starts when the first request is sent";
        # each rolls independently (a 60s window rolls many times inside a 5h
        # one). Requests are counted at admission (note_request); tokens/cost
        # land at completion (note_usage). Tracked for display + auto pacing;
        # not enforced for humans (upstream 429s + retry do that). All windows
        # restart on every tier change (see _restart_window). Each window keeps
        # a human/auto lane split with the invariant human + auto == count.
        self._windows: list[BudgetWindow] = [BudgetWindow(b) for b in self._active.budgets]
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
        # Applies to requests-metric budgets only.
        self._window_weights: dict[str, float] = dict(window_weights or {})
        self._default_window_weight = float(default_window_weight)
        # EWMA of per-request tokens/cost, fed by note_usage. Converts the
        # human *request* rate into token/$ rates for projections and pacing
        # without the limiter depending on the metrics module. None until the
        # first completion; the assumed_* config fallbacks apply until then.
        self._ewma_tokens_per_req: float | None = None
        self._ewma_cost_per_req: float | None = None
        self._assumed_tokens_per_req = 20000.0
        self._assumed_cost_per_req = 0.05
        self._n_requests = 0
        self._n_rate_limited = 0
        self._n_other_errors = 0
        self._n_concurrency_waits = 0
        self._n_promotions = 0
        self._n_demotions = 0
        self._n_probes_sent = 0

    @property
    def active(self) -> Tier:
        """The currently active tier (read-only; switches happen internally)."""
        return self._active

    @property
    def forced(self) -> str | None:
        """The pinned tier name when force_tier is set, else None."""
        return self._forced

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

    def _seed_requests_window(self, w: BudgetWindow, now: float) -> None:
        """Anchor a fresh requests window carrying the in-flight tally forward.

        Requests still in flight keep consuming quota under whatever window is
        current, so a restart/roll must not erase them from the indicator. Only
        requests-metric windows have a known in-flight weight; token/cost usage
        is unknown until completion and simply lands in whichever window is
        current then.
        """
        if self._inflight_weight > 0:
            w.start = now
            w.count = self._inflight_weight
            w.human = self._inflight_weight_human
            w.auto = self._inflight_weight_auto
        else:
            w.start = None
            w.count = w.human = w.auto = 0.0

    def _roll_if_elapsed(self, w: BudgetWindow, now: float) -> None:
        """Re-anchor `w` at `now` if it is dormant or its window has elapsed.

        Fresh requests windows carry the in-flight weight forward (those
        requests spill into the new window and keep consuming its quota);
        token/cost windows start from zero.
        """
        if w.start is None or now - w.start >= w.budget.window_seconds:
            w.start = now
            if w.budget.metric == "requests":
                w.count = self._inflight_weight
                w.human = self._inflight_weight_human
                w.auto = self._inflight_weight_auto
            else:
                w.count = w.human = w.auto = 0.0

    def _restart_window(self) -> None:
        """Restart every rolling quota window for the (new) active tier.

        Called on every tier change (promotion, demotion, boost, or a forced
        switch) so the timers reset and the windows re-anchor under the new
        tier's budgets. Requests still in flight are carried forward into the
        fresh requests windows (they keep consuming quota under the new tier,
        so they must stay counted); token/cost windows start empty — their
        usage lands at completion in whichever window is current then. When
        nothing is in flight all windows are left dormant for the next request
        to anchor. Synchronous and await-free; callers already hold
        `self._cond`.
        """
        now = time.time()
        self._windows = [BudgetWindow(b) for b in self._active.budgets]
        for w in self._windows:
            if w.budget.metric == "requests":
                self._seed_requests_window(w, now)
        # Every path that lands on HIGH means a fresh (large) quota window —
        # requests parked in a 429 backoff are waiting on a limit that just
        # reset, so release them to retry now. Callers set self._active before
        # calling here. Demotions to LOW don't wake: a demotion means upstream
        # just rate-limited us, so sleepers should keep backing off.
        if self._active is self._high:
            ev = self._rl_wake
            self._rl_wake = asyncio.Event()
            ev.set()

    def _reconcile_windows(self) -> None:
        """Adopt a changed budget list for the SAME active tier (config reload).

        Windows whose (metric, window_seconds) still exist keep their count and
        start (only the limit is picked up live); budgets that disappeared are
        dropped; new requests budgets are seeded from the in-flight tally like
        a restart, other new budgets start dormant. Synchronous and await-free;
        callers hold `self._cond`.
        """
        old = list(self._windows)
        now = time.time()
        fresh: list[BudgetWindow] = []
        for b in self._active.budgets:
            match = next((w for w in old if w.budget.metric == b.metric
                          and w.budget.window_seconds == b.window_seconds), None)
            if match is not None:
                old.remove(match)
                match.budget = b  # pick up a changed limit live
                fresh.append(match)
            else:
                w = BudgetWindow(b)
                if b.metric == "requests":
                    self._seed_requests_window(w, now)
                fresh.append(w)
        self._windows = fresh

    def note_request(self, model: str = "", lane: str = "human") -> tuple[float, Any]:
        """Count one client request against the rolling quota windows.

        Called once per client request (not per retry). The request counts as
        `_window_weight_for(model)` units toward every requests-metric window
        (a per-model factor); this affects only the window indicators, never
        the per-request metrics or per-model stats. Token/cost windows are
        anchored here too (the window starts when the first request is sent)
        but their counts only grow at completion via `note_usage`. The budgets
        come from the active tier, so they follow tier changes. Each window
        independently re-anchors when dormant or elapsed. Synchronous and
        await-free, so it's atomic under the single-threaded event loop.
        Returns `(weight, window_token)`; pass both back to `discount_request`
        to undo the count if the request ultimately fails (the token records
        each window's start so a since-rolled window is never wrongly
        decremented).
        """
        now = time.time()
        weight = self._window_weight_for(model)
        token: list[tuple[int, float]] = []
        for i, w in enumerate(self._windows):
            # Fresh windows carry forward requests already in flight; this
            # request's own weight is added below, after it joins the
            # in-flight set.
            self._roll_if_elapsed(w, now)
            if w.budget.metric != "requests":
                continue
            w.count += weight
            if lane == "auto":
                w.auto += weight
            else:
                w.human += weight
            token.append((i, w.start))
        self._inflight_weight += weight
        if lane == "auto":
            self._inflight_weight_auto += weight
        else:
            self._inflight_weight_human += weight
        return weight, token

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

    def discount_request(self, weight: float, window_token: Any,
                         lane: str = "human") -> None:
        """Reverse a previously-noted request that never consumed quota.

        Upstream rate-limit quota only counts requests that actually went
        through; a request that ultimately failed (rate-limited out, connection
        error, client abort) should not stay on the window counts. The token
        from `note_request` records each requests-window's start at note time;
        entries whose window has since rolled or been rebuilt (start mismatch)
        are skipped so a fresh window is never wrongly reduced. Only requests
        windows are involved — token/cost quota is only ever added for
        successful requests, so there is nothing to reverse. Synchronous and
        await-free like `note_request`.
        """
        if not window_token:
            return
        weight = max(0.0, float(weight))
        for i, start in window_token:
            if not (0 <= i < len(self._windows)):
                continue
            w = self._windows[i]
            if w.budget.metric != "requests" or w.start != start:
                continue
            w.count = max(0.0, w.count - weight)
            if lane == "auto":
                w.auto = max(0.0, w.auto - weight)
            else:
                w.human = max(0.0, w.human - weight)

    def note_usage(self, tokens: float, cost: float, lane: str = "human") -> None:
        """Fold one successful request's usage into the token/cost windows.

        Called once per successful request at completion (usage is unknown
        until then); failed requests report no usage and consume no token/cost
        quota, so there is no discount path. `tokens` is all four usage fields
        summed; `cost` is the priced USD (0 for unpriced models, so cost
        windows only fill when `model_pricing` is configured). A request that
        straddles a window roll or tier switch lands its usage in whichever
        window is current now — accepted, no attribution to the start window.
        Also updates the per-request token/cost EWMAs used for projections and
        auto pacing. Synchronous and await-free like `note_request`.
        """
        tokens = max(0.0, float(tokens))
        cost = max(0.0, float(cost))
        self._ewma_tokens_per_req = tokens if self._ewma_tokens_per_req is None \
            else 0.2 * tokens + 0.8 * self._ewma_tokens_per_req
        self._ewma_cost_per_req = cost if self._ewma_cost_per_req is None \
            else 0.2 * cost + 0.8 * self._ewma_cost_per_req
        now = time.time()
        for w in self._windows:
            units = tokens if w.budget.metric == "tokens" \
                else cost if w.budget.metric == "cost" else 0.0
            if units <= 0:
                continue
            self._roll_if_elapsed(w, now)
            w.count += units
            if lane == "auto":
                w.auto += units
            else:
                w.human += units

    def units_per_request(self, metric: str) -> float:
        """Estimated units of `metric` one request consumes (1.0 for requests).

        The token/cost estimates come from the note_usage EWMAs, falling back
        to the configured assumed values before any completion has been
        measured. Used to convert the human request rate into per-metric rates
        for projections and by the pacer to convert budget leftovers back into
        request rates.
        """
        if metric == "tokens":
            v = self._ewma_tokens_per_req
            return v if v is not None else self._assumed_tokens_per_req
        if metric == "cost":
            v = self._ewma_cost_per_req
            return v if v is not None else self._assumed_cost_per_req
        return 1.0

    def set_assumed_units(self, tokens_per_request: float, cost_per_request: float) -> None:
        self._assumed_tokens_per_req = max(0.0, float(tokens_per_request))
        self._assumed_cost_per_req = max(0.0, float(cost_per_request))

    def _window_snapshot(self) -> dict[str, Any]:
        now = time.time()

        def _n(x: float, nd: int = 2) -> float:
            return int(x) if float(x).is_integer() else round(x, nd)

        # A pending LOW->HIGH switch ends every window early: at the switch the
        # windows restart under HIGH, so everything (the pacer's drain rate, the
        # human/background projections, the dashboard countdowns) should treat
        # the switch time as the effective window end. Only relevant while LOW —
        # a HIGH->LOW switch doesn't shorten anything to drain.
        # `effective_remaining_seconds` is what every consumer should use;
        # `remaining_seconds` stays the true window remaining for reference.
        switch_at = self.scheduled_switch_at() if self._active is self._low else None
        if switch_at is not None and switch_at - now <= 0:
            switch_at = None  # already due; the switch itself will roll the windows

        human_rate = self.human_rate()
        windows: list[dict[str, Any]] = []
        binding = 0
        binding_util = -1.0
        for w in self._windows:
            b = w.budget
            nd = 4 if b.metric == "cost" else 2
            active = w.start is not None and now - w.start < b.window_seconds
            if not active:
                windows.append({
                    "active": False,
                    "metric": b.metric,
                    "limit": _n(b.limit, nd),
                    "window_seconds": b.window_seconds,
                    "count": 0, "count_human": 0, "count_auto": 0,
                    "utilization": 0.0,
                    "projected_human": 0,
                    "started_at": None,
                    "elapsed_seconds": None,
                    "remaining_seconds": None,
                    "effective_remaining_seconds": None,
                    "switch_at": None,
                })
                continue
            elapsed = now - w.start
            remaining = max(0.0, b.window_seconds - elapsed)
            effective_remaining = remaining
            if switch_at is not None:
                effective_remaining = min(effective_remaining, switch_at - now)
            # Estimated human total by window end: what humans have spent so far
            # plus their measured arrival rate (converted to this budget's units)
            # projected over the (effective) time left — raw, no safety factor;
            # this is a display estimate, not the pacer's reservation. The
            # background (auto) projection is added by AutoPacer.status() since
            # it owns the leftover-budget calculation.
            projected_human = w.human + human_rate * self.units_per_request(b.metric) * effective_remaining
            utilization = w.count / b.limit if b.limit > 0 else 0.0
            if utilization > binding_util:
                binding_util = utilization
                binding = len(windows)
            windows.append({
                "active": True,
                "metric": b.metric,
                "limit": _n(b.limit, nd),
                "window_seconds": b.window_seconds,
                "count": _n(w.count, nd),
                "count_human": _n(w.human, nd),
                "count_auto": _n(w.auto, nd),
                "utilization": round(utilization, 3),
                "projected_human": _n(min(projected_human, b.limit), nd),
                "started_at": w.start,
                "elapsed_seconds": elapsed,
                "remaining_seconds": remaining,
                "effective_remaining_seconds": effective_remaining,
                "switch_at": switch_at,
            })
        if not windows:  # defensive: a Tier always has at least one budget
            windows = [{
                "active": False, "metric": "requests", "limit": 0,
                "window_seconds": 0.0, "count": 0, "count_human": 0,
                "count_auto": 0, "utilization": 0.0, "projected_human": 0,
                "started_at": None, "elapsed_seconds": None,
                "remaining_seconds": None, "effective_remaining_seconds": None,
                "switch_at": None,
            }]
        # Top-level fields mirror the BINDING window (highest utilization among
        # active ones; index 0 when none is active) so the statusline and any
        # single-window consumer keep working unchanged.
        mirror = windows[binding]
        return {
            **{k: v for k, v in mirror.items()},
            "tier": self._active.name,
            "windows": windows,
            "binding": binding,
        }

    # -- manual window overrides + persistence (sync, await-free: atomic under
    #    the single-threaded loop, like note_request) --

    def _select_windows(self, metric: str | None,
                        window_seconds: float | None) -> list[BudgetWindow]:
        """Windows matching the optional (metric, window_seconds) selectors."""
        out = []
        for w in self._windows:
            if metric is not None and w.budget.metric != metric:
                continue
            if window_seconds is not None and w.budget.window_seconds != float(window_seconds):
                continue
            out.append(w)
        return out

    def set_window_count(self, count: float, metric: str | None = None,
                         window_seconds: float | None = None) -> dict[str, Any] | None:
        """Force one rolling window's current count.

        Selectors pick the window: `metric` defaults to "requests" and
        `window_seconds` to the first match, so legacy calls without selectors
        keep targeting the request window. If the selected window is not
        currently active (never started, or elapsed) a fresh one is anchored at
        now so the count is visible. Returns the new window snapshot, or None
        when no window matches the selectors.
        """
        matches = self._select_windows(metric or "requests", window_seconds)
        if not matches:
            return None
        w = matches[0]
        now = time.time()
        if w.start is None or now - w.start >= w.budget.window_seconds:
            w.start = now
        w.count = max(0.0, float(count))
        # Keep the lane split consistent: preserve the auto attribution (clamped
        # to the new total) and assign the remainder to humans.
        w.auto = min(w.auto, w.count)
        w.human = w.count - w.auto
        return self._window_snapshot()

    def set_window_start(self, started_at: float | None, metric: str | None = None,
                         window_seconds: float | None = None) -> dict[str, Any] | None:
        """Force rolling-window start timestamps (unix seconds).

        `None` clears everything via a full window restart, so the next request
        re-anchors fresh windows. A non-None value is applied to every window
        matching the optional selectors (all windows when none are given).
        Returns the new window snapshot, or None when selectors match nothing.
        """
        if started_at is None:
            self._restart_window()
            return self._window_snapshot()
        matches = self._select_windows(metric, window_seconds)
        if not matches:
            return None
        for w in matches:
            w.start = float(started_at)
        return self._window_snapshot()

    def window_state(self) -> dict[str, Any]:
        """Serializable window state for persistence across restarts.

        `started_at` (the earliest anchored start, or None) is kept at the top
        level so `save_window_file` can cheaply tell whether there is anything
        worth persisting.
        """
        entries = [{
            "metric": w.budget.metric,
            "window_seconds": w.budget.window_seconds,
            "started_at": w.start,
            "count": w.count,
            "count_human": w.human,
            "count_auto": w.auto,
        } for w in self._windows if w.start is not None]
        starts = [e["started_at"] for e in entries]
        return {
            "version": 2,
            "tier": self._active.name,
            "started_at": min(starts) if starts else None,
            "windows": entries,
        }

    def _load_window_entry(self, entry: dict[str, Any],
                           taken: set[int]) -> bool:
        """Restore one persisted window entry into a matching live window."""
        try:
            start = float(entry["started_at"])
            metric = str(entry.get("metric", "requests"))
            window_seconds = float(entry.get("window_seconds", 0) or 0)
            count = max(0.0, float(entry.get("count", 0) or 0))
            auto = max(0.0, float(entry.get("count_auto", 0) or 0))
        except (KeyError, TypeError, ValueError):
            return False
        if time.time() - start >= window_seconds:
            return False  # already elapsed
        for i, w in enumerate(self._windows):
            if i in taken:
                continue
            if w.budget.metric != metric or w.budget.window_seconds != window_seconds:
                continue
            taken.add(i)
            w.start = start
            w.count = count
            # The human share is derived from count - auto rather than read from
            # the saved count_human (which is still written, for inspectability),
            # so the lane-split invariant human + auto == count holds even if the
            # file was hand-edited into an inconsistent state.
            w.auto = min(auto, count)
            w.human = count - w.auto
            log.info(f"window: restored {w.budget} count={count} "
                     f"(human={w.human} auto={w.auto}) started_at={start}")
            return True
        return False

    def load_window_state(self, state: dict[str, Any] | None) -> bool:
        """Restore persisted windows, discarding any that already elapsed.

        v2 files carry one entry per budget window, matched to the current
        active tier's budgets by (metric, window_seconds) — entries whose
        budget no longer exists or whose window has elapsed are dropped. A
        legacy (pre-budgets) file restores its single request window into the
        first requests budget, using the saved duration for the elapsed check.
        Returns True if anything was restored.
        """
        if not isinstance(state, dict):
            return False
        entries = state.get("windows")
        if isinstance(entries, list):  # v2 format
            taken: set[int] = set()
            restored = False
            for entry in entries:
                if isinstance(entry, dict) and entry.get("started_at") is not None:
                    restored |= self._load_window_entry(entry, taken)
            if not restored:
                log.info("window: persisted state elapsed or unmatched; discarded")
            return restored
        # Legacy single-window format: {started_at, count, count_auto, window_seconds}
        start = state.get("started_at")
        if start is None:
            return False
        try:
            start = float(start)
            count = max(0.0, float(state.get("count", 0) or 0))
            window_seconds = float(state.get("window_seconds") or 0)
            auto = max(0.0, float(state.get("count_auto", 0) or 0))
        except (TypeError, ValueError):
            return False
        target = next((w for w in self._windows if w.budget.metric == "requests"), None)
        if target is None:
            return False
        if not window_seconds:
            window_seconds = target.budget.window_seconds
        if time.time() - start >= window_seconds:
            log.info("window: persisted state already elapsed; discarded")
            return False
        target.start = start
        target.count = count
        target.auto = min(auto, count)
        target.human = count - target.auto
        log.info(f"window: restored count={count} "
                 f"(human={target.human} auto={target.auto}) started_at={start}")
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

    async def rl_backoff_sleep(self, backoff: float) -> bool:
        """Sleep out a 429/503/529 retry backoff, ending early on a LOW->HIGH
        tier switch.

        The backoff usually honors the upstream Retry-After, which points at the
        *current* quota window's reset — if the tier is promoted to HIGH mid-sleep
        (probe, boost, or scheduled switch), that Retry-After is stale and the
        request should retry against the fresh window immediately instead of
        sleeping out the rest of it. Returns True when woken early.
        """
        ev = self._rl_wake
        try:
            await asyncio.wait_for(ev.wait(), timeout=max(0.0, backoff))
            return True
        except asyncio.TimeoutError:
            return False

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
                        f"budgets={self._high.budgets_str()})"
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
                    f"budgets={self._low.budgets_str()})"
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
                    f"budgets={self._high.budgets_str()})"
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
            self._switch_daily_tod = local_tod_seconds(float(ts))
            self._switch_daily_next = next_time_of_day(self._switch_daily_tod, time.time())
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
            self._switch_low_daily_tod = local_tod_seconds(float(ts))
            self._switch_low_daily_next = next_time_of_day(self._switch_low_daily_tod, time.time())
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
                self._switch_daily_next = next_time_of_day(self._switch_daily_tod, now)
            if self._switch_low_once_at is not None and now >= self._switch_low_once_at:
                low_at = self._switch_low_once_at
                self._switch_low_once_at = None
            if self._switch_low_daily_next is not None and now >= self._switch_low_daily_next:
                low_at = self._switch_low_daily_next if low_at is None else max(low_at, self._switch_low_daily_next)
                self._switch_low_daily_next = next_time_of_day(self._switch_low_daily_tod, now)
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
                    f"budgets={target.budgets_str()})"
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
            # A config change that forces a different tier restarts the windows
            # under the new tier's budgets, same as an auto switch. When the
            # tier is unchanged the budget list may still have changed, so the
            # live windows are reconciled against it: matching (metric,
            # window_seconds) budgets keep their count/start and pick up a
            # changed limit; added/removed budgets are created/dropped.
            if self._active.name != old_active_name:
                self._restart_window()
            else:
                self._reconcile_windows()
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
