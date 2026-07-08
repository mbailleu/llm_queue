import asyncio
import time

from anthropic_proxy.limiter import (
    Budget,
    Limiter,
    Tier,
    local_tod_seconds,
    next_time_of_day,
)


def make_limiter(**kw) -> Limiter:
    args = dict(
        low=Tier("low", 2, window_seconds=100, window_limit=10),
        high=Tier("high", 100, window_seconds=50, window_limit=1000),
        initial_tier="low",
        promotion_cooldown=300.0,
        forced=None,
    )
    args.update(kw)
    return Limiter(**args)


def make_budget_limiter(**kw) -> Limiter:
    """A LOW tier with one budget of each metric (differing window lengths)."""
    low = Tier("low", 2, budgets=[
        Budget("requests", 10, 100),
        Budget("tokens", 1000, 60),
        Budget("cost", 5.0, 300),
    ])
    args = dict(
        low=low,
        high=Tier("high", 100, window_seconds=50, window_limit=1000),
        initial_tier="low",
        promotion_cooldown=300.0,
        forced=None,
    )
    args.update(kw)
    return Limiter(**args)


def _win(snap, metric, window_seconds=None):
    for w in snap["windows"]:
        if w["metric"] == metric and (window_seconds is None
                                      or w["window_seconds"] == window_seconds):
            return w
    raise AssertionError(f"no {metric} window in snapshot")


# ---- rolling window accounting ----

def test_note_request_counts_lanes_and_invariant():
    lim = make_limiter(window_weights={"opus": 4.0}, default_window_weight=1.0)
    lim.note_request("claude-opus-4", "human")     # substring match -> 4
    lim.note_request("claude-haiku", "auto")       # default -> 1
    snap = lim.window_snapshot()
    assert snap["count"] == 5
    assert snap["count_human"] == 4
    assert snap["count_auto"] == 1
    assert snap["count_human"] + snap["count_auto"] == snap["count"]


def test_window_weight_exact_beats_substring():
    lim = make_limiter(window_weights={"claude-opus-4": 2.0, "opus": 4.0})
    w, _ = lim.note_request("claude-opus-4")
    assert w == 2.0


def test_discount_reverses_count_but_not_after_roll():
    lim = make_limiter()
    w, token = lim.note_request("m", "auto")
    assert lim.window_snapshot()["count"] == 1
    lim.discount_request(w, token, "auto")
    snap = lim.window_snapshot()
    assert snap["count"] == 0 and snap["count_auto"] == 0
    # Re-anchor a new window: an old token must not decrement it.
    w2, token2 = lim.note_request("m")
    lim.set_window_start(time.time())  # different start => old tokens mismatch
    lim.discount_request(w2, token2)
    assert lim.window_snapshot()["count"] == 1


def test_restart_window_carries_inflight_forward():
    lim = make_limiter()
    w, _ = lim.note_request("m", "auto")          # still in flight
    lim.set_window_start(None)                     # forces _restart_window()
    snap = lim.window_snapshot()
    assert snap["active"] and snap["count"] == 1 and snap["count_auto"] == 1
    lim.note_done(w, "auto")
    lim.set_window_start(None)                     # nothing in flight now
    assert lim.window_snapshot()["active"] is False


def test_set_window_count_keeps_lane_split_consistent():
    lim = make_limiter()
    lim.note_request("m", "auto")
    lim.note_request("m", "auto")
    snap = lim.set_window_count(1)                 # below current auto count
    assert snap["count"] == 1
    assert snap["count_auto"] == 1 and snap["count_human"] == 0


# ---- multi-budget windows (requests / tokens / cost) ----

def test_legacy_tier_compiles_to_one_requests_budget():
    t = Tier("low", 4, window_seconds=100, window_limit=10)
    assert len(t.budgets) == 1
    b = t.budgets[0]
    assert b.metric == "requests" and b.limit == 10 and b.window_seconds == 100


def test_note_request_feeds_only_requests_windows_but_anchors_all():
    lim = make_budget_limiter()
    lim.note_request("m", "human")
    snap = lim.window_snapshot()
    req = _win(snap, "requests")
    tok = _win(snap, "tokens")
    cost = _win(snap, "cost")
    assert req["count"] == 1
    # token/cost windows are anchored (clock running) but empty until usage.
    assert tok["active"] and tok["count"] == 0
    assert cost["active"] and cost["count"] == 0


def test_note_usage_feeds_token_and_cost_windows():
    lim = make_budget_limiter()
    w, _ = lim.note_request("m", "auto")
    lim.note_usage(400, 1.25, "auto")
    lim.note_done(w, "auto")
    snap = lim.window_snapshot()
    assert _win(snap, "requests")["count"] == 1
    tok = _win(snap, "tokens")
    assert tok["count"] == 400 and tok["count_auto"] == 400
    cost = _win(snap, "cost")
    assert cost["count"] == 1.25 and cost["count_auto"] == 1.25
    # lane invariant holds on every window
    for wnd in snap["windows"]:
        assert wnd["count_human"] + wnd["count_auto"] == wnd["count"]


def test_binding_is_most_utilized_window():
    lim = make_budget_limiter()
    lim.note_request("m", "human")          # requests: 1/10 = 0.1
    lim.note_usage(900, 0.5, "human")       # tokens: 900/1000 = 0.9, cost: 0.5/5 = 0.1
    snap = lim.window_snapshot()
    assert snap["windows"][snap["binding"]]["metric"] == "tokens"
    # top-level mirror follows the binding window
    assert snap["metric"] == "tokens" and snap["count"] == 900 and snap["limit"] == 1000


def test_windows_roll_independently():
    lim = make_budget_limiter()
    w, _ = lim.note_request("m", "human")
    lim.note_done(w, "human")
    lim.note_usage(100, 1.0, "human")
    # Expire only the 60s token window; requests (100s) and cost (300s) live on.
    lim.set_window_start(time.time() - 61, metric="tokens")
    lim.note_usage(50, 0.5, "human")        # re-anchors tokens fresh; adds to cost
    snap = lim.window_snapshot()
    assert _win(snap, "tokens")["count"] == 50       # fresh window, old 100 gone
    assert _win(snap, "cost")["count"] == 1.5        # kept accumulating
    assert _win(snap, "requests")["count"] == 1


def test_discount_only_reverses_unrolled_requests_windows():
    low = Tier("low", 2, budgets=[Budget("requests", 10, 100),
                                  Budget("requests", 100, 50)])
    lim = make_limiter(low=low)
    w, token = lim.note_request("m", "auto")
    # Roll only the second (50s) requests window before the discount.
    lim.set_window_start(time.time() - 51, metric="requests", window_seconds=50)
    lim.note_request("m2", "human")          # re-anchors the rolled window
    lim.discount_request(w, token, "auto")   # token mismatch on the rolled one
    snap = lim.window_snapshot()
    assert _win(snap, "requests", 100)["count"] == 1   # 2 noted - 1 discounted
    # The rolled window re-anchored carrying both in-flight requests forward
    # (1 carried + 1 newly noted); the stale token must not decrement it.
    assert _win(snap, "requests", 50)["count"] == 2


def test_tier_switch_resets_token_cost_but_carries_requests_inflight():
    async def run():
        lim = make_budget_limiter()
        w, _ = lim.note_request("m", "human")          # in flight
        lim.note_usage(500, 2.0, "human")
        assert await lim.boost_high()                  # tier switch -> restart
        snap = lim.window_snapshot()
        # HIGH tier (legacy single requests budget): in-flight carried forward.
        assert snap["windows"][0]["metric"] == "requests"
        assert snap["windows"][0]["count"] == 1
        lim.note_done(w, "human")
    asyncio.run(run())


def test_units_per_request_ewma_and_fallbacks():
    lim = make_budget_limiter()
    lim.set_assumed_units(1000, 0.02)
    assert lim.units_per_request("requests") == 1.0
    assert lim.units_per_request("tokens") == 1000   # assumed before data
    assert lim.units_per_request("cost") == 0.02
    lim.note_usage(400, 1.0)
    assert lim.units_per_request("tokens") == 400    # first sample seeds EWMA
    assert lim.units_per_request("cost") == 1.0
    lim.note_usage(800, 2.0)
    assert lim.units_per_request("tokens") == 0.2 * 800 + 0.8 * 400


def test_set_window_count_with_selectors():
    lim = make_budget_limiter()
    lim.note_request("m", "human")
    snap = lim.set_window_count(3.5, metric="cost")
    assert _win(snap, "cost")["count"] == 3.5
    assert _win(snap, "requests")["count"] == 1      # untouched
    assert lim.set_window_count(1, metric="cost", window_seconds=999) is None


def test_budget_window_state_roundtrip():
    lim = make_budget_limiter()
    w, _ = lim.note_request("m", "auto")
    lim.note_usage(200, 0.75, "auto")
    lim.note_done(w, "auto")
    state = lim.window_state()
    assert state["version"] == 2 and len(state["windows"]) == 3
    fresh = make_budget_limiter()
    assert fresh.load_window_state(state)
    snap = fresh.window_snapshot()
    assert _win(snap, "requests")["count"] == 1
    assert _win(snap, "tokens")["count"] == 200
    assert _win(snap, "cost")["count_auto"] == 0.75


def test_update_tiers_reconciles_same_tier_budgets():
    async def run():
        lim = make_budget_limiter()
        lim.note_request("m", "human")
        lim.note_usage(300, 1.0, "human")
        # Same tier name, changed budget list: tokens limit changes (state
        # kept), cost/300s replaced by cost/600s (state reset).
        new_low = Tier("low", 2, budgets=[
            Budget("requests", 10, 100),
            Budget("tokens", 2000, 60),
            Budget("cost", 5.0, 600),
        ])
        await lim.update_tiers(new_low, Tier("high", 100, 50, 1000), 300.0, None)
        snap = lim.window_snapshot()
        tok = _win(snap, "tokens")
        assert tok["limit"] == 2000 and tok["count"] == 300   # kept, new limit
        cost = _win(snap, "cost")
        assert cost["active"] is False and cost["count"] == 0  # replaced budget
    asyncio.run(run())


# ---- persistence round-trip ----

def test_window_state_roundtrip_and_invariant_repair():
    lim = make_limiter()
    lim.note_request("m", "auto")
    lim.note_request("m", "human")
    state = lim.window_state()
    fresh = make_limiter()
    assert fresh.load_window_state(state)
    snap = fresh.window_snapshot()
    assert snap["count"] == 2 and snap["count_auto"] == 1 and snap["count_human"] == 1
    # Inconsistent (hand-edited) file: human share is re-derived from count-auto.
    bad = make_limiter()
    assert bad.load_window_state({"started_at": time.time(), "count": 10,
                                  "count_human": 3, "count_auto": 4,
                                  "window_seconds": 100})
    snap = bad.window_snapshot()
    assert snap["count_human"] + snap["count_auto"] == snap["count"] == 10


def test_load_window_state_discards_elapsed():
    lim = make_limiter()
    assert not lim.load_window_state({"started_at": time.time() - 1000,
                                      "count": 5, "window_seconds": 100})
    assert not lim.load_window_state(None)
    assert not lim.load_window_state({"count": 5})  # no started_at


# ---- concurrency + lanes ----

def test_acquire_release_and_auto_reserve():
    async def run():
        lim = make_limiter(low=Tier("low", 2, 100, 10))
        lim.set_auto_params(concurrency_reserve=1, human_horizon=60)
        # Auto is capped at max_concurrent - reserve = 1.
        assert await lim.acquire("auto") is False
        blocked = asyncio.create_task(lim.acquire("auto"))
        await asyncio.sleep(0.01)
        assert not blocked.done()
        # A human still fits in the reserved slot.
        assert await lim.acquire("human") is False
        await lim.release_success(False, "human")
        await lim.release_success(False, "auto")
        await asyncio.sleep(0.01)
        assert blocked.done()                      # reserve freed -> auto admitted
        await lim.release_success(False, "auto")
        snap = lim.snapshot()
        assert snap["in_flight"] == 0 and snap["queued"] == 0
    asyncio.run(run())


def test_probe_promotes_on_success_and_demotes_on_rate_limit():
    async def run():
        lim = make_limiter(promotion_cooldown=0.0)
        await lim.acquire("human")
        await lim.acquire("human")                 # LOW saturated (max 2)
        was_probe = await lim.acquire("human")     # third human -> probe
        assert was_probe
        await lim.release_success(True, "human")   # successful probe -> HIGH
        assert lim.active.name == "high"
        await lim.release_rate_limited(False, "human")  # any 429 -> back to LOW
        assert lim.active.name == "low"
        await lim.release_success(False, "human")
        snap = lim.snapshot()
        assert snap["totals"]["promotions"] == 1
        assert snap["totals"]["demotions"] == 1
    asyncio.run(run())


def test_boost_high_refused_when_forced():
    async def run():
        lim = make_limiter(forced="low")
        assert not await lim.boost_high()
        lim2 = make_limiter()
        assert await lim2.boost_high()
        assert lim2.active.name == "high"
    asyncio.run(run())


# ---- scheduled switches ----

def test_scheduled_switch_effective_is_earlier_slot():
    lim = make_limiter()
    now = time.time()
    lim.set_oneshot_switch(now + 100)
    lim.set_daily_switch(now + 50)
    at = lim.scheduled_switch_at()
    # The daily slot re-arms to the next local occurrence; whichever of the two
    # resolved times is earlier must win.
    assert at is not None and at <= now + 100 + 1


def test_apply_scheduled_switch_fires_oneshot_once():
    async def run():
        lim = make_limiter()
        lim.set_oneshot_switch(time.time() - 1)    # already due
        assert await lim.apply_scheduled_switch()
        assert lim.active.name == "high"
        assert lim.scheduled_switch_at() is None   # one-shot cleared
        assert not await lim.apply_scheduled_switch()
    asyncio.run(run())


def test_apply_scheduled_low_switch():
    async def run():
        lim = make_limiter(initial_tier="high")
        lim.set_oneshot_low_switch(time.time() - 1)
        assert await lim.apply_scheduled_switch()
        assert lim.active.name == "low"
    asyncio.run(run())


def test_scheduled_switch_blocked_by_force():
    async def run():
        lim = make_limiter(forced="low")
        lim.set_oneshot_switch(time.time() - 1)
        assert not await lim.apply_scheduled_switch()
        assert lim.active.name == "low"
        assert lim.scheduled_switch_at() is not None  # left pending
    asyncio.run(run())


# ---- 429 backoff sleep (rl_backoff_sleep + the HIGH wake) ----

def test_rl_backoff_sleep_times_out_normally():
    async def run():
        lim = make_limiter()
        assert await lim.rl_backoff_sleep(0.01) is False
    asyncio.run(run())


def test_rl_backoff_sleep_woken_by_boost():
    async def run():
        lim = make_limiter()
        sleeper = asyncio.create_task(lim.rl_backoff_sleep(30.0))
        await asyncio.sleep(0.01)
        assert not sleeper.done()
        assert await lim.boost_high()              # LOW -> HIGH
        assert await asyncio.wait_for(sleeper, 1.0) is True
    asyncio.run(run())


def test_rl_backoff_sleep_woken_by_scheduled_switch():
    async def run():
        lim = make_limiter()
        sleeper = asyncio.create_task(lim.rl_backoff_sleep(30.0))
        await asyncio.sleep(0.01)
        lim.set_oneshot_switch(time.time() - 1)
        assert await lim.apply_scheduled_switch()
        assert await asyncio.wait_for(sleeper, 1.0) is True
    asyncio.run(run())


def test_rl_backoff_sleep_woken_by_probe_promotion():
    async def run():
        lim = make_limiter(promotion_cooldown=0.0)
        sleeper = asyncio.create_task(lim.rl_backoff_sleep(30.0))
        await asyncio.sleep(0.01)                  # let the sleeper grab the event
        await lim.acquire("human")
        await lim.acquire("human")                 # LOW saturated
        assert await lim.acquire("human")          # probe
        await lim.release_success(True, "human")   # promotion -> HIGH
        assert await asyncio.wait_for(sleeper, 1.0) is True
        await lim.release_success(False, "human")
        await lim.release_success(False, "human")
    asyncio.run(run())


def test_rl_backoff_sleep_not_woken_by_demotion():
    async def run():
        lim = make_limiter(initial_tier="high")
        sleeper = asyncio.create_task(lim.rl_backoff_sleep(0.2))
        await asyncio.sleep(0.01)
        await lim.acquire("human")
        await lim.release_rate_limited(False, "human")  # HIGH -> LOW
        assert lim.active.name == "low"
        assert await asyncio.wait_for(sleeper, 1.0) is False  # slept out fully
    asyncio.run(run())


# ---- time-of-day helpers ----

def test_next_time_of_day_today_or_tomorrow():
    now = time.time()
    tod_now = local_tod_seconds(now)
    nxt = next_time_of_day((tod_now + 3600) % 86400, now)
    assert now < nxt <= now + 86400
    past = next_time_of_day((tod_now - 3600) % 86400, now)
    assert now < past <= now + 86400


def test_window_snapshot_shortened_by_pending_switch():
    lim = make_limiter()
    lim.note_request("m")
    lim.set_oneshot_switch(time.time() + 10)       # well before the 100s window
    snap = lim.window_snapshot()
    assert snap["switch_at"] is not None
    assert snap["effective_remaining_seconds"] <= 10
    assert snap["remaining_seconds"] > 90
