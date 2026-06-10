import asyncio
import time

from anthropic_proxy.limiter import Limiter, Tier, local_tod_seconds, next_time_of_day


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
