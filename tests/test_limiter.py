"""Limiter: per-tier windows, tier-change restart, lane priority, persistence."""
import asyncio
import time

from anthropic_proxy.limiter import Tier, Limiter


def _lim(forced=None, reserve=0):
    low = Tier("low", 4, 18000, 600)
    high = Tier("high", 1000, 3600, 99999)
    lim = Limiter(low, high, "low", 300, forced)
    lim.set_auto_params(concurrency_reserve=reserve, human_horizon=3600)
    return lim


def test_per_tier_window_and_restart_on_switch():
    lim = _lim()
    lim.note_request("m")
    assert lim.window_snapshot()["limit"] == 600
    asyncio.run(lim.boost_high())                      # LOW -> HIGH restarts window
    snap = lim.window_snapshot()
    assert snap["limit"] == 99999 and snap["count"] == 0 and not snap["active"]
    lim.note_request("m")
    assert lim.window_snapshot()["count"] == 1


def test_set_window_count_and_start():
    lim = _lim()
    assert lim.set_window_count(120)["count"] == 120
    snap = lim.set_window_start(time.time() - 100)
    assert abs(snap["elapsed_seconds"] - 100) < 2


def test_reserve_caps_auto_and_human_fits():
    async def go():
        lim = _lim(reserve=1)
        for _ in range(3):
            await lim.acquire("auto")
        assert lim.snapshot()["lanes"]["auto"]["in_flight"] == 3
        blocked = asyncio.create_task(lim.acquire("auto"))   # 4th auto > cap(3)
        await asyncio.sleep(0.05)
        assert not blocked.done()
        await asyncio.wait_for(lim.acquire("human"), 0.5)    # human takes reserved slot
        assert lim.snapshot()["in_flight"] == 4
        await lim.release_success(False, "auto")
        await asyncio.wait_for(blocked, 0.5)
    asyncio.run(go())


def test_human_priority_for_freed_slot():
    async def go():
        lim = _lim(forced="low")                # forced disables probing
        for _ in range(4):
            await lim.acquire("auto")
        hw = asyncio.create_task(lim.acquire("human"))
        aw = asyncio.create_task(lim.acquire("auto"))
        await asyncio.sleep(0.05)
        await lim.release_success(False, "auto")
        await asyncio.sleep(0.05)
        assert hw.done() and not aw.done()
    asyncio.run(go())


def test_queued_human_probes_under_saturation():
    async def go():
        lim = _lim()
        for _ in range(4):
            await lim.acquire("auto")
        was_probe = await asyncio.wait_for(lim.acquire("human"), 0.5)
        assert was_probe is True
    asyncio.run(go())


def test_human_rate_amortizes_bursts():
    lim = _lim()
    now = time.monotonic()
    lim._started_at = now - 2400
    lim._human_times.extend(now - (4 - i) * 600 for i in range(5))  # 5 over 40min
    # Averaged over uptime/horizon, not the tight span -> well under 1/sec.
    assert 0 < lim.human_rate() < 0.01


def test_window_persistence_roundtrip_and_discard():
    lim = _lim()
    lim.note_request("m")
    state = lim.window_state()
    assert _lim().load_window_state(state) is True
    elapsed = {"started_at": time.time() - 4000, "count": 9, "window_seconds": 3600}
    assert _lim().load_window_state(elapsed) is False
    assert _lim().load_window_state(None) is False
