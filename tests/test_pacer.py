"""AutoPacer: leftover-budget pacing, accelerate near window end, park/disable."""
import asyncio
import time

from anthropic_proxy.limiter import Tier, Limiter
from anthropic_proxy.metrics import Metrics
from anthropic_proxy.pacer import AutoPacer


def _setup():
    low = Tier("low", 4, 18000, 600)
    high = Tier("high", 1000, 3600, 99999)
    lim = Limiter(low, high, "low", 300, None)
    lim.set_auto_params(0, 3600)
    met = Metrics(86400)
    cfg = {"auto_pacing_enabled": True, "human_demand_safety": 1.5,
           "human_quota_floor": 0, "auto_assumed_request_seconds": 5,
           "auto_poll_seconds": 0.2}
    return lim, AutoPacer(lim, met, cfg)


def test_rate_rises_toward_window_end():
    lim, pac = _setup()
    now = time.monotonic()
    lim._started_at = now - 2400
    lim._human_times.extend(now - (4 - i) * 600 for i in range(5))  # modest human rate
    lim.note_request("m")
    usable_early, rate_early = pac._usable_and_rate()
    lim._window_start = time.time() - (18000 - 2)                  # almost over
    _, rate_end = pac._usable_and_rate()
    assert usable_early > 0 and rate_early > 0
    assert rate_end > rate_early


def test_parks_when_window_spent():
    lim, pac = _setup()
    lim.note_request("m")
    lim.set_window_count(600)                                       # window full
    assert pac._usable_and_rate()[0] <= 0

    async def go():
        g = asyncio.create_task(pac.gate())
        await asyncio.sleep(0.1)
        parked = not g.done()
        g.cancel()
        return parked
    assert asyncio.run(go())


def test_disabled_is_noop():
    lim = _setup()[0]
    met = Metrics(86400)
    pac = AutoPacer(lim, met, {"auto_pacing_enabled": False})
    asyncio.run(asyncio.wait_for(pac.gate(), 0.2))                  # returns immediately
