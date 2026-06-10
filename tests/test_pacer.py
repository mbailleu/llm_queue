import asyncio
import time

from anthropic_proxy.limiter import Limiter, Tier
from anthropic_proxy.metrics import Metrics
from anthropic_proxy.pacer import AutoPacer


def make_setup(window_seconds=100.0, window_limit=10, human_events=0,
               cfg_overrides=None):
    lim = Limiter(Tier("low", 4, window_seconds, window_limit),
                  Tier("high", 100, 50, 1000), "low", 300.0, None)
    for _ in range(human_events):
        lim._note_human()
    met = Metrics()
    cfg = {
        "auto_pacing_enabled": True,
        "human_demand_safety": 1.0,
        "human_quota_floor": 0,
        "auto_assumed_request_seconds": 1.0,
        "auto_poll_seconds": 0.05,
        "human_demand_lookahead_seconds": 3600,
        "human_demand_horizon_seconds": 3600,
    }
    cfg.update(cfg_overrides or {})
    return lim, AutoPacer(lim, met, cfg)


def test_no_window_lets_first_request_through():
    lim, pacer = make_setup()
    usable, rate = pacer._usable_and_rate()
    assert usable == 1.0 and rate == float("inf")


def test_leftover_budget_and_even_spread_rate():
    lim, pacer = make_setup(window_seconds=100, window_limit=10)
    lim.note_request("m", "human")     # used = 1, no measured human rate decay
    usable, rate = pacer._usable_and_rate()
    # usable = 10 - 1 - safety*human_rate*horizon (human_rate small but nonzero
    # because note_request went through acquire? no: note_request doesn't note
    # humans — only acquire() does), so usable = 9 spread over <=100s.
    assert 8.5 < usable <= 9.0
    assert rate >= usable / 100.0


def test_humans_already_spent_window_parks_auto():
    lim, pacer = make_setup(window_seconds=100, window_limit=2)
    lim.note_request("m", "human")
    lim.note_request("m", "human")
    usable, rate = pacer._usable_and_rate()
    assert usable <= 0 and rate == 0.0
    assert pacer.status()["reason"] == "reserved"


def test_rate_capped_by_capacity():
    lim, pacer = make_setup(window_seconds=1.0, window_limit=100000,
                            cfg_overrides={"auto_assumed_request_seconds": 1.0})
    lim.note_request("m", "human")
    _, rate = pacer._usable_and_rate()
    # capacity = max_concurrent(4) / avg(1s) = 4/s, far below usable/remaining.
    assert rate <= 4.0 + 1e-9


def test_human_reservation_shrinks_usable():
    lim, pacer = make_setup(window_seconds=3600, window_limit=100,
                            human_events=60)   # measured human traffic
    lim.note_request("m", "human")
    usable, _ = pacer._usable_and_rate()
    no_humans_usable = 100 - 1
    assert usable < no_humans_usable           # something is reserved


def test_gate_disabled_returns_immediately():
    lim, pacer = make_setup(cfg_overrides={"auto_pacing_enabled": False})

    async def run():
        await asyncio.wait_for(pacer.gate(), timeout=0.1)
    asyncio.run(run())
    assert pacer.status()["enabled"] is False


def test_gate_open_when_no_window():
    lim, pacer = make_setup()

    async def run():
        await asyncio.wait_for(pacer.gate(), timeout=0.5)
    asyncio.run(run())


def test_gate_releases_parked_request_when_budget_appears():
    lim, pacer = make_setup(window_seconds=100, window_limit=1)
    lim.note_request("m", "human")     # budget fully spent -> auto parks

    async def run():
        task = asyncio.create_task(pacer.gate())
        await asyncio.sleep(0.1)
        assert not task.done()
        assert pacer.status()["parked"] == 1
        lim.set_window_count(0)        # budget freed
        await asyncio.wait_for(task, timeout=2.0)
        assert pacer.status()["parked"] == 0
    asyncio.run(run())


def test_status_projected_auto_includes_remaining_budget():
    lim, pacer = make_setup(window_seconds=100, window_limit=10)
    lim.note_request("m", "auto")
    st = pacer.status()
    assert st["count_auto"] == 1
    # projected = spent (1) + leftover usable (~9)
    assert 9 <= st["projected_auto"] <= 10
