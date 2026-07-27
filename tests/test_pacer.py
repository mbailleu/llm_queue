import asyncio
import time

from anthropic_proxy.limiter import Budget, Limiter, Tier
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
        # Tests exercise the pacing math on short windows; the production
        # default exempts windows shorter than 300s from pacing entirely.
        "auto_pace_min_window_seconds": 0,
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


def test_short_window_skips_human_reservation():
    # Heavy measured human rate, but a 60s window is shorter than
    # human_reserve_min_window_seconds — no predicted-human term, only what
    # was actually used counts against auto.
    lim, pacer = make_setup(window_seconds=60, window_limit=20, human_events=600,
                            cfg_overrides={"human_reserve_min_window_seconds": 300})
    lim.note_request("m", "human")
    usable, _ = pacer._usable_and_rate()
    assert 18.5 < usable <= 19.0


def test_reserve_min_window_zero_keeps_reservation_on_short_windows():
    lim, pacer = make_setup(window_seconds=60, window_limit=20, human_events=600,
                            cfg_overrides={"human_reserve_min_window_seconds": 0})
    lim.note_request("m", "human")
    usable, _ = pacer._usable_and_rate()
    assert usable < 18.5                   # reservation applies again


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


# ---- multi-budget pacing (binding budget sets the pace) ----

def make_multi_setup(budgets, assumed=(100.0, 0.01), cfg_overrides=None):
    lim = Limiter(Tier("low", 4, budgets=budgets),
                  Tier("high", 100, 50, 1000), "low", 300.0, None)
    lim.set_assumed_units(*assumed)
    met = Metrics()
    cfg = {
        "auto_pacing_enabled": True,
        "human_demand_safety": 1.0,
        "human_quota_floor": 0,
        "auto_assumed_request_seconds": 1.0,
        "auto_poll_seconds": 0.05,
        "human_demand_lookahead_seconds": 3600,
        "human_demand_horizon_seconds": 3600,
        "auto_pace_min_window_seconds": 0,
    }
    cfg.update(cfg_overrides or {})
    return lim, AutoPacer(lim, met, cfg)


def test_token_budget_binds_when_most_constraining():
    # requests would allow ~10/s; tokens (1000 units at 100 tok/req over 100s)
    # only ~0.1/s — the token budget must set the pace.
    lim, pacer = make_multi_setup([Budget("requests", 1000, 100),
                                   Budget("tokens", 1000, 100)])
    lim.note_request("m", "human")
    usable, rate = pacer._usable_and_rate()
    assert 9.5 <= usable <= 10.0          # 1000 tokens / 100 tok-per-req
    assert rate <= 0.11
    st = pacer.status()
    assert st["binding_metric"] == "tokens"
    assert st["binding_window_seconds"] == 100


def test_spent_cost_budget_parks_auto():
    lim, pacer = make_multi_setup([Budget("requests", 100, 100),
                                   Budget("cost", 1.0, 100)])
    lim.note_request("m", "human")
    lim.note_usage(10, 1.0, "human")       # cost window fully spent
    usable, rate = pacer._usable_and_rate()
    assert usable <= 0 and rate == 0.0
    st = pacer.status()
    assert st["reason"] == "reserved"
    assert st["binding_metric"] == "cost"


def test_unpriced_cost_budget_never_binds():
    # cost units-per-request of 0 (no pricing anywhere) makes the cost budget
    # unconvertible AND unfillable — pacing must fall through to requests.
    lim, pacer = make_multi_setup([Budget("requests", 10, 100),
                                   Budget("cost", 5.0, 100)],
                                  assumed=(100.0, 0.0))
    lim.note_request("m", "human")
    usable, rate = pacer._usable_and_rate()
    assert 8.5 < usable <= 9.0             # requests leftover, as legacy
    assert pacer.status()["binding_metric"] == "requests"


def test_auto_conversion_uses_auto_lane_estimate():
    lim, pacer = make_multi_setup([Budget("tokens", 1000, 100)],
                                  cfg_overrides={"human_reserve_min_window_seconds": 0})
    lim.note_request("m", "human")     # anchors the window
    lim.note_usage(500, 0.0, "human")  # big interactive request
    lim.note_usage(10, 0.0, "auto")    # small scripted request
    usable, rate = pacer._usable_and_rate()
    # leftover = 1000 - 510 = 490 tokens, converted at the AUTO lane's own
    # estimate (10 tok/req) -> 49 requests; the blended EWMA (~108 tok/req)
    # would have allowed only ~4.5.
    assert 48 <= usable <= 50


def test_human_reservation_sized_by_human_lane_estimate():
    lim, pacer = make_multi_setup([Budget("tokens", 200000, 3600)],
                                  cfg_overrides={"human_reserve_min_window_seconds": 300})
    lim._started_at = time.monotonic() - 3600   # a full horizon of uptime
    for _ in range(36):                # human_rate = 36/3600 = 0.01 req/s
        lim._note_human()
    lim.note_request("m", "human")
    lim.note_usage(5000, 0.0, "human")   # human requests are 5000 tok each
    lim.note_usage(10, 0.0, "auto")      # auto requests are tiny
    usable, _ = pacer._usable_and_rate()
    # reservation = 0.01 * 5000 * 3600 = 180000 tokens (human-sized), so
    # usable_units ~ 200000 - 5010 - 180000 ~ 15000 -> ~1500 auto requests.
    # Sizing it with the auto estimate (10 tok/req) would have reserved only
    # 360 tokens and allowed ~19500.
    assert 1300 <= usable <= 1600


# ---- short-window pacing exemption (auto_pace_min_window_seconds) ----

def test_spent_short_window_neither_binds_nor_parks():
    # A fully spent per-minute window must not pace auto at all — throttling
    # there is the queue's + upstream 429 backoff's job. With no other budget
    # the gate is simply open.
    lim, pacer = make_setup(window_seconds=60, window_limit=2,
                            cfg_overrides={"auto_pace_min_window_seconds": 300})
    lim.note_request("m", "human")
    lim.note_request("m", "human")     # minute window fully spent
    usable, rate = pacer._usable_and_rate()
    assert rate == float("inf")
    assert pacer.status()["reason"] == "open"


def test_short_window_exempt_pacing_falls_to_long_budget():
    # Spent requests/min window + healthy 5h cost budget: the long budget sets
    # the pace and auto keeps flowing instead of parking for the minute.
    lim, pacer = make_multi_setup([Budget("requests", 2, 60),
                                   Budget("cost", 30.0, 18000)],
                                  assumed=(100.0, 0.01),
                                  cfg_overrides={"auto_pace_min_window_seconds": 300})
    lim.note_request("m", "human")
    lim.note_request("m", "human")     # requests/min fully spent
    usable, rate = pacer._usable_and_rate()
    assert usable > 0 and rate > 0
    st = pacer.status()
    assert st["reason"] == "paced"
    assert st["binding_metric"] == "cost"
    assert st["binding_window_seconds"] == 18000
    # The exempt window is still evaluated for the dashboard.
    assert st["windows"][0]["usable_units"] is not None


def test_pace_min_window_zero_restores_short_window_pacing():
    lim, pacer = make_setup(window_seconds=60, window_limit=2,
                            cfg_overrides={"auto_pace_min_window_seconds": 0})
    lim.note_request("m", "human")
    lim.note_request("m", "human")
    usable, rate = pacer._usable_and_rate()
    assert usable <= 0 and rate == 0.0     # old behavior: spent window parks


def test_cost_pacing_with_cheap_model_is_not_punitive():
    # Fictional model at $0.50/Mtok input, $1.60/Mtok output: a 20k-in/2k-out
    # request costs $0.0132, so a $30/5h cost budget leaves ~2200 requests —
    # the pace must come out in whole requests per minute, not a crawl.
    lim = Limiter(Tier("low", 4, budgets=[Budget("cost", 30.0, 18000)]),
                  Tier("high", 100, 50, 1000), "low", 300.0, None)
    met = Metrics(pricing={"fict": {"input": 0.50, "output": 1.60}})
    pacer = AutoPacer(lim, met, {
        "auto_pacing_enabled": True,
        "human_demand_safety": 1.5,
        "human_quota_floor": 0,
        "auto_assumed_request_seconds": 30.0,
        "auto_poll_seconds": 0.05,
    })
    cost = met.cost_of("fict", {"input_tokens": 20000, "output_tokens": 2000})
    assert abs(cost - 0.0132) < 1e-9
    lim.note_request("fict", "auto")       # anchors the window
    for _ in range(5):                     # auto cost EWMA converges to $0.0132
        lim.note_usage(22000, cost, "auto")
    usable, rate = pacer._usable_and_rate()
    # usable = (30 - 5*0.0132) / 0.0132 ~ 2268 requests over <=5h
    assert 2200 <= usable <= 2300
    assert 7.0 <= rate * 60 <= 8.0         # ~7.6 req/min even-spread
    st = pacer.status()
    assert st["reason"] == "paced"
    assert st["binding_metric"] == "cost"


def test_status_exposes_per_window_projections():
    lim, pacer = make_multi_setup([Budget("requests", 10, 100),
                                   Budget("tokens", 1000, 100)])
    lim.note_request("m", "auto")
    lim.note_usage(200, 0.0, "auto")       # tokens spent so far: 200
    st = pacer.status()
    assert len(st["windows"]) == 2
    tok = st["windows"][1]
    assert tok["metric"] == "tokens"
    # projected_auto (token units) = 200 spent + leftover usable units
    assert tok["projected_auto"] >= 200


# ---- explanation of the current rate ----

def test_explain_shows_the_binding_budget_arithmetic():
    lim, pacer = make_multi_setup([Budget("requests", 100, 600)])
    lim._started_at = time.monotonic() - 3600
    for _ in range(36):                    # human_rate = 0.01 req/s
        lim._note_human()
    lim.note_request("m", "human")
    text = "\n".join(pacer.explain())
    assert "requests/10min" in text
    assert "limit 100" in text and "spent 1" in text
    assert "Human reserve = 1 safety" in text
    assert "req/min" in text
    assert "Throughput cap:" in text


def test_explain_names_the_reserve_when_it_parks_auto():
    lim, pacer = make_multi_setup([Budget("requests", 10, 3600)])
    lim._started_at = time.monotonic() - 3600
    for _ in range(3600):                  # a human rate that eats the budget
        lim._note_human()
    lim.note_request("m", "human")
    st = pacer.status()
    assert st["reason"] == "reserved"
    text = "\n".join(st["explain"])
    assert "human reserve alone parks automation" in text
    assert "human_demand_safety" in text and "human_demand_lookahead_seconds" in text


def test_explain_marks_windows_that_cannot_pace():
    lim, pacer = make_multi_setup([Budget("requests", 2, 60),
                                   Budget("cost", 30.0, 18000)],
                                  cfg_overrides={"auto_pace_min_window_seconds": 300})
    lim.note_request("m", "human")
    st = pacer.status()
    short, long_ = st["windows"][0], st["windows"][1]
    assert short["paces"] is False and long_["paces"] is True
    assert any("Not pacing" in s for s in short["explain"])
    assert st["binding_index"] == 1        # the short window never binds
    assert "not paced" in "\n".join(st["explain"])


def test_explain_handles_idle_and_open_windows():
    lim, pacer = make_setup(window_seconds=600, window_limit=10)
    st = pacer.status()                    # nothing has anchored the window yet
    assert st["reason"] == "open"
    assert st["binding_index"] is None
    assert st["windows"][0]["paces"] is True     # long enough, just not started
    assert any("idle" in s for s in st["windows"][0]["explain"])
    assert "no budget can pace" in "\n".join(st["explain"]).lower()


def test_explain_reports_disabled_pacing():
    lim, pacer = make_setup(cfg_overrides={"auto_pacing_enabled": False})
    st = pacer.status()
    assert st["binding_index"] is None
    assert "disabled" in "\n".join(st["explain"]).lower()


def test_status_windows_flag_unconvertible_budgets():
    lim, pacer = make_multi_setup([Budget("requests", 10, 600),
                                   Budget("cost", 5.0, 600)],
                                  assumed=(100.0, 0.0))
    lim.note_request("m", "human")
    cost = pacer.status()["windows"][1]
    assert cost["usable_units"] is None and cost["binds"] is False
    assert any("neither fill nor" in s for s in cost["explain"])


def test_capacity_cap_uses_the_effective_concurrency():
    # The adaptive HIGH estimate, not the tier's configured max, is what the
    # throughput cap divides by.
    lim, pacer = make_multi_setup([Budget("requests", 100000, 600)])
    lim._active = lim._high
    lim.set_adaptive_params(enabled=True, min_concurrent=1, max_concurrent=10)
    lim.note_request("m", "human")
    _, rate = pacer._usable_and_rate()
    assert rate <= 10.0 / 1.0 + 1e-9       # 10 slots / 1s assumed avg


# ---- manual controls: one-shot release + the throttle switch ----

def test_release_all_frees_everything_parked_once():
    lim, pacer = make_setup(window_seconds=600, window_limit=1)
    lim.note_request("m", "human")     # budget spent -> auto parks

    async def run():
        tasks = [asyncio.create_task(pacer.gate()) for _ in range(3)]
        await asyncio.sleep(0.15)
        assert pacer.status()["parked"] == 3
        assert pacer.release_all() == 3
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)
        assert pacer.status()["parked"] == 0
        # One-shot: the next request is paced (parked) again.
        nxt = asyncio.create_task(pacer.gate())
        await asyncio.sleep(0.15)
        assert not nxt.done() and pacer.status()["parked"] == 1
        nxt.cancel()
    asyncio.run(run())


def test_release_all_with_nothing_parked_grants_nothing():
    lim, pacer = make_setup(window_seconds=600, window_limit=1)
    lim.note_request("m", "human")
    assert pacer.release_all() == 0

    async def run():
        task = asyncio.create_task(pacer.gate())
        await asyncio.sleep(0.15)
        assert not task.done()         # no stale free pass to pick up
        task.cancel()
    asyncio.run(run())


def test_free_passes_expire():
    lim, pacer = make_setup(window_seconds=600, window_limit=1)
    lim.note_request("m", "human")

    async def run():
        parked = asyncio.create_task(pacer.gate())
        await asyncio.sleep(0.15)
        pacer.release_all()
        pacer._free_passes_until = time.monotonic() - 1.0   # pretend time passed
        await asyncio.sleep(0.15)
        assert not parked.done()       # expired pass isn't honored
        parked.cancel()
    asyncio.run(run())


def test_set_enabled_switches_throttling_off_and_on():
    lim, pacer = make_setup(window_seconds=600, window_limit=1)
    lim.note_request("m", "human")     # spent: pacing would park auto

    async def run():
        pacer.set_enabled(False)
        await asyncio.wait_for(pacer.gate(), timeout=0.5)   # straight through
        assert pacer.status()["enabled"] is False
        assert pacer.status()["reason"] == "disabled"
        pacer.set_enabled(True)
        task = asyncio.create_task(pacer.gate())
        await asyncio.sleep(0.15)
        assert not task.done()                              # parked again
        task.cancel()
    asyncio.run(run())
