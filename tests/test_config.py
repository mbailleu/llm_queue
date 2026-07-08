from anthropic_proxy.config import DEFAULT_CONFIG, make_tier, parse_budgets


def cfg_with(tiers):
    return {**DEFAULT_CONFIG, "tiers": tiers}


def test_parse_budgets_valid_list():
    cfg = cfg_with({"low": {"max_concurrent": 4, "limits": [
        {"metric": "requests", "limit": 20, "window_seconds": 60},
        {"metric": "tokens", "limit": 500000, "window_seconds": 60},
        {"metric": "cost", "limit": 30, "window_seconds": 18000},
    ]}})
    budgets = parse_budgets(cfg, "low")
    assert [b.metric for b in budgets] == ["requests", "tokens", "cost"]
    assert budgets[2].limit == 30 and budgets[2].window_seconds == 18000


def test_parse_budgets_drops_invalid_entries():
    cfg = cfg_with({"low": {"max_concurrent": 4, "limits": [
        {"metric": "bogus", "limit": 20, "window_seconds": 60},     # bad metric
        {"metric": "cost", "limit": -1, "window_seconds": 60},      # <= 0
        {"metric": "tokens", "limit": "x", "window_seconds": 60},   # non-numeric
        "not-a-dict",
        {"metric": "requests", "limit": 5, "window_seconds": 60},   # the survivor
    ]}})
    budgets = parse_budgets(cfg, "low")
    assert len(budgets) == 1 and budgets[0].metric == "requests"


def test_parse_budgets_all_invalid_falls_back_to_none():
    cfg = cfg_with({"low": {"max_concurrent": 4, "limits": [
        {"metric": "bogus", "limit": 1, "window_seconds": 1},
    ]}})
    assert parse_budgets(cfg, "low") is None
    assert parse_budgets(cfg_with({"low": {"max_concurrent": 4}}), "low") is None


def test_make_tier_legacy_keys_still_work():
    cfg = cfg_with({
        "low": {"max_concurrent": 4, "window_seconds": 18000, "window_limit": 600},
        "high": {"max_concurrent": 1000},   # falls to top-level rate_window_*
    })
    cfg["rate_window_seconds"] = 3600
    cfg["rate_window_limit"] = 99
    low = make_tier(cfg, "low")
    assert len(low.budgets) == 1
    assert low.budgets[0].metric == "requests"
    assert low.budgets[0].limit == 600 and low.budgets[0].window_seconds == 18000
    high = make_tier(cfg, "high")
    assert high.budgets[0].limit == 99 and high.budgets[0].window_seconds == 3600


def test_make_tier_limits_win_over_legacy_keys():
    cfg = cfg_with({"low": {
        "max_concurrent": 4,
        "window_seconds": 18000, "window_limit": 600,
        "limits": [{"metric": "cost", "limit": 30, "window_seconds": 18000}],
    }, "high": {"max_concurrent": 1000}})
    low = make_tier(cfg, "low")
    assert len(low.budgets) == 1 and low.budgets[0].metric == "cost"


def test_default_config_carries_new_plan_budgets():
    low = make_tier(DEFAULT_CONFIG, "low")
    high = make_tier(DEFAULT_CONFIG, "high")
    assert low.max_concurrent == 4 and high.max_concurrent == 1000
    assert len(low.budgets) == 4 and len(high.budgets) == 4
    assert {b.metric for b in low.budgets} == {"requests", "tokens", "cost"}
