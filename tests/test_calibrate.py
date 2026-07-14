from anthropic_proxy.calibrate import Calibrator


PRICES = {  # $/Mtok ground truth used to fabricate upstream cost counters
    "opus": {"input": 15.0, "cache_creation": 18.75, "cache_read": 1.5, "output": 75.0},
    "haiku": {"input": 1.0, "cache_creation": 1.25, "cache_read": 0.1, "output": 5.0},
}


def _costs(tokens):
    """Cumulative upstream cost counters implied by cumulative token counters."""
    unc = cached = out = 0.0
    for m, t in tokens.items():
        p = PRICES[m]
        unc += (t.get("input", 0) * p["input"]
                + t.get("cache_creation", 0) * p["cache_creation"]) / 1e6
        cached += t.get("cache_read", 0) * p["cache_read"] / 1e6
        out += t.get("output", 0) * p["output"] / 1e6
    return {"cost_input_uncached": unc, "cost_input_cached": cached,
            "cost_output": out}


def _feed(cal, series):
    """series: list of cumulative per-model token dicts -> snapshots."""
    for i, tokens in enumerate(series):
        cal.add_snapshot(_costs(tokens), tokens, at=1000.0 + i)


def test_single_model_two_snapshots_recovers_all_prices(tmp_path):
    cal = Calibrator(str(tmp_path / "cal.json"))
    _feed(cal, [
        {"opus": {"input": 0, "cache_creation": 0, "cache_read": 0, "output": 0}},
        {"opus": {"input": 2_000_000, "cache_creation": 500_000,
                  "cache_read": 4_000_000, "output": 300_000}},
        # A second interval with a different in/cc ratio separates input from
        # cache_creation inside the shared uncached-cost equation.
        {"opus": {"input": 2_500_000, "cache_creation": 3_000_000,
                  "cache_read": 5_000_000, "output": 400_000}},
    ])
    res = cal.solve()
    assert res["intervals"] == 2
    p = res["models"]["opus"]
    assert p["output"]["price"] == 75.0 and p["output"]["confidence"] == "direct"
    assert p["cache_read"]["price"] == 1.5
    assert p["input"]["price"] == 15.0
    assert p["cache_creation"]["price"] == 18.75
    for r in res["residuals"].values():
        assert abs(r["unexplained"]) < 1e-6


def test_mixed_models_recovered_by_regression(tmp_path):
    cal = Calibrator(str(tmp_path / "cal.json"))
    # Both models always active, but with varying mix -> solvable jointly.
    _feed(cal, [
        {"opus": {"output": 0}, "haiku": {"output": 0}},
        {"opus": {"output": 1_000_000}, "haiku": {"output": 500_000}},
        {"opus": {"output": 1_200_000}, "haiku": {"output": 3_000_000}},
        {"opus": {"output": 4_000_000}, "haiku": {"output": 3_100_000}},
    ])
    res = cal.solve()
    opus, haiku = res["models"]["opus"], res["models"]["haiku"]
    assert abs(opus["output"]["price"] - 75.0) < 1e-6
    assert abs(haiku["output"]["price"] - 5.0) < 1e-6
    assert opus["output"]["confidence"] == "regression"


def test_collinear_mix_is_unidentifiable(tmp_path):
    cal = Calibrator(str(tmp_path / "cal.json"))
    # The two models always consume output tokens in a 2:1 ratio — only the
    # blended price is determined, so both must come back unidentifiable.
    _feed(cal, [
        {"opus": {"output": 0}, "haiku": {"output": 0}},
        {"opus": {"output": 2_000_000}, "haiku": {"output": 1_000_000}},
        {"opus": {"output": 4_000_000}, "haiku": {"output": 2_000_000}},
        {"opus": {"output": 8_000_000}, "haiku": {"output": 4_000_000}},
    ])
    res = cal.solve()
    assert res["models"]["opus"]["output"]["price"] is None
    assert res["models"]["opus"]["output"]["confidence"] == "unidentifiable"
    assert res["models"]["haiku"]["output"]["price"] is None


def test_counter_reset_interval_is_skipped(tmp_path):
    cal = Calibrator(str(tmp_path / "cal.json"))
    good0 = {"opus": {"output": 0}}
    good1 = {"opus": {"output": 1_000_000}}
    cal.add_snapshot(_costs(good0), good0, at=1)
    cal.add_snapshot(_costs(good1), good1, at=2)
    # Provider reset its counters: cost goes backwards -> that interval drops,
    # but the pair after the reset is consistent again and still solves.
    reset0 = {"opus": {"output": 2_000_000}}
    cal.add_snapshot({"cost_input_uncached": 0, "cost_input_cached": 0,
                      "cost_output": 0}, reset0, at=3)
    reset1 = {"opus": {"output": 3_000_000}}
    cal.add_snapshot({"cost_input_uncached": 0, "cost_input_cached": 0,
                      "cost_output": 75.0}, reset1, at=4)
    res = cal.solve()
    assert res["snapshots"] == 4 and res["intervals"] == 2
    assert res["notes"]
    assert res["models"]["opus"]["output"]["price"] == 75.0


def test_snapshots_persist_across_instances(tmp_path):
    path = tmp_path / "cal.json"
    cal = Calibrator(str(path))
    _feed(cal, [
        {"haiku": {"output": 0}},
        {"haiku": {"output": 10_000_000}},
    ])
    again = Calibrator(str(path))
    assert again.snapshot_count() == 2
    assert again.solve()["models"]["haiku"]["output"]["price"] == 5.0
    assert again.reset() == 2
    assert Calibrator(str(path)).snapshot_count() == 0


def test_yaml_block_matches_model_pricing_schema(tmp_path):
    cal = Calibrator(str(tmp_path / "cal.json"))
    _feed(cal, [
        {"haiku": {"input": 0, "cache_read": 0, "output": 0}},
        {"haiku": {"input": 3_000_000, "cache_read": 2_000_000,
                   "output": 1_000_000}},
    ])
    yaml_block = cal.solve()["model_pricing_yaml"]
    assert yaml_block.startswith("model_pricing:")
    assert "  haiku:" in yaml_block
    assert "    input: 1.0" in yaml_block
    assert "    output: 5.0" in yaml_block
    assert "    cache_read: 0.1" in yaml_block
    assert "cache_creation" not in yaml_block   # no cache-write tokens seen


def test_split_mode_keeps_prices_unblended(tmp_path):
    cal = Calibrator(str(tmp_path / "cal.json"))
    _feed(cal, [
        {"haiku": {"input": 0, "cache_read": 0, "output": 0}},
        {"haiku": {"input": 3_000_000, "cache_read": 2_000_000,
                   "output": 1_000_000}},
    ])
    res = cal.solve()
    assert "blended" not in res["models"]["haiku"]["input"]
    assert set(res["residuals"]) == {"cost_output", "cost_input_cached",
                                     "cost_input_uncached"}


def test_no_cached_token_data_blends_the_input_price(tmp_path):
    # Upstream reports no cached-token count, so the proxy lumps all 5M prompt
    # tokens into `input` — while the provider still bills 1M of them as
    # uncached ($15/Mtok) and 4M as cached ($1.5/Mtok). The only price that can
    # (and should) come out is the blended (15 + 6) / 5 = $4.20/Mtok.
    cal = Calibrator(str(tmp_path / "cal.json"))
    for i, (inp, out) in enumerate([(0, 0), (5_000_000, 1_000_000)]):
        cal.add_snapshot(
            {"cost_input_uncached": (inp / 5) * 15.0 / 1e6,
             "cost_input_cached": (inp * 4 / 5) * 1.5 / 1e6,
             "cost_output": out * 75.0 / 1e6},
            {"opus": {"input": inp, "cache_creation": 0, "cache_read": 0,
                      "output": out}},
            at=1000.0 + i,
        )
    res = cal.solve()
    p = res["models"]["opus"]
    assert abs(p["input"]["price"] - 4.2) < 1e-6
    assert p["input"]["blended"] is True and p["input"]["confidence"] == "direct"
    assert p["output"]["price"] == 75.0 and "blended" not in p["output"]
    assert "cache_read" not in p  # never observed -> no unknown, no price
    # Both input counters are accounted for in one residual, fully explained.
    assert set(res["residuals"]) == {"cost_output",
                                     "cost_input_uncached+cost_input_cached"}
    for r in res["residuals"].values():
        assert abs(r["unexplained"]) < 1e-6
    assert any("blended" in n for n in res["notes"])
    assert "    input: 4.2  # blended: cached + uncached input" \
        in res["model_pricing_yaml"]


def test_corrupt_file_tolerated(tmp_path):
    path = tmp_path / "cal.json"
    path.write_text("{broken")
    cal = Calibrator(str(path))
    assert cal.snapshot_count() == 0
    assert cal.solve()["intervals"] == 0
