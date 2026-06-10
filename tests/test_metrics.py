import time

from anthropic_proxy.metrics import Metrics, compute_cost


PRICING = {
    "m1": {"input": 10.0, "output": 20.0, "cache_creation": 12.5, "cache_read": 1.0},
    "m2": {"input": 2.0, "output": 4.0},   # cache rates default to input rate
}


def test_compute_cost_with_explicit_cache_rates():
    c = compute_cost(PRICING, "m1", 1_000_000, 1_000_000, 1_000_000, 1_000_000)
    assert c == 10.0 + 20.0 + 12.5 + 1.0


def test_compute_cost_cache_defaults_to_input_rate():
    c = compute_cost(PRICING, "m2", 0, 0, 1_000_000, 1_000_000)
    assert c == 2.0 + 2.0


def test_compute_cost_unpriced_model_is_none():
    assert compute_cost(PRICING, "nope", 1, 1, 1, 1) is None


def test_metrics_summary_counts_and_tokens():
    m = Metrics(max_window_seconds=3600, pricing=PRICING)
    t = m.request_started("m1")
    m.request_finished("m1", t, 200, {"input_tokens": 100, "output_tokens": 50})
    t = m.request_started("m1")
    m.request_finished("m1", t, 500)
    s = m.summary()
    o = s["overall"]["1m"]
    assert o["count"] == 2 and o["success"] == 1 and o["errors"] == 1
    assert o["input_tokens"] == 100 and o["output_tokens"] == 50
    assert o["cost"] is not None and o["priced_requests"] == 2
    pm = s["per_model"]["m1"]
    assert pm["1m"]["count"] == 2
    assert pm["active"] == 0 and pm["has_pricing"] is True


def test_metrics_unpriced_model_cost_none():
    m = Metrics(pricing=PRICING)
    t = m.request_started("unknown")
    m.request_finished("unknown", t, 200, {"input_tokens": 5})
    assert m.summary()["overall"]["1m"]["cost"] is None


def test_metrics_active_model_listed_before_completion():
    m = Metrics()
    m.request_started("busy")
    s = m.summary()
    assert s["per_model"]["busy"]["active"] == 1
    assert s["total_active"] == 1


def test_avg_duration_fallback_then_ewma():
    m = Metrics()
    assert m.avg_duration(30.0) == 30.0
    t = time.time() - 1.0                       # ~1s request
    m.request_finished("m", t, 200)
    assert 0.5 < m.avg_duration(30.0) < 2.0


def test_old_completions_age_out():
    m = Metrics(max_window_seconds=0.05)
    t = m.request_started("m")
    m.request_finished("m", t, 200)
    time.sleep(0.08)
    assert m.summary()["overall"]["1m"]["count"] == 0
