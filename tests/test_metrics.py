"""Metrics: cost, rolling summary, and the EWMA avg the pacer relies on."""
import time

from anthropic_proxy.metrics import Metrics, compute_cost


PRICING = {"m": {"input": 3.0, "output": 15.0}}


def test_compute_cost_and_missing_model():
    # 1e6 input @ $3, 1e6 output @ $15
    assert compute_cost(PRICING, "m", 1_000_000, 1_000_000, 0, 0) == 18.0
    assert compute_cost(PRICING, "unknown", 100, 100, 0, 0) is None


def test_summary_counts_tokens_and_cost():
    met = Metrics(max_window_seconds=86400, pricing=PRICING)
    t = met.request_started("m")
    met.request_finished("m", t, 200, {"input_tokens": 1_000_000, "output_tokens": 1_000_000})
    s = met.summary()
    assert s["overall"]["1m"]["count"] == 1
    assert s["overall"]["1m"]["success"] == 1
    assert s["overall"]["1m"]["cost"] == 18.0
    assert s["per_model"]["m"]["1m"]["input_tokens"] == 1_000_000


def test_errors_counted_separately():
    met = Metrics(max_window_seconds=86400)
    t = met.request_started("m")
    met.request_finished("m", t, 500, None)
    s = met.summary()["overall"]["1m"]
    assert s["errors"] == 1 and s["success"] == 0


def test_avg_duration_ewma_and_fallback():
    met = Metrics(max_window_seconds=86400)
    assert met.avg_duration(7.5) == 7.5            # no data -> fallback
    now = time.time()
    met.request_finished("m", now - 1.0, 200, None)
    assert met.avg_duration(7.5) > 0               # EWMA now populated
