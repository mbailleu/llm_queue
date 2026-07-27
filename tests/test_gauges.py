import time

from anthropic_proxy.gauges import GaugeHistory


def test_sample_records_total_and_parts():
    g = GaugeHistory(sample_seconds=1.0, history_seconds=3600)
    g.sample(upstream=3, queued=2, backoff=1, parked=4)
    s = g.series()
    assert s["step"] == 1.0
    p = s["points"][0]
    assert p["upstream"] == 3 and p["queued"] == 2
    assert p["backoff"] == 1 and p["parked"] == 4
    assert p["total"] == 10          # everything the proxy is holding


def test_history_trims_to_the_window():
    g = GaugeHistory(sample_seconds=0.01, history_seconds=60)
    g.sample(1, 0, 0, 0)
    # Backdate the sample past the window instead of sleeping for a minute.
    g._samples[0] = (time.time() - 120, 1, 0, 0, 0)
    g.sample(2, 0, 0, 0)
    pts = g.series()["points"]
    assert len(pts) == 1 and pts[0]["upstream"] == 2


def test_configure_applies_new_window_immediately():
    g = GaugeHistory(sample_seconds=1.0, history_seconds=3600)
    g.sample(1, 0, 0, 0)
    g._samples[0] = (time.time() - 300, 1, 0, 0, 0)
    g.configure(sample_seconds=5.0, history_seconds=60)
    assert g.sample_seconds == 5.0
    assert g.series()["points"] == []     # the old sample is outside the new span


def test_negative_counts_are_clamped():
    g = GaugeHistory()
    g.sample(-1, -2, 0, 3)
    p = g.series()["points"][0]
    assert p["upstream"] == 0 and p["queued"] == 0 and p["total"] == 3


def test_empty_history_is_a_valid_series():
    s = GaugeHistory().series()
    assert s["points"] == [] and s["span"] >= 60
