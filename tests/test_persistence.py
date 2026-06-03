"""PersistentStats: record -> summary/series and on-disk round-trip."""
from anthropic_proxy.persistence import PersistentStats


PRICING = {"m": {"input": 3.0, "output": 15.0}}


def test_record_summary_and_series(tmp_path):
    ps = PersistentStats(path=str(tmp_path / "s.json"), pricing=PRICING)
    ps.record("m", 200, 1.0, {"input_tokens": 1_000_000, "output_tokens": 0})
    ps.record("m", 500, 2.0, None)
    summ = ps.summary()
    assert summ["lifetime"]["overall"]["count"] == 2
    assert summ["lifetime"]["overall"]["errors"] == 1
    assert summ["lifetime"]["overall"]["cost"] == 3.0          # 1e6 input @ $3
    series = ps.series("24h")
    assert series["window"] == "24h"
    assert sum(p["requests"] for p in series["points"]) == 2


def test_persist_roundtrip(tmp_path):
    path = str(tmp_path / "s.json")
    ps = PersistentStats(path=path)
    ps.record("m", 200, 1.0, {"input_tokens": 5, "output_tokens": 6})
    ps._write(ps._serialize())                                  # flush synchronously
    ps2 = PersistentStats(path=path)                            # reload from disk
    assert ps2.summary()["lifetime"]["overall"]["count"] == 1
    assert ps2.summary()["lifetime"]["overall"]["input_tokens"] == 5
