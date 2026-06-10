import asyncio
import json
import time

from anthropic_proxy.limiter import Limiter, Tier
from anthropic_proxy.persistence import (
    PersistentStats,
    load_window_file,
    save_window_file,
)


def test_record_and_summary(tmp_path):
    ps = PersistentStats(path=str(tmp_path / "stats.json"),
                         pricing={"m": {"input": 1.0, "output": 2.0}})
    ps.record("m", 200, 1.5, {"input_tokens": 1_000_000, "output_tokens": 0})
    ps.record("m", 500, 0.5, None)
    s = ps.summary()
    for label in ("24h", "7d", "30d", "lifetime"):
        o = s[label]["overall"]
        assert o["count"] == 2 and o["success"] == 1 and o["errors"] == 1
        assert o["avg_seconds"] == 1.0
        assert o["cost"] == 1.0           # 1M input tokens at $1/M
    assert s["lifetime"]["per_model"]["m"]["has_pricing"] is True


def test_flush_and_reload_roundtrip(tmp_path):
    path = tmp_path / "stats.json"
    ps = PersistentStats(path=str(path))
    ps.record("m", 200, 1.0, {"input_tokens": 7})
    asyncio.run(ps.maybe_flush(force=True))
    assert path.exists()
    again = PersistentStats(path=str(path))
    o = again.summary()["lifetime"]["overall"]
    assert o["count"] == 1 and o["input_tokens"] == 7


def test_corrupt_stats_file_is_tolerated(tmp_path):
    path = tmp_path / "stats.json"
    path.write_text("{not json")
    ps = PersistentStats(path=str(path))
    assert ps.summary()["lifetime"]["overall"]["count"] == 0


def test_series_shape_and_buckets(tmp_path):
    ps = PersistentStats(path=str(tmp_path / "stats.json"))
    ps.record("m", 200, 1.0, {"input_tokens": 3, "output_tokens": 4})
    data = ps.series("24h")
    assert data["window"] == "24h" and data["step"] == 3600
    assert len(data["points"]) == 24
    last = data["points"][-1]
    assert last["requests"] == 1 and last["input_tokens"] == 3
    assert sum(p["requests"] for p in data["points"][:-1]) == 0
    assert ps.series("bogus")["window"] == "lifetime"


# ---- window.json helpers ----

def _limiter_with_window() -> Limiter:
    lim = Limiter(Tier("low", 4, 100, 10), Tier("high", 100, 50, 99),
                  "low", 300.0, None)
    lim.note_request("m", "auto")
    return lim


def test_window_save_load_roundtrip(tmp_path):
    path = tmp_path / "window.json"
    lim = _limiter_with_window()
    save_window_file(lim, path)
    state = load_window_file(path)
    assert state is not None and state["count"] == 1.0
    fresh = Limiter(Tier("low", 4, 100, 10), Tier("high", 100, 50, 99),
                    "low", 300.0, None)
    assert fresh.load_window_state(state)
    assert fresh.window_snapshot()["count_auto"] == 1


def test_window_save_clears_file_when_idle(tmp_path):
    path = tmp_path / "window.json"
    path.write_text(json.dumps({"started_at": time.time(), "count": 5}))
    idle = Limiter(Tier("low", 4, 100, 10), Tier("high", 100, 50, 99),
                   "low", 300.0, None)
    save_window_file(idle, path)      # no active window -> stale file removed
    assert not path.exists()


def test_load_window_file_bad_content(tmp_path):
    path = tmp_path / "window.json"
    assert load_window_file(path) is None          # missing
    path.write_text("[1,2,3]")
    assert load_window_file(path) is None          # not a dict
    path.write_text("{broken")
    assert load_window_file(path) is None          # invalid json
