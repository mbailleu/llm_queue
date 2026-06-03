"""All /_proxy/* observability + control endpoints, and the dashboard.

The dashboard markup lives in dashboard/index.html and links its CSS/JS from
dashboard/styles.css and dashboard/app.js, served as static files (no build
step, no inline 600-line string in Python any more)."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from . import runtime
from .metrics import METRIC_WINDOWS
from .server import app

_DASHBOARD_DIR = Path(__file__).parent / "dashboard"
INDEX_HTML = (_DASHBOARD_DIR / "index.html").read_text()

# styles.css / app.js are served from here; index.html links them.
app.mount("/_proxy/static", StaticFiles(directory=str(_DASHBOARD_DIR)), name="static")


@app.get("/_proxy/", response_class=HTMLResponse)
@app.get("/_proxy", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(INDEX_HTML)


@app.get("/_proxy/metrics")
async def metrics_endpoint():
    return {
        "upstream": runtime.config["upstream_base_url"],
        "limiter": runtime.limiter.snapshot(),
        **runtime.metrics.summary(),
        "persistent": runtime.pstats.summary(),
    }


@app.get("/_proxy/series")
async def series_endpoint(req: Request):
    """Bucketed time series for graphs. ?window=24h|7d|30d|lifetime."""
    window = req.query_params.get("window", "24h")
    if window not in ("24h", "7d", "30d", "lifetime"):
        window = "24h"
    return runtime.pstats.series(window)


@app.get("/_proxy/status")
async def status_endpoint():
    return runtime.limiter.snapshot()


def _fmt_dur_short(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


@app.get("/_proxy/statusline", response_class=PlainTextResponse)
async def statusline_endpoint(req: Request):
    """Compact one-line status for tmux / Claude Code status bars.

    Query params:
      fmt=plain | tmux | ansi   (default: plain)
      window=1m | 10m | 1h | 5h | 24h   (which throughput window to show; default 1m)
    """
    fmt = req.query_params.get("fmt", "plain")
    window = req.query_params.get("window", "1m")
    if window not in {w for w, _ in METRIC_WINDOWS}:
        window = "1m"

    snap = runtime.limiter.snapshot()
    summ = runtime.metrics.summary()
    tier = snap["active_tier"].upper()
    w = summ["overall"].get(window, {"count": 0, "errors": 0, "avg_seconds": None})

    def color(s: str, name: str) -> str:
        if fmt == "tmux":
            mapping = {
                "green":  "#[fg=green]",
                "yellow": "#[fg=yellow]",
                "red":    "#[fg=red]",
                "cyan":   "#[fg=cyan]",
                "reset":  "#[default]",
            }
        elif fmt == "ansi":
            mapping = {
                "green":  "\x1b[32m",
                "yellow": "\x1b[33m",
                "red":    "\x1b[31m",
                "cyan":   "\x1b[36m",
                "reset":  "\x1b[0m",
            }
        else:
            return s
        return f"{mapping[name]}{s}{mapping['reset']}"

    tier_color = "green" if tier == "HIGH" else "yellow"
    queued_color = "red" if snap["queued"] > 0 else "reset"
    err_color = "red" if w["errors"] > 0 else "reset"

    parts = [
        color(tier, tier_color),
        f"{snap['in_flight']}/{snap['max_concurrent']}",
        color(f"q{snap['queued']}", queued_color) if snap['queued'] > 0 else f"q{snap['queued']}",
        f"{window}:{w['count']}",
        _fmt_dur_short(w["avg_seconds"]),
    ]
    if req.query_params.get("cost") in ("1", "true", "yes") and w.get("cost") is not None:
        c = w["cost"]
        if c < 0.01:
            cs = "<$0.01"
        elif c < 1:
            cs = f"${c:.3f}"
        elif c < 100:
            cs = f"${c:.2f}"
        else:
            cs = f"${c:.0f}"
        parts.append(cs)
    if w["errors"] > 0:
        parts.append(color(f"!{w['errors']}err", err_color))
    if snap.get("probe_in_flight"):
        parts.append(color("probe", "cyan"))

    return " ".join(parts)


@app.get("/_proxy/config")
async def config_endpoint():
    return {"path": str(runtime.CONFIG_PATH), "loaded_mtime": runtime.config_mtime,
            "values": runtime.config}


@app.post("/_proxy/force_tier")
async def force_tier_endpoint(req: Request):
    body = await req.json()
    tier = body.get("tier")
    if tier not in (None, "low", "high"):
        return JSONResponse({"error": "tier must be 'low', 'high', or null"}, status_code=400)
    cfg = {**runtime.config, "force_tier": tier}
    await runtime.apply_config_change(cfg)
    return runtime.limiter.snapshot()


@app.post("/_proxy/boost")
async def boost_endpoint():
    """Temporarily switch to HIGH, keeping auto-demotion enabled.

    The first rate-limited response (429/503/529) drops back to LOW on its own.
    Use force_tier="high" instead if you want HIGH pinned permanently.
    """
    ok = await runtime.limiter.boost_high()
    if not ok:
        return JSONResponse(
            {"error": "cannot boost while force_tier is set; clear force_tier first"},
            status_code=409,
        )
    return runtime.limiter.snapshot()


@app.post("/_proxy/window/count")
async def set_window_count_endpoint(req: Request):
    """Set the current rolling-window request count for the active session.

    Body: {"count": <number >= 0>}. Anchors a fresh window at now if none is
    active. Example:
      curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'
    """
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "count" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'count'"}, status_code=400)
    try:
        count = float(body["count"])
    except (TypeError, ValueError):
        return JSONResponse({"error": "'count' must be a number"}, status_code=400)
    if count < 0:
        return JSONResponse({"error": "'count' must be >= 0"}, status_code=400)
    snap = runtime.limiter.set_window_count(count)
    await asyncio.to_thread(runtime.save_window_file)
    return {"window": snap}


@app.post("/_proxy/window/start")
async def set_window_start_endpoint(req: Request):
    """Set the current rolling-window start time (unix seconds).

    Body: {"started_at": <unix seconds>} to anchor the window, or
    {"started_at": null} to clear it (the next request re-anchors). Example:
      curl -X POST localhost:8787/_proxy/window/start -d '{"started_at": 1733250000}'
    """
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "started_at" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'started_at'"}, status_code=400)
    started_at = body["started_at"]
    if started_at is not None:
        try:
            started_at = float(started_at)
        except (TypeError, ValueError):
            return JSONResponse(
                {"error": "'started_at' must be a unix timestamp (seconds) or null"},
                status_code=400,
            )
    snap = runtime.limiter.set_window_start(started_at)
    await asyncio.to_thread(runtime.save_window_file)
    return {"window": snap}
