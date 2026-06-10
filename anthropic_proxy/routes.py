"""All /_proxy/* endpoints: dashboard, metrics JSON, statusline, runtime control.

Every endpoint reads the AppState from `request.app.state.proxy` — there are
no module globals here.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

from .config import apply_config_change, parse_switch_time
from .metrics import METRIC_WINDOWS
from .persistence import save_window_file
from .state import AppState

router = APIRouter()

_DASHBOARD_DIR = Path(__file__).resolve().parent / "dashboard"
_STATIC_TYPES = {
    "styles.css": "text/css; charset=utf-8",
    "app.js": "application/javascript; charset=utf-8",
}


def _st(request: Request) -> AppState:
    return request.app.state.proxy


# ---------- dashboard ----------

@router.get("/_proxy/", response_class=HTMLResponse)
@router.get("/_proxy", response_class=HTMLResponse)
async def dashboard():
    # Read per request (the files are a few KB): edits to the dashboard show up
    # on reload without restarting the proxy.
    return HTMLResponse((_DASHBOARD_DIR / "index.html").read_text())


@router.get("/_proxy/static/{name}")
async def dashboard_static(name: str):
    media_type = _STATIC_TYPES.get(name)  # fixed allowlist: no path traversal
    if media_type is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return Response((_DASHBOARD_DIR / name).read_bytes(), media_type=media_type)


# ---------- read-only state ----------

@router.get("/_proxy/metrics")
async def metrics_endpoint(req: Request):
    st = _st(req)
    return {
        "upstream": st.config["upstream_base_url"],
        "limiter": st.limiter.snapshot(),
        "pacer": st.pacer.status(),
        **st.metrics.summary(),
        "persistent": st.pstats.summary(),
    }


@router.get("/_proxy/series")
async def series_endpoint(req: Request):
    """Bucketed time series for graphs. ?window=24h|7d|30d|lifetime."""
    window = req.query_params.get("window", "24h")
    if window not in ("24h", "7d", "30d", "lifetime"):
        window = "24h"
    return _st(req).pstats.series(window)


@router.get("/_proxy/status")
async def status_endpoint(req: Request):
    return _st(req).limiter.snapshot()


@router.get("/_proxy/config")
async def config_endpoint(req: Request):
    st = _st(req)
    return {"path": str(st.config_path), "loaded_mtime": st.config_mtime,
            "values": st.config}


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


@router.get("/_proxy/statusline", response_class=PlainTextResponse)
async def statusline_endpoint(req: Request):
    """Compact one-line status for tmux / Claude Code status bars.

    Query params:
      fmt=plain | tmux | ansi   (default: plain)
      window=1m | 10m | 1h | 5h | 24h   (which throughput window to show; default 1m)
    """
    st = _st(req)
    fmt = req.query_params.get("fmt", "plain")
    window = req.query_params.get("window", "1m")
    if window not in {w for w, _ in METRIC_WINDOWS}:
        window = "1m"

    snap = st.limiter.snapshot()
    pace = st.pacer.status()
    summ = st.metrics.summary()
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
    err_color = "red" if w["errors"] > 0 else "reset"

    parts = [
        color(tier, tier_color),
        f"{snap['in_flight']}/{snap['max_concurrent']}",
        color(f"q{snap['queued']}", "red") if snap["queued"] > 0 else f"q{snap['queued']}",
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
    parked = pace.get("parked", 0)
    if parked > 0:
        nxt = pace.get("next_seconds")
        if pace.get("reason") == "reserved":
            held = f"⏸{parked}"  # paused: quota reserved for humans
        elif nxt is not None and nxt >= 0.05:
            held = f"⏳{parked}→{_fmt_dur_short(nxt)}"
        else:
            held = f"⏳{parked}"
        parts.append(color(held, "yellow"))
    rl_wait = snap.get("rate_limited_waiting", 0)
    if rl_wait > 0:
        # Requests waiting out upstream rate-limiting (e.g. the backend's quota).
        parts.append(color(f"⏳429×{rl_wait}", "red"))
    if w["errors"] > 0:
        parts.append(color(f"!{w['errors']}err", err_color))
    if snap.get("probe_in_flight"):
        parts.append(color("probe", "cyan"))
    sched = snap.get("schedule") or {}
    if sched.get("pending"):
        parts.append(color(f"⏰HIGH→{_fmt_dur_short(sched.get('seconds_until'))}", "cyan"))
    if sched.get("low_pending"):
        parts.append(color(f"⏰LOW→{_fmt_dur_short(sched.get('low_seconds_until'))}", "cyan"))

    return " ".join(parts)


# ---------- runtime control ----------

@router.post("/_proxy/force_tier")
async def force_tier_endpoint(req: Request):
    st = _st(req)
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    tier = body.get("tier") if isinstance(body, dict) else None
    if tier not in (None, "low", "high"):
        return JSONResponse({"error": "tier must be 'low', 'high', or null"}, status_code=400)
    cfg = {**st.config, "force_tier": tier}
    await apply_config_change(st, cfg)
    return st.limiter.snapshot()


@router.post("/_proxy/boost")
async def boost_endpoint(req: Request):
    """Temporarily switch to HIGH, keeping auto-demotion enabled.

    The first rate-limited response (429/503/529) drops back to LOW on its own.
    Use force_tier="high" instead if you want HIGH pinned permanently.
    """
    st = _st(req)
    ok = await st.limiter.boost_high()
    if not ok:
        return JSONResponse(
            {"error": "cannot boost while force_tier is set; clear force_tier first"},
            status_code=409,
        )
    return st.limiter.snapshot()


async def _set_oneshot_switch(req: Request, direction: str):
    """Shared body parsing/validation for the two one-shot schedule endpoints."""
    st = _st(req)
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict) or "at" not in body:
        return JSONResponse({"error": "body must be a JSON object with 'at'"}, status_code=400)
    raw = body["at"]
    ts = parse_switch_time(raw)
    if raw is not None and ts is None:
        return JSONResponse(
            {"error": "'at' must be unix seconds, 'HH:MM[:SS]', an ISO-8601 "
                      "datetime, or null"},
            status_code=400,
        )
    if direction == "high":
        return st.limiter.set_oneshot_switch(ts)
    return st.limiter.set_oneshot_low_switch(ts)


@router.post("/_proxy/schedule_high")
async def schedule_high_endpoint(req: Request):
    """Arm (or clear) a ONE-SHOT automatic LOW->HIGH switch.

    Body: {"at": <value>} where value is unix epoch seconds, "HH:MM"/"HH:MM:SS"
    (next future local occurrence), an ISO-8601 datetime, or null to clear. At
    that time the tier promotes to HIGH once (auto-demotes on the next
    rate-limit; use force_tier to pin). Before it fires, the pacer drains the
    current LOW window's leftover over the shorter horizon ending at the switch.

    This is independent of the recurring DAILY switch configured via
    `scheduled_high_at` (which keeps firing every day); the proxy acts on
    whichever comes first. Example:
      curl -X POST localhost:8787/_proxy/schedule_high -d '{"at": "14:30"}'
    """
    return await _set_oneshot_switch(req, "high")


@router.post("/_proxy/schedule_low")
async def schedule_low_endpoint(req: Request):
    """Arm (or clear) a ONE-SHOT automatic HIGH->LOW switch.

    Mirror of POST /_proxy/schedule_high, and independent of the recurring DAILY
    switch configured via `scheduled_low_at`; the proxy acts on whichever comes
    first. Example:
      curl -X POST localhost:8787/_proxy/schedule_low -d '{"at": "09:00"}'
    """
    return await _set_oneshot_switch(req, "low")


@router.post("/_proxy/window/count")
async def set_window_count_endpoint(req: Request):
    """Set the current rolling-window request count for the active session.

    Body: {"count": <number >= 0>}. Anchors a fresh window at now if none is
    active. Example:
      curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'
    """
    st = _st(req)
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
    snap = st.limiter.set_window_count(count)
    await asyncio.to_thread(save_window_file, st.limiter, st.window_persist_path())
    return {"window": snap}


@router.post("/_proxy/window/start")
async def set_window_start_endpoint(req: Request):
    """Set the current rolling-window start time (unix seconds).

    Body: {"started_at": <unix seconds>} to anchor the window, or
    {"started_at": null} to clear it (the next request re-anchors). Example:
      curl -X POST localhost:8787/_proxy/window/start -d '{"started_at": 1733250000}'
    """
    st = _st(req)
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
    snap = st.limiter.set_window_start(started_at)
    await asyncio.to_thread(save_window_file, st.limiter, st.window_persist_path())
    return {"window": snap}
