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

from .calibrate import COST_KEYS
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


@router.get("/_proxy/gauges")
async def gauges_endpoint(req: Request):
    """Sampled history of how many requests are in the proxy right now.

    Points carry `upstream` (holding a concurrency slot, i.e. at the API),
    `queued`, `backoff` (sleeping out a 429) and `parked` (held by the pacer),
    plus their sum as `total`. The gap between `total` and `upstream` is what
    the proxy is holding back. In memory only — empty right after a restart,
    covering `gauge_history_seconds` once warmed up.
    """
    return _st(req).gauges.series()


@router.get("/_proxy/status")
async def status_endpoint(req: Request):
    return _st(req).limiter.snapshot()


@router.get("/_proxy/config")
async def config_endpoint(req: Request):
    st = _st(req)
    return {"path": str(st.config_path), "loaded_mtime": st.config_mtime,
            "values": st.config}


def _fmt_qty(metric: str, v: float) -> str:
    """Compact quota quantity for the statusline: $12.30 / 312k / 140."""
    if metric == "cost":
        return f"${v:.2f}" if v < 100 else f"${v:.0f}"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}k"
    return f"{v:.0f}"


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
    # Binding quota window (the most-utilized budget): used/limit + unit, e.g.
    # "7/20" (requests), "312k/500kT" (tokens), "$12.30/$30" (cost).
    win = snap.get("window") or {}
    if win.get("active"):
        metric = win.get("metric", "requests")
        suffix = "T" if metric == "tokens" else ""
        util = float(win.get("utilization") or 0)
        qtxt = f"{_fmt_qty(metric, float(win.get('count') or 0))}/" \
               f"{_fmt_qty(metric, float(win.get('limit') or 0))}{suffix}"
        qcolor = "red" if util >= 0.95 else "yellow" if util >= 0.8 else "reset"
        parts.append(color(qtxt, qcolor) if qcolor != "reset" else qtxt)
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


@router.post("/_proxy/probe_enabled")
async def probe_enabled_endpoint(req: Request):
    """Turn speculative LOW->HIGH probing on or off.

    Body: {"enabled": true|false}, or omit it to toggle. Off means a saturated
    LOW tier just queues instead of sending a probe, so LOW->HIGH only happens
    on a scheduled switch or an explicit user action (force_tier / boost). Goes
    through the live config, so it is exactly equivalent to editing
    `probe_high_enabled` in config.yaml — and a later edit of that file wins.
      curl -X POST localhost:8787/_proxy/probe_enabled -d '{"enabled": false}'
    """
    st = _st(req)
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict):
        return JSONResponse({"error": "body must be a JSON object"}, status_code=400)
    if "enabled" in body:
        enabled = body["enabled"]
        if not isinstance(enabled, bool):
            return JSONResponse({"error": "'enabled' must be true or false"},
                                status_code=400)
    else:
        enabled = not bool(st.config.get("probe_high_enabled", True))
    await apply_config_change(st, {**st.config, "probe_high_enabled": enabled})
    return {"enabled": enabled, "limiter": st.limiter.snapshot()}


@router.post("/_proxy/pacer/release")
async def pacer_release_endpoint(req: Request):
    """Release every automation request currently parked by the pacer, once.

    A one-shot override: each request parked in the gate right now gets a free
    pass, the computed rate is untouched, and the next arrivals are paced
    normally again. Use it when you know the remaining budget is fine and just
    want the backlog to go. Returns how many passes were granted.
      curl -X POST localhost:8787/_proxy/pacer/release
    """
    st = _st(req)
    return {"released": st.pacer.release_all(), "pacer": st.pacer.status()}


@router.post("/_proxy/pacer/enabled")
async def pacer_enabled_endpoint(req: Request):
    """Turn automation-lane throttling on or off (the dashboard's switch).

    Body: {"enabled": true|false}, or omit it to toggle. Off makes the gate a
    no-op — automation is then admitted like human traffic (still behind the
    concurrency queue, still yielding to humans), which is the "no throttle"
    position. This goes through the live config, so it is exactly equivalent to
    editing `auto_pacing_enabled` in config.yaml — and a later edit of that file
    wins over it.
      curl -X POST localhost:8787/_proxy/pacer/enabled -d '{"enabled": false}'
    """
    st = _st(req)
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict):
        return JSONResponse({"error": "body must be a JSON object"}, status_code=400)
    if "enabled" in body:
        enabled = body["enabled"]
        if not isinstance(enabled, bool):
            return JSONResponse({"error": "'enabled' must be true or false"},
                                status_code=400)
    else:
        enabled = not bool(st.config.get("auto_pacing_enabled", True))
    await apply_config_change(st, {**st.config, "auto_pacing_enabled": enabled})
    return {"enabled": enabled, "pacer": st.pacer.status()}


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


# ---------- price calibration ----------

@router.post("/_proxy/calibrate/snapshot")
async def calibrate_snapshot_endpoint(req: Request):
    """Record one price-calibration snapshot.

    Body: the provider's CUMULATIVE cost counters (since your plan changed),
    all in the provider's billing unit (USD):
      {"cost_input_uncached": X, "cost_input_cached": Y, "cost_output": Z}
    The proxy pairs them with its own cumulative per-model token counters at
    this moment and persists the pair. Take another snapshot whenever
    convenient (irregular intervals are fine); every consecutive pair becomes
    one calibration interval. Intervals where the model mix differs — ideally
    some where only one model ran — are what make per-model prices solvable.
    Example:
      curl -X POST localhost:8787/_proxy/calibrate/snapshot \\
           -d '{"cost_input_uncached": 12.31, "cost_input_cached": 0.85, "cost_output": 7.02}'
    """
    st = _st(req)
    try:
        body = await req.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if not isinstance(body, dict):
        return JSONResponse({"error": "body must be a JSON object"}, status_code=400)
    costs = {}
    for k in COST_KEYS:
        if k not in body:
            return JSONResponse(
                {"error": f"missing '{k}' (need all of: {', '.join(COST_KEYS)})"},
                status_code=400,
            )
        try:
            costs[k] = float(body[k])
        except (TypeError, ValueError):
            return JSONResponse({"error": f"'{k}' must be a number"}, status_code=400)
        if costs[k] < 0:
            return JSONResponse({"error": f"'{k}' must be >= 0"}, status_code=400)
    snap = st.calibrator.add_snapshot(costs, st.pstats.lifetime_tokens())
    return {"snapshots": st.calibrator.snapshot_count(), "recorded": snap}


@router.get("/_proxy/calibrate/prices")
async def calibrate_prices_endpoint(req: Request):
    """Solve all stored snapshots into per-model $/Mtok price estimates.

    Per model+class: a price with confidence "direct" (an interval isolated
    it), "regression" (joint least squares), or null/"unidentifiable" (the
    data can't separate it yet — vary the model mix between snapshots).
    `model_pricing_yaml` is a ready-to-paste config block; `residuals` show
    how much upstream cost the proxy's traffic explains (persistently
    unexplained cost = something is spending quota without going through the
    proxy, or a price changed — POST /_proxy/calibrate/reset then).

    If the upstream never reported a cached-token count, `input` comes back
    with `"blended": true` — one price covering cached + uncached input,
    which is what this traffic can be billed with (the proxy can't see the
    cached split either).
    """
    return _st(req).calibrator.solve()


@router.post("/_proxy/calibrate/reset")
async def calibrate_reset_endpoint(req: Request):
    """Drop all calibration snapshots (e.g. the provider reset its counters
    or changed prices again)."""
    return {"discarded": _st(req).calibrator.reset()}


def _window_selectors(body: dict) -> tuple[str | None, float | None, str | None] | JSONResponse:
    """Parse the optional metric / window_seconds / group window selectors."""
    metric = body.get("metric")
    if metric is not None:
        metric = str(metric).strip().lower()
        if metric not in ("requests", "tokens", "cost"):
            return JSONResponse(
                {"error": "'metric' must be 'requests', 'tokens', or 'cost'"},
                status_code=400,
            )
    window_seconds = body.get("window_seconds")
    if window_seconds is not None:
        try:
            window_seconds = float(window_seconds)
        except (TypeError, ValueError):
            return JSONResponse({"error": "'window_seconds' must be a number"},
                                status_code=400)
    group = body.get("group")
    if group is not None:
        group = str(group).strip() or None
    return metric, window_seconds, group


@router.post("/_proxy/window/count")
async def set_window_count_endpoint(req: Request):
    """Set one rolling quota window's current count.

    Body: {"count": <number >= 0>} plus optional selectors "metric"
    ("requests" | "tokens" | "cost"; default "requests"), "window_seconds" and
    "group" to pick which budget window when a tier tracks several. Anchors a
    fresh window at now if the selected one isn't active (along with the rest
    of its budget group, which shares its timer). Examples:
      curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'
      curl -X POST localhost:8787/_proxy/window/count \\
           -d '{"count": 12.5, "metric": "cost", "window_seconds": 18000}'
      curl -X POST localhost:8787/_proxy/window/count \\
           -d '{"count": 5, "metric": "requests", "group": "minute"}'
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
    sel = _window_selectors(body)
    if isinstance(sel, JSONResponse):
        return sel
    snap = st.limiter.set_window_count(count, metric=sel[0], window_seconds=sel[1],
                                       group=sel[2])
    if snap is None:
        return JSONResponse(
            {"error": "no quota window matches the given metric/window_seconds/group"},
            status_code=404,
        )
    await asyncio.to_thread(save_window_file, st.limiter, st.window_persist_path())
    return {"window": snap}


@router.post("/_proxy/window/start")
async def set_window_start_endpoint(req: Request):
    """Set rolling quota window start times (unix seconds).

    Body: {"started_at": <unix seconds>} to anchor, or {"started_at": null} to
    clear everything (the next request re-anchors fresh windows). Optional
    selectors "metric" / "window_seconds" / "group" restrict which windows are
    anchored (default: all of them); a selected window's budget-group siblings
    are always anchored with it, since a group is one timer. Example:
      curl -X POST localhost:8787/_proxy/window/start -d '{"started_at": 1733250000}'
      curl -X POST localhost:8787/_proxy/window/start \\
           -d '{"started_at": 1733250000, "group": "session"}'
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
    sel = _window_selectors(body)
    if isinstance(sel, JSONResponse):
        return sel
    snap = st.limiter.set_window_start(started_at, metric=sel[0], window_seconds=sel[1],
                                       group=sel[2])
    if snap is None:
        return JSONResponse(
            {"error": "no quota window matches the given metric/window_seconds/group"},
            status_code=404,
        )
    await asyncio.to_thread(save_window_file, st.limiter, st.window_persist_path())
    return {"window": snap}
