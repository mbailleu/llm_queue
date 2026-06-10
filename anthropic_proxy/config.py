"""Config: defaults, file loading, hot-reload, and the watch loop.

`DEFAULT_CONFIG` is the source of truth for every configurable key. New keys
must be added here, wired through `build_state` AND `apply_config_change`
(plus `pacer.configure()` / `limiter.set_auto_params()` if pacing-related),
and documented in config.yaml.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import os
import time
from pathlib import Path
from typing import Any

import yaml

from .limiter import Limiter, Tier
from .metrics import Metrics
from .pacer import AutoPacer
from .persistence import PersistentStats, load_window_file, save_window_file
from .state import AppState

log = logging.getLogger("proxy.config")

DEFAULT_CONFIG: dict[str, Any] = {
    "upstream_base_url": "https://api.anthropic.com",
    "listen_host": "127.0.0.1",
    "listen_port": 8787,            # human lane (unthrottled)
    # Automation lane (paced). A second listening port for scripts / tight loops;
    # set to null to disable the second lane entirely. Both ports share the same
    # upstream, queue, tier auto-detection, and quota window — they differ only
    # in admission policy: the human port is never throttled and has concurrency
    # priority; the auto port is paced by AutoPacer to spend only the leftover
    # budget without starving the human (statistically).
    "throttle_listen_port": 8788,
    "auto_pacing_enabled": True,
    # Predicted human demand = human_demand_safety * observed_human_rate *
    # time_left. Higher safety leaves more headroom for humans (slower auto).
    "human_demand_safety": 1.5,
    # Trailing horizon (seconds) over which the human request rate is measured.
    "human_demand_horizon_seconds": 3600,
    # How far ahead (seconds) to project that measured human rate when reserving
    # quota: the reservation term is human_demand_safety * human_rate *
    # min(time_left, this). Capping the projection stops a long (e.g. 5h LOW)
    # window from reserving almost the entire quota off a small human rate.
    "human_demand_lookahead_seconds": 3600,
    # Optional hard floor of requests always kept free for humans (0 = purely
    # statistical, the default the user asked for).
    "human_quota_floor": 0,
    # Concurrency slots reserved for humans (auto in-flight is capped at
    # max_concurrent - this). 0 relies on human queue-priority alone.
    "auto_concurrency_reserve": 0,
    # Assumed request seconds before any latency has been measured (pacer uses
    # the live EWMA once traffic exists).
    "auto_assumed_request_seconds": 30.0,
    # How often a parked/over-pace auto request re-checks whether it may go.
    "auto_poll_seconds": 1.0,
    "initial_tier": "low",
    "force_tier": None,
    # Optional RECURRING DAILY automatic LOW->HIGH switch. Accepts unix epoch
    # seconds, "HH:MM"/"HH:MM:SS", or an ISO-8601 datetime — only the local
    # time-of-day is used, and the switch fires at that time every day. The tier
    # is promoted to HIGH (auto-demotes on the next rate-limit, like a boost — use
    # force_tier to pin instead). Before each fire, the pacer treats the current
    # LOW window as ending at the switch time, so background traffic drains the
    # leftover over the shorter horizon. null = no daily switch. Hot-reloadable.
    # A separate ONE-SHOT switch is available via
    # POST /_proxy/schedule_high {"at": ...}; the two are independent.
    "scheduled_high_at": None,
    # Optional RECURRING DAILY automatic HIGH->LOW switch — the mirror of
    # scheduled_high_at. Same value formats (unix seconds, "HH:MM[:SS]", ISO-8601;
    # only the local time-of-day is used). At that time the tier drops to LOW
    # every day. A separate ONE-SHOT switch is available via
    # POST /_proxy/schedule_low {"at": ...}. null = no daily switch. Hot-reloadable.
    "scheduled_low_at": None,
    # Each tier has a max concurrency cap and its own rolling request-quota
    # window (window_limit requests per window_seconds). The active tier's window
    # drives the dashboard indicator and restarts whenever the tier switches.
    # window_seconds/window_limit fall back to the top-level rate_window_* below
    # when a tier omits them.
    "tiers": {
        "low":  {"max_concurrent": 4,    "window_seconds": 18000, "window_limit": 600},
        "high": {"max_concurrent": 1000, "window_seconds": 3600,  "window_limit": 99999},
    },
    "promotion_cooldown_seconds": 300,
    "retry_max_attempts": 12,
    "retry_base_delay": 1.0,
    "retry_max_delay": 60.0,
    # How long a rate-limited (429/503/529) request may keep waiting+retrying
    # before the proxy gives up and returns the error to the client. Defaults
    # to a bit over the 5h quota window so a queued request can outlast a full
    # window and run once the quota resets. (retry_max_attempts still bounds
    # connection-error retries, which a long wait won't fix.)
    "retry_max_elapsed_seconds": 18900,
    # Fallback rolling request-quota window for the dashboard "X / N this window"
    # indicator, used only when a tier omits its own window_seconds/window_limit.
    # Tracked, not enforced.
    "rate_window_seconds": 18000,   # 5h
    "rate_window_limit": 600,
    # Per-model weighting for the window count: a request for a listed model
    # counts as `factor` requests toward the window (only the window — metrics
    # and per-model stats still count each request once). Models not listed use
    # `default_window_weight`. Keys match by exact model name first, then by
    # substring (so "opus" matches "claude-opus-4-20250514").
    "model_window_weights": {},
    "default_window_weight": 1,
    "upstream_timeout": 600,
    "log_level": "INFO",
    "config_poll_seconds": 2.0,
    "metrics_window_seconds": 86400,
    "model_pricing": {},
    # Long-horizon persisted stats (weekly/monthly/lifetime + graphs).
    "stats_persist_path": "stats.json",
    "stats_flush_seconds": 60.0,
    "stats_retention_days": 120,
    # Current rolling-window state (count + start) persisted across restarts.
    # Restored on boot unless it has already elapsed past its window.
    "window_persist_path": "window.json",
}


def default_config_path() -> Path:
    return Path(os.environ.get("CONFIG_PATH", "config.yaml")).resolve()


def load_config_file(path: Path) -> dict[str, Any]:
    merged: dict[str, Any] = {**DEFAULT_CONFIG, "tiers": dict(DEFAULT_CONFIG["tiers"])}
    if not path.exists():
        log.warning(f"config file {path} not found; using defaults")
        return merged
    with open(path) as f:
        loaded = yaml.safe_load(f) or {}
    for k, v in loaded.items():
        if k == "tiers" and isinstance(v, dict):
            merged["tiers"] = {**merged["tiers"], **v}
        else:
            merged[k] = v
    return merged


def make_tier(cfg: dict[str, Any], name: str) -> Tier:
    """Build a Tier from config.

    Each tier may carry its own `window_seconds` / `window_limit` (its rolling
    request-quota budget). When omitted they fall back to the top-level
    `rate_window_seconds` / `rate_window_limit` so older single-window configs
    keep working.
    """
    t = cfg["tiers"][name]
    return Tier(
        name=name,
        max_concurrent=int(t["max_concurrent"]),
        window_seconds=float(t.get("window_seconds", cfg.get("rate_window_seconds", 18000))),
        window_limit=int(t.get("window_limit", cfg.get("rate_window_limit", 600))),
    )


def parse_window_weights(cfg: dict[str, Any]) -> tuple[dict[str, float], float]:
    """Read per-model window weights + default weight from config, validated.

    Non-positive or non-numeric weights are dropped/ignored (falling back to the
    default) so a bad config line can't silently zero out the window count.
    """
    raw = cfg.get("model_window_weights") or {}
    weights: dict[str, float] = {}
    if isinstance(raw, dict):
        for model, w in raw.items():
            try:
                wf = float(w)
            except (TypeError, ValueError):
                log.warning(f"model_window_weights[{model!r}]={w!r} not numeric; ignored")
                continue
            if wf <= 0:
                log.warning(f"model_window_weights[{model!r}]={w!r} must be > 0; ignored")
                continue
            weights[str(model)] = wf
    try:
        default = float(cfg.get("default_window_weight", 1))
    except (TypeError, ValueError):
        default = 1.0
    if default <= 0:
        default = 1.0
    return weights, default


def parse_switch_time(value: Any, now: float | None = None) -> float | None:
    """Resolve a scheduled-switch time (config key or API body) to unix seconds.

    Accepts:
      - None / "" / 0           -> None (no scheduled switch)
      - a number (or numeric    -> taken as unix epoch seconds
        string)
      - "HH:MM" / "HH:MM:SS"     -> the next future occurrence in local time
      - an ISO-8601 datetime     -> parsed in local time if it has no offset
        ("YYYY-MM-DDTHH:MM[:SS]")

    Returns None (with a warning) for anything unparseable, so a bad value never
    arms a switch at a surprising time.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        pass
    now = time.time() if now is None else now
    local_now = datetime.datetime.fromtimestamp(now)
    parts = s.split(":")
    if len(parts) in (2, 3) and all(p.isdigit() for p in parts):
        hh, mm = int(parts[0]), int(parts[1])
        ss = int(parts[2]) if len(parts) == 3 else 0
        if 0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60:
            cand = local_now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
            if cand.timestamp() <= now:
                cand += datetime.timedelta(days=1)  # already passed today -> tomorrow
            return cand.timestamp()
    try:
        return datetime.datetime.fromisoformat(s).timestamp()
    except ValueError:
        log.warning(f"scheduled switch time: could not parse {value!r}; ignoring")
        return None


def build_state(config_path: Path | None = None) -> AppState:
    """Load config and build the whole application state (no I/O loops yet).

    This is the old module-global bootstrap: limiter + metrics + persistent
    stats + pacer wired together, scheduled switches armed, and the persisted
    window restored (unless already elapsed).
    """
    path = config_path or default_config_path()
    cfg = load_config_file(path)
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        mtime = 0.0
    logging.getLogger().setLevel(str(cfg.get("log_level", "INFO")).upper())

    forced = cfg.get("force_tier") if cfg.get("force_tier") in ("low", "high") else None
    window_weights, default_window_weight = parse_window_weights(cfg)
    limiter = Limiter(
        low=make_tier(cfg, "low"),
        high=make_tier(cfg, "high"),
        initial_tier=cfg.get("initial_tier", "low"),
        promotion_cooldown=float(cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_weights=window_weights,
        default_window_weight=default_window_weight,
    )
    limiter.set_auto_params(
        concurrency_reserve=int(cfg.get("auto_concurrency_reserve", 0)),
        human_horizon=float(cfg.get("human_demand_horizon_seconds", 3600)),
    )
    limiter.set_daily_switch(parse_switch_time(cfg.get("scheduled_high_at")))
    limiter.set_daily_low_switch(parse_switch_time(cfg.get("scheduled_low_at")))
    pricing = cfg.get("model_pricing") or {}
    if not isinstance(pricing, dict):
        pricing = {}
    pstats = PersistentStats(
        path=str(cfg.get("stats_persist_path", "stats.json")),
        flush_seconds=float(cfg.get("stats_flush_seconds", 60.0)),
        retention_days=float(cfg.get("stats_retention_days", 120)),
        pricing=pricing,
    )
    metrics = Metrics(
        max_window_seconds=float(cfg["metrics_window_seconds"]),
        pricing=pricing,
        persist=pstats,
    )
    pacer = AutoPacer(limiter, metrics, cfg)
    state = AppState(
        config=cfg, config_path=path, config_mtime=mtime,
        limiter=limiter, metrics=metrics, pstats=pstats, pacer=pacer,
    )
    limiter.load_window_state(load_window_file(state.window_persist_path()))
    return state


async def apply_config_change(state: AppState, new_cfg: dict[str, Any]) -> None:
    """Apply a (re)loaded config to the live state.

    Everything hot-reloads except upstream_base_url / listen_host / the two
    listen ports (those need a restart).
    """
    if new_cfg.get("upstream_base_url") != state.config.get("upstream_base_url"):
        log.warning("config: upstream_base_url changed -> restart required")
    if (state.client is not None
            and new_cfg.get("upstream_timeout") != state.config.get("upstream_timeout")):
        import httpx
        state.client.timeout = httpx.Timeout(float(new_cfg["upstream_timeout"]), connect=15.0)
        log.info(f"config: upstream_timeout -> {new_cfg['upstream_timeout']}s")
    log_level = str(new_cfg.get("log_level", "INFO")).upper()
    logging.getLogger().setLevel(log_level)
    forced = new_cfg.get("force_tier") if new_cfg.get("force_tier") in ("low", "high") else None
    window_weights, default_window_weight = parse_window_weights(new_cfg)
    await state.limiter.update_tiers(
        low=make_tier(new_cfg, "low"),
        high=make_tier(new_cfg, "high"),
        promotion_cooldown=float(new_cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_weights=window_weights,
        default_window_weight=default_window_weight,
    )
    state.limiter.set_auto_params(
        concurrency_reserve=int(new_cfg.get("auto_concurrency_reserve", 0)),
        human_horizon=float(new_cfg.get("human_demand_horizon_seconds", 3600)),
    )
    state.limiter.set_daily_switch(parse_switch_time(new_cfg.get("scheduled_high_at")))
    state.limiter.set_daily_low_switch(parse_switch_time(new_cfg.get("scheduled_low_at")))
    state.pacer.configure(new_cfg)
    state.metrics.set_max_age(float(new_cfg["metrics_window_seconds"]))
    new_pricing = new_cfg.get("model_pricing") or {}
    new_pricing = new_pricing if isinstance(new_pricing, dict) else {}
    state.metrics.set_pricing(new_pricing)
    state.pstats.set_pricing(new_pricing)
    state.pstats.configure(
        flush_seconds=float(new_cfg.get("stats_flush_seconds", 60.0)),
        retention_days=float(new_cfg.get("stats_retention_days", 120)),
    )
    state.config = new_cfg
    log.info(f"config reloaded from {state.config_path} (force_tier={forced})")


async def config_watch_loop(state: AppState) -> None:
    """Poll the config file for changes and apply due scheduled tier switches."""
    while True:
        try:
            if state.config_path.exists():
                mt = state.config_path.stat().st_mtime
                if mt != state.config_mtime:
                    new_cfg = load_config_file(state.config_path)
                    await apply_config_change(state, new_cfg)
                    state.config_mtime = mt
            # Apply a due scheduled switch (cheap no-op when nothing is armed).
            # Polled here so the switch lands within config_poll_seconds of its
            # target time.
            if await state.limiter.apply_scheduled_switch():
                await asyncio.to_thread(save_window_file, state.limiter,
                                        state.window_persist_path())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"config reload failed: {e!r}")
        await asyncio.sleep(float(state.config.get("config_poll_seconds", 2.0)))
