"""Shared runtime state + config loading/hot-reload.

Owns the process-wide mutable state (config, limiter, metrics, pstats, pacer,
client) in one module instead of scattered globals, plus everything that builds
or reloads it. server.py / routes.py reference these as ``runtime.<name>``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
import yaml

from ._log import log
from .limiter import Tier, Limiter
from .metrics import Metrics
from .persistence import PersistentStats
from .pacer import AutoPacer


CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config.yaml")).resolve()

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

RATE_LIMIT_STATUSES = {429, 503, 529}
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
    "content-encoding",  # stripped because we forward already-decoded chunks
}


config: dict[str, Any] = {}
config_mtime: float = 0.0
limiter: Limiter | None = None
metrics: Metrics | None = None
pstats: PersistentStats | None = None
pacer: AutoPacer | None = None
client: httpx.AsyncClient | None = None


def load_config_file() -> dict[str, Any]:
    merged: dict[str, Any] = {**DEFAULT_CONFIG, "tiers": dict(DEFAULT_CONFIG["tiers"])}
    if not CONFIG_PATH.exists():
        log.warning(f"config file {CONFIG_PATH} not found; using defaults")
        return merged
    with open(CONFIG_PATH) as f:
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


def init_from_config(cfg: dict[str, Any]) -> tuple[Limiter, Metrics, PersistentStats, AutoPacer]:
    forced = cfg.get("force_tier") if cfg.get("force_tier") in ("low", "high") else None
    window_weights, default_window_weight = parse_window_weights(cfg)
    lim = Limiter(
        low=make_tier(cfg, "low"),
        high=make_tier(cfg, "high"),
        initial_tier=cfg.get("initial_tier", "low"),
        promotion_cooldown=float(cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_weights=window_weights,
        default_window_weight=default_window_weight,
    )
    lim.set_auto_params(
        concurrency_reserve=int(cfg.get("auto_concurrency_reserve", 0)),
        human_horizon=float(cfg.get("human_demand_horizon_seconds", 3600)),
    )
    pricing = cfg.get("model_pricing") or {}
    if not isinstance(pricing, dict):
        pricing = {}
    ps = PersistentStats(
        path=str(cfg.get("stats_persist_path", "stats.json")),
        flush_seconds=float(cfg.get("stats_flush_seconds", 60.0)),
        retention_days=float(cfg.get("stats_retention_days", 120)),
        pricing=pricing,
    )
    met = Metrics(
        max_window_seconds=float(cfg["metrics_window_seconds"]),
        pricing=pricing,
        persist=ps,
    )
    pc = AutoPacer(lim, met, cfg)
    return lim, met, ps, pc


async def apply_config_change(new_cfg: dict[str, Any]) -> None:
    global config
    if new_cfg.get("upstream_base_url") != config.get("upstream_base_url"):
        log.warning("config: upstream_base_url changed -> restart required")
    log_level = str(new_cfg.get("log_level", "INFO")).upper()
    logging.getLogger().setLevel(log_level)
    forced = new_cfg.get("force_tier") if new_cfg.get("force_tier") in ("low", "high") else None
    window_weights, default_window_weight = parse_window_weights(new_cfg)
    await limiter.update_tiers(
        low=make_tier(new_cfg, "low"),
        high=make_tier(new_cfg, "high"),
        promotion_cooldown=float(new_cfg["promotion_cooldown_seconds"]),
        forced=forced,
        window_weights=window_weights,
        default_window_weight=default_window_weight,
    )
    limiter.set_auto_params(
        concurrency_reserve=int(new_cfg.get("auto_concurrency_reserve", 0)),
        human_horizon=float(new_cfg.get("human_demand_horizon_seconds", 3600)),
    )
    pacer.configure(new_cfg)
    metrics.set_max_age(float(new_cfg["metrics_window_seconds"]))
    new_pricing = new_cfg.get("model_pricing") or {}
    new_pricing = new_pricing if isinstance(new_pricing, dict) else {}
    metrics.set_pricing(new_pricing)
    pstats.set_pricing(new_pricing)
    pstats.configure(
        flush_seconds=float(new_cfg.get("stats_flush_seconds", 60.0)),
        retention_days=float(new_cfg.get("stats_retention_days", 120)),
    )
    config = new_cfg
    log.info(f"config reloaded from {CONFIG_PATH} (force_tier={forced})")


async def config_watch_loop() -> None:
    global config_mtime
    while True:
        try:
            if CONFIG_PATH.exists():
                mt = CONFIG_PATH.stat().st_mtime
                if mt != config_mtime:
                    new_cfg = load_config_file()
                    await apply_config_change(new_cfg)
                    config_mtime = mt
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"config reload failed: {e!r}")
        await asyncio.sleep(float(config.get("config_poll_seconds", 2.0)))


def _window_persist_path() -> Path:
    return Path(config.get("window_persist_path", "window.json")).resolve()


def load_window_file() -> dict[str, Any] | None:
    """Read the persisted rolling-window state (count + start), if any."""
    p = _window_persist_path()
    try:
        if not p.exists():
            return None
        with open(p) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, ValueError) as e:
        log.warning(f"window: could not load {p}: {e!r}")
        return None


def save_window_file() -> None:
    """Persist the current rolling-window state for the next restart.

    Skips writing when no window is active (nothing worth restoring). Written
    atomically via a temp file + os.replace.
    """
    state = limiter.window_state()
    p = _window_persist_path()
    if state.get("started_at") is None:
        # No active window — clear any stale file so a restart starts fresh.
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass
        return
    tmp = p.with_name(p.name + ".tmp")
    try:
        tmp.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(state, f, separators=(",", ":"))
        os.replace(tmp, p)
    except OSError as e:
        log.warning(f"window: save to {p} failed: {e!r}")


async def persist_loop() -> None:
    """Flush aggregated stats + window state to disk on a fixed interval."""
    while True:
        try:
            await pstats.maybe_flush()
            await asyncio.to_thread(save_window_file)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"stats: periodic flush error: {e!r}")


def bootstrap() -> None:
    """Populate the runtime state from config on process start."""
    global config, config_mtime, limiter, metrics, pstats, pacer
    config = load_config_file()
    try:
        config_mtime = CONFIG_PATH.stat().st_mtime
    except FileNotFoundError:
        config_mtime = 0.0
    logging.getLogger().setLevel(str(config.get("log_level", "INFO")).upper())
    limiter, metrics, pstats, pacer = init_from_config(config)
    limiter.load_window_state(load_window_file())
