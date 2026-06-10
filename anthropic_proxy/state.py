"""AppState: the one object holding everything the running proxy needs.

Replaces the former module globals (config/limiter/metrics/pstats/pacer/
client). The FastAPI app stores it on `app.state.proxy`; routes and the proxy
handler read it from there, which is what makes the other modules importable
and testable without a running server.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from .limiter import Limiter
from .metrics import Metrics
from .pacer import AutoPacer
from .persistence import PersistentStats


@dataclass
class AppState:
    config: dict[str, Any]
    config_path: Path
    limiter: Limiter
    metrics: Metrics
    pstats: PersistentStats
    pacer: AutoPacer
    config_mtime: float = 0.0
    # Created in startup() / closed in shutdown(); shared by both lanes' ports.
    client: httpx.AsyncClient | None = None
    bg_tasks: list[asyncio.Task] = field(default_factory=list)

    def window_persist_path(self) -> Path:
        return Path(self.config.get("window_persist_path", "window.json")).resolve()
