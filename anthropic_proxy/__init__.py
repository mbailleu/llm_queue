"""anthropic_proxy — a two-lane queueing proxy for LLM APIs.

The pure modules (`limiter`, `pacer`, `metrics`, `usage`, `persistence`) import
without FastAPI. `app` / `serve` are resolved lazily so importing those pure
modules (e.g. in tests) doesn't pull in the web stack.
"""
from __future__ import annotations

from typing import Any

__all__ = ["app", "serve"]


def __getattr__(name: str) -> Any:  # PEP 562 lazy attribute access
    if name in ("app", "serve"):
        from . import server
        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
