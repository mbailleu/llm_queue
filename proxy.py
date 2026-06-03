# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "fastapi>=0.110",
#     "httpx>=0.27",
#     "uvicorn>=0.29",
#     "pyyaml>=6.0",
# ]
# ///
"""Compatibility shim so `python proxy.py` / `uv run proxy.py` keep working.

The implementation now lives in the ``anthropic_proxy`` package; this just
re-exports the ASGI app (for `uvicorn proxy:app`) and runs both lanes.
"""
from __future__ import annotations

import asyncio

from anthropic_proxy.server import app, serve  # noqa: F401  (app used by uvicorn)

if __name__ == "__main__":
    asyncio.run(serve())
