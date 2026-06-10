# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "fastapi>=0.110",
#     "httpx>=0.27",
#     "uvicorn>=0.29",
#     "pyyaml>=6.0",
# ]
# ///
"""Thin shim — the application lives in the anthropic_proxy package.

Kept so the documented entry points keep working:
  python proxy.py        (or: uv run proxy.py — the PEP 723 header above)
  uvicorn proxy:app      (single port, human lane only)
Equivalent package entry point: python -m anthropic_proxy
"""
from anthropic_proxy.server import app, main, serve, state  # noqa: F401

if __name__ == "__main__":
    main()
