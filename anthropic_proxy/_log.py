"""Shared logger for the anthropic_proxy package."""
from __future__ import annotations

import logging

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("proxy")
