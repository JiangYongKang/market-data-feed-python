"""Structured logging helpers.

Logs are emitted as single-line JSON so test output can contain both the
raw input and the deterministic reason for a decision.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=str)
    if hasattr(value, "__dict__"):
        return value.__dict__
    return repr(value)


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the package logger."""
    logger = logging.getLogger("market_feed")
    logger.setLevel(level)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def event_log(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Write one structured event log."""
    payload: dict[str, Any] = {"event": event}
    payload.update(fields)
    logger.info(json.dumps(payload, ensure_ascii=False, default=_json_default, sort_keys=True))
