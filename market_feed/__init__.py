"""Deterministic, in-memory multi-source market-data feed."""

from .history import HistoryStore
from .models import (
    HandoffEnvelope,
    HistoryStats,
    ResumePosition,
)

__all__ = [
    "__version__",
    "HistoryStore",
    "HandoffEnvelope",
    "HistoryStats",
    "ResumePosition",
]
__version__ = "0.2.0"
