"""Controllable, local-only market data sources."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, Sequence

from .enums import SourceId


class MarketDataSource(Protocol):
    source_id: SourceId
    connected: bool

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def fetch_batch(self) -> Sequence[Any]: ...


class ScriptedSource:
    """A deterministic in-process source driven by a list of records.

    Records are delivered in order, but callers may seek the cursor to emulate
    replay after disconnect/reconnect. The source never performs network I/O.
    """

    def __init__(self, source_id: SourceId, records: list[Any] | None = None) -> None:
        self.source_id = source_id
        self._records: list[Any] = list(records or [])
        self._cursor = 0
        self.connected = False
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._lock:
            self.connected = True

    async def disconnect(self) -> None:
        async with self._lock:
            self.connected = False

    async def fetch_batch(self, batch_size: int | None = None) -> Sequence[Any]:
        async with self._lock:
            if not self.connected:
                raise RuntimeError(f"source {self.source_id.value} is disconnected")
            end = len(self._records) if batch_size is None else min(self._cursor + batch_size, len(self._records))
            batch = list(self._records[self._cursor:end])
            self._cursor = end
            return batch

    def append(self, record: Any) -> None:
        self._records.append(record)

    def seek(self, index: int) -> None:
        if not 0 <= index <= len(self._records):
            raise IndexError("cursor is outside the scripted record range")
        self._cursor = index

    def seek_sequence(self, sequence_field: str, sequence: int) -> bool:
        for index, record in enumerate(self._records):
            if isinstance(record, dict) and record.get(sequence_field) == sequence:
                self._cursor = index
                return True
        return False

    @property
    def cursor(self) -> int:
        return self._cursor

    @property
    def record_count(self) -> int:
        return len(self._records)
