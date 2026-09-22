"""HTTP request/response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .enums import BackpressurePolicy, SourceId


class SubscribeRequest(BaseModel):
    subscriber_id: str = Field(min_length=1)
    source: SourceId
    symbol: str = Field(min_length=1)
    queue_size: int = Field(default=8, ge=1, le=10_000)
    backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK


class UnsubscribeRequest(BaseModel):
    subscriber_id: str = Field(min_length=1)
    source: SourceId
    symbol: str = Field(min_length=1)


class IngestRequest(BaseModel):
    record: dict[str, Any]


class ReconnectRequest(BaseModel):
    replay_records: list[dict[str, Any]] = Field(default_factory=list)
