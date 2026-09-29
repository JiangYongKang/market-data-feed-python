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
    # When set, deliver a bounded snapshot of the most recent confirmed quotes
    # ahead of the live stream.
    snapshot: int | None = Field(default=None, ge=1, le=10_000)
    # When set, resume strictly after this last-processed sequence.
    resume_from_sequence: int | None = Field(default=None, ge=0)
    wait_for_consistency: bool = False
    consistency_timeout: float = Field(default=2.0, ge=0.0, le=60.0)


class UnsubscribeRequest(BaseModel):
    subscriber_id: str = Field(min_length=1)
    source: SourceId
    symbol: str = Field(min_length=1)


class IngestRequest(BaseModel):
    record: dict[str, Any]


class ReconnectRequest(BaseModel):
    replay_records: list[dict[str, Any]] = Field(default_factory=list)
