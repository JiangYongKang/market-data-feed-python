from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from .enums import (
    BackpressurePolicy,
    DeliveryKind,
    GapState,
    IngestStatus,
    RecordDisposition,
    RejectReason,
    SourceId,
    SubscriptionErrorKind,
)


@dataclass(frozen=True, slots=True)
class SubscriptionKey:
    source: SourceId
    symbol: str


@dataclass(frozen=True, slots=True)
class Quote:
    source: SourceId
    symbol: str
    sequence: int
    event_time: datetime
    received_time: datetime
    price: Decimal
    quantity: Decimal | None
    raw_record: dict[str, Any]
    record_id: str


@dataclass(frozen=True, slots=True)
class RejectedRecord:
    source: SourceId | None
    symbol: str | None
    reason: RejectReason
    detail: str
    raw_record: Any
    received_time: datetime


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    key: SubscriptionKey | None = None
    quote: Quote | None = None
    rejection: RejectedRecord | None = None
    expected_sequence: int | None = None
    actual_sequence: int | None = None
    detail: str = ""


@dataclass(frozen=True, slots=True)
class SequenceDecision:
    disposition: RecordDisposition
    release: tuple[Quote, ...]
    gap_events: tuple[GapEvent, ...]
    accepted_as_latest: bool
    detail: str


@dataclass(frozen=True, slots=True)
class SequenceState:
    key: SubscriptionKey
    state: GapState
    last_sequence: int | None
    expected_sequence: int | None
    last_event_time: datetime | None
    missing_from: int | None = None
    missing_to: int | None = None
    buffered_count: int = 0


@dataclass(frozen=True, slots=True)
class GapEvent:
    key: SubscriptionKey
    state: GapState
    missing_from: int
    missing_to: int | None
    detected_time: datetime
    missing_sequences: tuple[int, ...] = field(default_factory=tuple)
    recovered_sequences: tuple[int, ...] = field(default_factory=tuple)
    detail: str = ""


@dataclass(frozen=True, slots=True)
class DeliveryEvent:
    kind: DeliveryKind
    timestamp: datetime
    payload: Any


@dataclass(frozen=True, slots=True)
class DropAudit:
    subscriber_id: str
    policy: BackpressurePolicy
    dropped_kind: DeliveryKind
    reason: str
    sequence: int | None
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class SubscriptionResult:
    ok: bool
    subscriber_id: str
    key: SubscriptionKey | None
    kind: SubscriptionErrorKind | None
    detail: str
    changed: bool


@dataclass(frozen=True, slots=True)
class SubscriberOptions:
    queue_size: int = 8
    backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK
    slow_callback_delay: float = 0.0
