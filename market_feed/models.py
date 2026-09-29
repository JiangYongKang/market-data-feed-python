from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from .enums import (
    BackpressurePolicy,
    DeliveryKind,
    GapState,
    HandoffMode,
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
    handoff: "HandoffEnvelope | None" = None


@dataclass(frozen=True, slots=True)
class SubscriberOptions:
    queue_size: int = 8
    backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK
    slow_callback_delay: float = 0.0


@dataclass(frozen=True, slots=True)
class ResumePosition:
    """A consumer-side checkpoint: the last sequence it fully processed."""

    source: SourceId
    symbol: str
    sequence: int

    @property
    def key(self) -> SubscriptionKey:
        return SubscriptionKey(self.source, self.symbol)


@dataclass(frozen=True, slots=True)
class HistoryStats:
    """Observable retention state for one (source, symbol)."""

    key: SubscriptionKey
    retained_count: int
    capacity: int
    oldest_sequence: int | None
    newest_sequence: int | None
    evicted_total: int
    # When set, retained history starts at this barrier sequence (exclusive
    # lower bound); records before it belong to an invalidated pre-gap region.
    barrier_sequence: int | None
    # False while the key is currently inside a recoverable gap window.
    complete: bool


@dataclass(frozen=True, slots=True)
class HandoffEnvelope:
    """Bounded historical view handed to a subscriber ahead of the live stream.

    ``quotes`` are delivered as events preceding this envelope? See delivery
    order in the service: each quote is enqueued first, then the envelope acts
    as the boundary marker. ``live_from_sequence`` is the first sequence that
    may arrive on the normal stream (already enqueued quotes stop strictly
    below it), so a subscriber never needs its own de-duplication.
    """

    mode: HandoffMode
    key: SubscriptionKey
    quotes: tuple[Quote, ...]
    # The checkpoint the replay was based on (resume mode) / None for snapshot.
    resume_from_sequence: int | None
    live_from_sequence: int | None
    complete: bool
    stats: HistoryStats
    detail: str = ""
