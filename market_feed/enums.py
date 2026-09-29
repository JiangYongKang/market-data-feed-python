from enum import Enum


class SourceId(str, Enum):
    SOURCE_A = "source_a"
    SOURCE_B = "source_b"


class RejectReason(str, Enum):
    MALFORMED_RECORD = "malformed_record"
    UNKNOWN_SOURCE = "unknown_source"
    UNKNOWN_SYMBOL = "unknown_symbol"
    MISSING_FIELD = "missing_field"
    INVALID_NUMBER = "invalid_number"
    INVALID_TIMESTAMP = "invalid_timestamp"
    NON_UTC_TIMESTAMP = "non_utc_timestamp"
    INVALID_SEQUENCE = "invalid_sequence"
    ILLEGAL_PRICE = "illegal_price"
    UNSUPPORTED_RECORD = "unsupported_record"


class SubscriptionErrorKind(str, Enum):
    UNKNOWN_SOURCE = "unknown_source"
    UNKNOWN_SYMBOL = "unknown_symbol"
    INVALID_SUBSCRIBER = "invalid_subscriber"
    ALREADY_SUBSCRIBED = "already_subscribed"
    NOT_SUBSCRIBED = "not_subscribed"


class RecordDisposition(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    STALE = "stale"
    BUFFERED = "buffered"
    RECOVERED = "recovered"
    GAP_DETECTED = "gap_detected"
    GAP_RESOLVED = "gap_resolved"
    GAP_UNRECOVERABLE = "gap_unrecoverable"
    REJECTED = "rejected"


class GapState(str, Enum):
    NORMAL = "normal"
    RECOVERING = "recovering"
    UNRECOVERABLE = "unrecoverable"


class BackpressurePolicy(str, Enum):
    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"
    COALESCE_LATEST = "coalesce_latest"
    REJECT = "reject"


class DeliveryKind(str, Enum):
    QUOTE = "quote"
    RECOVERED_QUOTE = "recovered_quote"
    REJECTION = "rejection"
    GAP = "gap"
    SUBSCRIPTION = "subscription"
    DROPPED = "dropped"
    COALESCED = "coalesced"
    HISTORY_BOUNDARY = "history_boundary"


class HistoryJoinStatus(str, Enum):
    """Outcome of a snapshot/resume request at subscription time."""

    SNAPSHOT_READY = "snapshot_ready"
    RESUME_READY = "resume_ready"
    GAP_PENDING = "gap_pending"
    UNRECOVERABLE_GAP = "unrecoverable_gap"
    CHECKPOINT_TOO_OLD = "checkpoint_too_old"
    CHECKPOINT_AHEAD = "checkpoint_ahead"
    ALREADY_SUBSCRIBED = "already_subscribed"
    REJECTED = "rejected"


class IngestStatus(str, Enum):
    OK = "ok"
    REJECTED = "rejected"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    STALE = "stale"
    BUFFERED = "buffered"
    RECOVERED = "recovered"
    GAP = "gap"
