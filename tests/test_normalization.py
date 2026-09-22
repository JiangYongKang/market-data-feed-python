from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from market_feed.enums import RejectReason, SourceId
from market_feed.models import Quote, RejectedRecord
from market_feed.normalization import NormalizationRouter


def received():
    return datetime(2026, 9, 22, tzinfo=timezone.utc)


def test_source_a_decimal_text_and_millisecond_time_normalize_to_utc():
    router = NormalizationRouter()
    raw = {"trade_id": "a-1", "sym": "AAA", "seq": 7, "ts_ms": 1_700_000_000_123, "px": "12.3400", "qty": 100}
    result = router.normalize(SourceId.SOURCE_A, raw, received())

    assert isinstance(result, Quote)
    assert result.source is SourceId.SOURCE_A
    assert result.symbol == "AAA"
    assert result.sequence == 7
    assert result.price == Decimal("12.3400")
    assert str(result.price) == "12.3400"
    assert result.quantity == Decimal(100)
    assert result.event_time.tzinfo.utcoffset(result.event_time) == timedelta(0)
    assert result.record_id == "a-1"


def test_source_b_fixed_point_price_and_offset_time_normalize_to_utc():
    router = NormalizationRouter()
    raw = {
        "id": "b-9",
        "ticker": "BBB",
        "sequence": "9",
        "event_time": "2026-09-22T10:00:00.123456+02:00",
        "price_scaled": 12345,
        "price_scale": 4,
        "size": "0.250",
    }
    result = router.normalize(SourceId.SOURCE_B, raw, received())

    assert isinstance(result, Quote)
    assert result.price == Decimal("1.2345")
    assert result.quantity == Decimal("0.250")
    assert result.event_time == datetime(2026, 9, 22, 8, 0, 0, 123456, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("source", "raw", "reason"),
    [
        (SourceId.SOURCE_A, [], RejectReason.MALFORMED_RECORD),
        (SourceId.SOURCE_A, {"trade_id": "", "sym": "AAA", "seq": 1, "ts_ms": 1, "px": "1", "qty": "1"}, RejectReason.MISSING_FIELD),
        (SourceId.SOURCE_A, {"trade_id": "x", "sym": "AAA", "seq": 1, "ts_ms": 1, "px": 1.25, "qty": "1"}, RejectReason.INVALID_NUMBER),
        (SourceId.SOURCE_A, {"trade_id": "x", "sym": "AAA", "seq": 1, "ts_ms": 1, "px": "0", "qty": "1"}, RejectReason.INVALID_NUMBER),
        (SourceId.SOURCE_A, {"trade_id": "x", "sym": "AAA", "seq": -1, "ts_ms": 1, "px": "1", "qty": "1"}, RejectReason.INVALID_SEQUENCE),
        (SourceId.SOURCE_B, {"id": "x", "ticker": "AAA", "sequence": 1, "event_time": "2026-09-22T10:00:00", "price_scaled": 1, "price_scale": 2, "size": "1"}, RejectReason.INVALID_TIMESTAMP),
        (SourceId.SOURCE_B, {"id": "x", "ticker": "AAA", "sequence": "x", "event_time": "2026-09-22T10:00:00Z", "price_scaled": 1, "price_scale": 2, "size": "1"}, RejectReason.INVALID_SEQUENCE),
    ],
)
def test_malformed_or_illegal_records_have_distinct_reasons(source, raw, reason):
    result = NormalizationRouter().normalize(source, raw, received())
    assert isinstance(result, RejectedRecord)
    assert result.reason is reason
    assert result.detail
    assert result.raw_record is raw


def test_source_local_time_without_offset_is_never_interpreted_as_global_time():
    raw = {
        "id": "x",
        "ticker": "AAA",
        "sequence": 1,
        "event_time": "2026-09-22T10:00:00",
        "price_scaled": 1,
        "price_scale": 2,
        "size": "1",
    }
    result = NormalizationRouter().normalize(SourceId.SOURCE_B, raw, received())
    assert isinstance(result, RejectedRecord)
    assert "offset" in result.detail


def test_source_b_nullable_size_normalizes_to_none_without_affecting_price():
    raw = {
        "id": "b-null",
        "ticker": "BBB",
        "sequence": 10,
        "event_time": "2026-09-22T08:00:00Z",
        "price_scaled": 12345,
        "price_scale": 4,
        "size": None,
    }
    result = NormalizationRouter().normalize(SourceId.SOURCE_B, raw, received())
    assert isinstance(result, Quote)
    assert result.price == Decimal("1.2345")
    assert result.quantity is None


def test_missing_required_identifier_has_missing_field_reason():
    raw = {"sym": "AAA", "seq": 1, "ts_ms": 1, "px": "1", "qty": "1"}
    result = NormalizationRouter().normalize(SourceId.SOURCE_A, raw, received())
    assert isinstance(result, RejectedRecord)
    assert result.reason is RejectReason.MISSING_FIELD


def test_source_a_non_numeric_timestamp_is_timestamp_error():
    raw = {"trade_id": "x", "sym": "AAA", "seq": 1, "ts_ms": "soon", "px": "1", "qty": "1"}
    result = NormalizationRouter().normalize(SourceId.SOURCE_A, raw, received())
    assert isinstance(result, RejectedRecord)
    assert result.reason is RejectReason.INVALID_TIMESTAMP


def test_source_b_missing_size_also_normalizes_to_none():
    raw = {
        "id": "b-missing-size",
        "ticker": "BBB",
        "sequence": 11,
        "event_time": "2026-09-22T08:00:00Z",
        "price_scaled": 200,
        "price_scale": 2,
    }
    result = NormalizationRouter().normalize(SourceId.SOURCE_B, raw, received())
    assert isinstance(result, Quote)
    assert result.price == Decimal("2.00")
    assert result.quantity is None
