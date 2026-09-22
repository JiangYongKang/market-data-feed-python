"""Normalizers for structurally different upstream record formats."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from .enums import RejectReason, SourceId
from .models import Quote, RejectedRecord


def _reject(
    source: SourceId | None,
    symbol: str | None,
    reason: RejectReason,
    detail: str,
    raw: Any,
    received_time: datetime,
) -> RejectedRecord:
    return RejectedRecord(
        source=source,
        symbol=symbol,
        reason=reason,
        detail=detail,
        raw_record=raw,
        received_time=received_time,
    )


def _symbol_from(raw: Any, key: str) -> str | None:
    if isinstance(raw, dict):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _exact_decimal(value: Any, field: str, *, allow_float: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field} must not be boolean")
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, str) and value.strip():
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"{field} is not a valid decimal") from exc
    elif isinstance(value, float) and allow_float:
        # Decimal(float) exposes the binary float value rather than the shorter
        # decimal text a caller may have intended; concrete source normalizers
        # opt in only when that source documents binary doubles.
        return Decimal(value)
    else:
        raise ValueError(f"{field} must be an integer or non-empty decimal string")
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


def _positive_decimal(value: Any, field: str) -> Decimal:
    result = _exact_decimal(value, field)
    if result <= 0:
        raise ValueError(f"{field} must be greater than zero")
    return result


def _non_negative_decimal(value: Any, field: str) -> Decimal:
    result = _exact_decimal(value, field)
    if result < 0:
        raise ValueError(f"{field} must not be negative")
    return result


def _optional_non_negative_decimal(value: Any, field: str) -> Decimal | None:
    if value is None:
        return None
    return _non_negative_decimal(value, field)


def _nonnegative_int(value: Any, field: str, *, accept_digits: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must not be boolean")
    if isinstance(value, int):
        result = value
    elif accept_digits and isinstance(value, str) and value.isdigit():
        result = int(value)
    else:
        raise ValueError(f"{field} must be a non-negative integer")
    if result < 0:
        raise ValueError(f"{field} must not be negative")
    return result


def _positive_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()



class Normalizer(Protocol):
    source: SourceId

    def normalize(self, raw: Any, received_time: datetime) -> Quote | RejectedRecord:
        """Convert one source record into the canonical model."""


class SourceANormalizer:
    """JSON-like source A.

    Field names: sym/seq/ts_ms/px/qty/trade_id. Time is integer UTC
    milliseconds. Price and quantity must arrive as integers or decimal text,
    never as a JSON binary floating-point value.
    """

    source = SourceId.SOURCE_A

    def normalize(self, raw: Any, received_time: datetime) -> Quote | RejectedRecord:
        symbol = _symbol_from(raw, "sym")
        if not isinstance(raw, dict):
            return _reject(self.source, None, RejectReason.MALFORMED_RECORD, "record must be an object", raw, received_time)
        try:
            record_id = _positive_text(raw.get("trade_id"), "trade_id")
            symbol = _positive_text(raw.get("sym"), "sym")
            sequence = _nonnegative_int(raw.get("seq"), "seq")
            ts_ms = _nonnegative_int(raw.get("ts_ms"), "ts_ms")
            price = _positive_decimal(raw.get("px"), "px")
            quantity = _non_negative_decimal(raw.get("qty"), "qty")
        except ValueError as exc:
            message = str(exc)
            if message.startswith("ts_ms"):
                reason = RejectReason.INVALID_TIMESTAMP
            elif message.startswith("seq"):
                reason = RejectReason.INVALID_SEQUENCE
            elif message.startswith(("px", "qty")):
                reason = RejectReason.INVALID_NUMBER
            elif message.startswith(("trade_id", "sym")):
                reason = RejectReason.MISSING_FIELD
            else:
                reason = RejectReason.MALFORMED_RECORD
            return _reject(self.source, symbol, reason, message, raw, received_time)

        event_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return Quote(
            source=self.source,
            symbol=symbol,
            sequence=sequence,
            event_time=event_time,
            received_time=received_time,
            price=price,
            quantity=quantity,
            raw_record=raw,
            record_id=record_id,
        )


class SourceBNormalizer:
    """JSON-like source B.

    Field names: ticker/sequence/event_time/price_scaled/price_scale/size/id.
    Time is ISO-8601 and must carry an offset; it is converted to UTC. Price
    is an explicit fixed-point integer so JSON doubles are never involved.
    """

    source = SourceId.SOURCE_B

    def normalize(self, raw: Any, received_time: datetime) -> Quote | RejectedRecord:
        symbol = _symbol_from(raw, "ticker")
        if not isinstance(raw, dict):
            return _reject(self.source, None, RejectReason.MALFORMED_RECORD, "record must be an object", raw, received_time)
        try:
            record_id = _positive_text(raw.get("id"), "id")
            symbol = _positive_text(raw.get("ticker"), "ticker")
            sequence = _nonnegative_int(raw.get("sequence"), "sequence", accept_digits=True)
            event_text = _positive_text(raw.get("event_time"), "event_time")
            parsed_time = datetime.fromisoformat(event_text.replace("Z", "+00:00"))
            if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
                raise ValueError("event_time must include a UTC offset; source-local naive time is rejected")
            scaled = _nonnegative_int(raw.get("price_scaled"), "price_scaled")
            scale = _nonnegative_int(raw.get("price_scale"), "price_scale")
            if scale > 28:
                raise ValueError("price_scale must be at most 28")
            price_value = Decimal(scaled).scaleb(-scale)
            if not price_value.is_finite():
                raise ValueError("price must be finite")
            if price_value <= 0:
                raise ValueError("price must be greater than zero")
            quantity = _optional_non_negative_decimal(raw.get("size"), "size")
        except ValueError as exc:
            message = str(exc)
            if message.startswith("event_time"):
                reason = RejectReason.INVALID_TIMESTAMP
            elif message.startswith("sequence"):
                reason = RejectReason.INVALID_SEQUENCE
            elif message.startswith(("price", "size")):
                reason = RejectReason.INVALID_NUMBER
            elif message.startswith(("id", "ticker")):
                reason = RejectReason.MISSING_FIELD
            else:
                reason = RejectReason.MALFORMED_RECORD
            return _reject(self.source, symbol, reason, message, raw, received_time)

        event_time = parsed_time.astimezone(timezone.utc)
        return Quote(
            source=self.source,
            symbol=symbol,
            sequence=sequence,
            event_time=event_time,
            received_time=received_time,
            price=price_value,
            quantity=quantity,
            raw_record=raw,
            record_id=record_id,
        )


class NormalizationRouter:
    """Select a source-specific normalizer."""

    def __init__(self) -> None:
        normalizers: list[Normalizer] = [SourceANormalizer(), SourceBNormalizer()]
        self._normalizers = {item.source: item for item in normalizers}

    def normalize(self, source: SourceId, raw: Any, received_time: datetime) -> Quote | RejectedRecord:
        normalizer = self._normalizers.get(source)
        if normalizer is None:
            return _reject(
                None,
                _symbol_from(raw, "sym") or _symbol_from(raw, "ticker"),
                RejectReason.UNKNOWN_SOURCE,
                f"unsupported source: {source}",
                raw,
                received_time,
            )
        return normalizer.normalize(raw, received_time)
