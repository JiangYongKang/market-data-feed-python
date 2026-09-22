"""行情来源适配器：将结构不同的原始记录归一化为统一模型。

- SourceA：字段 sym/seq/ts_ms/px/qty，毫秒 epoch 时间戳（UTC 语义），
  价格为字符串，qty 可空；
- SourceB：字段 instrument/sequenceNumber/eventTime/price/volume，
  ISO 本地时间（无时区，按来源配置的本地时区解释，绝不当作全局 UTC），
  价格为浮点（经 repr 转 Decimal 避免精度漂移），volume 可空。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from market_feed.errors import FeedError, NormalizationError
from market_feed.models import NormalizedTick, RejectReason


def _require(raw: Mapping[str, Any], field: str) -> Any:
    value = raw.get(field)
    if value is None:
        raise NormalizationError(RejectReason.MISSING_FIELD, f"缺少字段 {field!r}")
    return value


def _to_decimal(value: Any, field: str) -> Decimal:
    """严格转 Decimal：拒绝 bool/NaN/Inf，浮点经 repr 转换避免精度漂移。"""
    if isinstance(value, bool):
        raise NormalizationError(RejectReason.INVALID_PRICE, f"{field} 非法: {value!r}")
    try:
        if isinstance(value, float):
            dec = Decimal(repr(value))
        else:
            dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise NormalizationError(
            RejectReason.INVALID_PRICE, f"{field} 无法解析: {value!r}"
        ) from exc
    if not dec.is_finite():
        raise NormalizationError(RejectReason.INVALID_PRICE, f"{field} 非有限值: {value!r}")
    return dec


def _to_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and value.strip().lstrip("-").isdigit():
            return int(value.strip())
        raise NormalizationError(
            RejectReason.INVALID_SEQUENCE, f"{field} 非整数: {value!r}"
        )
    return value


class SourceAdapter:
    """来源适配器基类。"""

    name: str = ""

    def normalize(self, raw: Mapping[str, Any]) -> NormalizedTick:
        raise NotImplementedError

    def _raw_id(self, raw: Mapping[str, Any]) -> str:
        return f"{self.name}:{raw!r}"[:200]


class SourceAAdapter(SourceAdapter):
    """来源 A：毫秒 epoch（UTC），价格字符串，qty 可空。"""

    name = "source_a"

    def normalize(self, raw: Mapping[str, Any]) -> NormalizedTick:
        if not isinstance(raw, Mapping):
            raise NormalizationError(RejectReason.MALFORMED_RECORD, "记录不是键值结构")
        symbol = _require(raw, "sym")
        if not isinstance(symbol, str) or not symbol:
            raise NormalizationError(RejectReason.MALFORMED_RECORD, f"sym 非法: {symbol!r}")
        seq = _to_int(_require(raw, "seq"), "seq")
        ts_ms = _to_int(_require(raw, "ts_ms"), "ts_ms")
        try:
            event_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise NormalizationError(
                RejectReason.INVALID_TIMESTAMP, f"ts_ms 非法: {ts_ms!r}"
            ) from exc
        price = _to_decimal(_require(raw, "px"), "px")
        if price <= 0:
            raise NormalizationError(RejectReason.INVALID_PRICE, f"px 非正数: {price}")
        qty_raw = raw.get("qty")
        volume = _to_decimal(qty_raw, "qty") if qty_raw is not None else None
        return NormalizedTick(
            source=self.name,
            symbol=symbol,
            sequence=seq,
            event_time=event_time,
            price=price,
            volume=volume,
            raw_id=self._raw_id(raw),
        )


class SourceBAdapter(SourceAdapter):
    """来源 B：ISO 本地时间（按配置时区解释），价格浮点，volume 可空。"""

    name = "source_b"

    def __init__(self, local_utc_offset_hours: int = 8) -> None:
        self.local_tz = timezone(
            timedelta(hours=local_utc_offset_hours), name=f"UTC{local_utc_offset_hours:+d}"
        )

    def normalize(self, raw: Mapping[str, Any]) -> NormalizedTick:
        if not isinstance(raw, Mapping):
            raise NormalizationError(RejectReason.MALFORMED_RECORD, "记录不是键值结构")
        symbol = _require(raw, "instrument")
        if not isinstance(symbol, str) or not symbol:
            raise NormalizationError(
                RejectReason.MALFORMED_RECORD, f"instrument 非法: {symbol!r}"
            )
        seq = _to_int(_require(raw, "sequenceNumber"), "sequenceNumber")
        ts_raw = _require(raw, "eventTime")
        if not isinstance(ts_raw, str):
            raise NormalizationError(
                RejectReason.INVALID_TIMESTAMP, f"eventTime 非字符串: {ts_raw!r}"
            )
        try:
            parsed = datetime.fromisoformat(ts_raw)
        except ValueError as exc:
            raise NormalizationError(
                RejectReason.INVALID_TIMESTAMP, f"eventTime 无法解析: {ts_raw!r}"
            ) from exc
        if parsed.tzinfo is None:
            # 来源本地时间 -> 按来源时区解释后统一转 UTC，绝不当作全局时间
            parsed = parsed.replace(tzinfo=self.local_tz)
        event_time = parsed.astimezone(timezone.utc)
        price = _to_decimal(_require(raw, "price"), "price")
        if price <= 0:
            raise NormalizationError(RejectReason.INVALID_PRICE, f"price 非正数: {price}")
        vol_raw = raw.get("volume")
        volume = _to_decimal(vol_raw, "volume") if vol_raw is not None else None
        return NormalizedTick(
            source=self.name,
            symbol=symbol,
            sequence=seq,
            event_time=event_time,
            price=price,
            volume=volume,
            raw_id=self._raw_id(raw),
        )


_ADAPTERS: dict[str, SourceAdapter] = {
    SourceAAdapter.name: SourceAAdapter(),
    SourceBAdapter.name: SourceBAdapter(),
}


def get_adapter(source: str) -> SourceAdapter:
    """按来源名返回适配器；未知来源抛 FeedError(UNKNOWN_SOURCE)。"""
    adapter = _ADAPTERS.get(source)
    if adapter is None:
        raise FeedError(RejectReason.UNKNOWN_SOURCE, f"未知来源: {source!r}")
    return adapter
