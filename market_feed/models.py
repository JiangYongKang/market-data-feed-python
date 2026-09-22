"""统一行情模型与拒绝/状态枚举。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


class RejectReason(enum.Enum):
    """可区分的拒绝/处置原因。"""

    UNKNOWN_SOURCE = "UNKNOWN_SOURCE"
    UNKNOWN_SYMBOL = "UNKNOWN_SYMBOL"
    MALFORMED_RECORD = "MALFORMED_RECORD"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_SEQUENCE = "INVALID_SEQUENCE"
    NOT_SUBSCRIBED = "NOT_SUBSCRIBED"
    ALREADY_SUBSCRIBED = "ALREADY_SUBSCRIBED"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    STALE = "STALE"
    GAP_DETECTED = "GAP_DETECTED"
    GAP_UNRECOVERABLE = "GAP_UNRECOVERABLE"
    BACKPRESSURE_COALESCED = "BACKPRESSURE_COALESCED"
    BACKPRESSURE_DROPPED = "BACKPRESSURE_DROPPED"


class TickStatus(enum.Enum):
    """入站行情的处理结论。"""

    ACCEPTED = "ACCEPTED"
    BUFFERED = "BUFFERED"          # 缺口恢复期间被缓冲，未与正常行情混用
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"  # 迟到但可补入历史，不作为最新价
    STALE = "STALE"                # 时间戳过旧
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class NormalizedTick:
    """归一化后的统一行情模型。

    - price/volume 使用 Decimal，杜绝浮点精度丢失；
    - event_time 一律为带 UTC 时区的 datetime；
    - sequence 为来源内单调序号。
    """

    source: str
    symbol: str
    sequence: int
    event_time: datetime
    price: Decimal
    volume: Decimal | None
    raw_id: str


@dataclass(frozen=True)
class GapMarker:
    """缺口标记：无法恢复的缺口被显式记录而非静默跳过。"""

    source: str
    symbol: str
    from_sequence: int
    to_sequence: int
    reason: str


@dataclass
class InboundResult:
    """单条入站记录的处理结果（含判定依据）。"""

    status: TickStatus
    reason: RejectReason | None = None
    tick: NormalizedTick | None = None
    detail: str = ""
