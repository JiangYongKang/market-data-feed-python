"""服务内可区分的异常类型。"""
from __future__ import annotations

from market_feed.models import RejectReason


class FeedError(Exception):
    """带拒绝原因的服务异常基类。"""

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        super().__init__(f"{reason.value}: {detail}")
        self.reason = reason
        self.detail = detail


class NormalizationError(FeedError):
    """归一化失败（畸形/缺字段/非法价格/非法时间等）。"""


class SubscriptionError(FeedError):
    """订阅状态非法（重复订阅/取消未订阅/标的不存在）。"""
