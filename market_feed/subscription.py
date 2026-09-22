"""订阅状态管理：按 (来源, 标的) 维护订阅，非法操作显式拒绝。"""
from __future__ import annotations

import threading

from market_feed.errors import SubscriptionError
from market_feed.models import RejectReason


class SymbolRegistry:
    """可订阅标的注册表（本地可控，不依赖外部服务）。"""

    def __init__(self, symbols: dict[str, set[str]] | None = None) -> None:
        # source -> set(symbols)
        self._symbols: dict[str, set[str]] = {k: set(v) for k, v in (symbols or {}).items()}

    def exists(self, source: str, symbol: str) -> bool:
        return symbol in self._symbols.get(source, set())


class SubscriptionManager:
    """线程安全的订阅管理器。

    - 重复订阅 -> ALREADY_SUBSCRIBED（显式拒绝，不破坏已有订阅）；
    - 取消未订阅 -> NOT_SUBSCRIBED；
    - 订阅不存在标的 -> UNKNOWN_SYMBOL；
    所有校验与变更在同一把锁内完成，杜绝半更新状态。
    """

    def __init__(self, registry: SymbolRegistry) -> None:
        self._registry = registry
        self._lock = threading.Lock()
        self._subscriptions: set[tuple[str, str]] = set()

    def subscribe(self, source: str, symbol: str) -> None:
        with self._lock:
            if not self._registry.exists(source, symbol):
                raise SubscriptionError(
                    RejectReason.UNKNOWN_SYMBOL, f"{source}/{symbol} 不在注册表"
                )
            key = (source, symbol)
            if key in self._subscriptions:
                raise SubscriptionError(
                    RejectReason.ALREADY_SUBSCRIBED, f"{source}/{symbol} 已订阅"
                )
            self._subscriptions.add(key)

    def unsubscribe(self, source: str, symbol: str) -> None:
        with self._lock:
            key = (source, symbol)
            if key not in self._subscriptions:
                raise SubscriptionError(
                    RejectReason.NOT_SUBSCRIBED, f"{source}/{symbol} 未订阅"
                )
            self._subscriptions.discard(key)

    def is_subscribed(self, source: str, symbol: str) -> bool:
        with self._lock:
            return (source, symbol) in self._subscriptions

    def snapshot(self) -> frozenset[tuple[str, str]]:
        with self._lock:
            return frozenset(self._subscriptions)
