"""订阅状态测试：重复订阅、取消未订阅、订阅不存在标的。"""
from __future__ import annotations

import pytest

from market_feed.errors import SubscriptionError
from market_feed.models import RejectReason
from market_feed.subscription import SubscriptionManager
from tests.conftest import log_verdict


def _expect(manager: SubscriptionManager, op, reason: RejectReason, case: str) -> None:
    caught: SubscriptionError | None = None
    try:
        op()
    except SubscriptionError as exc:
        caught = exc
    verdict = "NO_ERROR(意外成功)" if caught is None else f"{caught.reason.value}: {caught.detail}"
    log_verdict(case, op, verdict)
    assert caught is not None and caught.reason is reason, verdict


def test_subscribe_and_unsubscribe(subscriptions: SubscriptionManager):
    subscriptions.subscribe("source_a", "BTCUSD")
    assert subscriptions.is_subscribed("source_a", "BTCUSD")
    subscriptions.unsubscribe("source_a", "BTCUSD")
    assert not subscriptions.is_subscribed("source_a", "BTCUSD")


def test_duplicate_subscribe_rejected_and_state_intact(subscriptions: SubscriptionManager):
    subscriptions.subscribe("source_a", "BTCUSD")
    _expect(subscriptions,
            lambda: subscriptions.subscribe("source_a", "BTCUSD"),
            RejectReason.ALREADY_SUBSCRIBED, "重复订阅")
    # 已有订阅不被破坏
    assert subscriptions.is_subscribed("source_a", "BTCUSD")


def test_unsubscribe_not_subscribed_rejected(subscriptions: SubscriptionManager):
    _expect(subscriptions,
            lambda: subscriptions.unsubscribe("source_a", "BTCUSD"),
            RejectReason.NOT_SUBSCRIBED, "取消未订阅")


def test_subscribe_unknown_symbol_rejected(subscriptions: SubscriptionManager):
    _expect(subscriptions,
            lambda: subscriptions.subscribe("source_a", "DOGEUSD"),
            RejectReason.UNKNOWN_SYMBOL, "订阅不存在标的")
    _expect(subscriptions,
            lambda: subscriptions.subscribe("source_b", "ETHUSD"),  # 另一来源无此标的
            RejectReason.UNKNOWN_SYMBOL, "跨来源不存在标的")
    assert subscriptions.snapshot() == frozenset()


def test_failed_operations_do_not_corrupt_existing(subscriptions: SubscriptionManager):
    subscriptions.subscribe("source_a", "BTCUSD")
    subscriptions.subscribe("source_b", "XAUUSD")
    with pytest.raises(SubscriptionError):
        subscriptions.subscribe("source_a", "DOGEUSD")
    with pytest.raises(SubscriptionError):
        subscriptions.unsubscribe("source_a", "ETHUSD")
    assert subscriptions.snapshot() == frozenset(
        {("source_a", "BTCUSD"), ("source_b", "XAUUSD")}
    )
