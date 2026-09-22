"""服务级断连恢复测试：不重复推送、不漏推、缺口显式标记。"""
from __future__ import annotations

from market_feed.distributor import Distributor, OverflowPolicy, Subscriber
from market_feed.models import RejectReason, TickStatus
from market_feed.service import MarketDataService
from market_feed.subscription import SubscriptionManager
from tests.conftest import log_verdict, raw_a


def _subscribed_service(subscriptions: SubscriptionManager, distributor: Distributor):
    subscriptions.subscribe("source_a", "BTCUSD")
    return MarketDataService(subscriptions, distributor)


def _drain(sub: Subscriber) -> list[int]:
    out = []
    while (t := sub.poll()) is not None:
        out.append(t.sequence)
    return out


def test_unsubscribed_ingest_rejected(service: MarketDataService):
    r = service.ingest("source_a", raw_a(1))
    log_verdict("未订阅推送", raw_a(1), f"{r.status.value} {r.reason.value}")
    assert r.status is TickStatus.REJECTED and r.reason is RejectReason.NOT_SUBSCRIBED


def test_recovery_no_duplicate_no_missing(
    subscriptions: SubscriptionManager, distributor: Distributor
):
    service = _subscribed_service(subscriptions, distributor)
    sub = Subscriber("s", 100, OverflowPolicy.REJECT)
    distributor.add_subscriber(sub)

    service.ingest("source_a", raw_a(1))
    service.ingest("source_a", raw_a(2))
    # 断连：seq 3,4 丢失；重连后来了 5
    r = service.ingest("source_a", raw_a(5))
    assert r.status is TickStatus.BUFFERED  # 缺口期间不混用
    # 恢复回补 3,4
    gap = service.recover("source_a", "BTCUSD", [raw_a(3), raw_a(4)])
    assert gap is None
    received = _drain(sub)
    log_verdict("恢复后推送序列", "live=[1,2,5] replay=[3,4]", received)
    assert received == [1, 2, 3, 4, 5]  # 不重不漏且有序
    assert service.latest("source_a", "BTCUSD").sequence == 5


def test_recovery_unrecoverable_gap_marked(
    subscriptions: SubscriptionManager, distributor: Distributor
):
    service = _subscribed_service(subscriptions, distributor)
    sub = Subscriber("s", 100, OverflowPolicy.REJECT)
    distributor.add_subscriber(sub)

    service.ingest("source_a", raw_a(1))
    service.ingest("source_a", raw_a(4))  # 缺 2,3
    gap = service.recover("source_a", "BTCUSD", [raw_a(2)])  # 只回补 2
    log_verdict("不可恢复缺口", "replay=[2]", gap)
    assert gap is not None and gap.from_sequence == 3
    assert service.gaps()[0].from_sequence == 3  # 显式标记而非静默跳过
    received = _drain(sub)
    assert received == [1, 2, 4]  # 缺失的 3 不被伪造
