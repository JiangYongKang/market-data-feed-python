"""并发测试：多来源/多订阅者并发写入读取，结果一致、无半更新、无串号。"""
from __future__ import annotations

import threading
from decimal import Decimal

from market_feed.distributor import Distributor, OverflowPolicy, Subscriber
from market_feed.service import MarketDataService
from market_feed.subscription import SubscriptionManager
from tests.conftest import log_verdict, raw_a, raw_b

N_PER_SOURCE = 200


def test_concurrent_multi_source_ingest(
    subscriptions: SubscriptionManager, distributor: Distributor
):
    subscriptions.subscribe("source_a", "BTCUSD")
    subscriptions.subscribe("source_b", "BTCUSD")
    service = MarketDataService(subscriptions, distributor)
    sub = Subscriber("s", N_PER_SOURCE * 2 + 10, OverflowPolicy.REJECT)
    distributor.add_subscriber(sub)
    errors: list[Exception] = []

    def feed_a():
        try:
            for seq in range(1, N_PER_SOURCE + 1):
                service.ingest("source_a", raw_a(seq, px=str(Decimal("100") + seq)))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def feed_b():
        try:
            for seq in range(1, N_PER_SOURCE + 1):
                service.ingest("source_b", raw_b(seq, price=float(200 + seq)))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    snapshots: list[tuple] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            ta = service.latest("source_a", "BTCUSD")
            tb = service.latest("source_b", "BTCUSD")
            if ta is not None:
                # 一致性：快照内 source/symbol/price 绝不串号
                assert ta.source == "source_a" and ta.symbol == "BTCUSD"
                assert ta.price >= Decimal("100")
                snapshots.append(("a", ta.sequence))
            if tb is not None:
                assert tb.source == "source_b" and tb.symbol == "BTCUSD"
                snapshots.append(("b", tb.sequence))

    threads = [threading.Thread(target=feed_a), threading.Thread(target=feed_b)]
    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    for t in readers:
        t.join()

    assert not errors
    la = service.latest("source_a", "BTCUSD")
    lb = service.latest("source_b", "BTCUSD")
    log_verdict("并发双源写入", f"各{N_PER_SOURCE}条",
                f"latest_a={la.sequence}/{la.price} latest_b={lb.sequence}/{lb.price}")
    # 最终最新价必为各自来源的最后一条（序号单调、无错配）
    assert la.sequence == N_PER_SOURCE and la.price == Decimal(str(100 + N_PER_SOURCE))
    assert lb.sequence == N_PER_SOURCE and lb.price == Decimal(str(200 + N_PER_SOURCE))
    # 订阅者收到总数 = 两源之和（无丢失、无多推）
    received = 0
    seen_a, seen_b = set(), set()
    while (t := sub.poll()) is not None:
        received += 1
        (seen_a if t.source == "source_a" else seen_b).add(t.sequence)
    log_verdict("并发接收完整性", "订阅者队列", f"received={received} a={len(seen_a)} b={len(seen_b)}")
    assert received == 2 * N_PER_SOURCE
    assert seen_a == set(range(1, N_PER_SOURCE + 1))
    assert seen_b == set(range(1, N_PER_SOURCE + 1))


def test_concurrent_subscribe_unsubscribe(subscriptions: SubscriptionManager):
    errors: list[Exception] = []

    def churn(symbol: str):
        from market_feed.errors import SubscriptionError
        for _ in range(100):
            try:
                subscriptions.subscribe("source_a", symbol)
            except SubscriptionError:
                pass
            try:
                subscriptions.unsubscribe("source_a", symbol)
            except SubscriptionError as exc:
                errors.append(exc)

    threads = [threading.Thread(target=churn, args=(s,)) for s in ("BTCUSD", "ETHUSD")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 取消已存在的订阅绝不应报 NOT_SUBSCRIBED 以外的错；状态最终一致
    assert all(e.reason.value == "NOT_SUBSCRIBED" for e in errors)
    snap = subscriptions.snapshot()
    log_verdict("并发订阅 churn", "2线程x100次", f"final={snap}")
    assert len(snap) <= 2
