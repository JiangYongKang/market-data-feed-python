"""背压与限速测试：合并、丢最旧、拒绝均可追溯；订阅者异常隔离。"""
from __future__ import annotations

import time

from market_feed.distributor import Distributor, OverflowPolicy, Subscriber
from tests.conftest import log_verdict, make_tick


def test_coalesce_latest_traceable():
    d = Distributor()
    d.add_subscriber(Subscriber("s", maxsize=2, policy=OverflowPolicy.COALESCE_LATEST))
    for seq in (1, 2, 3):
        d.publish(make_tick(seq=seq))
    drops = d.drop_log()
    log_verdict("合并背压", "seq=1,2,3 maxsize=2",
                [(r.tick.raw_id, r.detail) for r in drops])
    assert len(drops) == 1
    assert drops[0].tick.sequence == 1  # 被合并的是旧行情
    assert drops[0].policy is OverflowPolicy.COALESCE_LATEST
    sub = d._subscribers["s"]
    assert [sub.poll().sequence, sub.poll().sequence] == [2, 3]


def test_drop_oldest_traceable():
    d = Distributor()
    d.add_subscriber(Subscriber("s", maxsize=1, policy=OverflowPolicy.DROP_OLDEST))
    d.publish(make_tick(seq=1))
    d.publish(make_tick(seq=2))
    drops = d.drop_log()
    log_verdict("丢最旧", "seq=1,2 maxsize=1", [(r.tick.raw_id, r.detail) for r in drops])
    assert len(drops) == 1 and drops[0].tick.sequence == 1
    assert d._subscribers["s"].poll().sequence == 2


def test_reject_policy_traceable():
    d = Distributor()
    d.add_subscriber(Subscriber("s", maxsize=1, policy=OverflowPolicy.REJECT))
    d.publish(make_tick(seq=1))
    d.publish(make_tick(seq=2))
    drops = d.drop_log()
    log_verdict("拒绝背压", "seq=1,2 maxsize=1", [(r.tick.raw_id, r.detail) for r in drops])
    assert len(drops) == 1 and drops[0].tick.sequence == 2
    assert d._subscribers["s"].poll().sequence == 1


def test_memory_bounded_under_flood():
    d = Distributor()
    sub = Subscriber("s", maxsize=10, policy=OverflowPolicy.COALESCE_LATEST)
    d.add_subscriber(sub)
    for seq in range(1, 1001):
        d.publish(make_tick(seq=seq))
    log_verdict("洪泛背压", "1000条 maxsize=10",
                f"pending={sub.pending()} drops={len(d.drop_log())}")
    assert sub.pending() <= 10  # 无界占用内存被杜绝
    assert len(d.drop_log()) == 990  # 全部被合并且可追溯


def test_slow_subscriber_does_not_affect_others():
    d = Distributor()
    fast = Subscriber("fast", maxsize=100, policy=OverflowPolicy.REJECT)
    slow = Subscriber("slow", maxsize=1, policy=OverflowPolicy.REJECT)
    d.add_subscriber(fast)
    d.add_subscriber(slow)
    for seq in range(1, 6):
        d.publish(make_tick(seq=seq))
    received = []
    while (t := fast.poll()) is not None:
        received.append(t.sequence)
    log_verdict("慢订阅者隔离", "fast=100/slow=1", f"fast收到={received}")
    assert received == [1, 2, 3, 4, 5]  # fast 完整收到


def test_broken_subscriber_isolated():
    d = Distributor()

    class BrokenSubscriber(Subscriber):
        def offer(self, tick):
            raise RuntimeError("订阅者内部异常")

    d.add_subscriber(BrokenSubscriber("broken", 10, OverflowPolicy.REJECT))
    good = Subscriber("good", 10, OverflowPolicy.REJECT)
    d.add_subscriber(good)
    d.publish(make_tick(seq=1))  # 不抛异常
    assert good.poll().sequence == 1


def test_rate_limiting():
    d = Distributor(rate_per_sec=100.0, max_rate_wait=0.5)
    sub = Subscriber("s", maxsize=1000, policy=OverflowPolicy.REJECT)
    d.add_subscriber(sub)
    start = time.monotonic()
    for seq in range(1, 121):  # 超过桶容量 100，超出部分必须等待令牌
        d.publish(make_tick(seq=seq))
    elapsed = time.monotonic() - start
    log_verdict("限速", "120条@100/s", f"耗时={elapsed:.3f}s")
    assert elapsed >= 20 / 100.0 * 0.8  # 超出突发的 20 条被限速
    assert sub.pending() == 120
