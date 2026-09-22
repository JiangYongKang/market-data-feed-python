"""对外分发：多订阅者扇出、背压与限速、可追踪的合并/丢弃。"""
from __future__ import annotations

import enum
import logging
import queue
import threading
import time
from dataclasses import dataclass

from market_feed.models import NormalizedTick

logger = logging.getLogger("market_feed.distributor")


class OverflowPolicy(enum.Enum):
    COALESCE_LATEST = "COALESCE_LATEST"  # 队列满时按标的合并，仅保留最新
    DROP_OLDEST = "DROP_OLDEST"          # 丢弃最旧
    REJECT = "REJECT"                    # 拒绝写入（背压上抛）


@dataclass(frozen=True)
class DropRecord:
    """被合并/丢弃行情的可追溯记录。"""

    subscriber_id: str
    tick: NormalizedTick
    policy: OverflowPolicy
    detail: str


class Subscriber:
    """单个订阅者：有界队列，异常隔离。"""

    def __init__(self, subscriber_id: str, maxsize: int, policy: OverflowPolicy) -> None:
        self.subscriber_id = subscriber_id
        self.policy = policy
        self._queue: queue.Queue[NormalizedTick] = queue.Queue(maxsize=maxsize)

    def offer(self, tick: NormalizedTick) -> DropRecord | None:
        """投递一条行情；返回被合并/丢弃记录（若有）。本方法不抛异常。"""
        try:
            self._queue.put_nowait(tick)
            return None
        except queue.Full:
            pass
        if self.policy is OverflowPolicy.REJECT:
            return DropRecord(self.subscriber_id, tick, self.policy, "队列已满，拒绝写入")
        if self.policy is OverflowPolicy.DROP_OLDEST:
            return self._drop_oldest(tick)
        return self._coalesce(tick)

    def _drop_oldest(self, tick: NormalizedTick) -> DropRecord | None:
        try:
            oldest = self._queue.get_nowait()
        except queue.Empty:  # 竞态下已被消费，直接重试放入
            try:
                self._queue.put_nowait(tick)
            except queue.Full:
                return DropRecord(self.subscriber_id, tick, self.policy, "竞态下仍满")
            return None
        try:
            self._queue.put_nowait(tick)
        except queue.Full:
            pass
        return DropRecord(self.subscriber_id, oldest, self.policy, "丢弃最旧以容纳新行情")

    def _coalesce(self, tick: NormalizedTick) -> DropRecord | None:
        # 同标的旧行情被最新行情合并替换
        with self._queue.mutex:
            for i, old in enumerate(self._queue.queue):
                if old.symbol == tick.symbol and old.source == tick.source:
                    del self._queue.queue[i]
                    self._queue.unfinished_tasks -= 1
                    break
            else:
                old = None
        if old is None:
            return self._drop_oldest(tick)
        try:
            self._queue.put_nowait(tick)
        except queue.Full:
            pass
        return DropRecord(
            self.subscriber_id, old, self.policy,
            f"被同标的更新行情 seq={tick.sequence} 合并",
        )

    def poll(self, timeout: float = 0.0) -> NormalizedTick | None:
        try:
            return self._queue.get(timeout=timeout) if timeout > 0 else self._queue.get_nowait()
        except queue.Empty:
            return None

    def pending(self) -> int:
        return self._queue.qsize()


class _TokenBucket:
    """令牌桶限速器。"""

    def __init__(self, rate_per_sec: float) -> None:
        self._rate = rate_per_sec
        self._tokens = rate_per_sec
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, max_wait: float) -> bool:
        deadline = time.monotonic() + max_wait
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._rate, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(min(0.001, max(0.0, deadline - time.monotonic())))


class Distributor:
    """扇出器：单订阅者异常不影响其他订阅者；可选令牌桶限速。"""

    def __init__(self, rate_per_sec: float | None = None, max_rate_wait: float = 0.5) -> None:
        self._lock = threading.Lock()
        self._subscribers: dict[str, Subscriber] = {}
        self._drops: list[DropRecord] = []
        self._bucket = _TokenBucket(rate_per_sec) if rate_per_sec else None
        self._max_rate_wait = max_rate_wait

    def add_subscriber(self, subscriber: Subscriber) -> None:
        with self._lock:
            self._subscribers[subscriber.subscriber_id] = subscriber

    def remove_subscriber(self, subscriber_id: str) -> None:
        with self._lock:
            self._subscribers.pop(subscriber_id, None)

    def publish(self, tick: NormalizedTick) -> list[DropRecord]:
        if self._bucket is not None and not self._bucket.acquire(self._max_rate_wait):
            record = DropRecord("*", tick, OverflowPolicy.REJECT, "限速等待超时，行情被丢弃")
            logger.warning("DROP %s", record)
            with self._lock:
                self._drops.append(record)
            return [record]
        with self._lock:
            subscribers = list(self._subscribers.values())
        records: list[DropRecord] = []
        for sub in subscribers:
            try:
                record = sub.offer(tick)
            except Exception:  # 单订阅者异常隔离
                logger.exception("订阅者 %s 投递异常，已隔离", sub.subscriber_id)
                continue
            if record is not None:
                logger.info("DROP/COALESCE %s", record)
                records.append(record)
        if records:
            with self._lock:
                self._drops.extend(records)
        return records

    def drop_log(self) -> list[DropRecord]:
        with self._lock:
            return list(self._drops)
