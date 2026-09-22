"""Bounded, per-subscriber asynchronous event dispatch."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Awaitable, Callable, Deque

from .enums import BackpressurePolicy, DeliveryKind
from .logging_utils import event_log
from .models import DeliveryEvent, DropAudit, Quote, SubscriberOptions


def _coalesce_key(event: DeliveryEvent):
    payload = event.payload
    if not isinstance(payload, Quote):
        return None
    if event.kind not in (DeliveryKind.QUOTE, DeliveryKind.RECOVERED_QUOTE):
        return None
    return (event.kind, payload.source, payload.symbol)


class SubscriberQueue:
    """A bounded deque with backpressure and same-kind quote coalescing."""

    def __init__(self, subscriber_id: str, options: SubscriberOptions) -> None:
        if options.queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        self.subscriber_id = subscriber_id
        self.options = options
        self._items: Deque[DeliveryEvent] = deque()
        self._condition = asyncio.Condition()
        self._closed = False

    def qsize(self) -> int:
        return len(self._items)

    def is_closed(self) -> bool:
        return self._closed

    def _audit(
        self,
        dropped: DeliveryEvent,
        reason: str,
        *,
        into: DeliveryEvent | None = None,
        kind: DeliveryKind | None = None,
    ) -> DropAudit:
        payload = dropped.payload
        sequence = payload.sequence if isinstance(payload, Quote) else None
        return DropAudit(
            subscriber_id=self.subscriber_id,
            policy=self.options.backpressure,
            dropped_kind=kind or dropped.kind,
            reason=reason + (f" into_sequence={into.payload.sequence}" if into is not None else ""),
            sequence=sequence,
            timestamp=datetime.now(timezone.utc),
        )

    async def put(self, event: DeliveryEvent) -> list[DropAudit]:
        async with self._condition:
            if self._closed:
                return [
                    DropAudit(
                        subscriber_id=self.subscriber_id,
                        policy=self.options.backpressure,
                        dropped_kind=event.kind,
                        reason="subscriber queue is closed",
                        sequence=event.payload.sequence if isinstance(event.payload, Quote) else None,
                        timestamp=datetime.now(timezone.utc),
                    )
                ]

            audits: list[DropAudit] = []
            key = _coalesce_key(event)
            if self.options.backpressure is BackpressurePolicy.COALESCE_LATEST and key is not None:
                for index, existing in enumerate(self._items):
                    if _coalesce_key(existing) == key:
                        old_payload: Quote = existing.payload
                        new_payload: Quote = event.payload
                        chosen = existing if old_payload.sequence >= new_payload.sequence else event
                        audits.append(
                            self._audit(
                                existing if chosen is event else event,
                                "coalesced same subscriber/source/symbol/delivery-kind to latest sequence",
                                into=chosen,
                                kind=DeliveryKind.COALESCED,
                            )
                        )
                        if chosen is event:
                            # Preserve the older event's queue position.
                            self._items[index] = event
                        self._condition.notify()
                        return audits

            if len(self._items) < self.options.queue_size:
                self._items.append(event)
                self._condition.notify()
                return audits

            policy = self.options.backpressure
            if policy is BackpressurePolicy.BLOCK:
                await self._condition.wait_for(lambda: self._closed or len(self._items) < self.options.queue_size)
                if self._closed:
                    return [
                        self._audit(event, "subscriber queue closed while waiting for capacity")
                    ]
                self._items.append(event)
                self._condition.notify()
                return audits

            if policy is BackpressurePolicy.DROP_OLDEST:
                dropped = self._items.popleft()
                audits.append(self._audit(dropped, "queue full; oldest event dropped"))
                self._items.append(event)
                self._condition.notify()
                return audits

            if policy is BackpressurePolicy.COALESCE_LATEST:
                audits.append(self._audit(event, "queue full and no same-key event can be coalesced; new event rejected"))
                return audits

            audits.append(self._audit(event, "queue full; new event rejected"))
            return audits

    async def get(self) -> DeliveryEvent | None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._closed or bool(self._items))
            if not self._items:
                return None
            item = self._items.popleft()
            # A producer blocked by the BLOCK policy can use the free slot.
            self._condition.notify()
            return item

    async def close(self) -> list[DropAudit]:
        async with self._condition:
            audits = [self._audit(event, "subscriber removed; pending event discarded") for event in self._items]
            self._items.clear()
            self._closed = True
            self._condition.notify_all()
            return audits


class Dispatcher:
    """Own bounded delivery queues and isolate one subscriber's backlog."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._queues: dict[str, SubscriberQueue] = {}
        self._registry_lock = asyncio.Lock()
        self._logger = logger or logging.getLogger("market_feed")

    async def add_subscriber(self, subscriber_id: str, options: SubscriberOptions | None = None) -> SubscriberOptions:
        if not subscriber_id.strip():
            raise ValueError("subscriber_id must be non-empty")
        actual_options = options or SubscriberOptions()
        async with self._registry_lock:
            queue = self._queues.get(subscriber_id)
            if queue is None:
                queue = SubscriberQueue(subscriber_id, actual_options)
                self._queues[subscriber_id] = queue
                event_log(self._logger, "subscriber_added", subscriber_id=subscriber_id, options=actual_options)
            return queue.options

    def has_subscriber(self, subscriber_id: str) -> bool:
        return subscriber_id in self._queues

    async def remove_subscriber(self, subscriber_id: str) -> list[DropAudit]:
        async with self._registry_lock:
            queue = self._queues.pop(subscriber_id, None)
            if queue is None:
                return []
            return await queue.close()

    async def publish(self, subscriber_ids: list[str] | tuple[str, ...], event: DeliveryEvent) -> list[DropAudit]:
        audits: list[DropAudit] = []
        async with self._registry_lock:
            target_queues = [
                (subscriber_id, self._queues[subscriber_id])
                for subscriber_id in subscriber_ids
                if subscriber_id in self._queues
            ]
            # Holding the registry lock prevents removal from racing with an
            # in-flight enqueue. Per-queue conditions still isolate backlog.
            results = await asyncio.gather(*(queue.put(event) for _, queue in target_queues), return_exceptions=True)
        for (subscriber_id, _), result in zip(target_queues, results):
            if isinstance(result, Exception):
                event_log(
                    self._logger,
                    "dispatch_failed",
                    subscriber_id=subscriber_id,
                    kind=event.kind,
                    error=repr(result),
                )
                continue
            audits.extend(result)
        return audits

    async def get(self, subscriber_id: str) -> DeliveryEvent | None:
        queue = self._queues.get(subscriber_id)
        if queue is None:
            raise KeyError(subscriber_id)
        return await queue.get()

    def queue_snapshot(self) -> dict[str, int]:
        return {subscriber_id: queue.qsize() for subscriber_id, queue in self._queues.items()}


EventHandler = Callable[[DeliveryEvent], Awaitable[None]]


async def run_subscriber(
    dispatcher: Dispatcher,
    subscriber_id: str,
    handler: EventHandler,
    logger: logging.Logger | None = None,
) -> None:
    """Consume until the subscriber queue is closed.

    Handler exceptions are logged but do not cancel the subscriber loop or any
    other subscriber.
    """
    log = logger or logging.getLogger("market_feed")
    while True:
        event = await dispatcher.get(subscriber_id)
        if event is None:
            event_log(log, "subscriber_stopped", subscriber_id=subscriber_id)
            return
        try:
            await handler(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # subscriber isolation boundary
            event_log(
                log,
                "subscriber_handler_error",
                subscriber_id=subscriber_id,
                kind=event.kind,
                error=repr(exc),
            )
