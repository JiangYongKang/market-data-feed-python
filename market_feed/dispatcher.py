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
    """A bounded deque with backpressure, coalescing and handoff replay.

    Handoff replay events (bounded snapshot or resume backfill) are marked and
    always delivered before live traffic. They are never coalesced or dropped:
    removing any piece of history would make the snapshot/resume stream
    non-contiguous. When a drop policy cannot make room without touching
    history, it degrades to blocking rather than silently losing history.
    """

    def __init__(self, subscriber_id: str, options: SubscriberOptions) -> None:
        if options.queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        self.subscriber_id = subscriber_id
        self.options = options
        # (event, is_replay); replay items form a prefix consumed first.
        self._items: Deque[tuple[DeliveryEvent, bool]] = deque()
        self._replay_remaining = 0
        self._condition = asyncio.Condition()
        self._closed = False

    def qsize(self) -> int:
        return len(self._items)

    def is_closed(self) -> bool:
        return self._closed

    def in_handoff(self) -> bool:
        return self._replay_remaining > 0

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

    def _find_coalesce_index(self, key) -> int | None:
        for index, (existing, is_replay) in enumerate(self._items):
            if is_replay:
                continue
            if _coalesce_key(existing) == key:
                return index
        return None

    async def _wait_capacity(self) -> bool:
        """Wait for a free slot; return False if the queue closed meanwhile."""
        await self._condition.wait_for(lambda: self._closed or len(self._items) < self.options.queue_size)
        return not self._closed

    async def begin_handoff(self, events: list[DeliveryEvent]) -> list[DropAudit]:
        """Prepend historical events ahead of any already-queued live events.

        Backpressure behaves like ``put``: the bounded queue cannot grow, but
        replay history itself is protected from coalescing and dropping.
        """
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
                    for event in events
                ]

            audits: list[DropAudit] = []
            accepted: list[DeliveryEvent] = []
            for event in events:
                if len(self._items) >= self.options.queue_size:
                    policy = self.options.backpressure
                    if policy is BackpressurePolicy.DROP_OLDEST:
                        first_live = next(
                            (index for index, (_, is_replay) in enumerate(self._items) if not is_replay),
                            None,
                        )
                        if first_live is not None:
                            dropped, _ = self._items[first_live]
                            del self._items[first_live]
                            audits.append(
                                self._audit(dropped, "queue full during handoff; oldest live event dropped")
                            )
                        elif not await self._wait_capacity():
                            audits.append(self._audit(event, "subscriber queue closed while waiting for capacity"))
                            continue
                    elif not await self._wait_capacity():
                        # BLOCK waits; REJECT / COALESCE_LATEST also wait here
                        # because history is neither coalescible nor droppable,
                        # and rejecting it would hand out a broken view.
                        audits.append(self._audit(event, "subscriber queue closed while waiting for capacity"))
                        continue
                self._items.appendleft((event, True))
                accepted.append(event)
                self._replay_remaining += 1
            # Reversing fixes the left-append order so replay stays ascending.
            if accepted:
                replay_items = [item for item in self._items if item[1]]
                live_items = [item for item in self._items if not item[1]]
                replay_items.reverse()
                self._items = deque(replay_items + live_items)
                self._condition.notify_all()
            return audits

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
            # Coalescing is suspended while replay history is pending: the live
            # stream logically starts only after the handoff boundary.
            if key is not None and self._replay_remaining == 0 and self.options.backpressure is BackpressurePolicy.COALESCE_LATEST:
                index = self._find_coalesce_index(key)
                if index is not None:
                    existing, _ = self._items[index]
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
                        self._items[index] = (event, False)
                    self._condition.notify()
                    return audits

            if len(self._items) < self.options.queue_size:
                self._items.append((event, False))
                self._condition.notify()
                return audits

            policy = self.options.backpressure
            if policy is BackpressurePolicy.BLOCK:
                if not await self._wait_capacity():
                    return [self._audit(event, "subscriber queue closed while waiting for capacity")]
                self._items.append((event, False))
                self._condition.notify()
                return audits

            if policy is BackpressurePolicy.DROP_OLDEST:
                dropped_item = self._items.popleft()
                if dropped_item[1]:
                    # Never evict replay history from the head; wait it out.
                    self._items.appendleft(dropped_item)
                    if not await self._wait_capacity():
                        return [self._audit(event, "subscriber queue closed while waiting for capacity")]
                    self._items.append((event, False))
                    self._condition.notify()
                    return audits
                dropped, _ = dropped_item
                audits.append(self._audit(dropped, "queue full; oldest event dropped"))
                self._items.append((event, False))
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
            item, is_replay = self._items.popleft()
            if is_replay:
                self._replay_remaining -= 1
            # A producer blocked by the BLOCK policy can use the free slot.
            self._condition.notify()
            return item

    async def close(self) -> list[DropAudit]:
        async with self._condition:
            audits = [self._audit(event, "subscriber removed; pending event discarded") for event, _ in self._items]
            self._items.clear()
            self._replay_remaining = 0
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

    async def begin_handoff(self, subscriber_id: str, events: list[DeliveryEvent]) -> list[DropAudit]:
        async with self._registry_lock:
            queue = self._queues.get(subscriber_id)
            if queue is None:
                raise KeyError(subscriber_id)
            results = [await queue.begin_handoff(events)]
        return results[0]

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
