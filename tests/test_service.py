import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from market_feed.dispatcher import run_subscriber
from market_feed.enums import (
    BackpressurePolicy,
    DeliveryKind,
    GapState,
    IngestStatus,
    SourceId,
)
from market_feed.models import SubscriberOptions
from market_feed.service import MarketDataFeedService

BASE = datetime(2026, 9, 22, tzinfo=timezone.utc)


def record_a(sequence: int, symbol: str = "AAA", price: str = "100.00", seconds: int | None = None):
    ts_ms = int((BASE + timedelta(seconds=sequence if seconds is None else seconds)).timestamp() * 1000)
    return {"trade_id": f"a-{symbol}-{sequence}", "sym": symbol, "seq": sequence, "ts_ms": ts_ms, "px": price, "qty": "1"}


def record_b(sequence: int, symbol: str = "BBB", price_scaled: int = 10_000):
    return {
        "id": f"b-{symbol}-{sequence}",
        "ticker": symbol,
        "sequence": sequence,
        "event_time": (BASE + timedelta(seconds=sequence)).isoformat().replace("+00:00", "Z"),
        "price_scaled": price_scaled + sequence,
        "price_scale": 2,
        "size": "2.50",
    }


async def drain(dispatcher, subscriber_id: str, count: int):
    return [await dispatcher.get(subscriber_id) for _ in range(count)]


@pytest.mark.asyncio
async def test_malformed_record_is_rejected_with_reason_and_not_silent():
    service = MarketDataFeedService(logger=logging.getLogger("test"))
    await service.subscribe("s1", SourceId.SOURCE_A, "AAA")
    malformed = {"trade_id": "bad", "sym": "AAA", "seq": "x", "ts_ms": 1, "px": "1", "qty": "1"}

    result = await service.ingest(SourceId.SOURCE_A, malformed)

    assert result.status is IngestStatus.REJECTED
    assert result.rejection is not None
    event = await service.dispatcher.get("s1")
    assert event.kind is DeliveryKind.REJECTION
    assert event.payload.reason.value == "invalid_sequence"
    assert event.payload.raw_record == malformed


@pytest.mark.asyncio
async def test_gap_recovery_replays_once_and_keeps_recovered_quotes_distinguishable():
    service = MarketDataFeedService(max_recoverable_gap=3)
    await service.subscribe("s1", SourceId.SOURCE_A, "AAA")

    assert (await service.ingest(SourceId.SOURCE_A, record_a(0))).status is IngestStatus.OK
    assert (await service.ingest(SourceId.SOURCE_A, record_a(3))).status is IngestStatus.BUFFERED
    initial = await service.dispatcher.get("s1")
    assert initial.kind is DeliveryKind.QUOTE
    gap = await service.dispatcher.get("s1")
    assert gap.kind is DeliveryKind.GAP
    assert gap.payload.state is GapState.RECOVERING

    assert (await service.ingest(SourceId.SOURCE_A, record_a(1))).status is IngestStatus.RECOVERED
    assert (await service.ingest(SourceId.SOURCE_A, record_a(2))).status is IngestStatus.RECOVERED

    recovered_events = await drain(service.dispatcher, "s1", 3)
    resolved = await service.dispatcher.get("s1")

    assert [event.payload.sequence for event in recovered_events] == [1, 2, 3]
    assert all(event.kind is DeliveryKind.RECOVERED_QUOTE for event in recovered_events)
    assert resolved.kind is DeliveryKind.GAP
    assert resolved.payload.state is GapState.NORMAL
    assert resolved.payload.recovered_sequences == (1, 2)


@pytest.mark.asyncio
async def test_disconnect_rejects_then_duplicate_replay_does_not_duplicate_delivery():
    service = MarketDataFeedService()
    await service.subscribe("s1", SourceId.SOURCE_A, "AAA")
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    normal = await service.dispatcher.get("s1")
    assert normal.payload.sequence == 0

    await service.disconnect_source(SourceId.SOURCE_A)
    disconnected = await service.ingest(SourceId.SOURCE_A, record_a(1))
    assert disconnected.status is IngestStatus.REJECTED
    await service.dispatcher.get("s1")  # rejection event

    results = await service.reconnect_source(SourceId.SOURCE_A, [record_a(0), record_a(1)])
    assert [result.status for result in results] == [IngestStatus.DUPLICATE, IngestStatus.OK]
    events = await drain(service.dispatcher, "s1", 1)
    assert [event.payload.sequence for event in events] == [1]


@pytest.mark.asyncio
async def test_unrecoverable_gap_is_marked_and_requires_explicit_resume():
    service = MarketDataFeedService(max_recoverable_gap=1)
    await service.subscribe("s1", SourceId.SOURCE_A, "AAA")
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.dispatcher.get("s1")

    result = await service.ingest(SourceId.SOURCE_A, record_a(3))
    assert result.status is IngestStatus.GAP
    gap = await service.dispatcher.get("s1")
    assert gap.payload.state is GapState.UNRECOVERABLE
    assert gap.payload.missing_from == 1
    assert gap.payload.missing_to == 2

    blocked = await service.ingest(SourceId.SOURCE_A, record_a(3))
    assert blocked.status is IngestStatus.GAP
    await service.resume_after_gap(SourceId.SOURCE_A, "AAA", 3, BASE + timedelta(seconds=3))
    await service.dispatcher.get("s1")  # explicit resume marker
    accepted = await service.ingest(SourceId.SOURCE_A, record_a(4))
    assert accepted.status is IngestStatus.OK


@pytest.mark.asyncio
async def test_concurrent_multi_source_writes_do_not_cross_subscriber_keys():
    service = MarketDataFeedService()
    await service.subscribe("a-only", SourceId.SOURCE_A, "AAA", SubscriberOptions(queue_size=64))
    await service.subscribe("b-only", SourceId.SOURCE_B, "BBB", SubscriberOptions(queue_size=64))

    async def ingest_many_a():
        for sequence in range(20):
            await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    async def ingest_many_b():
        for sequence in range(20):
            await service.ingest(SourceId.SOURCE_B, record_b(sequence))

    await asyncio.gather(ingest_many_a(), ingest_many_b())

    a_events = await drain(service.dispatcher, "a-only", 20)
    b_events = await drain(service.dispatcher, "b-only", 20)
    assert [(event.payload.source, event.payload.symbol, event.payload.sequence) for event in a_events] == [
        (SourceId.SOURCE_A, "AAA", sequence) for sequence in range(20)
    ]
    assert [(event.payload.source, event.payload.symbol, event.payload.sequence) for event in b_events] == [
        (SourceId.SOURCE_B, "BBB", sequence) for sequence in range(20)
    ]


@pytest.mark.asyncio
async def test_one_subscriber_callback_exception_does_not_stop_other_subscriber():
    service = MarketDataFeedService()
    await service.subscribe("bad", SourceId.SOURCE_A, "AAA", SubscriberOptions(queue_size=4, backpressure=BackpressurePolicy.REJECT))
    await service.subscribe("good", SourceId.SOURCE_A, "AAA", SubscriberOptions(queue_size=4, backpressure=BackpressurePolicy.REJECT))
    seen_good = []

    async def bad_handler(_event):
        raise RuntimeError("subscriber failure")

    async def good_handler(event):
        seen_good.append(event.payload.sequence)

    bad_task = asyncio.create_task(run_subscriber(service.dispatcher, "bad", bad_handler))
    good_task = asyncio.create_task(run_subscriber(service.dispatcher, "good", good_handler))
    for sequence in range(2):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))
    await asyncio.sleep(0.02)

    assert seen_good == [0, 1]
    await service.remove_subscriber("bad")
    await service.remove_subscriber("good")
    await asyncio.gather(bad_task, good_task)


@pytest.mark.asyncio
async def test_rejected_subscription_does_not_create_orphan_delivery_queue():
    service = MarketDataFeedService()
    result = await service.subscribe("orphan", SourceId.SOURCE_A, "UNKNOWN")
    assert result.ok is False
    assert service.dispatcher.has_subscriber("orphan") is False
    assert service.subscription_snapshot() == {}


@pytest.mark.asyncio
async def test_invalid_subscriber_id_does_not_create_queue():
    service = MarketDataFeedService()
    result = await service.subscribe(" ", SourceId.SOURCE_A, "AAA")
    assert result.ok is False
    assert service.dispatcher.has_subscriber(" ") is False

