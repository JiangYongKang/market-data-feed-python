import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from market_feed.enums import (
    BackpressurePolicy,
    DeliveryKind,
    GapState,
    IngestStatus,
    SourceId,
    SubscriptionErrorKind,
)
from market_feed.models import ResumePosition, SubscriberOptions
from market_feed.service import MarketDataFeedService

BASE = datetime(2026, 9, 22, tzinfo=timezone.utc)


def record_a(sequence: int, symbol: str = "AAA", price: str = "100.00"):
    ts_ms = int((BASE + timedelta(seconds=sequence)).timestamp() * 1000)
    return {"trade_id": f"a-{symbol}-{sequence}", "sym": symbol, "seq": sequence, "ts_ms": ts_ms, "px": price, "qty": "1"}


def handoff_seqs(event) -> list[int]:
    return [quote.sequence for quote in event.payload.quotes]


async def drain(dispatcher, subscriber_id: str, count: int):
    return [await dispatcher.get(subscriber_id) for _ in range(count)]


@pytest.mark.asyncio
async def test_late_subscriber_gets_snapshot_then_smooth_live_stream_without_dup_or_gap():
    service = MarketDataFeedService(logger=logging.getLogger("test"))
    await service.subscribe("early", SourceId.SOURCE_A, "AAA")
    for sequence in range(5):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))
    # Existing subscriber drains the live traffic.
    await drain(service.dispatcher, "early", 5)

    # A consumer starts mid-stream and asks for a bounded recent snapshot.
    result = await service.subscribe(
        "late",
        SourceId.SOURCE_A,
        "AAA",
        SubscriberOptions(queue_size=16),
        snapshot=3,
    )
    assert result.ok
    assert result.handoff is not None
    assert [q.sequence for q in result.handoff.quotes] == [2, 3, 4]
    assert result.handoff.live_from_sequence == 5

    handoff_event = await service.dispatcher.get("late")
    assert handoff_event.kind is DeliveryKind.HANDOFF
    assert handoff_seqs(handoff_event) == [2, 3, 4]

    # Live updates continue exactly from the boundary: no 2/3/4 repeated.
    for sequence in range(5, 8):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))
    events = await drain(service.dispatcher, "late", 3)
    assert [event.payload.sequence for event in events] == [5, 6, 7]
    assert all(event.kind is DeliveryKind.QUOTE for event in events)


@pytest.mark.asyncio
async def test_resume_with_checkpoint_replays_only_missing_sequences():
    service = MarketDataFeedService(logger=logging.getLogger("test"))
    # "crash" happens after processing sequence 2.
    for sequence in range(6):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    result = await service.subscribe(
        "reconnected",
        SourceId.SOURCE_A,
        "AAA",
        SubscriberOptions(queue_size=16),
        resume_from=ResumePosition(SourceId.SOURCE_A, "AAA", 2),
    )
    assert result.ok
    assert result.handoff.mode.value == "resume"
    assert result.handoff.resume_from_sequence == 2
    assert [q.sequence for q in result.handoff.quotes] == [3, 4, 5]
    assert result.handoff.live_from_sequence == 6

    handoff_event = await service.dispatcher.get("reconnected")
    assert handoff_event.kind is DeliveryKind.HANDOFF
    assert handoff_seqs(handoff_event) == [3, 4, 5]

    await service.ingest(SourceId.SOURCE_A, record_a(6))
    live = await service.dispatcher.get("reconnected")
    assert live.kind is DeliveryKind.QUOTE
    assert live.payload.sequence == 6


@pytest.mark.asyncio
async def test_retention_capacity_evicts_old_data_and_stats_are_observable():
    service = MarketDataFeedService(history_capacity_per_key=3, logger=logging.getLogger("test"))
    for sequence in range(6):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    stats = service.history_stats(SourceId.SOURCE_A, "AAA")
    assert stats.capacity == 3
    assert stats.retained_count == 3
    assert stats.oldest_sequence == 3
    assert stats.newest_sequence == 5
    assert stats.evicted_total == 3

    result = await service.subscribe("s", SourceId.SOURCE_A, "AAA", snapshot=10)
    assert [q.sequence for q in result.handoff.quotes] == [3, 4, 5]


@pytest.mark.asyncio
async def test_resume_from_evicted_position_is_explicit_not_silently_skipped():
    service = MarketDataFeedService(history_capacity_per_key=3)
    for sequence in range(6):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    # retained 3..5; checkpoint 0 leaves evicted-but-unprocessed sequence 1/2.
    result = await service.subscribe(
        "stale",
        SourceId.SOURCE_A,
        "AAA",
        resume_from=ResumePosition(SourceId.SOURCE_A, "AAA", 0),
    )
    assert result.ok is False
    assert result.kind is SubscriptionErrorKind.RESUME_POSITION_EVICTED
    assert "snapshot" in result.detail
    # A failed handoff does not register a subscription or queue.
    assert "stale" not in dict(service.subscription_snapshot())


@pytest.mark.asyncio
async def test_resume_from_future_position_is_explicit():
    service = MarketDataFeedService()
    for sequence in range(3):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    result = await service.subscribe(
        "future",
        SourceId.SOURCE_A,
        "AAA",
        resume_from=ResumePosition(SourceId.SOURCE_A, "AAA", 42),
    )
    assert result.ok is False
    assert result.kind is SubscriptionErrorKind.RESUME_POSITION_AHEAD


@pytest.mark.asyncio
async def test_snapshot_during_recoverable_gap_is_refused_until_consistent():
    service = MarketDataFeedService(max_recoverable_gap=5)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.ingest(SourceId.SOURCE_A, record_a(3))  # gap 1,2

    refused = await service.subscribe("mid", SourceId.SOURCE_A, "AAA", snapshot=10)
    assert refused.ok is False
    assert refused.kind is SubscriptionErrorKind.SNAPSHOT_INCOMPLETE

    async def heal():
        await asyncio.sleep(0.03)
        await service.ingest(SourceId.SOURCE_A, record_a(1))
        await service.ingest(SourceId.SOURCE_A, record_a(2))

    asyncio.create_task(heal())
    waited = await service.subscribe(
        "patient",
        SourceId.SOURCE_A,
        "AAA",
        SubscriberOptions(queue_size=16),
        snapshot=10,
        wait_for_consistency=True,
        consistency_timeout=2.0,
    )
    assert waited.ok
    assert [q.sequence for q in waited.handoff.quotes] == [0, 1, 2, 3]
    # Recovered quotes are still marked recovered on the live stream; the
    # snapshot itself is a consistent initial view.
    event = await service.dispatcher.get("patient")
    assert event.kind is DeliveryKind.HANDOFF


@pytest.mark.asyncio
async def test_snapshot_wait_timeout_leaves_no_subscription():
    service = MarketDataFeedService(max_recoverable_gap=5)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.ingest(SourceId.SOURCE_A, record_a(3))  # never healed

    refused = await service.subscribe(
        "timeout",
        SourceId.SOURCE_A,
        "AAA",
        snapshot=10,
        wait_for_consistency=True,
        consistency_timeout=0.02,
    )
    assert refused.ok is False
    assert refused.kind is SubscriptionErrorKind.SNAPSHOT_INCOMPLETE
    assert "timeout" not in dict(service.subscription_snapshot())


@pytest.mark.asyncio
async def test_unrecoverable_gap_invalidates_history_until_explicit_resume():
    service = MarketDataFeedService(max_recoverable_gap=1)
    for sequence in range(3):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    # seq 6 with gap 3,4,5 -> unrecoverable; barrier at 6.
    result = await service.ingest(SourceId.SOURCE_A, record_a(6))
    assert result.status is IngestStatus.GAP
    stats = service.history_stats(SourceId.SOURCE_A, "AAA")
    assert stats.retained_count == 0
    assert stats.barrier_sequence == 6

    blocked = await service.subscribe("b", SourceId.SOURCE_A, "AAA", snapshot=10)
    assert blocked.ok is False
    assert blocked.kind is SubscriptionErrorKind.RESUME_BARRIER_PRESENT

    resume_cross = await service.subscribe(
        "c",
        SourceId.SOURCE_A,
        "AAA",
        resume_from=ResumePosition(SourceId.SOURCE_A, "AAA", 2),
    )
    assert resume_cross.ok is False
    assert resume_cross.kind is SubscriptionErrorKind.RESUME_BARRIER_PRESENT

    # Explicit resume anchors a fresh region; post-resume data is snapshotable.
    await service.resume_after_gap(SourceId.SOURCE_A, "AAA", 6, BASE + timedelta(seconds=6))
    await service.ingest(SourceId.SOURCE_A, record_a(7))
    ok = await service.subscribe("d", SourceId.SOURCE_A, "AAA", SubscriberOptions(queue_size=8), snapshot=10)
    assert ok.ok
    assert [q.sequence for q in ok.handoff.quotes] == [7]
    assert ok.handoff.live_from_sequence == 8


@pytest.mark.asyncio
async def test_snapshot_does_not_cross_sources_or_symbols():
    service = MarketDataFeedService()
    for sequence in range(3):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence, "AAA"))
    ts = int((BASE + timedelta(seconds=90)).timestamp() * 1000)
    b_record = {
        "id": "b-BBB-9",
        "ticker": "BBB",
        "sequence": 9,
        "event_time": (BASE + timedelta(seconds=90)).isoformat().replace("+00:00", "Z"),
        "price_scaled": 10000,
        "price_scale": 2,
        "size": "1",
    }
    await service.ingest(SourceId.SOURCE_B, b_record)

    result = await service.subscribe("s", SourceId.SOURCE_B, "BBB", snapshot=10)
    assert {q.source for q in result.handoff.quotes} == {SourceId.SOURCE_B}
    assert {q.symbol for q in result.handoff.quotes} == {"BBB"}
    assert [q.sequence for q in result.handoff.quotes] == [9]


@pytest.mark.asyncio
async def test_handoff_honours_bounded_queue_and_never_loses_history_under_drop_oldest():
    service = MarketDataFeedService(history_capacity_per_key=10)
    for sequence in range(6):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    # Queue size 3 while replay has 4 quotes: DROP_OLDEST must not evict
    # history; handoff degrades to waiting, and the full replay survives.
    result = await service.subscribe(
        "small",
        SourceId.SOURCE_A,
        "AAA",
        SubscriberOptions(queue_size=3, backpressure=BackpressurePolicy.DROP_OLDEST),
        resume_from=ResumePosition(SourceId.SOURCE_A, "AAA", 1),
    )
    assert result.ok
    received = []
    while True:
        event = await service.dispatcher.get("small")
        if event.kind is DeliveryKind.HANDOFF:
            received.extend(handoff_seqs(event))
            break
    assert received == [2, 3, 4, 5]


@pytest.mark.asyncio
async def test_concurrent_late_subscribers_each_get_contiguous_view():
    service = MarketDataFeedService(history_capacity_per_key=20)
    for sequence in range(10):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    results = await asyncio.gather(
        *[
            service.subscribe(
                f"s{index}",
                SourceId.SOURCE_A,
                "AAA",
                SubscriberOptions(queue_size=32),
                resume_from=ResumePosition(SourceId.SOURCE_A, "AAA", index),
            )
            for index in range(4)
        ]
    )
    for index, result in enumerate(results):
        assert result.ok
        expected = list(range(index + 1, 10))
        assert [q.sequence for q in result.handoff.quotes] == expected


@pytest.mark.asyncio
async def test_handoff_boundary_is_atomic_against_concurrent_ingest():
    service = MarketDataFeedService(history_capacity_per_key=50, logger=logging.getLogger("test"))
    # Pre-populate some history.
    for sequence in range(10):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence))

    produced = {"up_to": 9}

    async def producer():
        sequence = 10
        while sequence < 25:
            await service.ingest(SourceId.SOURCE_A, record_a(sequence))
            produced["up_to"] = sequence
            sequence += 1
            await asyncio.sleep(0)  # yield so subscribe can interleave

    task = asyncio.create_task(producer())
    try:
        result = await service.subscribe(
            "racer",
            SourceId.SOURCE_A,
            "AAA",
            SubscriberOptions(queue_size=128),
            snapshot=100,
        )
        assert result.ok
        boundary = result.handoff.live_from_sequence
        snapshot_seqs = [q.sequence for q in result.handoff.quotes]
        # Snapshot is a dense prefix ending exactly at boundary - 1.
        assert snapshot_seqs == list(range(snapshot_seqs[0], boundary))

        event = await service.dispatcher.get("racer")
        assert event.kind is DeliveryKind.HANDOFF
        await task  # ensure all live quotes 10..24 were published

        # Collect every quote from boundary up to the last produced one.
        live_count = produced["up_to"] - boundary + 1
        live_events = await drain(service.dispatcher, "racer", live_count)
        live_seqs = [event.payload.sequence for event in live_events]
        assert all(event.kind is DeliveryKind.QUOTE for event in live_events)
        assert live_seqs == list(range(boundary, produced["up_to"] + 1))

        seen = snapshot_seqs + live_seqs
        assert seen == sorted(set(seen))
        assert seen == list(range(seen[0], seen[-1] + 1))
    finally:
        await task
