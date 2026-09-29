"""Tests for bounded history snapshots and checkpoint resume (late joiners)."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from market_feed.enums import (
    BackpressurePolicy,
    DeliveryKind,
    GapState,
    HistoryJoinStatus,
    IngestStatus,
    SourceId,
)
from market_feed.history import HistoryStore
from market_feed.models import (
    HistoryRetention,
    ProgressMarker,
    SubscriptionKey,
    SubscriberOptions,
)
from market_feed.service import MarketDataFeedService

BASE = datetime(2026, 9, 22, tzinfo=timezone.utc)


def record_a(sequence: int, symbol: str = "AAA", price: str = "100.00"):
    ts_ms = int((BASE + timedelta(seconds=sequence)).timestamp() * 1000)
    return {
        "trade_id": f"a-{symbol}-{sequence}",
        "sym": symbol,
        "seq": sequence,
        "ts_ms": ts_ms,
        "px": price,
        "qty": "1",
    }


async def ingest_range(service, start, stop, symbol="AAA"):
    for sequence in range(start, stop):
        await service.ingest(SourceId.SOURCE_A, record_a(sequence, symbol))


async def queue_marker(service, subscriber_id):
    event = await asyncio.wait_for(service.dispatcher.get(subscriber_id), 1)
    assert event.kind is DeliveryKind.HISTORY_BOUNDARY
    return event.payload


# ---------------------------------------------------------------------------
# HistoryStore unit behaviour
# ---------------------------------------------------------------------------


def test_history_store_is_bounded_and_eviction_is_observable():
    from decimal import Decimal
    from market_feed.models import Quote, SubscriptionKey

    store = HistoryStore(HistoryRetention(max_quotes=3))
    key = SubscriptionKey(SourceId.SOURCE_A, "AAA")

    def quote(sequence):
        return Quote(
            source=SourceId.SOURCE_A, symbol="AAA", sequence=sequence,
            event_time=BASE + timedelta(seconds=sequence), received_time=BASE,
            price=Decimal(1), quantity=None, raw_record={}, record_id=f"r{sequence}",
        )

    evicted = store.append(key, tuple(quote(i) for i in range(5)))
    assert evicted == [0, 1]
    stats = store.stats(key)
    assert stats.retained == 3
    assert stats.capacity == 3
    assert (stats.oldest_sequence, stats.newest_sequence) == (2, 4)
    assert stats.total_evicted == 2
    assert stats.last_evicted_sequence == 1
    assert [quote.sequence for quote in store.snapshot(key)] == [2, 3, 4]
    assert [quote.sequence for quote in store.snapshot(key, after_sequence=2)] == [3, 4]
    assert [quote.sequence for quote in store.snapshot(key, limit=2)] == [3, 4]


def test_history_store_windows_are_isolated_per_source_and_symbol():
    from decimal import Decimal
    from market_feed.models import Quote, SubscriptionKey

    store = HistoryStore(HistoryRetention(max_quotes=10))

    def quote(source, symbol, sequence):
        return Quote(
            source=source, symbol=symbol, sequence=sequence,
            event_time=BASE + timedelta(seconds=sequence), received_time=BASE,
            price=Decimal(1), quantity=None, raw_record={}, record_id=f"{source.value}-{symbol}-{sequence}",
        )

    key_a = SubscriptionKey(SourceId.SOURCE_A, "AAA")
    key_b = SubscriptionKey(SourceId.SOURCE_B, "BBB")
    store.append(key_a, (quote(SourceId.SOURCE_A, "AAA", 0), quote(SourceId.SOURCE_A, "AAA", 1)))
    store.append(key_b, (quote(SourceId.SOURCE_B, "BBB", 7),))

    assert [q.sequence for q in store.snapshot(key_a)] == [0, 1]
    assert [q.sequence for q in store.snapshot(key_b)] == [7]
    assert store.snapshot(SubscriptionKey(SourceId.SOURCE_A, "BBB")) == ()


# ---------------------------------------------------------------------------
# Scenario 1: mid-stream subscriber gets snapshot then a seamless live flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_late_subscriber_gets_bounded_snapshot_then_seamless_live_flow():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=3))
    await ingest_range(service, 0, 5)  # confirmed 0..4, history keeps 2..4

    result = await service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA")

    assert result.status is HistoryJoinStatus.SNAPSHOT_READY
    assert [quote.sequence for quote in result.quotes] == [2, 3, 4]
    assert result.last_sequence == 4
    assert result.incomplete is False

    # The only queued event before new writes is the boundary marker.
    boundary = await queue_marker(service, "late")
    assert boundary.last_sequence == 4
    assert boundary.quote_count == 3

    # Live write after the snapshot handoff is delivered exactly once, with no
    # overlap and no gap relative to the snapshot watermark.
    await service.ingest(SourceId.SOURCE_A, record_a(5))
    event = await asyncio.wait_for(service.dispatcher.get("late"), 1)
    assert event.kind is DeliveryKind.QUOTE
    assert event.payload.sequence == 5

    await service.ingest(SourceId.SOURCE_A, record_a(6))
    event = await asyncio.wait_for(service.dispatcher.get("late"), 1)
    assert event.payload.sequence == 6


@pytest.mark.asyncio
async def test_snapshot_never_contains_buffered_or_unrecoverable_quotes():
    service = MarketDataFeedService(max_recoverable_gap=4)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    # seq 3 jumps ahead: it is buffered behind the open gap and must not be
    # presented as the latest confirmed quote in a snapshot.
    buffered = await service.ingest(SourceId.SOURCE_A, record_a(3))
    assert buffered.status is IngestStatus.BUFFERED

    result = await service.subscribe_with_history(
        "late", SourceId.SOURCE_A, "AAA", wait_for_gap=False
    )
    assert result.status is HistoryJoinStatus.GAP_PENDING
    assert result.incomplete is True
    assert [quote.sequence for quote in result.quotes] == [0]
    assert "gap" in result.detail
    # gap_pending must not register the subscription.
    assert not service.registry.is_subscribed("late", result.key)


@pytest.mark.asyncio
async def test_subscription_during_recovery_waits_and_returns_consistent_snapshot():
    service = MarketDataFeedService(max_recoverable_gap=4)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.ingest(SourceId.SOURCE_A, record_a(3))  # gap on 1,2

    join = asyncio.create_task(
        service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA", gap_wait_timeout=2)
    )
    await asyncio.sleep(0.05)
    assert not join.done()

    await service.ingest(SourceId.SOURCE_A, record_a(1))
    await service.ingest(SourceId.SOURCE_A, record_a(2))
    result = await asyncio.wait_for(join, 2)

    assert result.status is HistoryJoinStatus.SNAPSHOT_READY
    assert [quote.sequence for quote in result.quotes] == [0, 1, 2, 3]
    assert result.last_sequence == 3

    # The recovered quotes must not also arrive live to the late subscriber:
    # the marker is the sole queued event, followed strictly by future writes.
    boundary = await queue_marker(service, "late")
    assert boundary.last_sequence == 3
    await service.ingest(SourceId.SOURCE_A, record_a(4))
    event = await asyncio.wait_for(service.dispatcher.get("late"), 1)
    assert event.kind is DeliveryKind.QUOTE
    assert event.payload.sequence == 4


@pytest.mark.asyncio
async def test_gap_wait_timeout_reports_incomplete_window_without_registering():
    service = MarketDataFeedService(max_recoverable_gap=4)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.ingest(SourceId.SOURCE_A, record_a(3))

    result = await service.subscribe_with_history(
        "late", SourceId.SOURCE_A, "AAA", gap_wait_timeout=0.02
    )
    assert result.status is HistoryJoinStatus.GAP_PENDING
    assert result.incomplete is True
    assert [quote.sequence for quote in result.quotes] == [0]
    assert not service.registry.is_subscribed("late", result.key)
    assert service.dispatcher.has_subscriber("late") is False


@pytest.mark.asyncio
async def test_unrecoverable_gap_blocks_snapshot_until_explicit_resume():
    service = MarketDataFeedService(max_recoverable_gap=1)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.ingest(SourceId.SOURCE_A, record_a(3))  # unrecoverable

    blocked = await service.subscribe_with_history(
        "late", SourceId.SOURCE_A, "AAA", gap_wait_timeout=0.02
    )
    assert blocked.status is HistoryJoinStatus.UNRECOVERABLE_GAP
    assert blocked.quotes == ()
    assert not service.registry.is_subscribed("late", blocked.key)

    await service.resume_after_gap(SourceId.SOURCE_A, "AAA", 3, BASE + timedelta(seconds=3))
    # Pre-gap history is discarded: no spliced view across the hole.
    stats = service.history.stats(blocked.key)
    assert stats.retained == 0
    assert stats.continuity_reset is True
    assert stats.gap_state is GapState.NORMAL

    resumed = await service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA")
    assert resumed.status is HistoryJoinStatus.SNAPSHOT_READY
    assert resumed.quotes == ()
    await queue_marker(service, "late")
    await service.ingest(SourceId.SOURCE_A, record_a(4))
    event = await asyncio.wait_for(service.dispatcher.get("late"), 1)
    assert event.payload.sequence == 4


# ---------------------------------------------------------------------------
# Scenario 2: reconnect with a progress marker replays only the missing part
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_with_checkpoint_replays_only_missing_quotes_then_live():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=50))
    await ingest_range(service, 0, 10)

    # Consumer crashed after processing sequence 4 and reconnects.
    result = await service.subscribe_with_history(
        "resumer",
        SourceId.SOURCE_A,
        "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 4),
    )
    assert result.status is HistoryJoinStatus.RESUME_READY
    assert [quote.sequence for quote in result.quotes] == [5, 6, 7, 8, 9]
    assert result.last_sequence == 9

    boundary = await queue_marker(service, "resumer")
    assert boundary.status is HistoryJoinStatus.RESUME_READY
    assert boundary.last_sequence == 9
    assert boundary.checkpoint_sequence == 4

    await service.ingest(SourceId.SOURCE_A, record_a(10))
    event = await asyncio.wait_for(service.dispatcher.get("resumer"), 1)
    assert event.kind is DeliveryKind.QUOTE
    assert event.payload.sequence == 10


@pytest.mark.asyncio
async def test_checkpoint_at_window_edge_is_supported():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=3))
    await ingest_range(service, 0, 5)  # window 2..4

    result = await service.subscribe_with_history(
        "resumer", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 1),  # 2..4 all backlog
    )
    assert result.status is HistoryJoinStatus.RESUME_READY
    assert [quote.sequence for quote in result.quotes] == [2, 3, 4]

    # checkpoint == newest means nothing is missing; live continues after it.
    result2 = await service.subscribe_with_history(
        "resumer2", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 4),
    )
    assert result2.status is HistoryJoinStatus.RESUME_READY
    assert result2.quotes == ()
    assert result2.last_sequence == 4


@pytest.mark.asyncio
async def test_checkpoint_outside_window_is_rejected_not_silently_skipped():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=3))
    await ingest_range(service, 0, 5)  # oldest retained = 2

    too_old = await service.subscribe_with_history(
        "old", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 0),
    )
    assert too_old.status is HistoryJoinStatus.CHECKPOINT_TOO_OLD
    assert too_old.incomplete is True
    assert "fresh snapshot" in too_old.detail
    assert not service.registry.is_subscribed("old", too_old.key)
    assert service.dispatcher.has_subscriber("old") is False

    ahead = await service.subscribe_with_history(
        "ahead", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 99),
    )
    assert ahead.status is HistoryJoinStatus.CHECKPOINT_AHEAD
    assert ahead.incomplete is True
    assert not service.registry.is_subscribed("ahead", ahead.key)


@pytest.mark.asyncio
async def test_resume_across_an_unrecoverable_gap_requires_resnapshot():
    service = MarketDataFeedService(max_recoverable_gap=1, history_retention=HistoryRetention(max_quotes=50))
    await ingest_range(service, 0, 1)
    # Gap of 2 sequences (1..2 with last=0) exceeds the limit of 1.
    await service.ingest(SourceId.SOURCE_A, record_a(3))
    assert service.history.gap_state(
        SubscriptionKey(SourceId.SOURCE_A, "AAA")
    ) is GapState.UNRECOVERABLE

    result = await service.subscribe_with_history(
        "resumer", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 0),
        gap_wait_timeout=0.02,
    )
    assert result.status is HistoryJoinStatus.UNRECOVERABLE_GAP
    assert not service.registry.is_subscribed("resumer", result.key)


@pytest.mark.asyncio
async def test_resume_exactly_at_post_gap_baseline_backfills_nothing_then_lives():
    service = MarketDataFeedService(max_recoverable_gap=1)
    await service.ingest(SourceId.SOURCE_A, record_a(0))
    await service.ingest(SourceId.SOURCE_A, record_a(3))  # unrecoverable, history cleared
    await service.resume_after_gap(SourceId.SOURCE_A, "AAA", 3, BASE + timedelta(seconds=3))

    result = await service.subscribe_with_history(
        "resumer", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "AAA", 3),
    )
    assert result.status is HistoryJoinStatus.RESUME_READY
    assert result.quotes == ()
    assert result.last_sequence == 3
    await queue_marker(service, "resumer")
    await service.ingest(SourceId.SOURCE_A, record_a(4))
    event = await asyncio.wait_for(service.dispatcher.get("resumer"), 1)
    assert event.payload.sequence == 4


@pytest.mark.asyncio
async def test_checkpoint_mismatched_with_subscription_key_is_rejected():
    service = MarketDataFeedService()
    await ingest_range(service, 0, 2)
    result = await service.subscribe_with_history(
        "resumer", SourceId.SOURCE_A, "AAA",
        checkpoint=ProgressMarker(SourceId.SOURCE_A, "BBB", 0),
    )
    assert result.status is HistoryJoinStatus.REJECTED
    assert not service.registry.is_subscribed("resumer", result.key)


# ---------------------------------------------------------------------------
# Bounds, isolation, duplicate subscriptions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_limit_and_retention_are_both_respected():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=10))
    await ingest_range(service, 0, 10)
    result = await service.subscribe_with_history(
        "late", SourceId.SOURCE_A, "AAA", snapshot_limit=3
    )
    assert [quote.sequence for quote in result.quotes] == [7, 8, 9]


@pytest.mark.asyncio
async def test_history_does_not_cross_symbols_or_sources():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=50))
    await ingest_range(service, 0, 4, symbol="AAA")
    await ingest_range(service, 0, 2, symbol="BBB")

    aaa = await service.subscribe_with_history("a", SourceId.SOURCE_A, "AAA")
    bbb = await service.subscribe_with_history("b", SourceId.SOURCE_A, "BBB")
    assert {q.symbol for q in aaa.quotes} == {"AAA"}
    assert [q.sequence for q in aaa.quotes] == [0, 1, 2, 3]
    assert {q.symbol for q in bbb.quotes} == {"BBB"}
    assert [q.sequence for q in bbb.quotes] == [0, 1]


@pytest.mark.asyncio
async def test_repeated_history_subscription_does_not_resend_snapshot():
    service = MarketDataFeedService()
    await ingest_range(service, 0, 3)
    first = await service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA")
    assert first.status is HistoryJoinStatus.SNAPSHOT_READY
    await queue_marker(service, "late")

    second = await service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA")
    assert second.status is HistoryJoinStatus.ALREADY_SUBSCRIBED
    assert second.subscription.changed is False
    assert second.quotes == ()
    # No second marker was enqueued.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(service.dispatcher.get("late"), 0.05)


# ---------------------------------------------------------------------------
# Non-regression: snapshot joins coexist with live delivery and backpressure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_existing_subscriber_is_unaffected_by_late_snapshot_join():
    service = MarketDataFeedService(
        history_retention=HistoryRetention(max_quotes=10),
    )
    await service.subscribe("early", SourceId.SOURCE_A, "AAA", SubscriberOptions(queue_size=32))
    await ingest_range(service, 0, 3)
    early_seen = [
        (await asyncio.wait_for(service.dispatcher.get("early"), 1)).payload.sequence
        for _ in range(3)
    ]
    assert early_seen == [0, 1, 2]

    await service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA")
    late_marker = await asyncio.wait_for(service.dispatcher.get("late"), 1)
    assert late_marker.kind is DeliveryKind.HISTORY_BOUNDARY

    await service.ingest(SourceId.SOURCE_A, record_a(3))
    late_event = await asyncio.wait_for(service.dispatcher.get("late"), 1)
    early_event = await asyncio.wait_for(service.dispatcher.get("early"), 1)
    assert late_event.payload.sequence == 3
    assert early_event.payload.sequence == 3


@pytest.mark.asyncio
async def test_snapshot_join_respects_subscriber_backpressure_policy():
    service = MarketDataFeedService(
        history_retention=HistoryRetention(max_quotes=10),
    )
    await ingest_range(service, 0, 6)
    # Tiny reject queue: the boundary marker is the only item enqueued at join
    # time, so the join still succeeds; subsequent live floods follow policy.
    result = await service.subscribe_with_history(
        "late", SourceId.SOURCE_A, "AAA",
        options=SubscriberOptions(queue_size=1, backpressure=BackpressurePolicy.REJECT),
        snapshot_limit=2,
    )
    assert result.status is HistoryJoinStatus.SNAPSHOT_READY
    assert [q.sequence for q in result.quotes] == [4, 5]
    boundary = await queue_marker(service, "late")
    assert boundary.quote_count == 2


@pytest.mark.asyncio
async def test_snapshot_generation_does_not_block_writes_for_other_keys():
    service = MarketDataFeedService(max_recoverable_gap=4)
    await service.subscribe("other", SourceId.SOURCE_A, "BBB", SubscriberOptions(queue_size=64))
    await service.ingest(SourceId.SOURCE_A, record_a(0, "AAA"))
    # Open a recoverable gap on AAA: a snapshot join there will park waiting.
    await service.ingest(SourceId.SOURCE_A, record_a(3, "AAA"))

    parked = asyncio.create_task(
        service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA", gap_wait_timeout=2)
    )
    await asyncio.sleep(0.05)
    assert not parked.done()

    # Writes to a different symbol and a different source must proceed without
    # waiting on the parked snapshot request.
    async def write_other_symbol():
        for sequence in range(5):
            await service.ingest(SourceId.SOURCE_A, record_a(sequence, "BBB"))

    await asyncio.wait_for(write_other_symbol(), timeout=1)
    seen = [
        (await asyncio.wait_for(service.dispatcher.get("other"), 1)).payload.sequence
        for _ in range(5)
    ]
    assert seen == [0, 1, 2, 3, 4]

    await service.ingest(SourceId.SOURCE_A, record_a(1, "AAA"))
    await service.ingest(SourceId.SOURCE_A, record_a(2, "AAA"))
    result = await asyncio.wait_for(parked, 2)
    assert result.status is HistoryJoinStatus.SNAPSHOT_READY
    assert [quote.sequence for quote in result.quotes] == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_concurrent_late_joiners_each_get_independent_consistent_snapshots():
    service = MarketDataFeedService(history_retention=HistoryRetention(max_quotes=20))
    await ingest_range(service, 0, 8)

    async def join(subscriber_id):
        return await service.subscribe_with_history(subscriber_id, SourceId.SOURCE_A, "AAA")

    results = await asyncio.gather(join("j1"), join("j2"), join("j3"))
    for result in results:
        assert result.status is HistoryJoinStatus.SNAPSHOT_READY
        assert [quote.sequence for quote in result.quotes] == list(range(8))
    for subscriber_id in ("j1", "j2", "j3"):
        boundary = await queue_marker(service, subscriber_id)
        assert boundary.last_sequence == 7

    await service.ingest(SourceId.SOURCE_A, record_a(8))
    for subscriber_id in ("j1", "j2", "j3"):
        event = await asyncio.wait_for(service.dispatcher.get(subscriber_id), 1)
        assert event.kind is DeliveryKind.QUOTE
        assert event.payload.sequence == 8
