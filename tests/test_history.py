from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from market_feed.enums import SourceId, SubscriptionErrorKind
from market_feed.history import HistoryStore
from market_feed.models import Quote, SubscriptionKey

BASE = datetime(2026, 9, 22, tzinfo=timezone.utc)
KEY_A = SubscriptionKey(SourceId.SOURCE_A, "AAA")
KEY_B = SubscriptionKey(SourceId.SOURCE_B, "BBB")


def quote(key: SubscriptionKey, sequence: int) -> Quote:
    return Quote(
        source=key.source,
        symbol=key.symbol,
        sequence=sequence,
        event_time=BASE + timedelta(seconds=sequence),
        received_time=BASE + timedelta(seconds=sequence),
        price=Decimal("100"),
        quantity=Decimal("1"),
        raw_record={},
        record_id=f"{key.symbol}-{sequence}",
    )


def test_capacity_bounds_retention_and_eviction_is_observable():
    store = HistoryStore(capacity_per_key=3)
    for seq in range(5):
        store.record(quote(KEY_A, seq))

    stats = store.stats(KEY_A)
    assert stats.retained_count == 3
    assert stats.capacity == 3
    assert stats.oldest_sequence == 2
    assert stats.newest_sequence == 4
    assert stats.evicted_total == 2


def test_snapshot_tail_returns_most_recent_and_respects_limit():
    store = HistoryStore(capacity_per_key=10)
    for seq in range(6):
        store.record(quote(KEY_A, seq))

    assert [q.sequence for q in store.snapshot_tail(KEY_A)] == [0, 1, 2, 3, 4, 5]
    assert [q.sequence for q in store.snapshot_tail(KEY_A, limit=2)] == [4, 5]


def test_records_for_different_keys_never_cross():
    store = HistoryStore(capacity_per_key=10)
    for seq in range(4):
        store.record(quote(KEY_A, seq))
    for seq in range(2):
        store.record(quote(KEY_B, seq + 100))

    a = store.snapshot_tail(KEY_A)
    b = store.snapshot_tail(KEY_B)
    assert {q.source for q in a} == {SourceId.SOURCE_A}
    assert {q.symbol for q in a} == {"AAA"}
    assert [q.sequence for q in b] == [100, 101]
    assert store.stats(KEY_A).newest_sequence == 3
    assert store.stats(KEY_B).newest_sequence == 101


def test_replay_after_checkpoint_excludes_processed_and_keeps_rest():
    store = HistoryStore(capacity_per_key=10)
    for seq in range(6):
        store.record(quote(KEY_A, seq))

    replay = store.replay_after(KEY_A, 2)
    assert isinstance(replay, tuple)
    assert [q.sequence for q in replay] == [3, 4, 5]

    # Resuming exactly at the newest returns an empty, still-valid replay.
    assert store.replay_after(KEY_A, 5) == ()


def test_resume_beyond_retained_history_is_an_explicit_error():
    store = HistoryStore(capacity_per_key=3)
    for seq in range(5):
        store.record(quote(KEY_A, seq))  # retained: 2..4

    # 0 < oldest(2) - 1 = 1: a whole retained region (seq 1) was evicted.
    assert store.replay_after(KEY_A, 0) is SubscriptionErrorKind.RESUME_POSITION_EVICTED
    # checkpoint == oldest - 1 is contiguous (seq 2 is the first missing one).
    assert [q.sequence for q in store.replay_after(KEY_A, 1)] == [2, 3, 4]
    assert store.replay_after(KEY_A, 9) is SubscriptionErrorKind.RESUME_POSITION_AHEAD


def test_unrecoverable_gap_barrier_invalidates_history_and_blocks_cross_barrier_resume():
    store = HistoryStore(capacity_per_key=10)
    for seq in range(3):
        store.record(quote(KEY_A, seq))
    store.mark_barrier(KEY_A, sequence=6)

    stats = store.stats(KEY_A)
    assert stats.retained_count == 0
    assert stats.barrier_sequence == 6
    assert store.snapshot_tail(KEY_A) == ()

    # Checkpoints in the invalidated pre-barrier region cannot be continued.
    assert store.replay_after(KEY_A, 2) is SubscriptionErrorKind.RESUME_BARRIER_PRESENT


def test_baseline_after_resume_starts_a_fresh_region():
    store = HistoryStore(capacity_per_key=10)
    for seq in range(3):
        store.record(quote(KEY_A, seq))
    store.mark_barrier(KEY_A, sequence=6)
    store.install_baseline(KEY_A, sequence=6)
    store.record(quote(KEY_A, 7))

    stats = store.stats(KEY_A)
    assert stats.barrier_sequence == 6
    assert [q.sequence for q in store.snapshot_tail(KEY_A)] == [7]
    # Resuming at the baseline itself yields only post-resume quotes.
    assert [q.sequence for q in store.replay_after(KEY_A, 6)] == [7]
    assert store.replay_after(KEY_A, 2) is SubscriptionErrorKind.RESUME_BARRIER_PRESENT


def test_unknown_key_resume_semantics():
    store = HistoryStore(capacity_per_key=5)
    assert store.replay_after(KEY_A, 0) == ()
    assert store.replay_after(KEY_A, 3) is SubscriptionErrorKind.RESUME_POSITION_EVICTED
    stats = store.stats(KEY_A)
    assert stats.retained_count == 0
    assert stats.oldest_sequence is None


def test_invalid_capacity_rejected():
    with pytest.raises(ValueError):
        HistoryStore(capacity_per_key=0)
