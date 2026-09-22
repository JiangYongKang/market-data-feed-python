from datetime import datetime, timedelta, timezone

from decimal import Decimal

from market_feed.enums import GapState, RecordDisposition, SourceId
from market_feed.models import Quote, SubscriptionKey
from market_feed.sequence import SequenceTracker

KEY = SubscriptionKey(SourceId.SOURCE_A, "AAA")
BASE = datetime(2026, 9, 22, tzinfo=timezone.utc)


def quote(sequence: int, seconds: int | None = None, symbol: str = "AAA", source: SourceId = SourceId.SOURCE_A):
    event_time = BASE + timedelta(seconds=sequence if seconds is None else seconds)
    return Quote(
        source=source,
        symbol=symbol,
        sequence=sequence,
        event_time=event_time,
        received_time=event_time,
        price=Decimal(100 + sequence),
        quantity=Decimal(1),
        raw_record={"seq": sequence},
        record_id=f"r{sequence}",
    )


def test_contiguous_records_are_accepted_as_latest():
    tracker = SequenceTracker(KEY)
    first = tracker.observe(quote(1))
    second = tracker.observe(quote(2))

    assert first.disposition is RecordDisposition.ACCEPTED
    assert first.accepted_as_latest
    assert second.disposition is RecordDisposition.ACCEPTED
    assert [item.sequence for item in second.release] == [2]


def test_duplicate_is_separate_from_late_out_of_order():
    tracker = SequenceTracker(KEY)
    tracker.observe(quote(3))

    duplicate = tracker.observe(quote(3))
    late = tracker.observe(quote(2))

    assert duplicate.disposition is RecordDisposition.DUPLICATE
    assert duplicate.release == ()
    assert late.disposition is RecordDisposition.OUT_OF_ORDER
    assert late.accepted_as_latest is False


def test_non_monotonic_event_time_is_stale_even_if_sequence_advances():
    tracker = SequenceTracker(KEY)
    tracker.observe(quote(1, 10))
    stale = tracker.observe(quote(2, 9))

    assert stale.disposition is RecordDisposition.STALE
    assert stale.release == ()
    assert tracker.snapshot().last_sequence == 1


def test_bounded_gap_buffers_future_then_releases_recovery_quotes_in_order():
    tracker = SequenceTracker(KEY, max_recoverable_gap=3)
    tracker.observe(quote(0))
    future = tracker.observe(quote(3))

    assert future.disposition is RecordDisposition.BUFFERED
    assert future.gap_events[0].state is GapState.RECOVERING
    assert future.accepted_as_latest is False
    assert tracker.snapshot().buffered_count == 1

    first_missing = tracker.observe(quote(1))
    assert first_missing.disposition is RecordDisposition.RECOVERED
    assert [item.sequence for item in first_missing.release] == [1]

    recovered = tracker.observe(quote(2))
    assert recovered.disposition is RecordDisposition.RECOVERED
    assert [item.sequence for item in recovered.release] == [2, 3]
    assert recovered.gap_events[0].state is GapState.NORMAL
    assert tracker.snapshot().state is GapState.NORMAL


def test_gap_over_limit_is_explicitly_unrecoverable_and_does_not_publish_trigger():
    tracker = SequenceTracker(KEY, max_recoverable_gap=2)
    tracker.observe(quote(0))
    result = tracker.observe(quote(4))

    assert result.disposition is RecordDisposition.GAP_UNRECOVERABLE
    assert result.release == ()
    assert result.gap_events[0].missing_from == 1
    assert result.gap_events[0].missing_to == 3
    assert tracker.snapshot().state is GapState.UNRECOVERABLE

    blocked = tracker.observe(quote(5))
    assert blocked.disposition is RecordDisposition.GAP_UNRECOVERABLE
    assert blocked.release == ()


def test_explicit_resume_clears_unrecoverable_state():
    tracker = SequenceTracker(KEY, max_recoverable_gap=1)
    tracker.observe(quote(10))
    tracker.reset(100, BASE + timedelta(seconds=100))
    accepted = tracker.observe(quote(101, seconds=101))
    assert accepted.disposition is RecordDisposition.ACCEPTED
    assert tracker.snapshot().state is GapState.NORMAL


def test_old_replayed_sequence_is_duplicate_while_pre_baseline_sequence_is_out_of_order():
    tracker = SequenceTracker(KEY)
    for sequence in range(4):
        tracker.observe(quote(sequence))

    old_replay = tracker.observe(quote(1))
    assert old_replay.disposition is RecordDisposition.DUPLICATE

    high_start = SequenceTracker(KEY)
    high_start.observe(quote(10))
    late_unseen = high_start.observe(quote(9))
    assert late_unseen.disposition is RecordDisposition.OUT_OF_ORDER


def test_recovery_limit_counts_only_actually_missing_sequences():
    tracker = SequenceTracker(KEY, max_recoverable_gap=3)
    tracker.observe(quote(0))
    assert tracker.observe(quote(3)).disposition is RecordDisposition.BUFFERED
    # Existing seq=3 is buffered, so adding seq=5 introduces only one extra
    # actual missing sequence (4). Missing set is {1,2,4}, still recoverable.
    assert tracker.observe(quote(5)).disposition is RecordDisposition.BUFFERED
    assert tracker.observe(quote(1)).disposition is RecordDisposition.RECOVERED
    after_two = tracker.observe(quote(2))
    assert [item.sequence for item in after_two.release] == [2, 3]
    assert tracker.snapshot().state is GapState.RECOVERING
    final = tracker.observe(quote(4))
    assert [item.sequence for item in final.release] == [4, 5]
    assert tracker.snapshot().state is GapState.NORMAL


def test_extending_recovery_window_emits_updated_exact_missing_set():
    tracker = SequenceTracker(KEY, max_recoverable_gap=5)
    tracker.observe(quote(0))
    first = tracker.observe(quote(3))
    second = tracker.observe(quote(6))

    assert first.gap_events[0].missing_sequences == (1, 2)
    assert second.gap_events[0].state is GapState.RECOVERING
    assert second.gap_events[0].missing_sequences == (1, 2, 4, 5)
    assert second.disposition is RecordDisposition.BUFFERED
