"""Bounded, per-key confirmed history used for snapshots and resume."""

from __future__ import annotations

from collections import deque
from threading import RLock
from typing import Deque

from .enums import SubscriptionErrorKind
from .models import (
    HistoryStats,
    Quote,
    SubscriptionKey,
)


class _Entry:
    __slots__ = ("quotes", "barrier", "evicted_total")

    def __init__(self) -> None:
        self.quotes: Deque[Quote] = deque()
        # Exclusive lower bound of the current valid region. When set, every
        # retained sequence is strictly greater than the barrier.
        self.barrier: int | None = None
        self.evicted_total = 0


class HistoryStore:
    """Retain only fully validated quotes for each (source, symbol).

    Retention is bounded per key. The service feeds this store exclusively
    with quotes released by the sequence state machine (normal contiguous
    quotes and in-order recovered quotes). Out-of-order, stale and still
    buffered records never reach here. An unrecoverable gap installs a
    barrier and discards the pre-gap region, so a snapshot can never look
    contiguous across a hole.

    The store uses a plain reentrant lock: it never awaits, so snapshot
    generation cannot block the ingest hot path (which holds the asyncio key
    lock) for longer than copying a bounded slice.
    """

    def __init__(self, capacity_per_key: int = 64) -> None:
        if capacity_per_key < 1:
            raise ValueError("capacity_per_key must be at least 1")
        self._capacity = capacity_per_key
        self._entries: dict[SubscriptionKey, _Entry] = {}
        self._lock = RLock()

    def _entry(self, key: SubscriptionKey) -> _Entry:
        entry = self._entries.get(key)
        if entry is None:
            entry = _Entry()
            self._entries[key] = entry
        return entry

    def record(self, quote: Quote) -> None:
        """Retain one contiguous, fully validated quote.

        Quotes are appended in release order, which is strictly increasing
        sequence order for a single key. Older quotes beyond the capacity are
        evicted and counted.
        """
        key = SubscriptionKey(quote.source, quote.symbol)
        with self._lock:
            entry = self._entry(key)
            if entry.quotes and quote.sequence <= entry.quotes[-1].sequence:
                # Defensive: callers only forward released, in-order quotes.
                raise ValueError(
                    f"history for {key} already contains sequence "
                    f"{entry.quotes[-1].sequence}; refusing non-contiguous {quote.sequence}"
                )
            entry.quotes.append(quote)
            while len(entry.quotes) > self._capacity:
                entry.quotes.popleft()
                entry.evicted_total += 1

    def mark_barrier(self, key: SubscriptionKey, sequence: int) -> None:
        """Invalidate retained history after an unrecoverable gap.

        The triggering future quote is not retained. ``sequence`` is the first
        sequence of the post-gap, not-yet-valid region; the retained region is
        reset to an exclusive lower bound at it.
        """
        with self._lock:
            entry = self._entry(key)
            entry.quotes.clear()
            entry.barrier = sequence

    def install_baseline(self, key: SubscriptionKey, sequence: int) -> None:
        """Anchor the post-resume region at an authoritative sequence.

        The baseline sequence itself is not delivered as a quote; it becomes
        the exclusive lower bound of a fresh, empty valid region.
        """
        with self._lock:
            entry = self._entry(key)
            entry.quotes.clear()
            entry.barrier = sequence

    @staticmethod
    def _slice_after(entry: _Entry, sequence: int) -> tuple[Quote, ...] | SubscriptionErrorKind:
        # Region starts strictly after the barrier.
        if entry.barrier is not None and sequence < entry.barrier:
            return SubscriptionErrorKind.RESUME_BARRIER_PRESENT
        oldest = entry.quotes[0].sequence if entry.quotes else None
        if oldest is not None and sequence < oldest - 1:
            # The sequence at oldest - 1 was evicted without being processed,
            # so retained history cannot continue the checkpoint contiguously.
            return SubscriptionErrorKind.RESUME_POSITION_EVICTED
        newest = entry.quotes[-1].sequence if entry.quotes else None
        if newest is not None and sequence > newest:
            return SubscriptionErrorKind.RESUME_POSITION_AHEAD
        return tuple(quote for quote in entry.quotes if quote.sequence > sequence)

    def snapshot_tail(self, key: SubscriptionKey, limit: int | None = None) -> tuple[Quote, ...]:
        """Return up to ``limit`` most recent retained confirmed quotes."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or not entry.quotes:
                return ()
            if limit is None or limit >= len(entry.quotes):
                return tuple(entry.quotes)
            return tuple(entry.quotes)[-limit:]

    def replay_after(self, key: SubscriptionKey, sequence: int) -> tuple[Quote, ...] | SubscriptionErrorKind:
        """Return retained quotes strictly after a checkpoint.

        Error kinds instead of quotes when the checkpoint was evicted, lies
        beyond retained history, or is separated by an unrecoverable-gap
        barrier: callers must realign with a fresh snapshot in those cases.
        """
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                # No data has ever been retained for this key. A checkpoint at
                # the origin can only mean "nothing processed yet"; anything
                # larger references data this store never had, so the caller
                # must realign rather than silently starting in the middle.
                if sequence <= 0:
                    return ()
                return SubscriptionErrorKind.RESUME_POSITION_EVICTED
            return self._slice_after(entry, sequence)

    def stats(self, key: SubscriptionKey) -> HistoryStats:
        """Return observable retention/eviction state for one key."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return HistoryStats(
                    key=key,
                    retained_count=0,
                    capacity=self._capacity,
                    oldest_sequence=None,
                    newest_sequence=None,
                    evicted_total=0,
                    barrier_sequence=None,
                    complete=True,
                )
            return HistoryStats(
                key=key,
                retained_count=len(entry.quotes),
                capacity=self._capacity,
                oldest_sequence=entry.quotes[0].sequence if entry.quotes else None,
                newest_sequence=entry.quotes[-1].sequence if entry.quotes else None,
                evicted_total=entry.evicted_total,
                barrier_sequence=entry.barrier,
                complete=True,
            )

    def stats_snapshot(self) -> dict[SubscriptionKey, HistoryStats]:
        with self._lock:
            return {key: self.stats(key) for key in self._entries}
