"""Bounded, per-key history windows for late-joining consumers.

Only quotes that have fully passed sequence/event-time validation and were
released (normal or recovered) by the SequenceTracker are appended. Quotes
still buffered behind a recoverable gap, out-of-order/stale/duplicate records
and records on the far side of an unrecoverable gap never enter the window.
"""

from __future__ import annotations

from datetime import datetime
from threading import RLock

from .enums import GapState
from .models import (
    HistoryRetention,
    KeyHistoryStats,
    Quote,
    SubscriptionKey,
)


class HistoryStore:
    """Bounded confirmed-quote windows, isolated per (source, symbol).

    Each window is a strictly sequence-contiguous slice of confirmed quotes.
    Retention is bounded per key by ``HistoryRetention.max_quotes`` and an
    optional ``max_age_seconds`` evaluated against event_time. Every eviction
    is counted and reflected in ``stats`` so consumers can observe how much
    history remains and where old data was trimmed.
    """

    def __init__(self, retention: HistoryRetention | None = None) -> None:
        retention = retention or HistoryRetention()
        if retention.max_quotes < 1:
            raise ValueError("history max_quotes must be at least 1")
        if retention.max_age_seconds is not None and retention.max_age_seconds <= 0:
            raise ValueError("history max_age_seconds must be positive")
        self.retention = retention
        self._windows: dict[SubscriptionKey, list[Quote]] = {}
        self._totals: dict[SubscriptionKey, int] = {}
        self._last_evicted: dict[SubscriptionKey, int] = {}
        self._reset_detail: dict[SubscriptionKey, str] = {}
        self._continuity_reset: dict[SubscriptionKey, bool] = {}
        self._gap_state: dict[SubscriptionKey, GapState] = {}
        self._lock = RLock()

    def _window_locked(self, key: SubscriptionKey) -> list[Quote]:
        return self._windows.setdefault(key, [])

    def _record_evictions_locked(self, key: SubscriptionKey, evicted: list[Quote], reason: str) -> None:
        if not evicted:
            return
        self._totals[key] = self._totals.get(key, 0) + len(evicted)
        self._last_evicted[key] = evicted[-1].sequence
        self._reset_detail[key] = f"{len(evicted)} quote(s) evicted ({reason})"

    def append(self, key: SubscriptionKey, quotes: tuple[Quote, ...] | list[Quote]) -> list[int]:
        """Append freshly confirmed quotes and enforce the bounded window.

        Returns the sequences that were evicted (from this append or from the
        newly added tail, oldest first). Callers must only pass quotes released
        by the sequence tracker; contiguity with the existing window is
        asserted and never repaired silently.
        """
        if not quotes:
            return []
        evicted_sequences: list[int] = []
        with self._lock:
            window = self._window_locked(key)
            expected = window[-1].sequence + 1 if window else None
            for quote in quotes:
                if quote.source != key.source or quote.symbol != key.symbol:
                    raise ValueError("history quote does not match its subscription key")
                if expected is not None and quote.sequence != expected:
                    raise ValueError(
                        f"non-contiguous history append for {key}: "
                        f"expected {expected}, got {quote.sequence}"
                    )
                expected = quote.sequence + 1
                window.append(quote)

            if len(window) > self.retention.max_quotes:
                overflow = len(window) - self.retention.max_quotes
                dropped = window[:overflow]
                evicted_sequences.extend(quote.sequence for quote in dropped)
                del window[:overflow]
                self._record_evictions_locked(key, dropped, "max_quotes bound")

            if self.retention.max_age_seconds is not None and window:
                horizon = window[-1].event_time
                aged: list[Quote] = []
                cutoff_seconds = self.retention.max_age_seconds
                while window and (horizon - window[0].event_time).total_seconds() > cutoff_seconds:
                    aged.append(window.pop(0))
                evicted_sequences.extend(quote.sequence for quote in aged)
                self._record_evictions_locked(key, aged, "max_age_seconds bound")

            return evicted_sequences

    def snapshot(
        self,
        key: SubscriptionKey,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> tuple[Quote, ...]:
        """Return a contiguous confirmed slice, oldest first.

        ``after_sequence`` resumes strictly after a checkpoint; ``limit``
        returns at most the newest ``limit`` quotes. The returned tuple is a
        copy: callers never block writers by holding it.
        """
        with self._lock:
            window = list(self._windows.get(key, ()))
        if after_sequence is not None:
            window = [quote for quote in window if quote.sequence > after_sequence]
        if limit is not None and len(window) > limit:
            window = window[-limit:]
        return tuple(window)

    def newest_sequence(self, key: SubscriptionKey) -> int | None:
        with self._lock:
            window = self._windows.get(key)
            return window[-1].sequence if window else None

    def oldest_sequence(self, key: SubscriptionKey) -> int | None:
        with self._lock:
            window = self._windows.get(key)
            return window[0].sequence if window else None

    def gap_state(self, key: SubscriptionKey) -> GapState:
        with self._lock:
            return self._gap_state.get(key, GapState.NORMAL)

    def mark_gap_state(self, key: SubscriptionKey, state: GapState) -> None:
        with self._lock:
            self._gap_state[key] = state

    def reset_continuity(self, key: SubscriptionKey, detail: str = "") -> list[int]:
        """Drop the whole window after an explicit resume past an unrecoverable gap.

        Pre-gap quotes can never be presented next to post-reset quotes, so the
        window is discarded wholesale and the continuity flag latched.
        """
        with self._lock:
            window = self._windows.get(key)
            dropped = list(window or ())
            evicted = [quote.sequence for quote in dropped]
            if window is not None:
                window.clear()
            self._record_evictions_locked(key, dropped, "unrecoverable gap resume")
            self._continuity_reset[key] = True
            self._reset_detail[key] = detail or "continuity reset after unrecoverable gap"
            self._gap_state[key] = GapState.NORMAL
            return evicted

    def stats(self, key: SubscriptionKey) -> KeyHistoryStats:
        with self._lock:
            window = self._windows.get(key)
            oldest = window[0] if window else None
            newest = window[-1] if window else None
            return KeyHistoryStats(
                key=key,
                retained=len(window or ()),
                capacity=self.retention.max_quotes,
                oldest_sequence=oldest.sequence if oldest else None,
                newest_sequence=newest.sequence if newest else None,
                oldest_event_time=oldest.event_time if oldest else None,
                newest_event_time=newest.event_time if newest else None,
                total_evicted=self._totals.get(key, 0),
                last_evicted_sequence=self._last_evicted.get(key),
                continuity_reset=self._continuity_reset.get(key, False),
                gap_state=self._gap_state.get(key, GapState.NORMAL),
                detail=self._reset_detail.get(key, ""),
            )

    def all_stats(self) -> dict[SubscriptionKey, KeyHistoryStats]:
        with self._lock:
            keys = set(self._windows) | set(self._gap_state)
        return {key: self.stats(key) for key in keys}
