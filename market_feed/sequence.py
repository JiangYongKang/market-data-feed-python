"""Per-source/per-symbol sequence and event-time state machine."""

from __future__ import annotations

from threading import RLock

from .enums import GapState, RecordDisposition
from .models import (
    GapEvent,
    Quote,
    SequenceDecision,
    SequenceState,
    SubscriptionKey,
)


class SequenceTracker:
    """Enforce strict one-by-one sequence ordering.

    Future records that fall inside a bounded gap are retained until missing
    records arrive. Records released from this recovery window are returned in
    sequence order and are explicitly distinguishable from normal records by
    the caller.
    """

    def __init__(self, key: SubscriptionKey, max_recoverable_gap: int = 8) -> None:
        if max_recoverable_gap < 0:
            raise ValueError("max_recoverable_gap must be non-negative")
        self.key = key
        self.max_recoverable_gap = max_recoverable_gap
        self._state = GapState.NORMAL
        self._last_sequence: int | None = None
        self._delivered_low_water: int | None = None
        self._expected: int | None = None
        self._last_event_time = None
        self._missing_from: int | None = None
        self._missing_to: int | None = None
        self._missing: set[int] = set()
        self._recovered: set[int] = set()
        self._buffer: dict[int, Quote] = {}
        self._lock = RLock()

    def reset(self, sequence: int, event_time=None) -> None:
        """Install a new authoritative baseline after explicit resume."""
        if sequence < 0:
            raise ValueError("sequence must be non-negative")
        with self._lock:
            self._clear_locked()
            self._state = GapState.NORMAL
            self._last_sequence = sequence
            self._delivered_low_water = sequence
            self._expected = sequence + 1
            self._last_event_time = event_time

    def _clear_locked(self) -> None:
        self._buffer.clear()
        self._missing_from = None
        self._missing_to = None
        self._missing.clear()
        self._recovered.clear()

    def _gap_event(
        self,
        state: GapState,
        missing_from: int,
        missing_to: int | None,
        quote: Quote | None = None,
        missing: tuple[int, ...] = (),
        recovered: tuple[int, ...] = (),
        detail: str = "",
    ) -> GapEvent:
        return GapEvent(
            key=self.key,
            state=state,
            missing_from=missing_from,
            missing_to=missing_to,
            detected_time=(quote.received_time if quote is not None else self._now()),
            missing_sequences=missing,
            recovered_sequences=recovered,
            detail=detail,
        )

    @staticmethod
    def _now():
        from datetime import datetime, timezone

        return datetime.now(timezone.utc)

    def _decision(
        self,
        disposition: RecordDisposition,
        *,
        release: tuple[Quote, ...] = (),
        gaps: tuple[GapEvent, ...] = (),
        latest: bool = False,
        detail: str = "",
    ) -> SequenceDecision:
        return SequenceDecision(
            disposition=disposition,
            release=release,
            gap_events=gaps,
            accepted_as_latest=latest,
            detail=detail,
        )

    def _is_stale(self, quote: Quote) -> bool:
        return self._last_event_time is not None and quote.event_time <= self._last_event_time

    def _enter_unrecoverable(self, quote: Quote, missing_from: int, missing_to: int, gap_length: int) -> SequenceDecision:
        gap = self._gap_event(
            GapState.UNRECOVERABLE,
            missing_from,
            missing_to,
            quote,
            tuple(sorted(self._missing)) or tuple(range(missing_from, missing_to + 1)),
            detail=(
                f"gap length {gap_length} exceeds bounded recovery limit "
                f"{self.max_recoverable_gap}; explicit resume is required"
            ),
        )
        self._clear_locked()
        self._state = GapState.UNRECOVERABLE
        # Keep the triggering quote as the proposed watermark but do not publish
        # it: it belongs to the region after an unresolved gap.
        self._last_sequence = quote.sequence
        self._expected = quote.sequence + 1
        self._last_event_time = quote.event_time
        return self._decision(
            RecordDisposition.GAP_UNRECOVERABLE,
            gaps=(gap,),
            latest=False,
            detail=gap.detail,
        )

    def _release_ready_locked(self) -> tuple[tuple[Quote, ...], GapEvent | None]:
        if self._expected is None:
            return (), None
        released: list[Quote] = []
        assert self._missing_from is not None
        while self._expected in self._buffer:
            quote = self._buffer.pop(self._expected)
            was_missing = quote.sequence in self._missing
            self._missing.discard(quote.sequence)
            if was_missing:
                self._recovered.add(quote.sequence)
            released.append(quote)
            self._last_sequence = quote.sequence
            self._last_event_time = quote.event_time
            self._expected = quote.sequence + 1

        if self._state == GapState.RECOVERING and not self._missing:
            recovered = tuple(sorted(self._recovered))
            final_quote = released[-1] if released else None
            resolved = self._gap_event(
                GapState.NORMAL,
                self._missing_from,
                self._missing_to,
                final_quote,
                (),
                recovered,
                detail="all missing sequences recovered; normal processing resumes",
            )
            self._state = GapState.NORMAL
            self._missing_from = None
            self._missing_to = None
            self._recovered.clear()
            return tuple(released), resolved
        return tuple(released), None

    def observe(self, quote: Quote) -> SequenceDecision:
        with self._lock:
            if quote.source != self.key.source or quote.symbol != self.key.symbol:
                return self._decision(
                    RecordDisposition.REJECTED,
                    detail="sequence tracker received a mismatched subscription key",
                )

            if self._state == GapState.UNRECOVERABLE:
                return self._decision(
                    RecordDisposition.GAP_UNRECOVERABLE,
                    detail="feed is behind an unrecoverable gap and must be resumed explicitly",
                )

            if self._expected is None:
                if self._is_stale(quote):
                    return self._decision(
                        RecordDisposition.STALE,
                        detail="event timestamp is not newer than the current baseline",
                    )
                self._last_sequence = quote.sequence
                self._delivered_low_water = quote.sequence
                self._expected = quote.sequence + 1
                self._last_event_time = quote.event_time
                return self._decision(
                    RecordDisposition.ACCEPTED,
                    release=(quote,),
                    latest=True,
                    detail="first accepted quote",
                )

            assert self._last_sequence is not None
            if quote.sequence in self._buffer:
                return self._decision(
                    RecordDisposition.DUPLICATE,
                    detail=f"sequence {quote.sequence} is already buffered for recovery",
                )
            if quote.sequence == self._last_sequence:
                return self._decision(
                    RecordDisposition.DUPLICATE,
                    detail=f"sequence {quote.sequence} already published",
                )
            if quote.sequence < self._expected:
                assert self._delivered_low_water is not None
                already_delivered = self._delivered_low_water <= quote.sequence <= self._last_sequence
                if already_delivered:
                    return self._decision(
                        RecordDisposition.DUPLICATE,
                        detail=(
                            f"sequence {quote.sequence} was already delivered; "
                            f"expected {self._expected}"
                        ),
                    )
                return self._decision(
                    RecordDisposition.OUT_OF_ORDER,
                    detail=(
                        f"late unseen sequence {quote.sequence}; expected {self._expected}, "
                        "it cannot become the latest quote"
                    ),
                )
            if self._is_stale(quote):
                return self._decision(
                    RecordDisposition.STALE,
                    detail=(
                        f"event_time {quote.event_time.isoformat()} is not newer than "
                        f"{self._last_event_time.isoformat()}"
                    ),
                )

            if quote.sequence == self._expected and self._state == GapState.NORMAL:
                self._last_sequence = quote.sequence
                self._last_event_time = quote.event_time
                self._expected = quote.sequence + 1
                return self._decision(
                    RecordDisposition.ACCEPTED,
                    release=(quote,),
                    latest=True,
                    detail="contiguous quote",
                )

            if quote.sequence > self._expected:
                missing_from = self._expected
                max_buffered = max(self._buffer) if self._buffer else quote.sequence - 1
                missing_to = max(quote.sequence - 1, max_buffered)
                # Existing buffered sequences inside this span are not missing.
                buffered_in_span = sum(1 for value in self._buffer if missing_from <= value <= missing_to)
                gap_length = missing_to - missing_from + 1 - buffered_in_span
                if gap_length > self.max_recoverable_gap:
                    return self._enter_unrecoverable(quote, missing_from, missing_to, gap_length)

                was_normal = self._state == GapState.NORMAL
                self._state = GapState.RECOVERING
                if self._missing_from is None:
                    self._missing_from = missing_from
                old_missing_to = self._missing_to
                new_missing = {
                    value
                    for value in range(missing_from, missing_to + 1)
                    if value != quote.sequence and value not in self._buffer
                }
                newly_missing = new_missing - self._missing
                self._missing.update(new_missing)
                self._missing_to = max(self._missing_to or missing_to, missing_to)
                self._buffer[quote.sequence] = quote
                gaps: tuple[GapEvent, ...] = ()
                if was_normal or newly_missing:
                    gap_start = self._missing_from if was_normal else min(newly_missing)
                    gap_end = missing_to if was_normal else max(new_missing)
                    gaps = (
                        self._gap_event(
                            GapState.RECOVERING,
                            gap_start,
                            gap_end,
                            quote,
                            tuple(sorted(self._missing)),
                            detail=f"missing {len(self._missing)} sequence(s); later records are buffered",
                        ),
                    )
                return self._decision(
                    RecordDisposition.BUFFERED,
                    gaps=gaps,
                    latest=False,
                    detail="record is buffered until the gap is recovered",
                )

            # Expected missing record has arrived during recovery.
            self._buffer[quote.sequence] = quote
            released, resolved = self._release_ready_locked()
            gap_events = (resolved,) if resolved is not None else ()
            if released:
                return self._decision(
                    RecordDisposition.RECOVERED,
                    release=released,
                    gaps=gap_events,
                    latest=False,
                    detail="recovered records released in sequence order",
                )
            return self._decision(
                RecordDisposition.BUFFERED,
                gaps=gap_events,
                latest=False,
                detail="still waiting for earlier missing sequences",
            )

    def snapshot(self) -> SequenceState:
        with self._lock:
            return SequenceState(
                key=self.key,
                state=self._state,
                last_sequence=self._last_sequence,
                expected_sequence=self._expected,
                last_event_time=self._last_event_time,
                missing_from=self._missing_from,
                missing_to=self._missing_to,
                buffered_count=len(self._buffer),
            )
