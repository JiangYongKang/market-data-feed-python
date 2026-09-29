"""Application service coordinating normalization, recovery and dispatch."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from .enums import (
    BackpressurePolicy,
    DeliveryKind,
    GapState,
    HistoryJoinStatus,
    IngestStatus,
    RecordDisposition,
    RejectReason,
    SourceId,
    SubscriptionErrorKind,
)
from .logging_utils import configure_logging, event_log
from .models import (
    DeliveryEvent,
    GapEvent,
    HistoryBoundary,
    HistoryRetention,
    HistorySubscriptionResult,
    IngestResult,
    KeyHistoryStats,
    ProgressMarker,
    Quote,
    RejectedRecord,
    SequenceState,
    SubscriptionKey,
    SubscriptionResult,
    SubscriberOptions,
)
from .history import HistoryStore
from .normalization import NormalizationRouter
from .sequence import SequenceTracker
from .subscriptions import SubscriptionRegistry
from .dispatcher import Dispatcher
from .sources import ScriptedSource


_DISPOSITION_STATUS = {
    RecordDisposition.ACCEPTED: IngestStatus.OK,
    RecordDisposition.RECOVERED: IngestStatus.RECOVERED,
    RecordDisposition.BUFFERED: IngestStatus.BUFFERED,
    RecordDisposition.DUPLICATE: IngestStatus.DUPLICATE,
    RecordDisposition.OUT_OF_ORDER: IngestStatus.OUT_OF_ORDER,
    RecordDisposition.STALE: IngestStatus.STALE,
    RecordDisposition.GAP_DETECTED: IngestStatus.GAP,
    RecordDisposition.GAP_UNRECOVERABLE: IngestStatus.GAP,
    RecordDisposition.GAP_RESOLVED: IngestStatus.RECOVERED,
    RecordDisposition.REJECTED: IngestStatus.REJECTED,
}


class MarketDataFeedService:
    """Asyncio-safe facade for the in-memory deterministic feed."""

    def __init__(
        self,
        known_symbols: dict[SourceId, frozenset[str]] | None = None,
        max_recoverable_gap: int = 8,
        history_retention: HistoryRetention | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.registry = SubscriptionRegistry(known_symbols)
        self.router = NormalizationRouter()
        self.dispatcher = Dispatcher(logger or configure_logging())
        self.history = HistoryStore(history_retention)
        self._trackers: dict[SubscriptionKey, SequenceTracker] = {}
        # The per-key condition also serializes state transitions for a key:
        # ingests acquire it as the "key lock", and snapshot/resume requests
        # can wait on it for an in-flight recoverable gap to resolve.
        self._key_locks: dict[SubscriptionKey, asyncio.Condition] = {}
        self._source_locks: dict[SourceId, asyncio.Lock] = {
            SourceId.SOURCE_A: asyncio.Lock(),
            SourceId.SOURCE_B: asyncio.Lock(),
        }
        self._source_state: dict[SourceId, bool] = {
            SourceId.SOURCE_A: True,
            SourceId.SOURCE_B: True,
        }
        self.max_recoverable_gap = max_recoverable_gap
        self._logger = logger or logging.getLogger("market_feed")

    def _tracker(self, key: SubscriptionKey) -> SequenceTracker:
        tracker = self._trackers.get(key)
        if tracker is None:
            tracker = SequenceTracker(key, self.max_recoverable_gap)
            self._trackers[key] = tracker
        return tracker


    def _key_lock(self, key: SubscriptionKey) -> asyncio.Condition:
        lock = self._key_locks.get(key)
        if lock is None:
            lock = asyncio.Condition()
            self._key_locks[key] = lock
        return lock

    def history_stats(self) -> dict[SubscriptionKey, KeyHistoryStats]:
        return self.history.all_stats()

    def sequence_snapshot(self, key: SubscriptionKey) -> SequenceState | None:
        tracker = self._trackers.get(key)
        return None if tracker is None else tracker.snapshot()

    def subscription_snapshot(self) -> dict[str, list[SubscriptionKey]]:
        return self.registry.snapshot()

    async def add_subscriber(
        self,
        subscriber_id: str,
        options: SubscriberOptions | None = None,
    ) -> SubscriberOptions:
        return await self.dispatcher.add_subscriber(subscriber_id, options)

    async def remove_subscriber(self, subscriber_id: str) -> SubscriptionResult:
        result = self.registry.remove_subscriber(subscriber_id)
        await self.dispatcher.remove_subscriber(subscriber_id)
        return result

    async def subscribe(
        self,
        subscriber_id: str,
        source: SourceId,
        symbol: str,
        options: SubscriberOptions | None = None,
    ) -> SubscriptionResult:
        if not isinstance(subscriber_id, str) or not subscriber_id.strip():
            result = SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id if isinstance(subscriber_id, str) else "",
                key=None,
                kind=SubscriptionErrorKind.INVALID_SUBSCRIBER,
                detail="subscription rejected: invalid subscriber_id",
                changed=False,
            )
            event_log(self._logger, "subscription_request", subscriber_id=result.subscriber_id, source=source, symbol=symbol, ok=False, changed=False, detail=result.detail)
            return result

        key = SubscriptionKey(source=source, symbol=symbol)
        validation_kind = self.registry.validation_error_kind(key)
        if validation_kind is not None:
            result = SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=validation_kind,
                detail=f"subscription rejected: {validation_kind.value}",
                changed=False,
            )
            event_log(
                self._logger,
                "subscription_request",
                subscriber_id=subscriber_id,
                source=source,
                symbol=symbol,
                ok=False,
                changed=False,
                detail=result.detail,
            )
            return result

        # Create delivery capacity only after key validation succeeds. Existing
        # subscribers retain their configured queue and backpressure policy.
        actual_options = await self.dispatcher.add_subscriber(subscriber_id, options)
        result = self.registry.subscribe(subscriber_id, key)
        event_log(
            self._logger,
            "subscription_request",
            subscriber_id=subscriber_id,
            source=source,
            symbol=symbol,
            ok=result.ok,
            changed=result.changed,
            detail=result.detail,
            queue_size=actual_options.queue_size,
            backpressure=actual_options.backpressure,
        )
        return result

    async def unsubscribe(self, subscriber_id: str, source: SourceId, symbol: str) -> SubscriptionResult:
        key = SubscriptionKey(source=source, symbol=symbol)
        result = self.registry.unsubscribe(subscriber_id, key)
        event_log(
            self._logger,
            "unsubscription_request",
            subscriber_id=subscriber_id,
            source=source,
            symbol=symbol,
            ok=result.ok,
            changed=result.changed,
            detail=result.detail,
        )
        return result

    async def subscribe_with_history(
        self,
        subscriber_id: str,
        source: SourceId,
        symbol: str,
        options: SubscriberOptions | None = None,
        *,
        checkpoint: ProgressMarker | None = None,
        snapshot_limit: int | None = None,
        wait_for_gap: bool = True,
        gap_wait_timeout: float = 5.0,
    ) -> HistorySubscriptionResult:
        """Subscribe with a bounded history snapshot or a checkpoint resume.

        - Without ``checkpoint``: attach the latest confirmed snapshot.
        - With ``checkpoint``: replay only the confirmed quotes strictly after
          the checkpoint, then continue live.
        - When the key is mid recovery the call waits for the gap to resolve so
          the returned window is contiguous end-to-end; on timeout it reports
          ``gap_pending`` without registering the subscription.
        - An unrecoverable gap or an out-of-range checkpoint is reported with
          an explicit status instead of silently splicing a discontinuous flow.

        The snapshot is returned out-of-band in the result; the live queue
        receives exactly one HISTORY_BOUNDARY marker, after which only live
        quotes above the snapshot watermark are delivered.
        """
        key = SubscriptionKey(source=source, symbol=symbol)

        if not isinstance(subscriber_id, str) or not subscriber_id.strip():
            rejected = SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id if isinstance(subscriber_id, str) else "",
                key=None,
                kind=SubscriptionErrorKind.INVALID_SUBSCRIBER,
                detail="subscription rejected: invalid subscriber_id",
                changed=False,
            )
            return self._history_result(rejected, HistoryJoinStatus.REJECTED, None, (), None, None, True, rejected.detail)

        validation_kind = self.registry.validation_error_kind(key)
        if validation_kind is not None:
            rejected = SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=validation_kind,
                detail=f"subscription rejected: {validation_kind.value}",
                changed=False,
            )
            return self._history_result(rejected, HistoryJoinStatus.REJECTED, key, (), None, None, True, rejected.detail)

        if checkpoint is not None and (checkpoint.source != source or checkpoint.symbol != symbol):
            rejected = SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=None,
                detail="checkpoint does not match the requested source/symbol",
                changed=False,
            )
            return self._history_result(rejected, HistoryJoinStatus.REJECTED, key, (), None, None, True, rejected.detail)
        if checkpoint is not None and checkpoint.sequence < 0:
            rejected = SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=None,
                detail="checkpoint sequence must be non-negative",
                changed=False,
            )
            return self._history_result(rejected, HistoryJoinStatus.REJECTED, key, (), None, None, True, rejected.detail)
        if snapshot_limit is not None and snapshot_limit < 1:
            raise ValueError("snapshot_limit must be at least 1")

        deadline = asyncio.get_running_loop().time() + gap_wait_timeout

        # All per-key state (tracker, history, subscriber index reads during
        # publish) is mutated while holding this condition, so check, wait and
        # finalize are atomic w.r.t. per-key ingests without taking the source
        # lock; waiting here never blocks a different key's writers.
        condition = self._key_lock(key)
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            async with condition:
                tracker = self._trackers.get(key)
                state = tracker.snapshot() if tracker is not None else None
                gap_state = self.history.gap_state(key)

                if gap_state is GapState.UNRECOVERABLE:
                    detail = "feed is behind an unrecoverable gap; pull a fresh snapshot after explicit resume"
                    result = self._history_result(
                        self._unchanged(subscriber_id, key),
                        HistoryJoinStatus.UNRECOVERABLE_GAP,
                        key, (),
                        state.last_sequence if state else None,
                        state.expected_sequence if state else None,
                        True, detail,
                    )
                    event_log(self._logger, "history_subscription", subscriber_id=subscriber_id, key=key, status=result.status.value, detail=detail)
                    return result

                if gap_state is GapState.RECOVERING and wait_for_gap and remaining > 0:
                    try:
                        await asyncio.wait_for(condition.wait(), timeout=remaining)
                    except TimeoutError:
                        pass
                    continue

                if gap_state is GapState.RECOVERING:
                    pending = self._pending_result(subscriber_id, key, state)
                    event_log(self._logger, "history_subscription", subscriber_id=subscriber_id, key=key, status=pending.status.value, detail=pending.detail)
                    return pending

                return await self._finalize_history_subscription(
                    subscriber_id=subscriber_id,
                    key=key,
                    options=options,
                    checkpoint=checkpoint,
                    snapshot_limit=snapshot_limit,
                    state=state,
                )

    def _history_result(
        self,
        subscription: SubscriptionResult,
        status: HistoryJoinStatus,
        key: SubscriptionKey | None,
        quotes: tuple[Quote, ...],
        last_sequence: int | None,
        expected_sequence: int | None,
        incomplete: bool,
        detail: str,
    ) -> HistorySubscriptionResult:
        return HistorySubscriptionResult(
            subscription=subscription,
            status=status,
            key=key,
            quotes=quotes,
            last_sequence=last_sequence,
            expected_sequence=expected_sequence,
            incomplete=incomplete,
            detail=detail,
        )

    def _unchanged(self, subscriber_id: str, key: SubscriptionKey) -> SubscriptionResult:
        return SubscriptionResult(
            ok=True,
            subscriber_id=subscriber_id,
            key=key,
            kind=None,
            detail="no subscription change",
            changed=False,
        )

    def _pending_result(self, subscriber_id: str, key: SubscriptionKey, state: SequenceState | None) -> HistorySubscriptionResult:
        partial = self.history.snapshot(key)
        detail = (
            "a recoverable gap is still open; the partial window ends at "
            f"sequence {partial[-1].sequence if partial else None}; retry later or subscribe without history"
        )
        return self._history_result(
            self._unchanged(subscriber_id, key),
            HistoryJoinStatus.GAP_PENDING,
            key,
            partial,
            state.last_sequence if state else None,
            state.expected_sequence if state else None,
            True,
            detail,
        )

    async def _finalize_history_subscription(
        self,
        *,
        subscriber_id: str,
        key: SubscriptionKey,
        options: SubscriberOptions | None,
        checkpoint: ProgressMarker | None,
        snapshot_limit: int | None,
        state: SequenceState | None,
    ) -> HistorySubscriptionResult:
        """Run with both the source lock and the key condition held."""
        last_sequence = state.last_sequence if state else None
        expected_sequence = state.expected_sequence if state else None

        if self.registry.is_subscribed(subscriber_id, key):
            result = self._history_result(
                SubscriptionResult(
                    ok=True, subscriber_id=subscriber_id, key=key, kind=None,
                    detail="subscription already exists; no snapshot is re-sent",
                    changed=False,
                ),
                HistoryJoinStatus.ALREADY_SUBSCRIBED,
                key, (), last_sequence, expected_sequence, False,
                "subscriber already holds this subscription; resend requires unsubscribe first",
            )
            event_log(self._logger, "history_subscription", subscriber_id=subscriber_id, key=key, status=result.status.value)
            return result

        if checkpoint is not None:
            status, quotes, detail = self._resume_window(key, checkpoint.sequence, state)
            incomplete = status is not HistoryJoinStatus.RESUME_READY
        else:
            quotes = self.history.snapshot(key, limit=snapshot_limit)
            status = HistoryJoinStatus.SNAPSHOT_READY
            detail = f"snapshot of {len(quotes)} confirmed quote(s)"
            incomplete = False

        if status in (HistoryJoinStatus.CHECKPOINT_TOO_OLD, HistoryJoinStatus.CHECKPOINT_AHEAD):
            result = self._history_result(
                self._unchanged(subscriber_id, key),
                status, key, (), last_sequence, expected_sequence, True, detail,
            )
            event_log(self._logger, "history_subscription", subscriber_id=subscriber_id, key=key, status=status.value, detail=detail)
            return result

        watermark = quotes[-1].sequence if quotes else (checkpoint.sequence if checkpoint is not None else last_sequence)

        actual_options = await self.dispatcher.add_subscriber(subscriber_id, options)
        subscription = self.registry.subscribe(subscriber_id, key)
        assert subscription.ok and subscription.changed

        boundary = HistoryBoundary(
            source=key.source,
            symbol=key.symbol,
            status=status,
            last_sequence=watermark,
            quote_count=len(quotes),
            checkpoint_sequence=checkpoint.sequence if checkpoint is not None else None,
            incomplete=False,
            detail=detail,
        )
        # The boundary belongs only to the newly joining subscriber; existing
        # subscribers must keep receiving a plain live stream.
        audits = await self.dispatcher.publish(
            [subscriber_id],
            self._delivery(DeliveryKind.HISTORY_BOUNDARY, boundary),
        )
        for audit in audits:
            event_log(self._logger, "backpressure_decision", audit=audit, key=key)
        result = self._history_result(
            subscription, status, key, quotes, watermark, expected_sequence, False, detail,
        )
        event_log(
            self._logger,
            "history_subscription",
            subscriber_id=subscriber_id,
            key=key,
            status=status.value,
            quote_count=len(quotes),
            watermark=watermark,
            queue_size=actual_options.queue_size,
            backpressure=actual_options.backpressure,
        )
        return result

    def _resume_window(
        self,
        key: SubscriptionKey,
        checkpoint: int,
        state: SequenceState | None,
    ) -> tuple[HistoryJoinStatus, tuple[Quote, ...], str]:
        last_sequence = state.last_sequence if state else None
        if last_sequence is None:
            return (
                HistoryJoinStatus.CHECKPOINT_AHEAD,
                (),
                f"checkpoint {checkpoint} is ahead: no confirmed quote exists yet",
            )
        if checkpoint > last_sequence:
            return (
                HistoryJoinStatus.CHECKPOINT_AHEAD,
                (),
                f"checkpoint {checkpoint} is ahead of the latest confirmed sequence {last_sequence}",
            )
        oldest = self.history.oldest_sequence(key)
        if oldest is None:
            # An empty window is only resumable when the checkpoint sits exactly
            # on the current baseline (e.g. right after an explicit resume);
            # there is nothing to backfill and live flow continues.
            if checkpoint == last_sequence:
                return HistoryJoinStatus.RESUME_READY, (), f"resume at baseline {checkpoint}: no backlog"
            return (
                HistoryJoinStatus.CHECKPOINT_TOO_OLD,
                (),
                f"checkpoint {checkpoint} is outside the bounded window (no retained history); pull a fresh snapshot",
            )
        # checkpoint == oldest - 1 can still be backfilled by the full window.
        if checkpoint + 1 < oldest:
            return (
                HistoryJoinStatus.CHECKPOINT_TOO_OLD,
                (),
                f"checkpoint {checkpoint} is outside the bounded window (oldest retained sequence is {oldest}); pull a fresh snapshot",
            )
        quotes = self.history.snapshot(key, after_sequence=checkpoint)
        return (
            HistoryJoinStatus.RESUME_READY,
            quotes,
            f"resume after sequence {checkpoint}: {len(quotes)} backlog quote(s) confirmed",
        )

    def _delivery(
        self,
        kind: DeliveryKind,
        payload: Any,
        received_time: datetime | None = None,
    ) -> DeliveryEvent:
        return DeliveryEvent(
            kind=kind,
            timestamp=received_time or datetime.now(timezone.utc),
            payload=payload,
        )

    async def _publish_to_key_subscribers(
        self,
        key: SubscriptionKey,
        event: DeliveryEvent,
    ) -> None:
        subscriber_ids = self.registry.subscribers_for(key)
        audits = await self.dispatcher.publish(subscriber_ids, event)
        for audit in audits:
            event_log(self._logger, "backpressure_decision", audit=audit, key=key)

    async def _publish_rejection(self, rejection: RejectedRecord) -> None:
        event_log(self._logger, "record_rejected", rejection=rejection)
        if rejection.source is not None and rejection.symbol:
            key = SubscriptionKey(rejection.source, rejection.symbol)
            await self._publish_to_key_subscribers(key, self._delivery(DeliveryKind.REJECTION, rejection, rejection.received_time))

    async def _publish_gaps(self, key: SubscriptionKey, gaps: tuple[GapEvent, ...]) -> None:
        for gap in gaps:
            event_log(self._logger, "sequence_gap", gap=gap)
            await self._publish_to_key_subscribers(key, self._delivery(DeliveryKind.GAP, gap, gap.detected_time))

    async def ingest(self, source: SourceId, raw: object) -> IngestResult:
        received_time = datetime.now(timezone.utc)
        event_log(self._logger, "ingest_input", source=source, raw=raw, received_time=received_time)

        if not isinstance(source, SourceId):
            rejection = RejectedRecord(
                source=None,
                symbol=None,
                reason=RejectReason.UNKNOWN_SOURCE,
                detail=f"unsupported source value: {source!r}",
                raw_record=raw,
                received_time=received_time,
            )
            await self._publish_rejection(rejection)
            return IngestResult(status=IngestStatus.REJECTED, rejection=rejection, detail=rejection.detail)

        async with self._source_locks[source]:
            return await self._ingest_locked(source, raw, received_time)

    async def _ingest_locked(self, source: SourceId, raw: object, received_time: datetime) -> IngestResult:
        if self._source_state.get(source, False) is False:
            rejection = RejectedRecord(
                source=source,
                symbol=(raw.get("sym") or raw.get("ticker")) if isinstance(raw, dict) else None,
                reason=RejectReason.UNSUPPORTED_RECORD,
                detail=f"source {source.value} is disconnected; replay after reconnect",
                raw_record=raw,
                received_time=received_time,
            )
            await self._publish_rejection(rejection)
            return IngestResult(status=IngestStatus.REJECTED, rejection=rejection, detail=rejection.detail)

        normalized = self.router.normalize(source, raw, received_time)
        if isinstance(normalized, RejectedRecord):
            await self._publish_rejection(normalized)
            return IngestResult(
                status=IngestStatus.REJECTED,
                rejection=normalized,
                detail=f"{normalized.reason.value}: {normalized.detail}",
            )

        quote: Quote = normalized
        key = SubscriptionKey(quote.source, quote.symbol)
        async with self._key_lock(key):
            validation_kind = self.registry.validation_error_kind(key)
            if validation_kind is not None:
                reason = (
                    RejectReason.UNKNOWN_SOURCE
                    if validation_kind.value == "unknown_source"
                    else RejectReason.UNKNOWN_SYMBOL
                )
                rejection = RejectedRecord(
                    source=quote.source,
                    symbol=quote.symbol,
                    reason=reason,
                    detail=f"unknown source or symbol: {key.source.value}/{key.symbol}",
                    raw_record=raw,
                    received_time=received_time,
                )
                await self._publish_rejection(rejection)
                return IngestResult(status=IngestStatus.REJECTED, key=key, rejection=rejection, detail=rejection.detail)

            tracker = self._tracker(key)
            decision = tracker.observe(quote)
            status = _DISPOSITION_STATUS[decision.disposition]
            state = tracker.snapshot()
            event_log(
                self._logger,
                "sequence_decision",
                source=source,
                symbol=quote.symbol,
                sequence=quote.sequence,
                disposition=decision.disposition,
                accepted_as_latest=decision.accepted_as_latest,
                detail=decision.detail,
                state=state,
            )

            # Keep the bounded history window in sync with what the sequence
            # state machine actually released: only fully validated quotes
            # (normal or recovered) are retained; buffered, stale, out-of-order
            # and unrecoverable-gap quotes never enter history.
            history_evicted: list[int] = []
            if decision.release:
                history_evicted = self.history.append(key, decision.release)
                if history_evicted:
                    event_log(
                        self._logger,
                        "history_eviction",
                        key=key,
                        evicted_sequences=tuple(history_evicted),
                        retained=self.history.stats(key).retained,
                    )
            if decision.disposition is RecordDisposition.GAP_UNRECOVERABLE:
                self.history.mark_gap_state(key, GapState.UNRECOVERABLE)
            elif state.state is GapState.RECOVERING:
                self.history.mark_gap_state(key, GapState.RECOVERING)
            elif state.state is GapState.NORMAL:
                self.history.mark_gap_state(key, GapState.NORMAL)

            # Release recovered quotes before the gap-resolved marker.
            # Initial or extended gap markers precede future buffered quotes,
            # and those future quotes are not released at this point.
            if decision.release:
                for released in decision.release:
                    kind = DeliveryKind.QUOTE if decision.disposition is RecordDisposition.ACCEPTED else DeliveryKind.RECOVERED_QUOTE
                    await self._publish_to_key_subscribers(
                        key,
                        self._delivery(kind, released, released.received_time),
                    )
            await self._publish_gaps(key, decision.gap_events)
            # Wake snapshot/resume waiters after state and history are updated
            # and the live events are enqueued. The key condition is already
            # held here; notify_all requires the lock and must not re-acquire it.
            self._key_locks[key].notify_all()

            return IngestResult(
                status=status,
                key=key,
                quote=quote,
                expected_sequence=state.expected_sequence,
                actual_sequence=quote.sequence,
                detail=decision.detail,
            )

    async def disconnect_source(self, source: SourceId) -> None:
        async with self._source_locks[source]:
            self._source_state[source] = False
            event_log(self._logger, "source_disconnected", source=source)

    async def resume_after_gap(
        self,
        source: SourceId,
        symbol: str,
        sequence: int,
        event_time: datetime | None = None,
    ) -> IngestResult:
        """Install an authoritative baseline after an unrecoverable gap."""
        if event_time is None or event_time.tzinfo is None:
            raise ValueError("event_time is required and must be timezone-aware")
        event_time = event_time.astimezone(timezone.utc)
        key = SubscriptionKey(source, symbol)
        async with self._source_locks[source]:
            async with self._key_lock(key):
                tracker = self._tracker(key)
                tracker.reset(sequence, event_time)
                # Pre-gap history can never be presented next to the new
                # baseline; discard it instead of splicing across the hole.
                evicted = self.history.reset_continuity(
                    key,
                    detail="operator explicitly resumed after an unrecoverable gap",
                )
                if evicted:
                    event_log(
                        self._logger,
                        "history_eviction",
                        key=key,
                        evicted_sequences=tuple(evicted),
                        retained=0,
                        reason="unrecoverable gap resume",
                    )
                event_log(self._logger, "explicit_gap_resume", key=key, sequence=sequence, event_time=event_time)
                await self._publish_gaps(
                    key,
                    (
                        GapEvent(
                            key=key,
                            state=GapState.NORMAL,
                            missing_from=sequence,
                            missing_to=sequence,
                            detected_time=datetime.now(timezone.utc),
                            missing_sequences=(),
                            recovered_sequences=(),
                            detail="operator explicitly resumed after an unrecoverable gap",
                        ),
                    ),
                )
                self._key_lock(key).notify_all()
                state = tracker.snapshot()
                return IngestResult(
                    status=IngestStatus.RECOVERED,
                    key=key,
                    expected_sequence=state.expected_sequence,
                    actual_sequence=sequence,
                    detail="baseline installed",
                )

    async def reconnect_source(self, source: SourceId, replay_records: list[Any] | None = None) -> list[IngestResult]:
        """Reconnect and replay records.

        Duplicate replay records are recognized by sequence state, producing no
        duplicate external quote events. Missing replay records that leave an
        existing recovery window unresolved retain their explicit gap marker.
        """
        async with self._source_locks[source]:
            self._source_state[source] = True
            event_log(self._logger, "source_reconnected", source=source, replay_count=len(replay_records or []))
            results: list[IngestResult] = []
            for record in replay_records or []:
                received_time = datetime.now(timezone.utc)
                event_log(self._logger, "ingest_input", source=source, raw=record, received_time=received_time)
                results.append(await self._ingest_locked(source, record, received_time))
            return results

    async def drain_scripted_source(
        self,
        source: ScriptedSource,
        batch_size: int = 1,
        max_batches: int | None = None,
    ) -> list[IngestResult]:
        results: list[IngestResult] = []
        batches = 0
        while True:
            if max_batches is not None and batches >= max_batches:
                break
            batch = await source.fetch_batch(batch_size)
            if not batch:
                break
            for record in batch:
                results.append(await self.ingest(source.source_id, record))
            batches += 1
        return results
