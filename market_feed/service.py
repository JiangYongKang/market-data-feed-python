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
    HandoffMode,
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
    HandoffEnvelope,
    HistoryStats,
    IngestResult,
    Quote,
    RejectedRecord,
    ResumePosition,
    SequenceState,
    SubscriptionKey,
    SubscriptionResult,
    SubscriberOptions,
)
from .normalization import NormalizationRouter
from .sequence import SequenceTracker
from .subscriptions import SubscriptionRegistry
from .dispatcher import Dispatcher
from .history import HistoryStore
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
        history_capacity_per_key: int = 64,
        logger: logging.Logger | None = None,
    ) -> None:
        self.registry = SubscriptionRegistry(known_symbols)
        self.router = NormalizationRouter()
        self.dispatcher = Dispatcher(logger or configure_logging())
        self.history = HistoryStore(history_capacity_per_key)
        self._trackers: dict[SubscriptionKey, SequenceTracker] = {}
        self._key_locks: dict[SubscriptionKey, asyncio.Lock] = {}
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


    def _key_lock(self, key: SubscriptionKey) -> asyncio.Lock:
        lock = self._key_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._key_locks[key] = lock
        return lock

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
        *,
        snapshot: int | None = None,
        resume_from: ResumePosition | None = None,
        wait_for_consistency: bool = False,
        consistency_timeout: float = 2.0,
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

        wants_handoff = snapshot is not None or resume_from is not None
        if resume_from is not None and resume_from.sequence < 0:
            return SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=SubscriptionErrorKind.RESUME_POSITION_EVICTED,
                detail="resume rejected: sequence must be non-negative",
                changed=False,
            )
        if snapshot is not None and snapshot < 1:
            return SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=SubscriptionErrorKind.SNAPSHOT_INCOMPLETE,
                detail="snapshot rejected: limit must be at least 1",
                changed=False,
            )

        # Optionally wait for an in-flight recoverable gap to close before
        # taking the consistent-view decision. Poll without holding the key
        # lock so normal ingest/recovery is never blocked.
        if wait_for_consistency:
            deadline = asyncio.get_running_loop().time() + max(consistency_timeout, 0.0)
            while True:
                state = self.sequence_snapshot(key)
                if state is None or state.state is not GapState.RECOVERING:
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    break
                await asyncio.sleep(0.005)

        # The whole register-and-handoff transition is atomic with respect to
        # ingest for this key: no live release can appear between choosing the
        # historical slice and enqueueing it ahead of the live stream.
        async with self._key_lock(key):
            if wants_handoff and subscriber_id in self.registry.subscribers_for(key):
                result = SubscriptionResult(
                    ok=False,
                    subscriber_id=subscriber_id,
                    key=key,
                    kind=SubscriptionErrorKind.ALREADY_SUBSCRIBED,
                    detail=(
                        "handoff rejected: subscriber already holds this subscription; "
                        "unsubscribe before requesting a new snapshot/resume"
                    ),
                    changed=False,
                )
                event_log(self._logger, "subscription_request", subscriber_id=subscriber_id, source=source, symbol=symbol, ok=False, changed=False, detail=result.detail)
                return result

            state = self.sequence_snapshot(key)
            if state is not None and state.state is not GapState.NORMAL:
                kind = (
                    SubscriptionErrorKind.SNAPSHOT_INCOMPLETE
                    if state.state is GapState.RECOVERING
                    else SubscriptionErrorKind.RESUME_BARRIER_PRESENT
                )
                detail = (
                    f"handoff rejected: {key.source.value}/{key.symbol} is {state.state.value} "
                    f"for sequences {state.missing_from}..{state.missing_to}; "
                    "retry after recovery or realign with resume_after_gap + snapshot"
                )
                result = SubscriptionResult(
                    ok=False,
                    subscriber_id=subscriber_id,
                    key=key,
                    kind=kind,
                    detail=detail,
                    changed=False,
                )
                event_log(self._logger, "subscription_request", subscriber_id=subscriber_id, source=source, symbol=symbol, ok=False, changed=False, detail=detail)
                return result

            mode = HandoffMode.RESUME if resume_from is not None else HandoffMode.SNAPSHOT
            replay: tuple[Quote, ...] = ()
            checkpoint: int | None = None
            if wants_handoff:
                if resume_from is not None:
                    if resume_from.source is not source or resume_from.symbol != symbol:
                        return SubscriptionResult(
                            ok=False,
                            subscriber_id=subscriber_id,
                            key=key,
                            kind=SubscriptionErrorKind.RESUME_POSITION_EVICTED,
                            detail="resume rejected: checkpoint does not match the subscribed source/symbol",
                            changed=False,
                        )
                    checkpoint = resume_from.sequence
                    replay_or_error = self.history.replay_after(key, checkpoint)
                    if isinstance(replay_or_error, SubscriptionErrorKind):
                        detail_map = {
                            SubscriptionErrorKind.RESUME_POSITION_EVICTED: (
                                "resume rejected: checkpoint is older than retained history; "
                                "realign by pulling a fresh snapshot"
                            ),
                            SubscriptionErrorKind.RESUME_POSITION_AHEAD: (
                                "resume rejected: checkpoint is ahead of retained history; "
                                "realign by pulling a fresh snapshot"
                            ),
                            SubscriptionErrorKind.RESUME_BARRIER_PRESENT: (
                                "resume rejected: an unrecoverable gap lies between checkpoint and "
                                "current data; realign by pulling a fresh snapshot"
                            ),
                        }
                        result = SubscriptionResult(
                            ok=False,
                            subscriber_id=subscriber_id,
                            key=key,
                            kind=replay_or_error,
                            detail=detail_map[replay_or_error],
                            changed=False,
                        )
                        event_log(self._logger, "subscription_request", subscriber_id=subscriber_id, source=source, symbol=symbol, ok=False, changed=False, detail=result.detail, checkpoint=checkpoint)
                        return result
                    replay = replay_or_error
                else:
                    replay = self.history.snapshot_tail(key, snapshot)

            # Create delivery capacity only after key validation succeeds. Existing
            # subscribers retain their configured queue and backpressure policy.
            actual_options = await self.dispatcher.add_subscriber(subscriber_id, options)
            result = self.registry.subscribe(subscriber_id, key)

            envelope: HandoffEnvelope | None = None
            if wants_handoff:
                stats = self.history.stats(key)
                if replay:
                    live_from = replay[-1].sequence + 1
                elif resume_from is not None:
                    live_from = resume_from.sequence + 1
                elif state is not None and state.expected_sequence is not None:
                    live_from = state.expected_sequence
                else:
                    live_from = None
                envelope = HandoffEnvelope(
                    mode=mode,
                    key=key,
                    quotes=replay,
                    resume_from_sequence=checkpoint,
                    live_from_sequence=live_from,
                    complete=True,
                    stats=stats,
                    detail="historical view delivered ahead of the live stream",
                )
                event = self._delivery(DeliveryKind.HANDOFF, envelope)
                audits = await self.dispatcher.begin_handoff(subscriber_id, [event])
                for audit in audits:
                    event_log(self._logger, "backpressure_decision", audit=audit, key=key, handoff=True)
                event_log(
                    self._logger,
                    "handoff_delivered",
                    subscriber_id=subscriber_id,
                    key=key,
                    mode=mode.value,
                    quotes=len(replay),
                    checkpoint=checkpoint,
                    live_from=live_from,
                )

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
                handoff=mode.value if wants_handoff else None,
            )
            return SubscriptionResult(
                ok=result.ok,
                subscriber_id=result.subscriber_id,
                key=result.key,
                kind=result.kind,
                detail=result.detail,
                changed=result.changed,
                handoff=envelope,
            )

    def history_stats(self, source: SourceId, symbol: str) -> HistoryStats:
        """Observable retention/eviction state for one source/symbol."""
        return self.history.stats(SubscriptionKey(source, symbol))

    def history_stats_snapshot(self) -> dict[SubscriptionKey, HistoryStats]:
        return self.history.stats_snapshot()

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

            # Release recovered quotes before the gap-resolved marker.
            # Initial or extended gap markers precede future buffered quotes,
            # and those future quotes are not released at this point.
            if decision.disposition is RecordDisposition.GAP_UNRECOVERABLE:
                # Everything before the triggering sequence is now an
                # invalidated region: discard retained history and anchor a
                # barrier so snapshots can never bridge the hole.
                self.history.mark_barrier(key, quote.sequence)
                event_log(
                    self._logger,
                    "history_barrier",
                    key=key,
                    barrier_sequence=quote.sequence,
                )
            if decision.release:
                for released in decision.release:
                    kind = DeliveryKind.QUOTE if decision.disposition is RecordDisposition.ACCEPTED else DeliveryKind.RECOVERED_QUOTE
                    # Only fully validated, in-order releases enter history.
                    self.history.record(released)
                    await self._publish_to_key_subscribers(
                        key,
                        self._delivery(kind, released, released.received_time),
                    )
            await self._publish_gaps(key, decision.gap_events)

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
                # Fresh region anchor: pre-resume history can never be served
                # as a contiguous view with post-resume data.
                self.history.install_baseline(key, sequence)
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
