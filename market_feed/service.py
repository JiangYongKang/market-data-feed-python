"""行情服务编排：归一化 -> 订阅校验 -> 连续性判定 -> 快照 -> 分发。"""
from __future__ import annotations

import logging
import threading
from typing import Any, Mapping

from market_feed.distributor import Distributor
from market_feed.errors import FeedError
from market_feed.models import GapMarker, InboundResult, NormalizedTick, RejectReason, TickStatus
from market_feed.sequencer import SequenceTracker
from market_feed.sources import get_adapter
from market_feed.subscription import SubscriptionManager

logger = logging.getLogger("market_feed.service")


class MarketDataService:
    """对外统一入口；多来源/多订阅者并发下保证一致快照。

    快照更新与连续性判定在同一把锁内完成，杜绝半更新状态；
    最新价按 (source, symbol) 与 symbol 两个维度维护。
    """

    def __init__(
        self,
        subscriptions: SubscriptionManager,
        distributor: Distributor,
        stale_tolerance_ms: int = 0,
    ) -> None:
        self._subscriptions = subscriptions
        self._distributor = distributor
        self._stale_tolerance_ms = stale_tolerance_ms
        self._lock = threading.Lock()
        self._trackers: dict[tuple[str, str], SequenceTracker] = {}
        self._latest: dict[tuple[str, str], NormalizedTick] = {}
        self._gaps: list[GapMarker] = []

    # ------------------------------------------------------------------ #
    def _tracker(self, source: str, symbol: str) -> SequenceTracker:
        key = (source, symbol)
        tracker = self._trackers.get(key)
        if tracker is None:
            tracker = SequenceTracker(source, symbol, self._stale_tolerance_ms)
            self._trackers[key] = tracker
        return tracker

    def ingest(self, source: str, raw: Mapping[str, Any]) -> InboundResult:
        """处理一条原始行情记录；任何非法记录都返回可区分结论而非静默丢弃。"""
        logger.info("INGEST source=%s raw=%r", source, raw)
        try:
            tick = get_adapter(source).normalize(raw)
        except FeedError as exc:
            logger.info("REJECT reason=%s detail=%s", exc.reason.value, exc.detail)
            return InboundResult(TickStatus.REJECTED, exc.reason, detail=exc.detail)

        with self._lock:
            if not self._subscriptions.is_subscribed(source, tick.symbol):
                result = InboundResult(
                    TickStatus.REJECTED, RejectReason.NOT_SUBSCRIBED, tick,
                    f"{source}/{tick.symbol} 未订阅",
                )
                logger.info("REJECT reason=NOT_SUBSCRIBED tick=%s", tick.raw_id)
                return result
            tracker = self._tracker(source, tick.symbol)
            result = tracker.classify(tick)
            if result.status is TickStatus.ACCEPTED:
                self._latest[(source, tick.symbol)] = tick
            elif result.reason is RejectReason.GAP_DETECTED:
                logger.warning(
                    "GAP source=%s symbol=%s detail=%s", source, tick.symbol, result.detail
                )
        logger.info("CLASSIFY status=%s reason=%s tick=%s",
                    result.status.value,
                    result.reason.value if result.reason else None,
                    tick.raw_id)
        if result.status is TickStatus.ACCEPTED:
            self._distributor.publish(tick)
        return result

    def recover(self, source: str, symbol: str, replay: list[Mapping[str, Any]]) -> GapMarker | None:
        """断连重连后的缺口恢复。

        恢复期间到达的正常行情已被 tracker 缓冲；回补数据经 apply_replay 校验，
        缺口无法闭合时返回 GapMarker 显式标记。恢复结束后缓冲行情重新判定，
        保证不重复推送、不漏推。
        """
        with self._lock:
            tracker = self._tracker(source, symbol)
            if not tracker.recovering:
                # 断连期间未收到任何行情：无法判定缺口，保守起见不进入恢复
                logger.info("RECOVER source=%s symbol=%s 无待恢复缺口", source, symbol)
                return None
            replay_results: list[InboundResult] = []
            for raw in replay:
                try:
                    tick = get_adapter(source).normalize(raw)
                except FeedError as exc:
                    replay_results.append(
                        InboundResult(TickStatus.REJECTED, exc.reason, detail=exc.detail)
                    )
                    continue
                if tick.symbol != symbol:
                    replay_results.append(InboundResult(
                        TickStatus.REJECTED, RejectReason.UNKNOWN_SYMBOL, tick,
                        f"回补标的 {tick.symbol} 与恢复标的 {symbol} 不符",
                    ))
                    continue
                replay_results.append(tracker.apply_replay(tick))
            gap = tracker.finish_recovery()
            drained = tracker.drain_buffered()
            accepted = [r for r in replay_results if r.status is TickStatus.ACCEPTED]
            # 回补数据按序号排序后更新快照并补推（标记为恢复数据，与正常行情区分）
            for r in sorted(accepted, key=lambda r: r.tick.sequence):  # type: ignore[union-attr]
                tick = r.tick
                assert tick is not None
                current = self._latest.get((source, symbol))
                if current is None or tick.event_time >= current.event_time:
                    self._latest[(source, symbol)] = tick
                self._distributor.publish(tick)
            for r in drained:
                if r.status is TickStatus.ACCEPTED and r.tick is not None:
                    self._latest[(source, symbol)] = r.tick
                    self._distributor.publish(r.tick)
            if gap is not None:
                self._gaps.append(gap)
                logger.warning("GAP_UNRECOVERABLE %s", gap)
        return gap

    def latest(self, source: str, symbol: str) -> NormalizedTick | None:
        with self._lock:
            return self._latest.get((source, symbol))

    def gaps(self) -> list[GapMarker]:
        with self._lock:
            return list(self._gaps)
