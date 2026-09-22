"""序号/时间戳连续性校验与断连恢复。

每个 (source, symbol) 一个 SequenceTracker，既定规则：
- 顺序到达：ACCEPTED，可作为最新价；
- 重复（序号已处理）：DUPLICATE；
- 乱序（序号小于期望且未见过）：OUT_OF_ORDER，仅补历史，不作最新价；
- 跳变（序号大于期望）：自动进入恢复态，该条及后续正常行情 BUFFERED，
  不与恢复数据混用；
- 过期（event_time 早于最新已接受时间超过容忍度）：STALE，序号被消费
  （避免阻塞流），但不更新最新价；
- 恢复回补：apply_replay 校验回补是否落在缺口区间；
- finish_recovery：缺口闭合则继续；未闭合返回 GapMarker 显式标记，
  缺失序号被跳过（显式记录，非静默）。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from market_feed.models import GapMarker, InboundResult, NormalizedTick, RejectReason, TickStatus


class SequenceTracker:
    """单 (source, symbol) 的连续性状态机。"""

    def __init__(self, source: str, symbol: str, stale_tolerance_ms: int = 0) -> None:
        self.source = source
        self.symbol = symbol
        self.stale_tolerance = timedelta(milliseconds=stale_tolerance_ms)
        self._lock = threading.Lock()
        self._initialized = False
        self._next_expected = 0
        self._seen: set[int] = set()
        self._last_event_time: datetime = datetime.min.replace(tzinfo=timezone.utc)
        # 恢复态
        self._recovering = False
        self._gap_from = 0
        self._gap_to = 0
        self._replay_seen: set[int] = set()
        self._buffered: list[NormalizedTick] = []

    # ------------------------------------------------------------------ #
    @property
    def recovering(self) -> bool:
        with self._lock:
            return self._recovering

    @property
    def last_event_time(self) -> datetime:
        with self._lock:
            return self._last_event_time

    # ------------------------------------------------------------------ #
    def classify(self, tick: NormalizedTick) -> InboundResult:
        """判定一条归一化行情的处置结论。"""
        with self._lock:
            if self._recovering:
                self._buffered.append(tick)
                return InboundResult(
                    TickStatus.BUFFERED,
                    RejectReason.GAP_DETECTED,
                    tick,
                    f"缺口恢复中，缓冲 seq={tick.sequence}",
                )

            seq = tick.sequence
            if not self._initialized:
                self._initialized = True
                self._next_expected = seq + 1
                self._seen.add(seq)
                self._last_event_time = tick.event_time
                return InboundResult(TickStatus.ACCEPTED, tick=tick, detail="首条建立基线")

            if seq in self._seen:
                return InboundResult(
                    TickStatus.DUPLICATE, RejectReason.DUPLICATE, tick,
                    f"seq={seq} 已处理",
                )

            if seq < self._next_expected:
                # 迟到历史：补入，但不作为最新价
                self._seen.add(seq)
                return InboundResult(
                    TickStatus.OUT_OF_ORDER, RejectReason.OUT_OF_ORDER, tick,
                    f"seq={seq} 落后于期望 {self._next_expected}，仅补历史",
                )

            if seq > self._next_expected:
                # 跳变：进入恢复态并缓冲本条
                self._recovering = True
                self._gap_from = self._next_expected
                self._gap_to = seq - 1
                self._replay_seen = set()
                self._buffered.append(tick)
                return InboundResult(
                    TickStatus.BUFFERED, RejectReason.GAP_DETECTED, tick,
                    f"检测到缺口 [{self._gap_from}, {self._gap_to}]，进入恢复",
                )

            # seq == next_expected：顺序到达，检查是否过期
            self._seen.add(seq)
            self._next_expected += 1
            if tick.event_time < self._last_event_time - self.stale_tolerance:
                return InboundResult(
                    TickStatus.STALE, RejectReason.STALE, tick,
                    f"event_time={tick.event_time.isoformat()} 早于最新 "
                    f"{self._last_event_time.isoformat()}，序号已消费但不更新最新价",
                )
            self._last_event_time = tick.event_time
            return InboundResult(TickStatus.ACCEPTED, tick=tick)

    # ------------------------------------------------------------------ #
    def begin_recovery(self, expected_from: int, expected_to: int) -> None:
        """显式进入/扩展恢复状态（如断连重连后由服务触发）。"""
        with self._lock:
            if self._recovering:
                self._gap_from = min(self._gap_from, expected_from)
                self._gap_to = max(self._gap_to, expected_to)
            else:
                self._recovering = True
                self._gap_from = expected_from
                self._gap_to = expected_to
                self._replay_seen = set()

    def apply_replay(self, tick: NormalizedTick) -> InboundResult:
        """应用一条回补数据。"""
        with self._lock:
            if not self._recovering:
                return InboundResult(
                    TickStatus.REJECTED, RejectReason.MALFORMED_RECORD, tick,
                    "当前不在恢复状态，回补被拒绝",
                )
            seq = tick.sequence
            if seq in self._replay_seen or seq in self._seen:
                return InboundResult(
                    TickStatus.DUPLICATE, RejectReason.DUPLICATE, tick,
                    f"回补 seq={seq} 重复",
                )
            if not (self._gap_from <= seq <= self._gap_to):
                return InboundResult(
                    TickStatus.REJECTED, RejectReason.OUT_OF_ORDER, tick,
                    f"回补 seq={seq} 不在缺口 [{self._gap_from}, {self._gap_to}] 内",
                )
            self._replay_seen.add(seq)
            return InboundResult(TickStatus.ACCEPTED, tick=tick, detail="回补接受")

    def finish_recovery(self) -> GapMarker | None:
        """结束恢复；缺口未闭合则返回 GapMarker 显式标记。"""
        with self._lock:
            if not self._recovering:
                return None
            missing = [
                s for s in range(self._gap_from, self._gap_to + 1)
                if s not in self._replay_seen
            ]
            # 已回补的序号标记为已处理；未回补的缺失序号不标记，
            # 以便其迟到到达时被判为 OUT_OF_ORDER（补历史）而非 DUPLICATE
            self._seen.update(self._replay_seen)
            self._next_expected = self._gap_to + 1
            self._recovering = False
            if missing:
                return GapMarker(
                    source=self.source,
                    symbol=self.symbol,
                    from_sequence=missing[0],
                    to_sequence=missing[-1],
                    reason=f"回补未闭合缺口，缺失 {len(missing)} 条: {missing}",
                )
            return None

    def drain_buffered(self) -> list[InboundResult]:
        """恢复结束后取出缓冲的正常行情并重新判定。"""
        with self._lock:
            buffered = self._buffered
            self._buffered = []
        return [self.classify(t) for t in buffered]
