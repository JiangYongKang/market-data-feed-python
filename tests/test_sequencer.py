"""连续性与恢复测试：乱序、重复、跳变、过期、断连恢复、缺口标记。"""
from __future__ import annotations

from market_feed.models import TickStatus
from market_feed.sequencer import SequenceTracker
from tests.conftest import log_verdict, make_tick


def _classify(tracker: SequenceTracker, seq: int, seconds: int | None = None, case: str = ""):
    tick = make_tick(seq=seq, seconds=seconds if seconds is not None else seq)
    result = tracker.classify(tick)
    log_verdict(case or f"classify seq={seq}", tick.raw_id,
                f"{result.status.value} {result.reason.value if result.reason else ''} {result.detail}")
    return result


def test_in_order_accepted():
    tr = SequenceTracker("source_a", "BTCUSD")
    for seq in (1, 2, 3):
        assert _classify(tr, seq).status is TickStatus.ACCEPTED


def test_duplicate_rejected():
    tr = SequenceTracker("source_a", "BTCUSD")
    _classify(tr, 1)
    assert _classify(tr, 1, case="重复").status is TickStatus.DUPLICATE


def test_out_of_order_not_latest():
    tr = SequenceTracker("source_a", "BTCUSD")
    _classify(tr, 1)
    _classify(tr, 2)
    r = _classify(tr, 1, seconds=99, case="乱序迟到")  # 未见过的旧序号
    assert r.status is TickStatus.DUPLICATE  # seq=1 已处理 -> 重复
    # 构造真正乱序：跳过 2 后 3 建立缺口恢复，恢复后迟到的 2
    tr2 = SequenceTracker("source_a", "ETHUSD")
    _classify(tr2, 1)
    _classify(tr2, 3)  # 触发恢复
    tr2.apply_replay(make_tick(seq=2, seconds=2))
    assert tr2.finish_recovery() is None
    tr2.drain_buffered()
    # 此时再来的 seq=2 是重复
    assert _classify(tr2, 2, case="恢复后重复").status is TickStatus.DUPLICATE


def test_gap_triggers_recovery_and_buffers():
    tr = SequenceTracker("source_a", "BTCUSD")
    _classify(tr, 1)
    r = _classify(tr, 5, case="跳变")
    assert r.status is TickStatus.BUFFERED and tr.recovering
    # 恢复期间正常行情被缓冲，不与恢复数据混用
    assert _classify(tr, 6, case="恢复期间行情").status is TickStatus.BUFFERED


def test_recovery_complete_no_gap_marker():
    tr = SequenceTracker("source_a", "BTCUSD")
    _classify(tr, 1)
    _classify(tr, 4)
    for seq in (2, 3):
        r = tr.apply_replay(make_tick(seq=seq, seconds=seq))
        log_verdict(f"回补 seq={seq}", seq, r.status.value)
        assert r.status is TickStatus.ACCEPTED
    assert tr.finish_recovery() is None
    drained = tr.drain_buffered()
    assert [r.status for r in drained] == [TickStatus.ACCEPTED]


def test_recovery_incomplete_marks_gap():
    tr = SequenceTracker("source_a", "BTCUSD")
    _classify(tr, 1)
    _classify(tr, 4)
    tr.apply_replay(make_tick(seq=2, seconds=2))  # seq=3 缺失
    gap = tr.finish_recovery()
    log_verdict("缺口未闭合", "replay=[2]", gap)
    assert gap is not None and gap.from_sequence == 3 and gap.to_sequence == 3
    # 缺失的 3 迟到到达 -> 乱序补历史，不作最新价
    assert _classify(tr, 3, case="缺口迟到数据").status is TickStatus.OUT_OF_ORDER


def test_replay_out_of_gap_rejected():
    tr = SequenceTracker("source_a", "BTCUSD")
    _classify(tr, 1)
    _classify(tr, 3)
    r = tr.apply_replay(make_tick(seq=9, seconds=9))
    log_verdict("回补越界", 9, f"{r.status.value} {r.detail}")
    assert r.status is TickStatus.REJECTED


def test_stale_timestamp_not_latest():
    tr = SequenceTracker("source_a", "BTCUSD", stale_tolerance_ms=0)
    _classify(tr, 1, seconds=10)
    r = _classify(tr, 2, seconds=5, case="过期行情")  # 序号顺序但时间回退
    assert r.status is TickStatus.STALE
    # 序号被消费，后续可继续
    assert _classify(tr, 3, seconds=11).status is TickStatus.ACCEPTED
