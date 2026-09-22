"""归一化测试：畸形记录、缺字段、非法价格/时间、时区语义、精度。"""
from __future__ import annotations

from datetime import timezone
from decimal import Decimal

import pytest

from market_feed.errors import NormalizationError
from market_feed.models import RejectReason
from market_feed.sources import SourceAAdapter, SourceBAdapter, get_adapter
from tests.conftest import log_verdict, raw_a, raw_b


def _expect_error(adapter, raw, reason: RejectReason, case: str) -> None:
    caught: NormalizationError | None = None
    try:
        adapter.normalize(raw)
    except NormalizationError as exc:
        caught = exc
    verdict = "NO_ERROR(意外成功)" if caught is None else f"{caught.reason.value}: {caught.detail}"
    log_verdict(case, raw, verdict)
    assert caught is not None and caught.reason is reason, verdict


class TestSourceA:
    adapter = SourceAAdapter()

    def test_valid(self):
        tick = self.adapter.normalize(raw_a(1, px="65000.10", qty="0.5"))
        log_verdict("A合法", raw_a(1, px="65000.10", qty="0.5"), tick)
        assert tick.price == Decimal("65000.10")
        assert tick.volume == Decimal("0.5")
        assert tick.event_time.tzinfo is timezone.utc

    def test_missing_price(self):
        raw = raw_a(1)
        del raw["px"]
        _expect_error(self.adapter, raw, RejectReason.MISSING_FIELD, "A缺价格")

    def test_missing_symbol(self):
        raw = raw_a(1)
        del raw["sym"]
        _expect_error(self.adapter, raw, RejectReason.MISSING_FIELD, "A缺标的")

    def test_invalid_price_string(self):
        _expect_error(self.adapter, raw_a(1, px="abc"), RejectReason.INVALID_PRICE, "A价格非数值")

    def test_negative_price(self):
        _expect_error(self.adapter, raw_a(1, px="-3"), RejectReason.INVALID_PRICE, "A负价格")

    def test_nan_price(self):
        _expect_error(self.adapter, raw_a(1, px="NaN"), RejectReason.INVALID_PRICE, "A价格NaN")

    def test_bool_price(self):
        _expect_error(self.adapter, raw_a(1, px=True), RejectReason.INVALID_PRICE, "A价格布尔")

    def test_invalid_timestamp(self):
        _expect_error(self.adapter, raw_a(1, ts_ms="not-a-ts"),
                      RejectReason.INVALID_SEQUENCE, "A时间戳非整数")

    def test_nullable_qty_absent(self):
        tick = self.adapter.normalize(raw_a(1))
        assert tick.volume is None


class TestSourceB:
    adapter = SourceBAdapter(local_utc_offset_hours=8)

    def test_local_time_interpreted_as_source_tz(self):
        # 本地 18:00:01 (UTC+8) 必须解释为 10:00:01 UTC，而非当作 UTC 18:00
        tick = self.adapter.normalize(raw_b(1))
        log_verdict("B本地时间", raw_b(1), tick)
        assert tick.event_time.hour == 10 and tick.event_time.minute == 0
        assert tick.event_time.tzinfo is timezone.utc

    def test_float_price_no_precision_drift(self):
        tick = self.adapter.normalize(raw_b(1, price=65000.1))
        log_verdict("B浮点价格", raw_b(1, price=65000.1), tick)
        assert tick.price == Decimal("65000.1")  # 非 65000.100000000006...

    def test_missing_price(self):
        raw = raw_b(1)
        del raw["price"]
        _expect_error(self.adapter, raw, RejectReason.MISSING_FIELD, "B缺价格")

    def test_bad_event_time(self):
        _expect_error(self.adapter, raw_b(1, event_time="not-a-time"),
                      RejectReason.INVALID_TIMESTAMP, "B时间非法")

    def test_volume_nullable(self):
        assert self.adapter.normalize(raw_b(1)).volume is None
        assert self.adapter.normalize(raw_b(1, volume=3)).volume == Decimal("3")


def test_unknown_source():
    raw = {"whatever": 1}
    try:
        get_adapter("source_x")
        verdict = "NO_ERROR(意外成功)"
    except Exception as exc:
        verdict = f"{type(exc).__name__}: {exc}"
    log_verdict("未知来源", raw, verdict)
    with pytest.raises(Exception):
        get_adapter("source_x")


def test_cross_source_price_equality():
    # 同一价格经两个来源归一化后数值相等，不出现精度错配
    ta = SourceAAdapter().normalize(raw_a(1, px="65000.10"))
    tb = SourceBAdapter().normalize(raw_b(1, price=65000.1))
    log_verdict("跨源价格一致", {"a": "65000.10", "b": 65000.1}, f"{ta.price} vs {tb.price}")
    assert ta.price == tb.price
