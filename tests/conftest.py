"""测试公共设施：构造输入、打印输入与判定依据。"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from market_feed.distributor import Distributor
from market_feed.models import NormalizedTick
from market_feed.service import MarketDataService
from market_feed.subscription import SubscriptionManager, SymbolRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("tests")

T0 = datetime(2026, 9, 22, 10, 0, 0, tzinfo=timezone.utc)


def make_tick(source: str = "source_a", symbol: str = "BTCUSD", seq: int = 1,
              seconds: int = 0, price: str = "100.00") -> NormalizedTick:
    return NormalizedTick(
        source=source, symbol=symbol, sequence=seq,
        event_time=T0 + timedelta(seconds=seconds),
        price=Decimal(price), volume=None, raw_id=f"{source}:{symbol}:{seq}",
    )


def raw_a(seq: int, symbol: str = "BTCUSD", ts_ms: int | None = None,
          px: Any = "100.00", qty: Any = None) -> dict[str, Any]:
    if ts_ms is None:
        ts_ms = int((T0 + timedelta(seconds=seq)).timestamp() * 1000)
    raw: dict[str, Any] = {"sym": symbol, "seq": seq, "ts_ms": ts_ms, "px": px}
    if qty is not None:
        raw["qty"] = qty
    return raw


def raw_b(seq: int, symbol: str = "BTCUSD", event_time: str | None = None,
          price: Any = 100.0, volume: Any = None) -> dict[str, Any]:
    # source_b 使用 UTC+8 本地时间（无时区后缀）
    if event_time is None:
        local = T0 + timedelta(hours=8, seconds=seq)
        event_time = local.strftime("%Y-%m-%dT%H:%M:%S")
    raw: dict[str, Any] = {
        "instrument": symbol, "sequenceNumber": seq,
        "eventTime": event_time, "price": price,
    }
    if volume is not None:
        raw["volume"] = volume
    return raw


@pytest.fixture()
def registry() -> SymbolRegistry:
    return SymbolRegistry({
        "source_a": {"BTCUSD", "ETHUSD"},
        "source_b": {"BTCUSD", "XAUUSD"},
    })


@pytest.fixture()
def subscriptions(registry: SymbolRegistry) -> SubscriptionManager:
    return SubscriptionManager(registry)


@pytest.fixture()
def distributor() -> Distributor:
    return Distributor()


@pytest.fixture()
def service(subscriptions: SubscriptionManager, distributor: Distributor) -> MarketDataService:
    return MarketDataService(subscriptions, distributor)


def log_verdict(case: str, raw: Any, result: Any) -> None:
    """在日志中打印输入与判定依据。"""
    logger.info("CASE=%s INPUT=%r VERDICT=%s", case, raw, result)
