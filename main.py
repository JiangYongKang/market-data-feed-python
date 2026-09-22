"""多数据源实时行情接入与分发服务（FastAPI 入口）。

所有来源均为本地可控模拟，不依赖外部网络/凭据。
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from market_feed.distributor import Distributor, OverflowPolicy, Subscriber
from market_feed.errors import FeedError
from market_feed.models import InboundResult, TickStatus
from market_feed.service import MarketDataService
from market_feed.subscription import SubscriptionManager, SymbolRegistry

# 本地可订阅标的注册表（两个来源、结构不同）
REGISTRY = SymbolRegistry({
    "source_a": {"BTCUSD", "ETHUSD"},
    "source_b": {"BTCUSD", "XAUUSD"},
})

subscriptions = SubscriptionManager(REGISTRY)
distributor = Distributor()
service = MarketDataService(subscriptions, distributor, stale_tolerance_ms=0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="market-data-feed", lifespan=lifespan)


def _result_dict(r: InboundResult) -> dict[str, Any]:
    return {
        "status": r.status.value,
        "reason": r.reason.value if r.reason else None,
        "detail": r.detail,
        "tick": None if r.tick is None else {
            "source": r.tick.source,
            "symbol": r.tick.symbol,
            "sequence": r.tick.sequence,
            "event_time": r.tick.event_time.isoformat(),
            "price": str(r.tick.price),
            "volume": None if r.tick.volume is None else str(r.tick.volume),
        },
    }


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "market-data-feed"}


@app.post("/subscribe/{source}/{symbol}", status_code=201)
def subscribe(source: str, symbol: str) -> dict[str, str]:
    try:
        subscriptions.subscribe(source, symbol)
    except FeedError as exc:
        raise HTTPException(status_code=409, detail=f"{exc.reason.value}: {exc.detail}")
    return {"subscribed": f"{source}/{symbol}"}


@app.delete("/subscribe/{source}/{symbol}")
def unsubscribe(source: str, symbol: str) -> dict[str, str]:
    try:
        subscriptions.unsubscribe(source, symbol)
    except FeedError as exc:
        raise HTTPException(status_code=409, detail=f"{exc.reason.value}: {exc.detail}")
    return {"unsubscribed": f"{source}/{symbol}"}


@app.get("/subscriptions")
def list_subscriptions() -> list[str]:
    return sorted(f"{s}/{sym}" for s, sym in subscriptions.snapshot())


@app.post("/ingest/{source}")
def ingest(source: str, raw: dict[str, Any]) -> dict[str, Any]:
    result = service.ingest(source, raw)
    if result.status is TickStatus.REJECTED and result.reason is not None:
        # 拒绝不静默：返回 422 + 可区分原因
        raise HTTPException(
            status_code=422,
            detail={"reason": result.reason.value, "detail": result.detail},
        )
    return _result_dict(result)


@app.post("/recover/{source}/{symbol}")
def recover(source: str, symbol: str, replay: list[dict[str, Any]]) -> dict[str, Any]:
    gap = service.recover(source, symbol, replay)
    return {"gap": None if gap is None else {
        "source": gap.source, "symbol": gap.symbol,
        "from_sequence": gap.from_sequence, "to_sequence": gap.to_sequence,
        "reason": gap.reason,
    }}


@app.get("/latest/{source}/{symbol}")
def latest(source: str, symbol: str) -> dict[str, Any]:
    tick = service.latest(source, symbol)
    if tick is None:
        raise HTTPException(status_code=404, detail="无最新行情")
    return {
        "source": tick.source, "symbol": tick.symbol, "sequence": tick.sequence,
        "event_time": tick.event_time.isoformat(), "price": str(tick.price),
        "volume": None if tick.volume is None else str(tick.volume),
    }


@app.get("/gaps")
def gaps() -> list[dict[str, Any]]:
    return [{
        "source": g.source, "symbol": g.symbol,
        "from_sequence": g.from_sequence, "to_sequence": g.to_sequence,
        "reason": g.reason,
    } for g in service.gaps()]


@app.post("/subscribers/{subscriber_id}")
def add_subscriber(subscriber_id: str, maxsize: int = 100,
                   policy: str = "COALESCE_LATEST") -> dict[str, str]:
    distributor.add_subscriber(
        Subscriber(subscriber_id, maxsize, OverflowPolicy(policy))
    )
    return {"subscriber": subscriber_id}


@app.get("/subscribers/{subscriber_id}/poll")
def poll(subscriber_id: str) -> dict[str, Any]:
    sub = distributor._subscribers.get(subscriber_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="订阅者不存在")
    tick = sub.poll()
    return {"tick": None if tick is None else tick.raw_id}


@app.get("/drops")
def drops() -> list[dict[str, Any]]:
    return [{
        "subscriber_id": r.subscriber_id, "policy": r.policy.value,
        "tick": r.tick.raw_id, "detail": r.detail,
    } for r in distributor.drop_log()]
