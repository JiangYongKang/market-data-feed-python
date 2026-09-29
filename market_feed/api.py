"""FastAPI wiring for local, deterministic verification."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from fastapi import FastAPI, HTTPException

from .enums import SourceId
from .models import ResumePosition, SubscriberOptions
from .schemas import IngestRequest, ReconnectRequest, SubscribeRequest, UnsubscribeRequest
from .service import MarketDataFeedService


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    return repr(value)


def _ingest_result_to_dict(result):
    return {
        "status": result.status.value,
        "source": result.key.source.value if result.key else None,
        "symbol": result.key.symbol if result.key else None,
        "sequence": result.actual_sequence,
        "expected_sequence": result.expected_sequence,
        "detail": result.detail,
    }


def create_app(service: MarketDataFeedService | None = None) -> FastAPI:
    feed = service or MarketDataFeedService()
    app = FastAPI(title="Deterministic Multi-Source Market Data Feed", version="0.1.0")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/state/subscriptions")
    async def subscriptions() -> dict[str, list[dict[str, str]]]:
        snapshot = feed.subscription_snapshot()
        return {
            subscriber_id: [{"source": key.source.value, "symbol": key.symbol} for key in keys]
            for subscriber_id, keys in snapshot.items()
        }

    @app.get("/state/queues")
    async def queues() -> dict[str, int]:
        return feed.dispatcher.queue_snapshot()

    @app.get("/state/history/{source}/{symbol}")
    async def history(source: SourceId, symbol: str) -> dict[str, Any]:
        stats = feed.history_stats(source, symbol)
        return _jsonable(stats)

    @app.post("/subscriptions")
    async def subscribe(request: SubscribeRequest):
        options = SubscriberOptions(queue_size=request.queue_size, backpressure=request.backpressure)
        resume_from = (
            ResumePosition(request.source, request.symbol, request.resume_from_sequence)
            if request.resume_from_sequence is not None
            else None
        )
        result = await feed.subscribe(
            request.subscriber_id,
            request.source,
            request.symbol,
            options,
            snapshot=request.snapshot,
            resume_from=resume_from,
            wait_for_consistency=request.wait_for_consistency,
            consistency_timeout=request.consistency_timeout,
        )
        response = {
            "ok": result.ok,
            "changed": result.changed,
            "detail": result.detail,
            "kind": result.kind.value if result.kind else None,
        }
        if result.handoff is not None:
            envelope = result.handoff
            response["handoff"] = {
                "mode": envelope.mode.value,
                "source": envelope.key.source.value,
                "symbol": envelope.key.symbol,
                "resume_from_sequence": envelope.resume_from_sequence,
                "live_from_sequence": envelope.live_from_sequence,
                "complete": envelope.complete,
                "quotes": [
                    {"sequence": quote.sequence, "record_id": quote.record_id}
                    for quote in envelope.quotes
                ],
                "stats": _jsonable(envelope.stats),
            }
        return response

    @app.delete("/subscriptions")
    async def unsubscribe(request: UnsubscribeRequest):
        result = await feed.unsubscribe(request.subscriber_id, request.source, request.symbol)
        return {
            "ok": result.ok,
            "changed": result.changed,
            "detail": result.detail,
            "kind": result.kind.value if result.kind else None,
        }

    @app.post("/sources/{source}/ingest")
    async def ingest(source: SourceId, request: IngestRequest):
        result = await feed.ingest(source, request.record)
        return _ingest_result_to_dict(result)

    @app.post("/sources/{source}/disconnect")
    async def disconnect(source: SourceId):
        await feed.disconnect_source(source)
        return {"ok": True, "source": source.value, "connected": False}

    @app.post("/sources/{source}/reconnect")
    async def reconnect(source: SourceId, request: ReconnectRequest):
        results = await feed.reconnect_source(source, request.replay_records)
        return {
            "ok": True,
            "source": source.value,
            "connected": True,
            "results": [_ingest_result_to_dict(result) for result in results],
        }

    @app.get("/subscribers/{subscriber_id}/events")
    async def next_event(subscriber_id: str):
        if not feed.dispatcher.has_subscriber(subscriber_id):
            raise HTTPException(status_code=404, detail="subscriber not found")
        event = await feed.dispatcher.get(subscriber_id)
        if event is None:
            return {"closed": True}
        return _jsonable(event)

    return app
