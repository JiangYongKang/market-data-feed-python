from fastapi.testclient import TestClient

from market_feed.api import create_app
from market_feed.service import MarketDataFeedService


def record(sequence: int):
    return {"trade_id": f"a{sequence}", "sym": "AAA", "seq": sequence, "ts_ms": 1790035200000 + sequence * 1000, "px": "10.25", "qty": "1"}


def test_http_subscription_ingest_dedup_and_event_json_are_reproducible():
    client = TestClient(create_app(MarketDataFeedService()))

    subscribed = client.post("/subscriptions", json={
        "subscriber_id": "web",
        "source": "source_a",
        "symbol": "AAA",
        "queue_size": 8,
        "backpressure": "reject",
    })
    assert subscribed.status_code == 200
    assert subscribed.json()["changed"] is True

    first = client.post("/sources/source_a/ingest", json={"record": record(0)})
    assert first.status_code == 200
    assert first.json()["status"] == "ok"

    duplicate = client.post("/sources/source_a/ingest", json={"record": record(0)})
    assert duplicate.json()["status"] == "duplicate"

    event = client.get("/subscribers/web/events").json()
    assert event["kind"] == "quote"
    assert event["payload"]["source"] == "source_a"
    assert event["payload"]["symbol"] == "AAA"
    assert event["payload"]["price"] == "10.25"
    assert event["payload"]["raw_record"]["px"] == "10.25"

    queues = client.get("/state/queues").json()
    assert queues["web"] == 0


def test_http_unknown_symbol_and_malformed_record_are_distinct():
    client = TestClient(create_app(MarketDataFeedService()))
    client.post("/subscriptions", json={"subscriber_id": "web", "source": "source_a", "symbol": "AAA"})

    bad_number = dict(record(1), px=1.25)
    response = client.post("/sources/source_a/ingest", json={"record": bad_number})
    assert response.json()["status"] == "rejected"
    event = client.get("/subscribers/web/events").json()
    assert event["kind"] == "rejection"
    assert event["payload"]["reason"] == "invalid_number"
