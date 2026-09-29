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


def test_http_snapshot_subscribe_and_history_stats_endpoints():
    client = TestClient(create_app(MarketDataFeedService(history_capacity_per_key=5)))
    client.post("/subscriptions", json={"subscriber_id": "seed", "source": "source_a", "symbol": "AAA"})
    for sequence in range(6):
        client.post("/sources/source_a/ingest", json={"record": record(sequence)})

    history = client.get("/state/history/source_a/AAA").json()
    assert history["retained_count"] == 5
    assert history["oldest_sequence"] == 1
    assert history["newest_sequence"] == 5
    assert history["evicted_total"] == 1

    response = client.post("/subscriptions", json={
        "subscriber_id": "late",
        "source": "source_a",
        "symbol": "AAA",
        "snapshot": 3,
    })
    body = response.json()
    assert body["ok"] is True
    assert body["handoff"]["mode"] == "snapshot"
    assert [quote["sequence"] for quote in body["handoff"]["quotes"]] == [3, 4, 5]
    assert body["handoff"]["live_from_sequence"] == 6

    event = client.get("/subscribers/late/events").json()
    assert event["kind"] == "handoff"
    assert [quote["sequence"] for quote in event["payload"]["quotes"]] == [3, 4, 5]


def test_http_resume_beyond_retention_is_explicit():
    client = TestClient(create_app(MarketDataFeedService(history_capacity_per_key=2)))
    client.post("/subscriptions", json={"subscriber_id": "seed", "source": "source_a", "symbol": "AAA"})
    for sequence in range(5):
        client.post("/sources/source_a/ingest", json={"record": record(sequence)})

    response = client.post("/subscriptions", json={
        "subscriber_id": "stale",
        "source": "source_a",
        "symbol": "AAA",
        "resume_from_sequence": 0,
    })
    body = response.json()
    assert body["ok"] is False
    assert body["kind"] == "resume_position_evicted"
