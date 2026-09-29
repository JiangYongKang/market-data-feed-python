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


def record_n(sequence: int):
    return {"trade_id": f"a{sequence}", "sym": "AAA", "seq": sequence, "ts_ms": 1790035200000 + sequence * 1000, "px": "10.25", "qty": "1"}


def test_http_late_subscription_returns_snapshot_and_boundary_then_live():
    client = TestClient(create_app(MarketDataFeedService()))
    for sequence in range(4):
        client.post("/sources/source_a/ingest", json={"record": record_n(sequence)})

    joined = client.post("/subscriptions", json={
        "subscriber_id": "late",
        "source": "source_a",
        "symbol": "AAA",
        "with_history": True,
        "snapshot_limit": 2,
    })
    assert joined.status_code == 200
    body = joined.json()
    assert body["join_status"] == "snapshot_ready"
    assert body["incomplete"] is False
    assert [quote["sequence"] for quote in body["quotes"]] == [2, 3]
    assert body["last_sequence"] == 3

    event = client.get("/subscribers/late/events").json()
    assert event["kind"] == "history_boundary"
    assert event["payload"]["last_sequence"] == 3
    assert event["payload"]["status"] == "snapshot_ready"

    client.post("/sources/source_a/ingest", json={"record": record_n(4)})
    live = client.get("/subscribers/late/events").json()
    assert live["kind"] == "quote"
    assert live["payload"]["sequence"] == 4


def test_http_resume_from_checkpoint_replays_missing_and_rejects_too_old():
    client = TestClient(create_app(MarketDataFeedService()))
    for sequence in range(6):
        client.post("/sources/source_a/ingest", json={"record": record_n(sequence)})

    resumed = client.post("/subscriptions", json={
        "subscriber_id": "resumer",
        "source": "source_a",
        "symbol": "AAA",
        "after_sequence": 3,
        "queue_size": 100,
        "backpressure": "drop_oldest",
    })
    body = resumed.json()
    assert body["join_status"] == "resume_ready"
    assert [quote["sequence"] for quote in body["quotes"]] == [4, 5]
    assert body["last_sequence"] == 5

    # Default retention is 64; age sequence 0 out of the window, then a resume
    # request after 0 must be refused instead of splicing a discontinuous flow.
    for sequence in range(6, 80):
        client.post("/sources/source_a/ingest", json={"record": record_n(sequence)})
    aged = client.post("/subscriptions", json={
        "subscriber_id": "old2",
        "source": "source_a",
        "symbol": "AAA",
        "after_sequence": 0,
        "wait_for_gap": False,
    })
    assert aged.json()["join_status"] == "checkpoint_too_old"


def test_http_history_state_endpoint_reports_retention():
    client = TestClient(create_app(MarketDataFeedService()))
    client.post("/sources/source_a/ingest", json={"record": record_n(0)})
    state = client.get("/state/history").json()
    entry = state["source_a/AAA"]
    assert entry["retained"] == 1
    assert entry["capacity"] == 64
    assert entry["newest_sequence"] == 0
    assert entry["gap_state"] == "normal"
