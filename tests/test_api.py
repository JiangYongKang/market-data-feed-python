"""HTTP API 端到端测试：订阅 -> 推送 -> 最新价 -> 恢复 -> 缺口。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app, distributor, service, subscriptions
from tests.conftest import log_verdict, raw_a


@pytest.fixture()
def client():
    # 每个用例重置全局状态，保证可复现
    subscriptions._subscriptions.clear()
    service._trackers.clear()
    service._latest.clear()
    service._gaps.clear()
    distributor._subscribers.clear()
    distributor._drops.clear()
    return TestClient(app)


def test_full_flow(client: TestClient):
    r = client.post("/subscribe/source_a/BTCUSD")
    assert r.status_code == 201

    r = client.post("/ingest/source_a", json=raw_a(1, px="65000.10"))
    log_verdict("API推送", raw_a(1, px="65000.10"), r.json())
    assert r.status_code == 200 and r.json()["status"] == "ACCEPTED"

    r = client.get("/latest/source_a/BTCUSD")
    assert r.status_code == 200
    body = r.json()
    assert body["price"] == "65000.10" and body["sequence"] == 1


def test_malformed_record_rejected_with_reason(client: TestClient):
    client.post("/subscribe/source_a/BTCUSD")
    r = client.post("/ingest/source_a", json={"sym": "BTCUSD", "seq": 1})
    log_verdict("API畸形记录", {"sym": "BTCUSD", "seq": 1}, r.json())
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "MISSING_FIELD"


def test_unknown_source_rejected(client: TestClient):
    r = client.post("/ingest/source_x", json={"a": 1})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "UNKNOWN_SOURCE"


def test_duplicate_subscribe_conflict(client: TestClient):
    assert client.post("/subscribe/source_a/BTCUSD").status_code == 201
    r = client.post("/subscribe/source_a/BTCUSD")
    assert r.status_code == 409 and "ALREADY_SUBSCRIBED" in r.json()["detail"]
    r = client.post("/subscribe/source_a/DOGEUSD")
    assert r.status_code == 409 and "UNKNOWN_SYMBOL" in r.json()["detail"]


def test_gap_recovery_via_api(client: TestClient):
    client.post("/subscribe/source_a/BTCUSD")
    client.post("/ingest/source_a", json=raw_a(1))
    r = client.post("/ingest/source_a", json=raw_a(4))  # 缺 2,3
    assert r.json()["status"] == "BUFFERED"
    r = client.post("/recover/source_a/BTCUSD", json=[raw_a(2)])  # 只回补 2
    log_verdict("API恢复", "replay=[2]", r.json())
    assert r.json()["gap"]["from_sequence"] == 3
    r = client.get("/gaps")
    assert len(r.json()) == 1 and r.json()[0]["to_sequence"] == 3
