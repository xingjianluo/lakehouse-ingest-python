"""HTTP API 端到端测试。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from lakehouse.api import create_app


@pytest.fixture
def client(tmp_data_root):
    return TestClient(create_app(tmp_data_root))


SCHEMA = {
    "columns": [
        {"name": "id", "type": "long", "nullable": False},
        {"name": "tag", "type": "string"},
    ]
}


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_commit_then_read_current_and_snapshot(client, caplog):
    caplog.set_level(logging.INFO, logger="lakehouse")
    r = client.post(
        "/tables/orders/commits",
        json={"batch_id": "b1", "records": [{"id": 1}], "schema": SCHEMA},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version"] == 1 and body["batch_id"] == "b1"

    cur = client.get("/tables/orders/current").json()
    assert cur["version"] == 1 and cur["records"] == [{"id": 1, "tag": None}]

    snap = client.get("/tables/orders/snapshots/1").json()
    assert snap["batch_id"] == "b1"
    # 测试日志中含版本、批次、判定依据
    logged = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "version=1" in logged and "b1" in logged and "decision=" in logged


def test_duplicate_batch_idempotent_over_http(client):
    payload = {"batch_id": "b1", "records": [{"id": 1}], "schema": SCHEMA}
    r1 = client.post("/tables/orders/commits", json=payload)
    r2 = client.post("/tables/orders/commits", json=payload)
    assert r1.json()["version"] == r2.json()["version"] == 1
    assert client.get("/tables/orders/current").json()["records"] == [
        {"id": 1, "tag": None}
    ]


def test_conflicting_batch_returns_409_with_reason(client):
    client.post(
        "/tables/orders/commits",
        json={"batch_id": "b1", "records": [{"id": 1}], "schema": SCHEMA},
    )
    r = client.post(
        "/tables/orders/commits",
        json={"batch_id": "b1", "records": [{"id": 2}], "schema": SCHEMA},
    )
    assert r.status_code == 409
    assert r.json()["error"] == "batch_conflict"


def test_incompatible_schema_returns_422_with_sub_reason(client):
    client.post(
        "/tables/orders/commits",
        json={"batch_id": "b1", "records": [{"id": 1, "tag": "a"}], "schema": SCHEMA},
    )
    # 删除 tag 字段
    r = client.post(
        "/tables/orders/commits",
        json={
            "batch_id": "b2",
            "records": [{"id": 2}],
            "schema": {"columns": [{"name": "id", "type": "long", "nullable": False}]},
        },
    )
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "schema_incompatible"
    assert body["sub_reason"] == "field_removed"
    assert body["field"] == "tag"


def test_missing_version_404_and_expired_version_410_are_distinct(client):
    client.post(
        "/tables/orders/commits",
        json={"batch_id": "b1", "records": [{"id": 1}], "schema": SCHEMA},
    )
    client.post(
        "/tables/orders/commits",
        json={"batch_id": "b2", "records": [{"id": 2}], "schema": SCHEMA},
    )
    r_missing = client.get("/tables/orders/snapshots/99")
    assert r_missing.status_code == 404
    assert r_missing.json()["error"] == "version_not_found"

    expired = client.post("/tables/orders/expire?retain=1").json()
    assert expired["expired_versions"] == [1]
    r_expired = client.get("/tables/orders/snapshots/1")
    assert r_expired.status_code == 410
    assert r_expired.json()["error"] == "version_expired"


def test_history_endpoint_lists_commits(client):
    client.post(
        "/tables/orders/commits",
        json={"batch_id": "b1", "records": [{"id": 1}], "schema": SCHEMA},
    )
    hist = client.get("/tables/orders/history").json()["commits"]
    assert [c["version"] for c in hist] == [1]
    assert hist[0]["batch_id"] == "b1"
