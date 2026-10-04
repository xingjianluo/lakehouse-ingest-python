"""HTTP 层冒烟：提交、幂等重试、批次冲突、批次对账、结构预检、快照错误码。"""

import importlib
import logging

import pytest
from fastapi.testclient import TestClient

log = logging.getLogger("lakehouse-test")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_TABLE_ROOT", str(tmp_path / "api-table"))
    import main

    importlib.reload(main)
    return TestClient(main.app)


def test_commit_and_snapshot(client):
    r = client.post(
        "/batches",
        json={
            "batch_id": "api-1",
            "records": [{"id": 1}],
            "schema": [{"name": "id", "type": "int"}],
        },
    )
    assert r.status_code == 201
    assert r.json()["deduplicated"] is False
    version = r.json()["version"]
    log.info("HTTP 提交: version=%d batch=api-1", version)

    r = client.get("/snapshot", params={"version": version})
    assert r.status_code == 200
    assert r.json()["records"] == [{"id": 1}]


def test_identical_retry_returns_first_result(client):
    body = {
        "batch_id": "api-2",
        "records": [{"id": 1}],
        "schema": [{"name": "id", "type": "int"}],
    }
    r1 = client.post("/batches", json=body)
    assert r1.status_code == 201
    r2 = client.post("/batches", json=body)
    assert r2.status_code == 200 or r2.status_code == 201
    assert r2.json()["version"] == r1.json()["version"]
    assert r2.json()["deduplicated"] is True
    log.info(
        "HTTP 一致重试: batch=api-2 返回 version=%d 判定依据=deduplicated 标记",
        r2.json()["version"],
    )


def test_conflicting_batch_409(client):
    base = {
        "batch_id": "api-3",
        "records": [{"id": 1}],
        "schema": [{"name": "id", "type": "int"}],
    }
    assert client.post("/batches", json=base).status_code == 201
    r = client.post("/batches", json={**base, "records": [{"id": 999}]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "batch_conflict"
    assert detail["committed_version"] == 1
    log.info(
        "HTTP 批次冲突: batch=api-3 状态=409 committed_version=%d",
        detail["committed_version"],
    )


def test_batch_status_endpoint(client):
    client.post(
        "/batches",
        json={
            "batch_id": "api-4",
            "records": [{"id": 1}, {"id": 2}],
            "schema": [{"name": "id", "type": "int"}],
        },
    )
    r = client.get("/batches/api-4")
    assert r.status_code == 200
    assert r.json()["status"] == "committed"
    assert r.json()["version"] == 1
    assert r.json()["record_count"] == 2
    log.info("HTTP 对账: batch=api-4 status=committed version=1 行数=2")

    r = client.get("/batches/ghost")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "batch_not_found"


def test_schema_check_endpoint_matches_commit(client):
    client.post(
        "/batches",
        json={
            "batch_id": "api-5",
            "records": [{"id": 1, "name": "a"}],
            "schema": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        },
    )
    # 预检：删除 name 字段 -> 拒绝，原因可区分。
    r = client.post("/schema/check", json={"schema": [{"name": "id", "type": "int"}]})
    assert r.status_code == 200
    assert r.json()["accepted"] is False
    assert r.json()["reason"] == "field_removed"
    log.info("HTTP 预检拒绝: 原因=%s 字段=%s", r.json()["reason"], r.json()["field"])

    # 预检：新增字段 -> 放行；随后真实提交确实成功（预检与提交结论一致）。
    r = client.post(
        "/schema/check",
        json={
            "schema": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "score", "type": "float", "default": 0.0},
            ]
        },
    )
    assert r.json()["accepted"] is True
    r = client.post(
        "/batches",
        json={
            "batch_id": "api-6",
            "records": [{"id": 2, "name": "b", "score": 1.5}],
            "schema": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "score", "type": "float", "default": 0.0},
            ],
        },
    )
    assert r.status_code == 201
    log.info("HTTP 预检放行后提交成功: version=%d", r.json()["version"])


def test_incompatible_schema_422_with_reason(client):
    client.post(
        "/batches",
        json={
            "batch_id": "api-7",
            "records": [{"id": 1, "name": "a"}],
            "schema": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        },
    )
    r = client.post(
        "/batches",
        json={
            "batch_id": "api-8",
            "records": [{"id": 2}],
            "schema": [{"name": "id", "type": "int"}],  # 删除 name 字段
        },
    )
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["error"] == "incompatible_schema"
    assert detail["reason"] == "field_removed"
    log.info("HTTP 结构拒绝: batch=api-8 状态=422 原因=%s", detail["reason"])


def test_version_errors(client):
    client.post(
        "/batches",
        json={
            "batch_id": "api-9",
            "records": [{"id": 1}],
            "schema": [{"name": "id", "type": "int"}],
        },
    )
    r = client.get("/snapshot", params={"version": 99})
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "version_not_found"
    log.info("HTTP 版本拒绝: version=99 状态=404 判定依据=version_not_found")
