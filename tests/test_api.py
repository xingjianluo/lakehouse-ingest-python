"""HTTP 层冒烟：提交、重复批次、不兼容结构、快照与版本错误码可区分。"""

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
    r = client.post("/batches", json={
        "batch_id": "api-1",
        "records": [{"id": 1}],
        "schema": [{"name": "id", "type": "int"}],
    })
    assert r.status_code == 201
    version = r.json()["version"]
    log.info("HTTP 提交: version=%d batch=api-1", version)

    r = client.get("/snapshot", params={"version": version})
    assert r.status_code == 200
    assert r.json()["records"] == [{"id": 1}]


def test_duplicate_batch_409(client):
    body = {
        "batch_id": "api-2",
        "records": [{"id": 1}],
        "schema": [{"name": "id", "type": "int"}],
    }
    assert client.post("/batches", json=body).status_code == 201
    r = client.post("/batches", json=body)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "duplicate_batch"
    log.info("HTTP 重复批次: batch=api-2 状态=409 判定依据=duplicate_batch")


def test_incompatible_schema_422_with_reason(client):
    client.post("/batches", json={
        "batch_id": "api-3",
        "records": [{"id": 1, "name": "a"}],
        "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
    })
    r = client.post("/batches", json={
        "batch_id": "api-4",
        "records": [{"id": 2}],
        "schema": [{"name": "id", "type": "int"}],  # 删除 name 字段
    })
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["error"] == "incompatible_schema"
    assert detail["reason"] == "field_removed"
    log.info("HTTP 结构拒绝: batch=api-4 状态=422 原因=%s", detail["reason"])


def test_version_errors(client):
    client.post("/batches", json={
        "batch_id": "api-5",
        "records": [{"id": 1}],
        "schema": [{"name": "id", "type": "int"}],
    })
    r = client.get("/snapshot", params={"version": 99})
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "version_not_found"
    log.info("HTTP 版本拒绝: version=99 状态=404 判定依据=version_not_found")


def test_idempotent_retry_and_conflict_409(client):
    body = {
        "batch_id": "api-6",
        "records": [{"id": 1}],
        "schema": [{"name": "id", "type": "int"}],
    }
    assert client.post("/batches", json=body).status_code == 201
    # 一致重试：409 duplicate_batch，带回首次版本与条数。
    r = client.post("/batches", json=body)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "duplicate_batch"
    assert detail["committed_version"] == 1
    assert detail["record_count"] == 1
    assert detail["idempotent"] is True
    log.info("HTTP 一致重试: batch=api-6 409 duplicate_batch committed_version=1")

    # 不一致冲突：409 batch_conflict，明确指出已落版本。
    r = client.post("/batches", json={**body, "records": [{"id": 2}]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "batch_conflict"
    assert detail["committed_version"] == 1
    log.info("HTTP 内容冲突: batch=api-6 409 batch_conflict committed_version=1")


def test_batch_status_endpoint(client):
    client.post("/batches", json={
        "batch_id": "api-7",
        "records": [{"id": 1, "name": "a"}],
        "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
    })
    client.post("/batches", json={
        "batch_id": "api-7",
        "records": [{"id": 9, "name": "different"}],
        "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
    })
    r = client.get("/batches/api-7")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "committed"
    assert body["version"] == 1
    assert body["record_count"] == 1
    assert body["rejections"][0]["reason"] == "batch_conflict"
    log.info("HTTP 对账: batch=api-7 state=committed version=1 被拒=%s",
             [x["reason"] for x in body["rejections"]])

    assert client.get("/batches/ghost").status_code == 404


def test_schema_precheck_endpoint(client):
    client.post("/batches", json={
        "batch_id": "api-8",
        "records": [{"id": 1, "name": "a"}],
        "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
    })
    r = client.post("/schema/precheck", json={
        "batch_id": "probe",
        "records": [],
        "schema": [{"name": "id", "type": "int"}],  # 删除 name
    })
    assert r.status_code == 200
    body = r.json()
    assert body["compatible"] is False
    assert body["reason"] == "field_removed"
    assert body["field"] == "name"
    log.info("HTTP 预检拒绝: batch=probe reason=field_removed field=name, 无写入")

    r = client.post("/schema/precheck", json={
        "batch_id": "probe2",
        "records": [],
        "schema": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "note", "type": "string", "default": None},
        ],
    })
    assert r.status_code == 200
    assert r.json()["compatible"] is True
    log.info("HTTP 预检放行: 新增可空字段 note, 无写入")
