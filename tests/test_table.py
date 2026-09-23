"""提交幂等、事务原子性、快照读取/时间旅行/过期测试。"""

from __future__ import annotations

import logging

import pytest

from lakehouse.errors import (
    BatchConflictError,
    SchemaIncompatibleError,
    VersionExpiredError,
    VersionNotFoundError,
)
from lakehouse.table import LakehouseTable

SCHEMA_V1 = {
    "columns": [
        {"name": "id", "type": "long", "nullable": False},
        {"name": "amt", "type": "double"},
    ]
}
SCHEMA_V2 = {
    "columns": SCHEMA_V1["columns"] + [{"name": "tag", "type": "string"}]
}


@pytest.fixture
def table(tmp_data_root):
    return LakehouseTable(tmp_data_root, "orders")


def test_first_commit_creates_version_one(table, caplog):
    caplog.set_level(logging.INFO, logger="lakehouse")
    e = table.commit("b-001", [{"id": 1, "amt": 1.0}], SCHEMA_V1)
    assert e.version == 1
    assert e.batch_id == "b-001"
    assert e.parent_version is None
    assert table.current_version() == 1
    # 日志必须包含提交版本、批次标识与判定依据
    body = "\n".join(r.getMessage() for r in caplog.records)
    assert "version=1" in body and "b-001" in body and "decision=accept" in body


def test_duplicate_batch_is_idempotent_and_returns_same_version(table):
    e1 = table.commit("b-001", [{"id": 1}, {"id": 2}], SCHEMA_V1)
    e2 = table.commit("b-001", [{"id": 1}, {"id": 2}], SCHEMA_V1)
    assert e1.version == e2.version == 1
    assert table.current_version() == 1
    snap = table.read()
    assert len(snap["records"]) == 2  # 无重复


def test_duplicate_batch_id_with_different_payload_is_distinguishable_conflict(table):
    table.commit("b-001", [{"id": 1}], SCHEMA_V1)
    with pytest.raises(BatchConflictError) as ei:
        table.commit("b-001", [{"id": 2}], SCHEMA_V1)
    assert ei.value.reason == "batch_conflict"
    assert "b-001" in str(ei.value)
    # 冲突不产生新版本、不污染数据
    assert table.current_version() == 1
    assert [r["id"] for r in table.read()["records"]] == [1]


def test_duplicate_batch_id_with_different_schema_is_conflict(table):
    table.commit("b-001", [{"id": 1}], SCHEMA_V1)
    with pytest.raises(BatchConflictError) as ei:
        table.commit("b-001", [{"id": 1}], SCHEMA_V2)
    assert ei.value.reason == "batch_conflict"


def test_failed_commit_is_all_or_nothing(table):
    table.commit("ok-1", [{"id": 1}], SCHEMA_V1)
    # 批次内第二条非法 -> 整批拒绝，版本与数据保持不变
    with pytest.raises(SchemaIncompatibleError):
        table.commit("bad-1", [{"id": 2}, {"id": "bad"}], SCHEMA_V1)
    assert table.current_version() == 1
    assert [r["id"] for r in table.read()["records"]] == [1]
    # 同一 batch_id 失败后可原样重试成功（未登记为已提交）
    e = table.commit("bad-1", [{"id": 2}, {"id": 3}], SCHEMA_V1)
    assert e.version == 2
    assert [r["id"] for r in table.read()["records"]] == [1, 2, 3]


def test_snapshot_version_is_immutable_and_reproducible_after_more_commits(table):
    table.commit("b1", [{"id": 1}], SCHEMA_V1)
    table.commit("b2", [{"id": 2}], SCHEMA_V2)
    table.commit("b3", [{"id": 3, "tag": "z"}], SCHEMA_V2)
    v1 = table.read(1)
    v1_again = table.read(1)
    assert v1["records"] == v1_again["records"] == [{"id": 1, "amt": None}]
    assert len(v1["schema"]["columns"]) == 2  # 老快照保留老结构
    cur = table.read()
    assert cur["version"] == 3 and len(cur["records"]) == 3
    # 历史行在新快照中新增字段缺省为 null
    assert cur["records"][0]["tag"] is None


def test_as_of_time_travel(table):
    table.commit("b1", [{"id": 1}], SCHEMA_V1)
    ts1 = table.read(1)["created_at"]
    table.commit("b2", [{"id": 2}], SCHEMA_V1)
    assert table.read(as_of=ts1)["version"] == 1
    assert table.read(as_of="2999-01-01T00:00:00+00:00")["version"] == 2


def test_missing_and_future_versions_are_rejected_distinctly(table):
    table.commit("b1", [{"id": 1}], SCHEMA_V1)
    for bad in (0, -1, 2, 99):
        with pytest.raises(VersionNotFoundError) as ei:
            table.read(bad)
        assert ei.value.reason == "version_not_found"
    with pytest.raises(VersionNotFoundError):
        table.read(as_of="2000-01-01T00:00:00+00:00")


def test_expired_version_rejected_but_history_remains(table):
    for i in range(1, 4):
        table.commit(f"b{i}", [{"id": i}], SCHEMA_V1)
    expired = table.expire_versions(retain=1)
    assert expired == [1, 2]
    with pytest.raises(VersionExpiredError) as ei:
        table.read(1)
    assert ei.value.reason == "version_expired"
    # 现存版本仍可读
    assert table.read(3)["version"] == 3
    # 元数据历史永久保留
    hist = table.history()
    assert [c["version"] for c in hist] == [1, 2, 3]
    assert hist[0]["expired"] and not hist[2]["expired"]


def test_idempotent_replay_still_works_after_snapshot_expired(table):
    table.commit("b1", [{"id": 1}, {"id": 2}], SCHEMA_V1)
    table.commit("b2", [{"id": 3}], SCHEMA_V1)
    table.expire_versions(retain=1)
    # v1 数据文件已删除，但批次指纹固化在 catalog，仍可幂等判定
    e = table.commit("b1", [{"id": 1}, {"id": 2}], SCHEMA_V1)
    assert e.version == 1
    with pytest.raises(BatchConflictError):
        table.commit("b1", [{"id": 1}], SCHEMA_V1)


def test_state_survives_reopen(tmp_data_root):
    t1 = LakehouseTable(tmp_data_root, "orders")
    t1.commit("b1", [{"id": 1}], SCHEMA_V1)
    t1.commit("b2", [{"id": 2, "tag": "x"}], SCHEMA_V2)
    t2 = LakehouseTable(tmp_data_root, "orders")
    assert t2.current_version() == 2
    assert len(t2.read(2)["records"]) == 2
    assert t2.read(1)["records"] == [{"id": 1, "amt": None}]


def test_record_level_validation_reasons_are_distinguishable(table):
    table.commit("b1", [{"id": 1}], SCHEMA_V1)
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b2", [{"id": 2, "unknown": 1}], SCHEMA_V1)
    assert ei.value.sub_reason == SchemaIncompatibleError.RECORD_UNKNOWN_FIELD
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b3", [{"amt": 1.0}], SCHEMA_V1)  # 缺必填 id
    assert ei.value.sub_reason == SchemaIncompatibleError.RECORD_MISSING_FIELD
    assert table.current_version() == 1
