"""批次对账：同批次号一致重试 / 不一致冲突 / 被拒记录可查。"""

import logging

import pytest

from lakehouse import (
    BatchConflictError,
    DuplicateBatchError,
    Field,
    Schema,
    Table,
)

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"), Field("name", "string")))


def test_same_content_retry_returns_first_result_without_writes(table_dir):
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("dup-1", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    log.info("首次提交: batch=dup-1 version=%d 行数=2", v1)

    with pytest.raises(DuplicateBatchError) as exc:
        t.commit_batch("dup-1", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    assert exc.value.committed_version == v1
    assert exc.value.record_count == 2
    log.info("一致重试: batch=dup-1 committed_version=%d record_count=%d 判定依据=指纹一致, 不写数据",
             exc.value.committed_version, exc.value.record_count)

    assert t.current_version() == 1
    assert len(t.storage.read_log()) == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_different_content_same_batch_id_conflicts(table_dir):
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("dup-2", [{"id": 1, "name": "a"}])
    log.info("首次提交: batch=dup-2 version=%d", v1)

    with pytest.raises(BatchConflictError) as exc:
        t.commit_batch("dup-2", [{"id": 1, "name": "CHANGED"}])
    assert exc.value.batch_id == "dup-2"
    assert exc.value.committed_version == v1
    log.info("冲突拒绝: batch=dup-2 committed_version=%d 判定依据=同批次号指纹不一致",
             exc.value.committed_version)

    # 冲突不产生任何写入，原数据原封不动。
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}]
    assert len(t.storage.read_log()) == 1

    # 冲突被拒进台账，可按批次号查到。
    status = t.batch_status("dup-2")
    assert status.state == "committed"
    assert status.version == 1
    assert status.batch_size == 1
    assert [r.reason for r in status.rejections] == ["batch_conflict"]
    assert status.rejections[0].committed_version == 1
    log.info("对账查询: batch=dup-2 state=committed version=1 被拒次数=%d 原因=batch_conflict",
             len(status.rejections))


def test_unknown_and_rejected_batch_status(table_dir):
    from lakehouse import IncompatibleSchemaError, RecordValidationError

    base = Schema((Field("id", "int"),))
    t = Table(table_dir, base)

    unknown = t.batch_status("never-seen")
    assert unknown.state == "unknown"
    log.info("对账查询: batch=never-seen state=unknown 判定依据=日志与台账均无记录")

    t.commit_batch("ok-1", [{"id": 1}])

    # 结构拒绝：int -> bool，可按结构原因查到。
    with pytest.raises(IncompatibleSchemaError):
        t.commit_batch("rej-schema", [{"id": True}], Schema((Field("id", "bool"),)))
    status = t.batch_status("rej-schema")
    assert status.state == "rejected"
    assert status.version is None
    assert status.rejections[0].reason == "incompatible_schema"
    assert status.rejections[0].schema_reason == "type_changed"
    assert status.rejections[0].field == "id"
    log.info("对账查询: batch=rej-schema state=rejected schema_reason=type_changed, 不占版本")

    # 记录校验拒绝也入台账。
    with pytest.raises(RecordValidationError):
        t.commit_batch("rej-record", [{"id": "not-int"}])
    status2 = t.batch_status("rej-record")
    assert status2.state == "rejected"
    assert status2.rejections[0].reason == "record_validation"
    log.info("对账查询: batch=rej-record state=rejected reason=record_validation, 不占版本")

    # 被拒不占版本、不留数据。
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1}]
