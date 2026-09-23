"""重复批次：重复提交或重试后数据不得重复或丢失，提交整体生效或整体不生效。"""

import logging

import pytest

from lakehouse import DuplicateBatchError, Schema, Field, Table

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"), Field("name", "string")))


def test_duplicate_batch_is_idempotent(table_dir):
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("batch-001", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    log.info("提交成功: version=%d batch=%s 判定依据=首次提交", v1, "batch-001")

    with pytest.raises(DuplicateBatchError) as exc:
        t.commit_batch("batch-001", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    log.info(
        "重复提交被拒绝: batch=%s 已提交版本=%d 判定依据=批次标识已存在于提交日志",
        exc.value.batch_id, exc.value.committed_version,
    )

    # 数据与元数据均未变化：无重复、无丢失。
    assert t.current_version() == v1
    assert t.snapshot() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert len(t.storage.read_log()) == 1
    log.info("校验通过: version=%d 行数=%d 日志条目=%d", t.current_version(), len(t.snapshot()), 1)


def test_retry_after_success_keeps_data(table_dir):
    t = Table(table_dir, SCHEMA)
    t.commit_batch("batch-002", [{"id": 1, "name": "x"}])
    t.commit_batch("batch-003", [{"id": 2, "name": "y"}])

    # 模拟重试第一批：不得覆盖、不得重复。
    with pytest.raises(DuplicateBatchError):
        t.commit_batch("batch-002", [{"id": 1, "name": "x"}])
    assert t.current_version() == 2
    assert t.snapshot() == [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]
    log.info("重试批次=batch-002 判定依据=幂等拒绝, 当前版本=%d 行数=%d",
             t.current_version(), len(t.snapshot()))


def test_reopen_table_still_detects_duplicate(table_dir):
    t = Table(table_dir, SCHEMA)
    t.commit_batch("batch-010", [{"id": 7, "name": "z"}])

    # 重新打开表（模拟进程重启后重试），元数据日志仍记得该批次。
    t2 = Table(table_dir, SCHEMA)
    with pytest.raises(DuplicateBatchError) as exc:
        t2.commit_batch("batch-010", [{"id": 7, "name": "z"}])
    log.info("重启后重试被拒绝: batch=%s committed_version=%d 判定依据=日志持久化",
             exc.value.batch_id, exc.value.committed_version)
    assert t2.snapshot() == [{"id": 7, "name": "z"}]
