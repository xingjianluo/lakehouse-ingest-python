"""时间旅行与版本清理：不存在或已过期的版本必须被明确拒绝。"""

import logging

import pytest

from lakehouse import (
    Field,
    Schema,
    Table,
    VersionExpiredError,
    VersionNotFoundError,
)

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"),))


def test_time_travel_reads_consistent_commit_point(table_dir):
    t = Table(table_dir, SCHEMA)
    for i in range(1, 4):
        t.commit_batch(f"b{i}", [{"id": i}])
    assert t.snapshot(1) == [{"id": 1}]
    assert t.snapshot(2) == [{"id": 1}, {"id": 2}]
    assert t.snapshot(3) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert t.snapshot() == t.snapshot(3)  # 默认读最新
    log.info("时间旅行: version=1..3 各自快照一致, 判定依据=逐版本全量快照")


def test_nonexistent_version_rejected(table_dir):
    t = Table(table_dir, SCHEMA)
    t.commit_batch("b1", [{"id": 1}])
    for bad in (0, -1, 2, 99):
        with pytest.raises(VersionNotFoundError) as exc:
            t.snapshot(bad)
        log.info("拒绝: version=%d 判定依据=版本从未存在", exc.value.version)


def test_expired_version_rejected_after_cleanup(table_dir):
    t = Table(table_dir, SCHEMA)
    for i in range(1, 6):
        t.commit_batch(f"b{i}", [{"id": i}])

    removed = t.expire_versions(keep_last=2)
    assert removed == [1, 2, 3]
    log.info("版本清理: 过期版本=%s 保留最近 2 个版本", removed)

    with pytest.raises(VersionExpiredError) as exc:
        t.snapshot(2)
    assert exc.value.version == 2
    assert exc.value.min_available == 4
    log.info(
        "拒绝: version=%d 判定依据=已过期, 最早可用版本=%d",
        exc.value.version,
        exc.value.min_available,
    )

    # 保留的版本仍可复现，且元数据日志完整保留。
    assert t.snapshot(4) == [{"id": i} for i in range(1, 5)]
    assert len(t.storage.read_log()) == 5
    log.info("校验: version=4 可复现, 元数据日志条目=%d", len(t.storage.read_log()))


def test_snapshot_on_empty_table_rejected(table_dir):
    t = Table(table_dir, SCHEMA)
    with pytest.raises(VersionNotFoundError):
        t.snapshot()
    log.info("拒绝: 空表读取 判定依据=尚无任何提交版本")
