"""批次重试与对账：同号同内容按重试返回首次结果；同号不同内容明确冲突失败。"""

import logging

import pytest

from lakehouse import BatchConflictError, Field, RecordValidationError, Schema, Table

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"), Field("name", "string")))
RECORDS = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_identical_retry_returns_first_result(table_dir):
    """同批次号 + 内容一致 -> 幂等重试：返回首次版本号，不再写任何数据。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("batch-001", RECORDS)
    log.info("首次提交: version=%d batch=batch-001 判定依据=新批次", v1)

    v2 = t.commit_batch("batch-001", RECORDS)
    log.info(
        "一致重试: batch=batch-001 返回 version=%d 判定依据=内容指纹与首次一致", v2
    )
    assert v2 == v1

    # 数据与元数据均未变化：无重复、无丢失、不占新版本。
    assert t.current_version() == v1
    assert t.snapshot() == RECORDS
    assert len(t.storage.read_log()) == 1
    log.info(
        "校验通过: version=%d 行数=%d 日志条目=1",
        t.current_version(),
        len(t.snapshot()),
    )


def test_conflicting_content_rejected_with_version(table_dir):
    """同批次号 + 内容变了 -> BatchConflictError，可定位批次与已落版本。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("batch-002", [{"id": 1, "name": "x"}])

    with pytest.raises(BatchConflictError) as exc:
        t.commit_batch("batch-002", [{"id": 1, "name": "CHANGED"}])
    assert exc.value.batch_id == "batch-002"
    assert exc.value.committed_version == v1
    log.info(
        "冲突拒绝: batch=%s 已提交版本=%d 判定依据=内容指纹与首次不一致",
        exc.value.batch_id,
        exc.value.committed_version,
    )

    # 冲突不产生任何写入：版本与数据保持首次提交的样子。
    assert t.current_version() == v1
    assert t.snapshot() == [{"id": 1, "name": "x"}]
    assert len(t.storage.read_log()) == 1


def test_retry_after_crash_mid_commit_no_duplication(table_dir):
    """写入方在"数据已落盘、日志未追加"处崩溃：重试整体重写，不多不少。"""
    t = Table(table_dir, SCHEMA)
    # 模拟崩溃残留：v1 数据文件已写（孤儿），日志里没有提交记录。
    t.storage.write_version_data(1, [{"id": 999, "name": "orphan"}])
    assert t.current_version() == 0

    v = t.commit_batch("batch-003", [{"id": 1, "name": "a"}])
    assert v == 1  # 版本号从日志重算，不被孤儿文件占用
    assert t.snapshot() == [{"id": 1, "name": "a"}]
    assert len(t.storage.read_log()) == 1
    log.info("崩溃重试: version=%d batch=batch-003 判定依据=孤儿数据文件被整体重写", v)


def test_reopen_table_retry_is_idempotent(table_dir):
    """进程重启后重试同一批次：仍按首次结果返回，不重复写入。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("batch-010", [{"id": 7, "name": "z"}])

    t2 = Table(table_dir, SCHEMA)  # 重新打开表（模拟进程重启）
    v2 = t2.commit_batch("batch-010", [{"id": 7, "name": "z"}])
    assert v2 == v1
    assert t2.current_version() == v1
    assert t2.snapshot() == [{"id": 7, "name": "z"}]
    log.info("重启重试: batch=batch-010 返回 version=%d 判定依据=指纹随日志持久化", v2)


def test_batch_status_committed_rejected_unknown(table_dir):
    """按批次号对账：成功可查版本与行数，被拒绝可查原因，未知批次可区分。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("ok-batch", RECORDS)

    committed = t.batch_status("ok-batch")
    assert committed.status == "committed"
    assert committed.version == v1
    assert committed.record_count == len(RECORDS)
    log.info(
        "对账[成功]: batch=ok-batch version=%d 行数=%d",
        committed.version,
        committed.record_count,
    )

    # 制造一个被拒绝的批次（记录类型不匹配）。
    with pytest.raises(RecordValidationError):
        t.commit_batch("bad-batch", [{"id": "not-an-int", "name": "a"}])
    rejected = t.batch_status("bad-batch")
    assert rejected.status == "rejected"
    assert rejected.reason == "record_validation"
    assert rejected.detail
    log.info(
        "对账[拒绝]: batch=bad-batch 原因=%s 判定依据=%s",
        rejected.reason,
        rejected.detail,
    )

    unknown = t.batch_status("never-seen")
    assert unknown.status == "unknown"
    log.info("对账[未知]: batch=never-seen status=unknown")


def test_rejected_batch_retry_is_judged_fresh(table_dir):
    """被拒绝的批次不占版本、不留数据；同号重试按全新提交重新判定。"""
    t = Table(table_dir, SCHEMA)
    with pytest.raises(RecordValidationError):
        t.commit_batch("retry-me", [{"id": "bad", "name": "a"}])
    assert t.current_version() == 0
    assert not t.storage.version_path(1).exists()

    # 同一批次号修正内容后重试：不受上次失败影响，正常提交。
    v = t.commit_batch("retry-me", [{"id": 1, "name": "a"}])
    assert v == 1
    assert t.batch_status("retry-me").status == "committed"
    log.info("拒绝后重试: batch=retry-me version=%d 判定依据=拒绝记录不参与提交判定", v)


def test_retry_without_schema_returns_first_result(table_dir):
    """首次提交带了表结构，重试不再重复带 -> 内容一致，按重试处理。"""
    t = Table(table_dir, SCHEMA)
    evolved = Schema((*SCHEMA.fields, Field("tag", "string", "untagged")))
    records = [{"id": 1, "name": "a", "tag": "x"}]
    v1 = t.commit_batch("batch-020", records, evolved)

    v2 = t.commit_batch("batch-020", records)  # 不再携带表结构
    assert v2 == v1
    assert t.current_version() == v1
    assert len(t.storage.read_log()) == 1, "重试不得产生任何写入"
    log.info(
        "无结构重试: batch=batch-020 返回 version=%d 判定依据=记录内容指纹一致", v2
    )


def test_retry_with_reordered_schema_fields_returns_first_result(table_dir):
    """重试时表结构字段书写顺序不同 -> 内容一致，按重试处理。"""
    t = Table(table_dir, SCHEMA)
    records = [{"id": 1, "name": "a"}]
    v1 = t.commit_batch("batch-021", records, SCHEMA)

    reordered = Schema((Field("name", "string"), Field("id", "int")))
    v2 = t.commit_batch("batch-021", records, reordered)
    assert v2 == v1
    assert t.current_version() == v1
    assert len(t.storage.read_log()) == 1
    log.info(
        "字段乱序重试: batch=batch-021 返回 version=%d 判定依据=字段顺序不影响内容指纹",
        v2,
    )


def test_batch_status_reports_batch_own_record_count(table_dir):
    """对账条数 = 本批自己的条数，而不是整张表当前的总条数。"""
    t = Table(table_dir, SCHEMA)
    t.commit_batch("batch-030", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    v2 = t.commit_batch(
        "batch-031",
        [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}, {"id": 5, "name": "e"}],
    )

    st = t.batch_status("batch-031")
    assert st.status == "committed"
    assert st.version == v2
    assert st.record_count == 3, "第二批 3 条，不能报成全表累计的 5 条"
    st_first = t.batch_status("batch-030")
    assert st_first.record_count == 2
    assert len(t.snapshot()) == 5  # 全表总条数仍是 5
    log.info(
        "对账条数: batch=batch-031 本批条数=%d (全表累计=%d) 判定依据=按批次自身计数",
        st.record_count,
        len(t.snapshot()),
    )


def test_conflicting_retry_batch_status_still_points_to_committed(table_dir):
    """冲突重试失败后，对账仍指向首次提交的版本与本批条数。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("batch-040", [{"id": 1, "name": "x"}])
    with pytest.raises(BatchConflictError) as exc:
        t.commit_batch("batch-040", [{"id": 1, "name": "CHANGED"}])
    assert exc.value.committed_version == v1

    st = t.batch_status("batch-040")
    assert st.status == "committed"
    assert st.version == v1
    assert st.record_count == 1
    log.info(
        "冲突后对账: batch=batch-040 仍落在 version=%d 判定依据=已提交记录优先于拒绝记录",
        st.version,
    )
