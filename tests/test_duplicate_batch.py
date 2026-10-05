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


def test_retry_without_schema_treated_as_same_attempt(table_dir):
    """重试时不重复携带结构、内容一致 -> 按重试处理，不冲突、不写入。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("retry-no-schema", RECORDS, SCHEMA)

    v2 = t.commit_batch("retry-no-schema", RECORDS)  # 这次不带 schema
    assert v2 == v1
    log.info(
        "不带结构重试: batch=retry-no-schema 返回 version=%d 判定依据=记录内容一致", v2
    )
    # 记录内 dict 键顺序不同也不影响（内容完全一样）。
    v3 = t.commit_batch(
        "retry-no-schema", [{"name": "a", "id": 1}, {"name": "b", "id": 2}]
    )
    assert v3 == v1
    assert t.current_version() == v1
    assert len(t.storage.read_log()) == 1


def test_retry_with_reordered_schema_fields_treated_as_same_attempt(table_dir):
    """重试时结构字段书写顺序不同、内容一致 -> 按重试处理。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("retry-reorder", RECORDS, SCHEMA)
    reordered = Schema((Field("name", "string"), Field("id", "int")))

    v2 = t.commit_batch("retry-reorder", RECORDS, reordered)
    assert v2 == v1
    log.info(
        "字段顺序不同的重试: batch=retry-reorder 返回 version=%d 判定依据=结构语义相同",
        v2,
    )
    # 反向组合：首次不带结构，重试带相同结构。
    v3 = t.commit_batch("retry-reorder-2", RECORDS)
    v4 = t.commit_batch("retry-reorder-2", RECORDS, reordered)
    assert v4 == v3
    assert t.current_version() == 2
    assert len(t.storage.read_log()) == 2


def test_retry_with_different_schema_intent_conflicts(table_dir):
    """记录一样但声明了不同的结构 -> 内容意图不同，仍然按冲突失败。"""
    base = Schema((Field("id", "int"),))
    evolved = Schema((Field("id", "int"), Field("tag", "string", "x")))
    t = Table(table_dir, base)
    v1 = t.commit_batch("retry-schema-change", [{"id": 1}], base)
    # 需要先让 tag 字段存在，再制造"同记录、不同结构意图"。
    t.commit_batch("evolve", [], evolved)
    with pytest.raises(BatchConflictError) as exc:
        t.commit_batch("retry-schema-change", [{"id": 1}], evolved)
    assert exc.value.committed_version == v1
    log.info(
        "结构意图冲突: batch=%s 已提交版本=%d 判定依据=声明结构不同",
        exc.value.batch_id,
        exc.value.committed_version,
    )


def test_batch_status_record_count_is_batch_own_count(table_dir):
    """对账条数 = 该批次自己的条数，不是整张表当前的总条数。"""
    t = Table(table_dir, SCHEMA)
    t.commit_batch("first", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    t.commit_batch(
        "second",
        [
            {"id": 3, "name": "c"},
            {"id": 4, "name": "d"},
            {"id": 5, "name": "e"},
        ],
    )
    st = t.batch_status("second")
    assert st.status == "committed"
    assert st.record_count == 3  # 第二批自己的 3 条，而非表总量 5
    log.info(
        "对账条数: batch=second 本批条数=%d 表总条数=%d 判定依据=batch_record_count",
        st.record_count,
        len(t.snapshot()),
    )
    assert t.batch_status("first").record_count == 2


def test_conflict_retry_visible_in_reconciliation(table_dir):
    """冲突失败也要能从对账看到原因与已落地版本。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("rc", [{"id": 1, "name": "a"}])
    with pytest.raises(BatchConflictError) as exc:
        t.commit_batch("rc", [{"id": 1, "name": "CHANGED"}])
    assert exc.value.committed_version == v1
    # 已落地批次的对账结果仍是 committed，但能看到最近一次失败尝试。
    st = t.batch_status("rc")
    assert st.status == "committed"
    assert st.version == v1
    assert st.last_rejection_reason == "batch_conflict"
    assert "1" in st.last_rejection_detail
    log.info(
        "对账[冲突尝试]: batch=rc version=%d 最近失败原因=%s",
        st.version,
        st.last_rejection_reason,
    )
    # 冲突失败后用正确内容重试仍然幂等，不受失败尝试影响。
    assert t.commit_batch("rc", [{"id": 1, "name": "a"}]) == v1
    assert t.current_version() == 1


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
