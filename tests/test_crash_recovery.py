"""崩溃恢复：写到一半的提交残留不能影响后续提交。

核心保证：只要 commit_batch 成功返回了版本号，重新打开表就一定能读到
这一版，数据不多不少；半截日志行、孤儿数据文件、半截拒绝日志都不能
让已成功的提交"消失"或让后续提交被"吞掉"。
"""

import logging
import multiprocessing as mp
import os
from pathlib import Path

import pytest

from lakehouse import Field, RecordValidationError, Schema, Table

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"),))


def _leave_partial_log_line(root: str) -> None:
    """模拟写入方在追加提交日志途中崩溃：留下半截日志行后异常退出。"""
    t = Table(Path(root), SCHEMA)
    # 崩溃点 1：版本数据文件已写（成为孤儿），日志只写了半截。
    t.storage.write_version_data(1, [{"id": 999}])
    with open(t.storage.log_path, "ab") as f:
        f.write(b'{"version": 1, "batch_id": "crashed-batch", "sch')
        f.flush()
        os.fsync(f.fileno())
    os._exit(1)  # 模拟进程崩溃：不做任何清理


def test_partial_log_line_does_not_swallow_commits(table_dir):
    """半截提交记录残留：重开表后提交返回新版本，重开必须读得到。"""
    t = Table(table_dir, SCHEMA)
    v1 = t.commit_batch("b1", [{"id": 1}])

    # 模拟另一次提交写到一半进程挂掉：日志里留下半截行（无换行结尾）。
    with open(t.storage.log_path, "ab") as f:
        f.write(b'{"version": 2, "batch_id": "crashed", "schema":')
    log.info("模拟崩溃: 日志残留半截行, 已提交版本=%d", v1)

    t2 = Table(table_dir, SCHEMA)  # 重新打开表
    assert t2.current_version() == v1, "半截行必须视为未提交"
    v2 = t2.commit_batch("b2", [{"id": 2}])
    assert v2 == v1 + 1
    log.info("崩溃后提交: version=%d batch=b2 判定依据=半截残留已被截断", v2)

    # 关键保证：接口返回了版本号，重新打开表就必须读到这一版。
    t3 = Table(table_dir, SCHEMA)
    assert t3.current_version() == v2
    assert t3.snapshot() == [{"id": 1}, {"id": 2}]

    # 后续每次提交都不能被吞。
    v3 = t3.commit_batch("b3", [{"id": 3}])
    t4 = Table(table_dir, SCHEMA)
    assert t4.current_version() == v3
    assert t4.snapshot() == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert len(t4.storage.read_log()) == 3
    log.info(
        "校验通过: version=%d 行数=%d 日志条目=3 判定依据=每条日志均为完整行",
        t4.current_version(),
        len(t4.snapshot()),
    )


def test_multi_process_commit_after_crash_all_visible(table_dir):
    """某进程中途挂掉后，多个进程继续提交：所有成功提交都读得到。"""
    ctx = mp.get_context("fork")
    crasher = ctx.Process(target=_leave_partial_log_line, args=(str(table_dir),))
    crasher.start()
    crasher.join(timeout=60)
    assert crasher.exitcode == 1, "崩溃进程应按预期异常退出"
    log.info("模拟崩溃: 子进程留半截日志行与孤儿数据文件后退出")

    # 4 个进程各提交 5 批（其中进程 0 的第一批重试崩溃进程的批次号，
    # 内容不同 -> 必须按新提交处理，因为崩溃批次从未提交成功）。
    from test_concurrent_processes import _run_processes

    jobs = [[(f"p{p}-b{i}", [{"id": p * 100 + i}]) for i in range(5)] for p in range(4)]
    results = _run_processes(table_dir, jobs)
    versions = sorted(
        v for res in results for (outcome, _, v) in res if outcome == "ok"
    )
    assert all(outcome == "ok" for res in results for (outcome, _, _) in res)
    assert versions == list(range(1, 21)), "崩溃残留不得影响版本号分配"

    # 重新打开表：20 个版本一个不少，数据不多不少。
    t = Table(table_dir, SCHEMA)
    assert t.current_version() == 20
    rows = t.snapshot()
    assert len(rows) == 20
    assert len({r["id"] for r in rows}) == 20
    assert len(t.storage.read_log()) == 20
    log.info(
        "多进程崩溃恢复: 进程数=4 版本=1..20 全部可读 行数=%d 判定依据=日志完整无吞没",
        len(rows),
    )


def test_orphan_data_file_rewritten_on_retry(table_dir):
    """崩溃点在"数据已写、日志未追加"：重试整体重写该版本，不多不少。"""
    t = Table(table_dir, SCHEMA)
    t.commit_batch("b1", [{"id": 1}])
    # 崩溃残留：v2 数据文件已写（内容还是旧的半成品），日志没有 v2。
    t.storage.write_version_data(2, [{"id": 1}, {"id": 777}])

    t2 = Table(table_dir, SCHEMA)
    v2 = t2.commit_batch("b2", [{"id": 2}])
    assert v2 == 2

    t3 = Table(table_dir, SCHEMA)
    assert t3.snapshot() == [{"id": 1}, {"id": 2}], "孤儿数据必须被整体重写"
    log.info("孤儿重写: version=2 batch=b2 判定依据=未提交版本的数据文件整体覆盖")


def test_partial_rejection_line_does_not_break_reconciliation(table_dir):
    """拒绝日志的半截残留：不影响后续拒绝记录与批次对账。"""
    t = Table(table_dir, SCHEMA)
    with pytest.raises(RecordValidationError):
        t.commit_batch("bad-1", [{"id": "not-an-int"}])

    # 模拟崩溃：拒绝日志写了半截。
    with open(t.storage.rejections_path, "ab") as f:
        f.write(b'{"batch_id": "bad-2", "rea')

    # 后续被拒绝的批次仍能完整记录、可对账。
    with pytest.raises(RecordValidationError):
        t.commit_batch("bad-3", [{"id": "also-bad"}])

    t2 = Table(table_dir, SCHEMA)
    s1 = t2.batch_status("bad-1")
    s3 = t2.batch_status("bad-3")
    assert s1.status == "rejected" and s1.reason == "record_validation"
    assert s3.status == "rejected" and s3.reason == "record_validation"
    assert s1.detail and s3.detail
    log.info(
        "拒绝对账: batch=bad-1 原因=%s; batch=bad-3 原因=%s 判定依据=半截拒绝行已被截断",
        s1.reason,
        s3.reason,
    )
