"""崩溃残留恢复：半截提交记录不得遮蔽后续提交，重开表必须读到完整新版本。

模拟写入端在"日志追加到一半"时进程死亡：log.jsonl 末尾留下没有换行的
残缺行。要求：残留被持排他锁的下一次提交修复，之后每一次成功提交在
重新打开表后都读得到，数据不多不少，且后续提交不会被持续吞掉。
"""

import logging
import multiprocessing as mp
import os
from pathlib import Path

from lakehouse import Field, Schema, Table

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"),))


def _append_partial_log_line(root: str) -> None:
    """写入一段没有换行、无法解析的半截提交记录（模拟崩溃前的最后动作）。"""
    logp = Path(root) / "metadata" / "log.jsonl"
    with open(logp, "a", encoding="utf-8") as f:
        f.write('{"version": 99, "batch_id": "dead", "schema": {"fields": [{"name"')
        f.flush()
        os.fsync(f.fileno())


def _crash_worker(root: str) -> None:
    """子进程：写半截日志行（已 fsync）后立即非正常死亡。"""
    _append_partial_log_line(root)
    os._exit(0)


def _commit_worker(root: str, batch_id: str, ids: list[int], queue) -> None:
    t = Table(Path(root))
    results = []
    for i in ids:
        v = t.commit_batch(f"{batch_id}-{i}", [{"id": i}])
        results.append(v)
    queue.put((os.getpid(), results))


def test_partial_log_line_does_not_swallow_following_commits(table_dir):
    t = Table(table_dir, SCHEMA)
    t.commit_batch("b1", [{"id": 1}])

    # 进程在追加 v2 日志时死亡：末尾留下半截行（数据文件是孤儿也无所谓）。
    t.storage.write_version_data(2, [{"id": 999}])  # 孤儿数据文件
    _append_partial_log_line(str(table_dir))

    t2 = Table(table_dir)  # 重新打开表（模拟进程重启）
    v2 = t2.commit_batch("b2", [{"id": 2}])
    log.info("残留后首次提交: batch=b2 接口返回 version=%d", v2)
    assert v2 == 2

    # 接口成功返回的版本，重新打开表一定读得到，数据不多不少。
    t3 = Table(table_dir)
    assert t3.current_version() == 2
    assert t3.snapshot() == [{"id": 1}, {"id": 2}]
    log.info(
        "重开表校验: version=%d 行数=%d 判定依据=残缺日志行已在持锁提交时截断",
        t3.current_version(),
        len(t3.snapshot()),
    )

    # 往后每次提交都不能再被吞。
    v3 = t3.commit_batch("b3", [{"id": 3}])
    v4 = t3.commit_batch("b4", [{"id": 4}])
    assert (v3, v4) == (3, 4)
    t4 = Table(table_dir)
    assert t4.current_version() == 4
    assert t4.snapshot() == [{"id": i} for i in range(1, 5)]
    assert len(t4.storage.read_log()) == 4
    log.info(
        "连续提交校验: 重开表 version=%d 行数=%d 日志条目=4 无丢失无重复",
        t4.current_version(),
        len(t4.snapshot()),
    )


def test_recovery_cleans_orphan_tmp_files(table_dir):
    """崩溃残留的临时数据文件在下一次持锁提交时被清理。"""
    t = Table(table_dir, SCHEMA)
    t.commit_batch("b1", [{"id": 1}])
    tmp = table_dir / "data" / ".tmp-deadbeef"
    tmp.write_text("partial", encoding="utf-8")
    t.commit_batch("b2", [{"id": 2}])
    assert not tmp.exists()
    assert sorted(p.name for p in (table_dir / "data").iterdir()) == ["v1.jsonl", "v2.jsonl"]
    log.info("临时文件清理: data/ 下只剩已提交版本数据文件")


def test_partial_rejections_line_does_not_block_commits(table_dir):
    """拒绝日志末尾的残缺行同样被修复，不影响提交与对账。"""
    t = Table(table_dir, SCHEMA)
    t.commit_batch("b1", [{"id": 1}])
    rp = table_dir / "metadata" / "rejections.jsonl"
    with open(rp, "a", encoding="utf-8") as f:
        f.write('{"batch_id": "dead", "reas')  # 半截拒绝记录
    v2 = t.commit_batch("b2", [{"id": 2}])
    assert v2 == 2
    t3 = Table(table_dir)
    assert t3.current_version() == 2
    log.info("拒绝日志残缺: 后续提交 version=%d 正常落地且可重开读到", v2)


def test_multi_process_crash_then_retry_everything_durable(table_dir):
    """多进程场景：一个进程写到一半死亡，其余进程提交 + 同批次重试均持久。"""
    t = Table(table_dir, SCHEMA)
    t.commit_batch("b0", [{"id": 0}])

    ctx = mp.get_context("fork")

    # 1) 一个进程制造半截日志行（fsync 后）立即非正常死亡。
    crasher = ctx.Process(target=_crash_worker, args=(str(table_dir),))
    crasher.start()
    crasher.join(timeout=30)
    assert crasher.exitcode == 0
    # 残缺行确实留在了日志里：此时读取只能看到崩溃前的 1 个版本。
    assert Table(table_dir).current_version() == 1

    # 2) 两个存活进程分别提交不同批次（持锁提交时顺带修复残留）。
    queue = ctx.Queue()
    workers = [
        ctx.Process(
            target=_commit_worker,
            args=(str(table_dir), "wa", [10, 11], queue),
        ),
        ctx.Process(
            target=_commit_worker,
            args=(str(table_dir), "wb", [20, 21, 22], queue),
        ),
    ]
    for p in workers:
        p.start()
    got = [queue.get(timeout=60) for _ in workers]
    for p in workers:
        p.join(timeout=60)
        assert p.exitcode == 0, f"存活进程异常退出 exitcode={p.exitcode}"
    committed = sorted(v for _, vs in got for v in vs)
    log.info("崩溃后存活进程提交版本: %s 判定依据=flock 串行 + 持锁修复日志", committed)
    assert committed == [2, 3, 4, 5, 6]

    # 3) 死亡进程那批用新批次号重交：得到下一个新版本且重开表读得到。
    retryer = ctx.Process(
        target=_commit_worker, args=(str(table_dir), "retry-dead", [99], queue)
    )
    retryer.start()
    rv = queue.get(timeout=60)[1][0]
    retryer.join(timeout=60)
    assert retryer.exitcode == 0
    assert rv == 7

    final = Table(table_dir)
    assert final.current_version() == 7
    rows = final.snapshot()
    ids = sorted(r["id"] for r in rows)
    assert ids == [0, 10, 11, 20, 21, 22, 99]
    assert len(final.storage.read_log()) == 7
    log.info(
        "多进程崩溃恢复校验: 重开表 version=%d 行 id=%s 日志条目=7",
        final.current_version(),
        ids,
    )
