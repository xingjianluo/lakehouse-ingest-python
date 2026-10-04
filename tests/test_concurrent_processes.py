"""多进程并发：多个终端进程同时写同一张表，版本唯一、批次重试安全。"""

import logging
import multiprocessing as mp
from pathlib import Path

from lakehouse import Field, Schema, Table

log = logging.getLogger("lakehouse-test")

SCHEMA_SPEC = (("id", "int", None),)
SCHEMA = Schema(tuple(Field(*f) for f in SCHEMA_SPEC))


def _worker(root: str, batches: list[tuple[str, list[dict]]], queue) -> None:
    """子进程入口：独立打开同一张表，逐个提交批次并回报结果。"""
    from lakehouse import BatchConflictError

    t = Table(Path(root), SCHEMA)
    results = []
    for batch_id, records in batches:
        try:
            v = t.commit_batch(batch_id, records)
            results.append(("ok", batch_id, v))
        except BatchConflictError as e:
            results.append(("conflict", batch_id, e.committed_version))
    queue.put(results)


def _run_processes(table_dir, jobs: list[list[tuple[str, list[dict]]]]) -> list:
    ctx = mp.get_context("fork")
    queue = ctx.Queue()
    procs = [
        ctx.Process(target=_worker, args=(str(table_dir), job, queue)) for job in jobs
    ]
    for p in procs:
        p.start()
    results = [queue.get(timeout=60) for _ in procs]
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0, f"子进程异常退出 exitcode={p.exitcode}"
    return results


def test_multi_process_distinct_batches(table_dir):
    """4 进程 × 5 个不同批次：20 个互不相同的版本，无覆盖无丢失。"""
    jobs = [[(f"p{p}-b{i}", [{"id": p * 100 + i}]) for i in range(5)] for p in range(4)]
    results = _run_processes(table_dir, jobs)

    versions = sorted(
        v for res in results for (outcome, _, v) in res if outcome == "ok"
    )
    assert all(outcome == "ok" for res in results for (outcome, _, _) in res)
    assert versions == list(range(1, 21)), "每次成功提交必须落在独立版本上"

    t = Table(table_dir, SCHEMA)
    assert len(t.storage.read_log()) == 20
    rows = t.snapshot()
    assert len(rows) == 20
    assert len({r["id"] for r in rows}) == 20
    log.info(
        "多进程提交: 进程数=4 版本=%d..%d 连续无冲突 判定依据=版本号唯一且日志条目=20",
        versions[0],
        versions[-1],
    )


def test_multi_process_identical_retry_single_commit(table_dir):
    """6 个进程同时重试同一批次（内容一致）：最多一个真写，其余拿到一致结果。"""
    records = [{"id": 42}]
    jobs = [[("shared-batch", records)] for _ in range(6)]
    results = _run_processes(table_dir, jobs)

    versions = {v for res in results for (outcome, _, v) in res}
    assert all(outcome == "ok" for res in results for (outcome, _, _) in res)
    assert len(versions) == 1, "所有重试者必须拿到同一个首次提交的版本号"

    t = Table(table_dir, SCHEMA)
    assert len(t.storage.read_log()) == 1, "同号同内容只允许一个真正写进去"
    assert t.snapshot() == records
    log.info(
        "多进程同号重试: 进程数=6 全部返回 version=%d 日志条目=1 判定依据=内容指纹一致",
        versions.pop(),
    )


def test_multi_process_conflicting_retry_exactly_one_wins(table_dir):
    """同号不同内容并发：恰好一个批次落地，其余拿到明确冲突失败。"""
    jobs = [[("race-batch", [{"id": k}])] for k in range(5)]
    results = _run_processes(table_dir, jobs)
    flat = [item for res in results for item in res]

    oks = [v for (outcome, _, v) in flat if outcome == "ok"]
    conflicts = [v for (outcome, _, v) in flat if outcome == "conflict"]
    assert len(set(oks)) == 1, "成功/一致重试只能对应同一个版本"
    assert len(oks) + len(conflicts) == 5
    assert len(conflicts) >= 1, "内容不一致的重试必须被明确拒绝"
    assert all(v == oks[0] for v in conflicts), "冲突失败必须携带已提交的版本号"

    t = Table(table_dir, SCHEMA)
    assert len(t.storage.read_log()) == 1
    rows = t.snapshot()
    assert len(rows) == 1 and rows[0]["id"] in range(5), "落地内容必须是某个完整批次"
    log.info(
        "多进程同号冲突: 落地版本=%d 冲突拒绝=%d 次 判定依据=指纹不一致即失败",
        oks[0],
        len(conflicts),
    )
