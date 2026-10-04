"""多进程多写入端：跨进程互斥、同批次并发重试、崩溃后重试不多不少。

日志中可看到每个批次的编号、落定版本与判定依据。
"""

import logging
import multiprocessing as mp
import os
import sys

import pytest

from lakehouse import BatchConflictError, DuplicateBatchError, Field, Schema, Table

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"),))


def _write_distinct(root: str, worker: int, count: int) -> None:
    t = Table(root, SCHEMA)
    for i in range(count):
        v = t.commit_batch(f"w{worker}-b{i}", [{"id": worker * 10000 + i}])
        print(f"[worker {os.getpid()}] batch=w{worker}-b{i} version={v} 判定依据=独占锁内顺序追加", flush=True)


def _write_same_batch(root: str, batch_id: str, value: int, q: mp.Queue) -> None:
    t = Table(root, SCHEMA)
    try:
        v = t.commit_batch(batch_id, [{"id": value}])
        q.put(("committed", v))
        print(f"[worker {os.getpid()}] batch={batch_id} version={v} 判定依据=抢到锁后首次提交", flush=True)
    except DuplicateBatchError as e:
        q.put(("duplicate", e.committed_version))
        print(f"[worker {os.getpid()}] batch={batch_id} committed_version={e.committed_version} "
              f"判定依据=指纹一致幂等重试", flush=True)
    except BatchConflictError as e:  # pragma: no cover - 内容一致时不该走到
        q.put(("conflict", e.committed_version))


def _write_and_die(root: str, batch_id: str) -> None:
    """写完版本数据文件后、进程被杀死前尽量逼近提交点。"""
    t = Table(root, SCHEMA)
    # 直接走正常 API 再 os._exit：覆盖“提交后客户端未收到响应”的真实重试形态。
    v = t.commit_batch(batch_id, [{"id": 42}])
    print(f"[dying worker {os.getpid()}] batch={batch_id} version={v} 提交成功后立刻被杀", flush=True)
    sys.stdout.flush()
    os._exit(1)


@pytest.mark.skipif(sys.platform == "win32", reason="flock/fork 语义仅在 POSIX 验证")
def test_multiple_processes_distinct_batches(table_dir):
    n_workers, per_worker = 4, 12
    procs = [
        mp.get_context("fork").Process(
            target=_write_distinct, args=(str(table_dir), w, per_worker)
        )
        for w in range(n_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    assert [p.exitcode for p in procs] == [0] * n_workers

    t = Table(table_dir, SCHEMA)
    total = n_workers * per_worker
    assert t.current_version() == total
    rows = t.snapshot()
    assert len(rows) == total
    ids = {r["id"] for r in rows}
    assert len(ids) == total, "多进程写入出现覆盖/吞批次"
    assert len(t.storage.read_log()) == total
    log.info("多进程写入: 进程=%d 批次=%d 最终版本=%d 判定依据=版本连续且全量去重行数一致",
             n_workers, total, t.current_version())


@pytest.mark.skipif(sys.platform == "win32", reason="flock/fork 语义仅在 POSIX 验证")
def test_same_batch_concurrent_retry_across_processes(table_dir):
    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    batch_id = "hot-batch-1"
    procs = [
        ctx.Process(
            target=_write_same_batch, args=(str(table_dir), batch_id, 7, q)
        )
        for _ in range(6)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    results = [q.get() for _ in procs]

    committed = [r for r in results if r[0] == "committed"]
    duplicates = [r for r in results if r[0] == "duplicate"]
    assert len(committed) == 1, f"同一批次必须只落一次，实际: {results}"
    assert len(duplicates) == len(procs) - 1
    version = committed[0][1]
    assert all(r[1] == version for r in duplicates), "幂等重试必须指向同一版本"

    t = Table(table_dir, SCHEMA)
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 7}]
    status = t.batch_status(batch_id)
    assert status.state == "committed"
    assert status.version == version
    assert status.batch_size == 1
    log.info("同批次跨进程重试: batch=%s 唯一版本=%d 首次=1 幂等重试=%d 判定依据=指纹一致",
             batch_id, version, len(duplicates))


@pytest.mark.skipif(sys.platform == "win32", reason="flock/fork 语义仅在 POSIX 验证")
def test_retry_after_worker_death_no_dup_no_loss(table_dir):
    ctx = mp.get_context("fork")
    p = ctx.Process(target=_write_and_die, args=(str(table_dir), "crash-batch"))
    p.start()
    p.join()
    assert p.exitcode != 0, "子进程确实以非零码退出"

    t = Table(table_dir, SCHEMA)
    if t.current_version() == 1:
        # 提交点已落盘但响应丢失：重试必须幂等，不新增版本、不重复数据。
        with pytest.raises(DuplicateBatchError) as exc:
            t.commit_batch("crash-batch", [{"id": 42}])
        assert exc.value.committed_version == 1
        log.info("崩溃后重试(提交已落盘): batch=crash-batch 判定依据=指纹一致幂等, 版本仍=1")
    else:
        # 进程在提交点前死亡：重试正常补提，数据不丢失。
        v = t.commit_batch("crash-batch", [{"id": 42}])
        assert v == 1
        log.info("崩溃后重试(提交未落盘): batch=crash-batch version=1 判定依据=残缺尾行截断后补提")

    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 42}]
    log.info("崩溃恢复校验: batch=crash-batch 版本=%d 行数=%d 判定依据=不多不少",
             t.current_version(), len(t.snapshot()))
