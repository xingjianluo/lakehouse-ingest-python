"""并发读写一致性测试。"""

from __future__ import annotations

import threading

import pytest

from lakehouse.errors import BatchConflictError
from lakehouse.table import LakehouseTable

SCHEMA = {"columns": [{"name": "id", "type": "long", "nullable": False}]}


@pytest.fixture
def table(tmp_data_root):
    t = LakehouseTable(tmp_data_root, "events")
    t.commit("seed", [{"id": 0}], SCHEMA)
    return t


def test_concurrent_commits_are_serialized_without_gaps_or_duplicates(table):
    n_threads, per_thread = 8, 25
    errors: list[Exception] = []

    def worker(tid: int) -> None:
        try:
            for i in range(per_thread):
                idx = tid * per_thread + i + 1
                table.commit(f"t{tid}-{i}", [{"id": idx}], SCHEMA)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(tid,)) for tid in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert errors == []
    assert table.current_version() == 1 + n_threads * per_thread
    ids = [r["id"] for r in table.read()["records"]]
    assert len(ids) == len(set(ids)) == 1 + n_threads * per_thread
    # 版本号严格连续
    assert [c["version"] for c in table.history()] == list(
        range(1, table.current_version() + 1)
    )


def test_concurrent_duplicate_batch_retries_commit_exactly_once(table):
    """同一批次被多个线程同时重试：只生效一次，其余全部返回同一版本。"""
    versions: list[int] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(6)

    def worker() -> None:
        try:
            barrier.wait()
            e = table.commit("dup-batch", [{"id": 100}, {"id": 101}], SCHEMA)
            versions.append(e.version)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert errors == []
    assert set(versions) == {2}  # 全部拿到同一个版本号
    ids = [r["id"] for r in table.read(2)["records"]]
    assert ids.count(100) == 1 and ids.count(101) == 1


def test_same_batch_concurrent_conflicting_payload_never_partially_applied(table):
    outcomes: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker(payload_id: int) -> None:
        try:
            barrier.wait()
            table.commit("race-batch", [{"id": payload_id}], SCHEMA)
            with lock:
                outcomes.append("accepted")
        except BatchConflictError:
            with lock:
                outcomes.append("conflict")

    threads = [threading.Thread(target=worker, args=(pid,)) for pid in (200, 201)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert sorted(outcomes) == ["accepted", "conflict"]
    assert table.current_version() == 2
    assert len(table.read(2)["records"]) == 2  # seed + 唯一胜出记录


def test_concurrent_readers_always_observe_consistent_commit_points(table):
    """写入期间读者只能看到完整提交点，快照行数与 record_count 始终一致。"""
    stop = threading.Event()
    violations: list[str] = []

    def reader() -> None:
        while not stop.is_set():
            snap = table.read()
            if len(snap["records"]) != next(
                c["record_count"] for c in table.history() if c["version"] == snap["version"]
            ):
                violations.append(
                    f"version={snap['version']} rows={len(snap['records'])}"
                )
            # 同一版本反复读必须完全一致
            again = table.read(snap["version"])
            if again["records"] != snap["records"]:
                violations.append("non-reproducible snapshot")

    rthreads = [threading.Thread(target=reader) for _ in range(4)]
    for th in rthreads:
        th.start()

    for i in range(1, 61):
        table.commit(f"w{i}", [{"id": i}], SCHEMA)

    stop.set()
    for th in rthreads:
        th.join()
    assert violations == []
    assert table.current_version() == 61


def test_optimistic_base_version_conflict_detected(table):
    from lakehouse.table import ConcurrentCommitError

    stale_version = table.current_version()  # 1
    table.commit("other", [{"id": 9}], SCHEMA)  # 其他人先提交 -> 2
    with pytest.raises(ConcurrentCommitError) as ei:
        table.commit("late", [{"id": 10}], SCHEMA, base_version=stale_version)
    assert ei.value.reason == "concurrent_modification"
    assert table.current_version() == 2


def test_cross_process_commits_are_serialized(tmp_data_root):
    """多进程并发提交：flock 保证版本连续、数据无重复。"""
    import multiprocessing as mp

    t0 = LakehouseTable(tmp_data_root, "events")
    t0.commit("seed", [{"id": 0}], SCHEMA)

    n_proc, per_proc = 4, 10

    def proc_worker(root: str, pid: int) -> None:
        tbl = LakehouseTable(root, "events")
        for i in range(per_proc):
            idx = pid * per_proc + i + 1
            tbl.commit(f"p{pid}-{i}", [{"id": idx}], SCHEMA)

    ctx = mp.get_context("fork")
    procs = [
        ctx.Process(target=proc_worker, args=(tmp_data_root, pid))
        for pid in range(n_proc)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
        assert p.exitcode == 0

    final = LakehouseTable(tmp_data_root, "events")
    assert final.current_version() == 1 + n_proc * per_proc
    ids = [r["id"] for r in final.read()["records"]]
    assert len(ids) == len(set(ids))
