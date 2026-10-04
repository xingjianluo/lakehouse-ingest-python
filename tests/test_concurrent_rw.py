"""并发读写：写入期间的快照查询必须看到一致的提交点，结果可复现。"""

import logging
import threading

from lakehouse import Field, Schema, Table

log = logging.getLogger("lakehouse-test")

SCHEMA = Schema((Field("id", "int"),))
N_BATCHES = 30
BATCH_SIZE = 10


def _populate(t: Table, n: int) -> None:
    for i in range(n):
        t.commit_batch(f"seed-{i}", [{"id": i}])


def test_concurrent_reads_during_writes(table_dir):
    t = Table(table_dir, SCHEMA)
    _populate(t, 5)
    errors: list[Exception] = []
    stop = threading.Event()
    snapshots_seen: list[tuple[int, int]] = []  # (version, row_count)

    def writer() -> None:
        for i in range(N_BATCHES):
            try:
                v = t.commit_batch(
                    f"w-{i}",
                    [{"id": 1000 + i * BATCH_SIZE + j} for j in range(BATCH_SIZE)],
                )
                log.info("写入提交: version=%d batch=w-%d", v, i)
            except Exception as e:  # pragma: no cover
                errors.append(e)

    def reader() -> None:
        while not stop.is_set():
            try:
                v = t.current_version()
                rows = t.snapshot(v)
                # 一致性判定：读到的行数必须等于该版本提交时的记录数，
                # 且所有 id 严格递增无空洞（不得读到半个批次）。
                assert (
                    len(rows) == 5 + (v - 5) * BATCH_SIZE or v <= 5 and len(rows) == v
                )
                ids = [r["id"] for r in rows]
                assert ids == sorted(ids), "快照内出现乱序/半批次数据"
                snapshots_seen.append((v, len(rows)))
            except Exception as e:  # pragma: no cover
                errors.append(e)

    w = threading.Thread(target=writer)
    readers = [threading.Thread(target=reader) for _ in range(4)]
    w.start()
    for r in readers:
        r.start()
    w.join()
    stop.set()
    for r in readers:
        r.join()

    assert not errors, f"并发期间出现错误: {errors[:3]}"
    assert snapshots_seen, "读者未获得任何快照"
    log.info(
        "并发期间读到 %d 个一致快照, 示例: %s", len(snapshots_seen), snapshots_seen[:3]
    )

    # 写完后最终版本数据完整：无重复、无丢失。
    final = t.snapshot()
    assert len(final) == 5 + N_BATCHES * BATCH_SIZE
    assert len({r["id"] for r in final}) == len(final)
    log.info(
        "最终校验: version=%d 行数=%d 判定依据=全量去重后行数一致",
        t.current_version(),
        len(final),
    )


def test_concurrent_writers_unique_versions(table_dir):
    t = Table(table_dir, SCHEMA)
    errors: list[Exception] = []
    versions: list[int] = []
    lock = threading.Lock()

    def writer(tid: int) -> None:
        for i in range(10):
            try:
                v = t.commit_batch(f"t{tid}-b{i}", [{"id": tid * 100 + i}])
                with lock:
                    versions.append(v)
            except Exception as e:  # pragma: no cover
                errors.append(e)

    threads = [threading.Thread(target=writer, args=(k,)) for k in range(5)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    # 50 个批次 -> 50 个互不相同的版本号，元数据与数据一一对应。
    assert sorted(versions) == list(range(1, 51))
    assert len(t.storage.read_log()) == 50
    assert len(t.snapshot()) == 50
    log.info(
        "并发写入: 提交版本数=%d 判定依据=版本号连续无冲突 1..%d",
        len(versions),
        t.current_version(),
    )


def test_concurrent_identical_retry_single_commit(table_dir):
    """多个任务同时重试同一批次（内容一致）：只有一个真写，其余拿到一致结果。"""
    t = Table(table_dir, SCHEMA)
    records = [{"id": 7}]
    versions: list[int] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def retryer() -> None:
        for _ in range(5):
            try:
                v = t.commit_batch("hot-batch", records)
                with lock:
                    versions.append(v)
            except Exception as e:  # pragma: no cover
                errors.append(e)

    threads = [threading.Thread(target=retryer) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert set(versions) == {1}, "8 任务 × 5 次重试必须全部拿到首次提交的版本号"
    assert len(t.storage.read_log()) == 1
    assert t.snapshot() == records
    log.info("并发同号重试: 任务数=8 重试次数=40 全部返回 version=1 日志条目=1")


def test_snapshot_reproducible_after_more_writes(table_dir):
    t = Table(table_dir, SCHEMA)
    _populate(t, 3)
    before = t.snapshot(2)
    log.info("记录历史快照: version=2 行数=%d", len(before))

    _populate_more(t)
    after = t.snapshot(2)
    assert after == before, "历史快照在后续写入后必须可复现"
    log.info("复现校验: version=2 两次读取一致, 当前最新版本=%d", t.current_version())


def _populate_more(t: Table) -> None:
    for i in range(3, 8):
        t.commit_batch(f"seed-{i}", [{"id": i}])
