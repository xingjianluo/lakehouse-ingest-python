"""本地存储：表数据文件、元数据日志、被拒台账与跨进程文件锁。

布局：
    <root>/metadata/lock           跨进程互斥锁（fcntl.flock 劝告锁）
    <root>/metadata/log.jsonl      提交日志（每行一个版本条目）
    <root>/metadata/rejected.jsonl 被拒批次台账（冲突 / 结构 / 记录校验）
    <root>/data/v<version>.jsonl   该版本全量快照数据（每行一条记录）

一致性手段：
- 所有数据文件先写临时文件再 os.replace 原子改名，避免半截文件；
- 提交日志只允许追加；读取时容忍尾部残缺行（崩溃残留视为未提交），
  持锁恢复时会把残缺尾行截掉，防止它挡住后续正常提交；
- 同一进程内的线程之间用 Table 的 RLock 串行化，
  不同进程之间用 metadata/lock 上的 flock 串行化；
- 被拒批次写 rejected.jsonl 台账，不占版本、不污染数据与提交日志。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from pathlib import Path

from .schema import Field, Schema


@dataclass(frozen=True)
class CommitEntry:
    """一条提交日志：版本、批次标识、结构、行数与内容指纹。"""

    version: int
    batch_id: str
    schema: Schema
    record_count: int       # 该版本全量快照行数
    batch_size: int = 0     # 本批次自带的记录条数
    fingerprint: str = ""   # 本批次归一化内容指纹，用于重试对账

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> CommitEntry:
        d = json.loads(line)
        schema = Schema(fields=tuple(Field(**f) for f in d["schema"]["fields"]))
        return CommitEntry(
            version=d["version"],
            batch_id=d["batch_id"],
            schema=schema,
            record_count=d["record_count"],
            batch_size=d.get("batch_size", 0),
            fingerprint=d.get("fingerprint", ""),
        )


@dataclass(frozen=True)
class RejectedRecord:
    """被拒批次台账中的一条：被拒也能按批次号查到原因。"""

    batch_id: str
    reason: str  # batch_conflict | incompatible_schema | record_validation
    detail: str
    rejected_at: str
    schema_reason: str | None = None
    field: str | None = None
    committed_version: int | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> RejectedRecord:
        d = json.loads(line)
        return RejectedRecord(
            batch_id=d["batch_id"],
            reason=d["reason"],
            detail=d["detail"],
            rejected_at=d["rejected_at"],
            schema_reason=d.get("schema_reason"),
            field=d.get("field"),
            committed_version=d.get("committed_version"),
        )


class LocalStorage:
    """以本地目录承载一张表的数据与元数据。"""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.data_dir = self.root / "data"
        self.metadata_dir = self.root / "metadata"
        self.log_path = self.metadata_dir / "log.jsonl"
        self.rejected_path = self.metadata_dir / "rejected.jsonl"
        self.lock_path = self.metadata_dir / "lock"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.touch(exist_ok=True)
        self.rejected_path.touch(exist_ok=True)
        self.lock_path.touch(exist_ok=True)

    # ---- 跨进程 / 跨线程锁 ----

    @contextlib.contextmanager
    def locked(self, exclusive: bool = True) -> Iterator[None]:
        """串行化对同一张表的提交与恢复。

        进程内用 RLock 防自递归/同进程多线程，进程间用 flock 互斥。
        调用方持锁期间不得再做耗时的外部操作；锁内只做校验与落盘。
        """
        with open(self.lock_path, "a+") as lf:
            fcntl.flock(
                lf.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            )
            try:
                yield
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    # ---- 元数据日志 ----

    def read_log(self) -> list[CommitEntry]:
        """读取全部提交日志；容忍尾部残缺行（崩溃残留），视为未提交。"""
        entries: list[CommitEntry] = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(CommitEntry.from_json(line))
                except (json.JSONDecodeError, KeyError, TypeError):
                    break  # 残缺行即未生效的提交
        return entries

    def recover_log(self) -> list[CommitEntry]:
        """持写锁时调用：读日志并截掉崩溃残留的残缺尾行。

        返回截至残缺点之前的全部有效条目。截断只删未提交的残留，
        不影响任何已提交版本。
        """
        data = self.log_path.read_bytes()
        entries: list[CommitEntry] = []
        good = 0
        for raw in data.splitlines(keepends=True):
            try:
                entries.append(CommitEntry.from_json(raw.decode("utf-8")))
            except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError):
                break
            good += len(raw)
        if good < len(data):
            with open(self.log_path, "r+b") as f:
                f.truncate(good)
                f.flush()
                os.fsync(f.fileno())
        return entries

    def append_log(self, entry: CommitEntry) -> None:
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(entry.to_json() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ---- 被拒批次台账 ----

    def append_rejected(self, record: RejectedRecord) -> None:
        with open(self.rejected_path, "a", encoding="utf-8") as f:
            f.write(record.to_json() + "\n")
            f.flush()
            os.fsync(f.fileno())

    def read_rejected(self, batch_id: str | None = None) -> list[RejectedRecord]:
        """读取被拒台账；可按批次号过滤，返回按时间顺序的记录。"""
        out: list[RejectedRecord] = []
        with open(self.rejected_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = RejectedRecord.from_json(line)
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
                if batch_id is None or rec.batch_id == batch_id:
                    out.append(rec)
        return out

    # ---- 版本数据文件 ----

    def version_path(self, version: int) -> Path:
        return self.data_dir / f"v{version}.jsonl"

    def write_version_data(self, version: int, records: list[dict]) -> None:
        """原子写入某版本的全量快照数据。"""
        target = self.version_path(version)
        fd, tmp = tempfile.mkstemp(dir=self.data_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def read_version_data(self, version: int) -> list[dict]:
        records: list[dict] = []
        with open(self.version_path(version), "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def remove_version_data(self, version: int) -> None:
        path = self.version_path(version)
        if path.exists():
            path.unlink()
