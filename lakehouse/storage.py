"""本地存储：表数据文件、元数据日志与批次对账日志的落盘布局。

布局：
    <root>/metadata/log.jsonl         提交日志（每行一个版本条目，含批次标识、
                                      结构与内容指纹）
    <root>/metadata/rejections.jsonl  拒绝日志（被结构/记录校验拒绝的批次，供对账）
    <root>/metadata/.lock             跨进程互斥锁文件（fcntl.flock）
    <root>/data/v<version>.jsonl      该版本全量快照数据（每行一条记录）

一致性手段：
- 所有数据文件先写临时文件再 os.replace 原子改名，避免半截文件；
- 提交日志只允许追加，读取时按行解析，容忍最后一行残缺（视为未提交）；
- 提交临界区由 fcntl.flock 排他锁保护：同一台机器上的多个进程/终端
  对同一张表的提交被串行化，且持锁期间重新读取日志，保证版本号唯一、
  批次判定基于最新已提交状态。
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

from .schema import Field, Schema


@dataclass(frozen=True)
class CommitEntry:
    """一条提交日志：版本、批次标识、该版本生效的结构与批次内容指纹。"""

    version: int
    batch_id: str
    schema: Schema
    record_count: int
    # 批次内容指纹（记录+结构的规范哈希），用于识别"同批次号不同内容"。
    # 旧格式日志行没有该字段，反序列化时缺省为 None。
    fingerprint: str | None = None

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> CommitEntry:
        d = json.loads(line)
        schema = Schema(fields=tuple(Field(**f) for f in d["schema"]["fields"]))
        return CommitEntry(
            version=d["version"],
            batch_id=d["batch_id"],
            schema=schema,
            record_count=d["record_count"],
            fingerprint=d.get("fingerprint"),
        )


@dataclass(frozen=True)
class RejectionEntry:
    """一条拒绝日志：批次号、可程序识别的原因与人类可读判定依据。"""

    batch_id: str
    reason: str
    detail: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> RejectionEntry:
        d = json.loads(line)
        return RejectionEntry(
            batch_id=d["batch_id"], reason=d["reason"], detail=d["detail"]
        )


class LocalStorage:
    """以本地目录承载一张表的数据与元数据。"""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.data_dir = self.root / "data"
        self.log_path = self.root / "metadata" / "log.jsonl"
        self.rejections_path = self.root / "metadata" / "rejections.jsonl"
        self.lock_path = self.root / "metadata" / ".lock"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.touch(exist_ok=True)
        self.lock_path.touch(exist_ok=True)

    # ---- 跨进程互斥 ----

    @contextmanager
    def locked(self, exclusive: bool = True) -> Iterator[None]:
        """对表级锁文件加 flock；exclusive=False 时加共享锁（读取用）。

        flock 按"打开文件描述"生效，进程退出/崩溃时由内核自动释放，
        不会留下死锁。进程内多线程的互斥由 Table 的线程锁先行保证。
        """
        with open(self.lock_path, "a+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    # ---- 元数据日志 ----

    def read_log(self) -> list[CommitEntry]:
        """读取全部提交日志；容忍最后一行残缺（崩溃残留），视为未提交。"""
        entries: list[CommitEntry] = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(CommitEntry.from_json(line))
                except json.JSONDecodeError, KeyError, TypeError:
                    break  # 残缺行即未生效的提交
        return entries

    def append_log(self, entry: CommitEntry) -> None:
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(entry.to_json() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ---- 拒绝日志（对账用） ----

    def read_rejections(self) -> list[RejectionEntry]:
        """读取全部拒绝日志；容忍最后一行残缺。"""
        if not self.rejections_path.exists():
            return []
        entries: list[RejectionEntry] = []
        with open(self.rejections_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(RejectionEntry.from_json(line))
                except json.JSONDecodeError, KeyError, TypeError:
                    break
        return entries

    def append_rejection(self, entry: RejectionEntry) -> None:
        with open(self.rejections_path, "a", encoding="utf-8") as f:
            f.write(entry.to_json() + "\n")
            f.flush()
            os.fsync(f.fileno())

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
