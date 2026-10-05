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
- 持排他锁提交前先修复日志尾部：崩溃残留的残缺行会被截断清除，
  之后所有提交都会被真正追加到有效日志末尾，杜绝"接口返回新版本但
  重开表读不到、后续提交持续被吞"；
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
    # 本批次自己带来的记录数（区别于 record_count——那是该版本全量行数）。
    # 旧格式日志行没有该字段时取 None，由上层兜底推算。
    batch_record_count: int | None = None

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
            batch_record_count=d.get("batch_record_count"),
        )


@dataclass(frozen=True)
class RejectionEntry:
    """一条拒绝日志：批次号、可程序识别的原因与人类可读判定依据。"""

    batch_id: str
    reason: str
    detail: str
    # 冲突拒绝时：该批次已经落地的版本号，便于对账定位。
    committed_version: int | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> RejectionEntry:
        d = json.loads(line)
        return RejectionEntry(
            batch_id=d["batch_id"],
            reason=d["reason"],
            detail=d["detail"],
            committed_version=d.get("committed_version"),
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

    def recover_logs(self) -> None:
        """修复崩溃残留：截断提交/拒绝日志末尾的残缺行，并清理孤儿临时文件。

        必须在持有表级排他锁时调用：此时没有任何其他进程/线程能并发追加，
        因此"最后一个换行符之后的任何内容"只可能来自崩溃进程写到一半的
        残留，截断它不会误伤任何进行中的提交。修复后后续追加都落在有效
        日志末尾，已提交版本不会被遮蔽。
        """
        self._truncate_trailing_garbage(self.log_path)
        self._truncate_trailing_garbage(self.rejections_path)
        # 清理崩溃进程留在数据目录里的临时文件（正常路径已自行清理）。
        for p in self.data_dir.glob(".tmp-*"):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _truncate_trailing_garbage(path: Path) -> None:
        """把文件截断到最后一个完整行（以 \\n 结尾）的末尾。"""
        if not path.exists() or path.stat().st_size == 0:
            return
        with open(path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = min(size, 1 << 16)
            f.seek(size - block)
            tail = f.read()
            idx = tail.rfind(b"\n")
            if idx == -1:
                f.seek(0)
                f.truncate()
            elif idx + 1 < len(tail):
                f.seek(size - block + idx + 1)
                f.truncate()

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
            self._fsync_dir(self.data_dir)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        """让目录项变更（os.replace 改名）落盘，崩溃后不留歧义。"""
        try:
            fd = os.open(str(path), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

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
