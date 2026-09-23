"""本地存储：表数据文件与元数据日志的落盘布局。

布局：
    <root>/metadata/log.jsonl      提交日志（每行一个版本条目，含批次标识与结构）
    <root>/data/v<version>.jsonl   该版本全量快照数据（每行一条记录）

一致性手段：
- 所有文件先写临时文件再 os.replace 原子改名，避免半截文件；
- 提交日志只允许追加，读取时按行解析，容忍最后一行残缺（视为未提交）。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from .schema import Field, Schema


@dataclass(frozen=True)
class CommitEntry:
    """一条提交日志：版本、批次标识与该版本生效的结构。"""

    version: int
    batch_id: str
    schema: Schema
    record_count: int

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> "CommitEntry":
        d = json.loads(line)
        schema = Schema(fields=tuple(Field(**f) for f in d["schema"]["fields"]))
        return CommitEntry(
            version=d["version"],
            batch_id=d["batch_id"],
            schema=schema,
            record_count=d["record_count"],
        )


class LocalStorage:
    """以本地目录承载一张表的数据与元数据。"""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.data_dir = self.root / "data"
        self.log_path = self.root / "metadata" / "log.jsonl"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.touch(exist_ok=True)

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
                except (json.JSONDecodeError, KeyError, TypeError):
                    break  # 残缺行即未生效的提交
        return entries

    def append_log(self, entry: CommitEntry) -> None:
        with open(self.log_path, "a", encoding="utf-8") as f:
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
