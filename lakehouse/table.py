"""表级 API：批次提交、结构演进、快照读取与时间旅行。

一致性设计：
- 提交点 = 元数据日志追加成功的那一刻；版本数据文件先于日志落盘，
  因此任何已提交的版本其数据文件必然完整（整体生效或整体不生效）；
- 批次标识唯一：重复提交同一 batch_id 抛出 DuplicateBatchError，
  数据与元数据保持不变（幂等安全）；
- 快照读取先在锁内确定提交点（版本条目），再读数据文件，
  并发写入期间读取到的始终是某个一致提交点。
"""

from __future__ import annotations

import threading
from pathlib import Path

from .errors import (
    DuplicateBatchError,
    RecordValidationError,
    VersionExpiredError,
    VersionNotFoundError,
)
from .schema import Field, Schema, check_compatibility
from .storage import CommitEntry, LocalStorage

_TYPE_CHECK = {
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "float": lambda v: (isinstance(v, (int, float)) and not isinstance(v, bool)),
    "string": lambda v: isinstance(v, str),
    "bool": lambda v: isinstance(v, bool),
}


class Table:
    """一张本地湖仓表，线程安全。"""

    def __init__(self, root: Path, schema: Schema | None = None) -> None:
        self.storage = LocalStorage(root)
        self._lock = threading.RLock()
        self._entries: list[CommitEntry] = self.storage.read_log()
        self._schema: Schema = (
            self._entries[-1].schema if self._entries else (schema or Schema())
        )

    # ---- 提交 ----

    def commit_batch(
        self,
        batch_id: str,
        records: list[dict],
        schema: Schema | None = None,
    ) -> int:
        """提交一个批次，返回新版本号。

        - batch_id 已提交过：抛 DuplicateBatchError（幂等，无副作用）；
        - schema 与当前结构不兼容：抛 IncompatibleSchemaError，不产生任何写入；
        - 记录与结构不匹配：抛 RecordValidationError，不产生任何写入。
        """
        with self._lock:
            for e in self._entries:
                if e.batch_id == batch_id:
                    raise DuplicateBatchError(batch_id, e.version)

            target_schema = self._schema
            if schema is not None:
                # 校验演进兼容性；兼容则得到合并后的结构。
                target_schema = check_compatibility(self._schema, schema)

            normalized = [self._normalize_record(r, target_schema) for r in records]

            version = self._entries[-1].version + 1 if self._entries else 1
            # 全量快照 = 历史数据（按新结构补缺省值） + 本批记录。
            snapshot_rows = []
            if self._entries:
                old = self.storage.read_version_data(self._entries[-1].version)
                snapshot_rows = [self._apply_defaults(r, target_schema) for r in old]
            snapshot_rows.extend(normalized)

            # 先写数据文件，再追加日志（提交点）。
            self.storage.write_version_data(version, snapshot_rows)
            entry = CommitEntry(version, batch_id, target_schema, len(snapshot_rows))
            self.storage.append_log(entry)
            self._entries.append(entry)
            self._schema = target_schema
            return version

    # ---- 读取 ----

    def current_version(self) -> int:
        """当前最新提交版本号，无提交时为 0。"""
        with self._lock:
            return self._entries[-1].version if self._entries else 0

    def schema(self) -> Schema:
        with self._lock:
            return self._schema

    def snapshot(self, version: int | None = None) -> list[dict]:
        """读取指定版本快照；version 为 None 时读最新版本。

        版本从未存在抛 VersionNotFoundError；已过期被清理抛
        VersionExpiredError。历史快照结果在后续写入后仍可复现。
        """
        with self._lock:
            if not self._entries:
                raise VersionNotFoundError(version if version is not None else 0)
            v = version if version is not None else self._entries[-1].version
            if v < 1 or v > self._entries[-1].version:
                raise VersionNotFoundError(v)
            entry = self._entries[v - 1]
        if not self.storage.version_path(v).exists():
            raise VersionExpiredError(v, self.min_available_version())
        return self.storage.read_version_data(v)

    def min_available_version(self) -> int:
        """当前仍可读取的最早版本。"""
        with self._lock:
            for e in self._entries:
                if self.storage.version_path(e.version).exists():
                    return e.version
            return self._entries[-1].version if self._entries else 0

    # ---- 版本清理 ----

    def expire_versions(self, keep_last: int) -> list[int]:
        """仅保留最近 keep_last 个版本的数据文件，返回被清理的版本号。

        元数据日志保留，过期版本读取时抛 VersionExpiredError。
        """
        with self._lock:
            if keep_last < 1:
                raise ValueError("keep_last 必须 >= 1")
            current = self._entries[-1].version if self._entries else 0
            cutoff = current - keep_last
            removed = []
            for e in self._entries:
                if e.version <= cutoff and self.storage.version_path(e.version).exists():
                    self.storage.remove_version_data(e.version)
                    removed.append(e.version)
            return removed

    # ---- 内部 ----

    @staticmethod
    def _apply_defaults(record: dict, schema: Schema) -> dict:
        """为记录补齐结构中缺失字段的缺省值（对历史数据稳定生效）。"""
        out = dict(record)
        for f in schema.fields:
            out.setdefault(f.name, f.default)
        return out

    @staticmethod
    def _normalize_record(record: dict, schema: Schema) -> dict:
        fields: dict[str, Field] = schema.field_map()
        unknown = set(record) - set(fields)
        if unknown:
            raise RecordValidationError(
                f"记录包含结构中不存在的字段 {sorted(unknown)}"
            )
        out = dict(record)
        for name, f in fields.items():
            if name in out:
                v = out[name]
                if v is not None and not _TYPE_CHECK[f.type](v):
                    raise RecordValidationError(
                        f"字段 {name!r} 期望类型 {f.type!r}，实际值 {v!r}"
                    )
            else:
                out[name] = f.default
        return out
