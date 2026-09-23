"""目录/元数据管理：版本提交点、批次去重、快照保留。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import LakehouseError
from .storage import FileStorage


@dataclass(frozen=True)
class CommitEntry:
    """一次成功提交对应的不可变快照版本。"""

    version: int
    batch_id: str
    schema: dict[str, Any]
    data_file: str
    record_count: int
    created_at: str  # ISO-8601 UTC
    parent_version: int | None = None
    # 批次负载（按提交结构投影后）的稳定指纹；快照过期后仍可据此做幂等判定
    fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "batch_id": self.batch_id,
            "schema": self.schema,
            "data_file": self.data_file,
            "record_count": self.record_count,
            "created_at": self.created_at,
            "parent_version": self.parent_version,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CommitEntry:
        return cls(
            version=int(d["version"]),
            batch_id=str(d["batch_id"]),
            schema=dict(d.get("schema") or {}),
            data_file=str(d["data_file"]),
            record_count=int(d.get("record_count", 0)),
            created_at=str(d["created_at"]),
            parent_version=d.get("parent_version"),
            fingerprint=d.get("fingerprint"),
        )


@dataclass
class CatalogState:
    """catalog.json 的内存模型（单写者，持锁更新）。"""

    table: str
    current_version: int = 0
    commits: list[CommitEntry] = field(default_factory=list)
    # 曾经存在、但快照数据已过期的版本号
    expired_versions: list[int] = field(default_factory=list)
    # 已提交的 batch_id -> version，用于幂等去重
    batch_index: dict[str, int] = field(default_factory=dict)

    def find(self, version: int) -> CommitEntry | None:
        for c in self.commits:
            if c.version == version:
                return c
        return None

    def is_expired(self, version: int) -> bool:
        return version in self.expired_versions


class Catalog:
    """catalog.json 的加载、原子持久化与查询。

    表目录布局::

        <root>/<table>/catalog.json
        <root>/<table>/data/v-000000000001.jsonl
        <root>/<table>/commit.lock
    """

    FILE_NAME = "catalog.json"

    def __init__(self, storage: FileStorage, table: str) -> None:
        self.storage = storage
        self.table = table

    @property
    def _rel(self) -> str:
        return f"{self.table}/{self.FILE_NAME}"

    def load(self) -> CatalogState:
        """读取 catalog；不存在则返回初始空状态。损坏时明确报错。"""
        if not self.storage.exists(self._rel):
            return CatalogState(table=self.table)
        try:
            raw = self.storage.read_json(self._rel)
        except Exception as exc:  # noqa: BLE001
            raise LakehouseError(
                f"catalog unreadable for table {self.table!r}: {exc}",
                table=self.table,
            ) from exc
        if not isinstance(raw, dict):
            raise LakehouseError(
                f"catalog for table {self.table!r} is malformed", table=self.table
            )
        commits = [CommitEntry.from_dict(c) for c in raw.get("commits", [])]
        batch_index = {c.batch_id: c.version for c in commits}
        return CatalogState(
            table=self.table,
            current_version=int(raw.get("current_version", 0)),
            commits=commits,
            expired_versions=[int(v) for v in raw.get("expired_versions", [])],
            batch_index=batch_index,
        )

    def store(self, state: CatalogState) -> None:
        """原子写回 catalog。"""
        payload = {
            "table": state.table,
            "current_version": state.current_version,
            "expired_versions": sorted(state.expired_versions),
            "commits": [c.to_dict() for c in state.commits],
        }
        self.storage.write_json(self._rel, payload)

    def append_commit(self, state: CatalogState, entry: CommitEntry) -> None:
        """在内存状态上追加一个提交并更新索引。"""
        if entry.version != state.current_version + 1:
            raise LakehouseError(
                f"non-contiguous version: {entry.version} after "
                f"{state.current_version}",
                table=self.table,
            )
        if entry.batch_id in state.batch_index:
            raise LakehouseError(
                f"batch {entry.batch_id!r} already committed", table=self.table
            )
        state.commits.append(entry)
        state.current_version = entry.version
        state.batch_index[entry.batch_id] = entry.version

    def mark_expired(self, state: CatalogState, versions: list[int]) -> None:
        """把版本标记为过期并删除其数据文件（catalog 条目永久保留）。"""
        for v in versions:
            entry = state.find(v)
            if entry is None or v in state.expired_versions:
                continue
            self.storage.remove(f"{self.table}/{entry.data_file}")
            state.expired_versions.append(v)
        state.expired_versions.sort()


__all__ = ["CommitEntry", "CatalogState", "Catalog"]
