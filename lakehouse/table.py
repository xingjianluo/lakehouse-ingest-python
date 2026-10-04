"""表级 API：批次提交、结构演进、快照读取、时间旅行与批次对账。

一致性设计：
- 提交点 = 元数据日志追加成功的那一刻；版本数据文件先于日志落盘，
  因此任何已提交的版本其数据文件必然完整（整体生效或整体不生效）；
- 提交临界区 = 进程内线程锁 + 表级 flock 排他锁，且持锁后重新读取
  日志：多线程、多进程、多终端对同一张表的提交被串行化，每次成功
  提交都落在独立的新版本上，互不覆盖；
- 批次幂等按"内容指纹"判定：同一 batch_id 内容一致 -> 直接返回首次
  提交的版本（重试安全，不产生任何写入）；内容不一致 -> 抛出
  BatchConflictError（携带已提交版本号，供对账定位）；
- 写入方中途崩溃：已写数据文件但未追加日志 = 未提交，重试会整体重写
  该版本，数据不多不少；
- 被结构/记录校验拒绝的批次：不留数据、不占版本，仅追加一条拒绝日志
  供 batch_status 对账查询；同批次号之后重试按全新提交重新判定；
- 快照读取先在共享锁内刷新并确定提交点（版本条目），再读数据文件，
  并发写入期间读取到的始终是某个一致提交点。
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from pathlib import Path

from .errors import (
    BatchConflictError,
    IncompatibleSchemaError,
    RecordValidationError,
    VersionExpiredError,
    VersionNotFoundError,
)
from .schema import (
    Field,
    Schema,
    SchemaCheckResult,
    check_compatibility,
    precheck_compatibility,
)
from .storage import CommitEntry, LocalStorage, RejectionEntry

_TYPE_CHECK = {
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "bool": lambda v: isinstance(v, bool),
}


@dataclass(frozen=True)
class BatchStatus:
    """批次对账视图：某个 batch_id 当前的处理情况。"""

    batch_id: str
    status: str  # "committed" | "rejected" | "unknown"
    version: int | None = None  # committed 时：落在哪个版本
    record_count: int | None = None  # committed 时：该版本总记录数
    reason: str | None = None  # rejected 时：可程序识别的原因
    detail: str | None = None  # rejected 时：人类可读判定依据


def _fingerprint(records: list[dict], schema: Schema | None) -> str:
    """批次内容指纹：记录与结构的规范哈希，用于识别"同号不同内容"。"""
    payload = {
        "records": records,
        "schema": (
            [[f.name, f.type, f.default] for f in schema.fields]
            if schema is not None
            else None
        ),
    }
    canonical = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=repr
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Table:
    """一张本地湖仓表，线程安全且可跨进程并发使用。"""

    def __init__(self, root: Path, schema: Schema | None = None) -> None:
        self.storage = LocalStorage(root)
        self._lock = threading.RLock()
        self._entries: list[CommitEntry] = self.storage.read_log()
        self._initial_schema = schema or Schema()
        if schema is not None and not self._entries:
            # 建表结构自身必须合法（未知类型/缺省值不匹配在建表时就暴露）。
            check_compatibility(Schema(), schema)
        self._schema: Schema = (
            self._entries[-1].schema if self._entries else self._initial_schema
        )

    # ---- 提交 ----

    def commit_batch(
        self,
        batch_id: str,
        records: list[dict],
        schema: Schema | None = None,
    ) -> int:
        """提交一个批次，返回该批次所在的版本号。

        - batch_id 已提交且内容一致：幂等重试，直接返回首次提交的版本号，
          不产生任何写入；
        - batch_id 已提交但内容不一致：抛 BatchConflictError（携带已提交
          版本号），不产生任何写入；
        - schema 与当前结构不兼容：抛 IncompatibleSchemaError，不留数据、
          不占版本，仅记录拒绝日志；
        - 记录与结构不匹配：抛 RecordValidationError，同上。
        """
        # 排他锁串行化多进程提交；持锁后刷新日志，基于最新已提交状态判定。
        with self._lock, self.storage.locked(exclusive=True):
            self._refresh()
            fp = _fingerprint(records, schema)

            for e in self._entries:
                if e.batch_id == batch_id:
                    if e.fingerprint is not None and e.fingerprint == fp:
                        return e.version  # 内容与首次一致：按重试处理
                    # 内容变了（或旧日志无指纹无法确认一致）：明确失败。
                    err = BatchConflictError(batch_id, e.version)
                    self._record_rejection(batch_id, "batch_conflict", str(err))
                    raise err

            target_schema = self._schema
            if schema is not None:
                try:
                    # 校验演进兼容性；兼容则得到合并后的结构。
                    target_schema = check_compatibility(self._schema, schema)
                except IncompatibleSchemaError as e:
                    self._record_rejection(batch_id, e.reason.value, str(e))
                    raise

            try:
                normalized = [self._normalize_record(r, target_schema) for r in records]
            except RecordValidationError as e:
                self._record_rejection(batch_id, "record_validation", str(e))
                raise

            version = self._entries[-1].version + 1 if self._entries else 1
            # 全量快照 = 历史数据（按新结构补缺省值） + 本批记录。
            snapshot_rows = []
            if self._entries:
                old = self.storage.read_version_data(self._entries[-1].version)
                snapshot_rows = [self._apply_defaults(r, target_schema) for r in old]
            snapshot_rows.extend(normalized)

            # 先写数据文件，再追加日志（提交点）。崩溃于两者之间 =
            # 未提交，重试整体重写该版本，不会多也不会少。
            self.storage.write_version_data(version, snapshot_rows)
            entry = CommitEntry(
                version, batch_id, target_schema, len(snapshot_rows), fp
            )
            self.storage.append_log(entry)
            self._entries.append(entry)
            self._schema = target_schema
            return version

    # ---- 结构预检 ----

    def check_schema(self, schema: Schema) -> SchemaCheckResult:
        """只检查不写入：该结构变更相对当前已提交结构能否被接受。

        与 commit_batch 共用同一份判定逻辑，预检结论与真正提交的结果一致。
        """
        with self._lock, self.storage.locked(exclusive=False):
            self._refresh()
            return precheck_compatibility(self._schema, schema)

    # ---- 批次对账 ----

    def batch_status(self, batch_id: str) -> BatchStatus:
        """按批次号查询处理情况：成功落在哪个版本/多少条，或失败原因。"""
        with self._lock, self.storage.locked(exclusive=False):
            self._refresh()
            for e in self._entries:
                if e.batch_id == batch_id:
                    return BatchStatus(
                        batch_id=batch_id,
                        status="committed",
                        version=e.version,
                        record_count=e.record_count,
                    )
            rejections = [
                r for r in self.storage.read_rejections() if r.batch_id == batch_id
            ]
            if rejections:
                last = rejections[-1]
                return BatchStatus(
                    batch_id=batch_id,
                    status="rejected",
                    reason=last.reason,
                    detail=last.detail,
                )
            return BatchStatus(batch_id=batch_id, status="unknown")

    # ---- 读取 ----

    def current_version(self) -> int:
        """当前最新提交版本号，无提交时为 0。"""
        with self._lock, self.storage.locked(exclusive=False):
            self._refresh()
            return self._entries[-1].version if self._entries else 0

    def schema(self) -> Schema:
        with self._lock, self.storage.locked(exclusive=False):
            self._refresh()
            return self._schema

    def snapshot(self, version: int | None = None) -> list[dict]:
        """读取指定版本快照；version 为 None 时读最新版本。

        版本从未存在抛 VersionNotFoundError；已过期被清理抛
        VersionExpiredError。历史快照结果在后续写入后仍可复现。
        """
        with self._lock, self.storage.locked(exclusive=False):
            self._refresh()
            if not self._entries:
                raise VersionNotFoundError(version if version is not None else 0)
            v = version if version is not None else self._entries[-1].version
            if v < 1 or v > self._entries[-1].version:
                raise VersionNotFoundError(v)
        # 提交点确定后读数据文件：已提交版本的数据文件不可变，读到的一定
        # 是该提交点的完整快照。
        if not self.storage.version_path(v).exists():
            raise VersionExpiredError(v, self.min_available_version())
        return self.storage.read_version_data(v)

    def min_available_version(self) -> int:
        """当前仍可读取的最早版本。"""
        with self._lock, self.storage.locked(exclusive=False):
            self._refresh()
            for e in self._entries:
                if self.storage.version_path(e.version).exists():
                    return e.version
            return self._entries[-1].version if self._entries else 0

    # ---- 版本清理 ----

    def expire_versions(self, keep_last: int) -> list[int]:
        """仅保留最近 keep_last 个版本的数据文件，返回被清理的版本号。

        元数据日志保留，过期版本读取时抛 VersionExpiredError。
        """
        with self._lock, self.storage.locked(exclusive=True):
            self._refresh()
            if keep_last < 1:
                raise ValueError("keep_last 必须 >= 1")
            current = self._entries[-1].version if self._entries else 0
            cutoff = current - keep_last
            removed = []
            for e in self._entries:
                if (
                    e.version <= cutoff
                    and self.storage.version_path(e.version).exists()
                ):
                    self.storage.remove_version_data(e.version)
                    removed.append(e.version)
            return removed

    # ---- 内部 ----

    def _refresh(self) -> None:
        """从磁盘重新读取提交日志，看见其他进程已提交的状态。"""
        self._entries = self.storage.read_log()
        if self._entries:
            self._schema = self._entries[-1].schema

    def _record_rejection(self, batch_id: str, reason: str, detail: str) -> None:
        """追加拒绝日志（对账用）；不影响版本与数据，也不影响该批次号
        之后按全新提交重新判定。"""
        self.storage.append_rejection(
            RejectionEntry(batch_id=batch_id, reason=reason, detail=detail)
        )

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
            raise RecordValidationError(f"记录包含结构中不存在的字段 {sorted(unknown)}")
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
