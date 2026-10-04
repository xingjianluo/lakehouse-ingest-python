"""表级 API：多写入端批次提交、重试对账、结构预检、快照读取。

一致性设计（同进程多线程 + 多进程多终端通用）：
- 进程内用 RLock 串行化，进程间用 metadata/lock 上的 flock 串行化；
  每次提交都在独占锁内从磁盘重读日志（并截断崩溃残缺尾行）后判定，
  因此多个写入端并发提交同一批次时，最多只有一个真正落盘；
- 提交点 = 元数据日志追加成功的那一刻；版本数据文件先于日志落盘
  （临时文件 + os.replace 原子改名），读取方只能看到完整提交点；
- 批次对账：成功条目带归一化内容指纹。同 batch_id 重试且指纹一致 →
  DuplicateBatchError（幂等，直接带回首次版本与条数，不写任何数据）；
  指纹不一致 → BatchConflictError（明确的程序化失败，指出已落版本），
  并记被拒台账；
- 被拒提交（冲突 / 结构 / 记录）只追加 rejected.jsonl 台账，
  不写数据、不占版本；台账不参与后续提交判定，同批次可再次提交；
- 快照读取在共享锁内确定提交点（版本条目），再读不可变的版本数据文件，
  并发写入期间读到的始终是某个一致提交点。
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .errors import (
    BatchConflictError,
    DuplicateBatchError,
    IncompatibleSchemaError,
    RecordValidationError,
    SchemaRejectionReason,
    VersionExpiredError,
    VersionNotFoundError,
)
from .schema import Field, Schema, check_compatibility
from .storage import CommitEntry, LocalStorage, RejectedRecord

_TYPE_CHECK = {
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "float": lambda v: (isinstance(v, (int, float)) and not isinstance(v, bool)),
    "string": lambda v: isinstance(v, str),
    "bool": lambda v: isinstance(v, bool),
}


@dataclass(frozen=True)
class SchemaPrecheck:
    """结构变更预检结论（只做检查，不写任何数据）。"""

    compatible: bool
    merged_schema: Schema | None
    reason: SchemaRejectionReason | None
    field: str | None
    detail: str | None


@dataclass(frozen=True)
class BatchStatus:
    """按批次号查到的处理情况：成功落点或历次被拒原因。"""

    batch_id: str
    state: str  # committed | rejected | unknown
    version: int | None            # 成功时落在哪个版本
    batch_size: int | None         # 成功时该批次的记录条数
    rejections: tuple[RejectedRecord, ...]


class Table:
    """一张本地湖仓表，线程安全；多个 Table 实例（可跨进程）共享目录亦安全。"""

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

        - 结构不兼容：抛 IncompatibleSchemaError（reason 可区分原因），无写入；
        - batch_id 已提交且内容一致：抛 DuplicateBatchError（带回首次版本与
          条数），无写入，调用方按成功重试处理；
        - batch_id 已提交但内容不一致：抛 BatchConflictError（指出已落版本），
          无写入；
        - 记录与结构不匹配：抛 RecordValidationError，无写入。
        所有失败都进被拒台账（可查原因），但不影响该批次后续重试判定。
        """
        with self._lock, self.storage.locked(exclusive=True):
            # 从磁盘重建最新视图，并清理崩溃残留的未提交尾行。
            self._entries = self.storage.recover_log()
            self._schema = (
                self._entries[-1].schema if self._entries else self._schema
            )

            try:
                target_schema = self._schema
                if schema is not None:
                    target_schema = check_compatibility(self._schema, schema)

                normalized = [
                    self._normalize_record(r, target_schema) for r in records
                ]
                fingerprint = self._fingerprint(target_schema, normalized)

                for e in self._entries:
                    if e.batch_id != batch_id:
                        continue
                    if not e.fingerprint:
                        # 历史条目无指纹（旧版本写入），无法证同，按明确失败处理。
                        raise BatchConflictError(batch_id, e.version)
                    if e.fingerprint == fingerprint:
                        raise DuplicateBatchError(
                            batch_id, e.version, e.batch_size
                        )
                    raise BatchConflictError(batch_id, e.version)

                version = self._entries[-1].version + 1 if self._entries else 1
                # 全量快照 = 历史数据（按新结构补缺省值） + 本批记录。
                snapshot_rows: list[dict] = []
                if self._entries:
                    old = self.storage.read_version_data(self._entries[-1].version)
                    snapshot_rows = [
                        self._apply_defaults(r, target_schema) for r in old
                    ]
                snapshot_rows.extend(normalized)

                # 先写数据文件，再追加日志（提交点）。
                self.storage.write_version_data(version, snapshot_rows)
                entry = CommitEntry(
                    version=version,
                    batch_id=batch_id,
                    schema=target_schema,
                    record_count=len(snapshot_rows),
                    batch_size=len(normalized),
                    fingerprint=fingerprint,
                )
                self.storage.append_log(entry)
                self._entries.append(entry)
                self._schema = target_schema
                return version
            except IncompatibleSchemaError as e:
                self._record_rejection(
                    batch_id,
                    "incompatible_schema",
                    str(e),
                    schema_reason=e.reason,
                    field=e.field,
                )
                raise
            except BatchConflictError as e:
                self._record_rejection(
                    batch_id,
                    "batch_conflict",
                    str(e),
                    committed_version=e.committed_version,
                )
                raise
            except RecordValidationError as e:
                self._record_rejection(batch_id, "record_validation", str(e))
                raise

    # ---- 结构预检 ----

    def precheck_schema(self, schema: Schema) -> SchemaPrecheck:
        """只检查结构变更能否接受，不写任何数据、不记台账。

        结论与真正提交时执行的是同一个 check_compatibility，
        因此预检可接受则提交时的结构判定必然通过（反之亦然）。
        """
        with self._lock, self.storage.locked(exclusive=False):
            entries = self.storage.read_log()
            current = entries[-1].schema if entries else self._schema
        try:
            merged = check_compatibility(current, schema)
        except IncompatibleSchemaError as e:
            return SchemaPrecheck(False, None, e.reason, e.field, str(e))
        return SchemaPrecheck(True, merged, None, None, None)

    # ---- 批次对账查询 ----

    def batch_status(self, batch_id: str) -> BatchStatus:
        """按批次号查询处理情况；被拒的批次也查得到。"""
        with self._lock, self.storage.locked(exclusive=False):
            entries = self.storage.read_log()
            rejections = tuple(self.storage.read_rejected(batch_id))
            committed = next(
                (e for e in entries if e.batch_id == batch_id), None
            )
        if committed is not None:
            return BatchStatus(
                batch_id=batch_id,
                state="committed",
                version=committed.version,
                batch_size=committed.batch_size,
                rejections=rejections,
            )
        if rejections:
            related = next(
                (r.committed_version for r in reversed(rejections)
                 if r.committed_version is not None),
                None,
            )
            return BatchStatus(
                batch_id=batch_id,
                state="rejected",
                version=related,
                batch_size=None,
                rejections=rejections,
            )
        return BatchStatus(batch_id, "unknown", None, None, ())

    # ---- 读取 ----

    def current_version(self) -> int:
        """当前最新提交版本号，无提交时为 0（对其他进程的提交可见）。"""
        with self._lock, self.storage.locked(exclusive=False):
            entries = self.storage.read_log()
            self._entries = entries
            return entries[-1].version if entries else 0

    def schema(self) -> Schema:
        with self._lock:
            return self._schema

    def snapshot(self, version: int | None = None) -> list[dict]:
        """读取指定版本快照；version 为 None 时读最新版本。

        版本从未存在抛 VersionNotFoundError；已过期被清理抛
        VersionExpiredError。历史快照结果在后续写入后仍可复现。
        """
        with self._lock, self.storage.locked(exclusive=False):
            entries = self.storage.read_log()
            self._entries = entries
            if not entries:
                raise VersionNotFoundError(version if version is not None else 0)
            v = version if version is not None else entries[-1].version
            if v < 1 or v > entries[-1].version:
                raise VersionNotFoundError(v)
        # 版本数据文件一旦提交即不可变，锁外读取安全。
        if not self.storage.version_path(v).exists():
            raise VersionExpiredError(v, self.min_available_version())
        return self.storage.read_version_data(v)

    def min_available_version(self) -> int:
        """当前仍可读取的最早版本。"""
        with self._lock, self.storage.locked(exclusive=False):
            entries = self.storage.read_log()
            for e in entries:
                if self.storage.version_path(e.version).exists():
                    return e.version
            return entries[-1].version if entries else 0

    # ---- 版本清理 ----

    def expire_versions(self, keep_last: int) -> list[int]:
        """仅保留最近 keep_last 个版本的数据文件，返回被清理的版本号。

        元数据日志保留，过期版本读取时抛 VersionExpiredError。
        """
        with self._lock, self.storage.locked(exclusive=True):
            if keep_last < 1:
                raise ValueError("keep_last 必须 >= 1")
            entries = self.storage.read_log()
            current = entries[-1].version if entries else 0
            cutoff = current - keep_last
            removed = []
            for e in entries:
                if e.version <= cutoff and self.storage.version_path(e.version).exists():
                    self.storage.remove_version_data(e.version)
                    removed.append(e.version)
            return removed

    # ---- 内部 ----

    def _record_rejection(
        self,
        batch_id: str,
        reason: str,
        detail: str,
        *,
        schema_reason: SchemaRejectionReason | None = None,
        field: str | None = None,
        committed_version: int | None = None,
    ) -> None:
        """把一次拒绝追加到台账。台账不参与提交判定，失败不影响重试。"""
        rec = RejectedRecord(
            batch_id=batch_id,
            reason=reason,
            detail=detail,
            rejected_at=datetime.now(UTC).isoformat(),
            schema_reason=schema_reason.value if schema_reason else None,
            field=field,
            committed_version=committed_version,
        )
        self.storage.append_rejected(rec)

    @staticmethod
    def _fingerprint(schema: Schema, normalized_records: list[dict]) -> str:
        """归一化内容指纹：目标结构字段定义 + 归一化记录，稳定可复现。"""
        payload = {
            "schema": [
                {"name": f.name, "type": f.type, "default": f.default}
                for f in schema.fields
            ],
            "records": normalized_records,
        }
        blob = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

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
