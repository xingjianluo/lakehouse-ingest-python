"""核心表能力：幂等批次提交、结构演进、版本快照读取与时间旅行。"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from .catalog import Catalog, CommitEntry, CatalogState
from .errors import (
    BatchConflictError,
    LakehouseError,
    SchemaIncompatibleError,
    VersionExpiredError,
    VersionNotFoundError,
)
from .storage import FileStorage
from .types import Column, LogicalType, Schema, ensure_compatible, validate_value

log = logging.getLogger("lakehouse")

_VERSION_WIDTH = 12


class ConcurrentCommitError(LakehouseError):
    """乐观并发检查失败：base_version 与当前版本不一致。"""

    reason = "concurrent_modification"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _fingerprint(
    records: list[dict[str, Any]], schema: Schema
) -> str:
    """对批次负载（按结构字段投影后规范化）计算稳定指纹。"""
    names = schema.names()
    norm = [[r.get(n) for n in names] for r in records]
    blob = json.dumps(
        {"schema": schema.to_dict(), "records": norm},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class LakehouseTable:
    """一张本地湖仓表。

    - 提交：全局文件锁串行化；写新快照文件 -> 原子切换 catalog，
      整体生效或完全不生效；``batch_id`` 提供幂等去重。
    - 读取：无锁，基于已发布的不可变快照文件与 catalog 提交点。
    """

    def __init__(self, root: str, table: str) -> None:
        if not table or "/" in table or table in (".", ".."):
            raise LakehouseError(f"invalid table name: {table!r}", table=table)
        self.storage = FileStorage(root)
        self.table = table
        self.storage.ensure_dir(f"{table}/data")
        self.catalog = Catalog(self.storage, table)

    # ------------------------------------------------------------------ #
    # 提交
    # ------------------------------------------------------------------ #
    def commit(
        self,
        batch_id: str,
        records: list[dict[str, Any]],
        new_schema: dict[str, Any] | Schema | None = None,
        *,
        base_version: int | None = None,
    ) -> CommitEntry:
        if not isinstance(batch_id, str) or not batch_id.strip():
            raise LakehouseError("batch_id must be a non-empty string", table=self.table)
        if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
            raise LakehouseError("records must be a list of objects", table=self.table)

        with self.storage.lock(f"{self.table}.lock"):
            state = self.catalog.load()

            # 1) 乐观并发检查（先于幂等短路，确保过期客户端被明确拒绝）
            if base_version is not None and base_version != state.current_version:
                raise ConcurrentCommitError(
                    f"base_version={base_version} != current_version="
                    f"{state.current_version}",
                    table=self.table,
                )

            # 2) 幂等：同一 batch_id 必须返回同一结果；负载不同则冲突
            if batch_id in state.batch_index:
                return self._replay_or_conflict(state, batch_id, records, new_schema)

            # 3) 确定并校验结构演进
            old_schema = self._schema_of(state)
            try:
                if new_schema is None:
                    if not old_schema.columns:
                        raise SchemaIncompatibleError(
                            "an explicit schema is required for the first commit",
                            sub_reason=SchemaIncompatibleError.EMPTY_SCHEMA,
                        )
                    target_schema = old_schema
                else:
                    target_schema = Schema.from_dict(new_schema)
                    if not target_schema.columns:
                        raise SchemaIncompatibleError(
                            "schema must contain at least one column",
                            sub_reason=SchemaIncompatibleError.EMPTY_SCHEMA,
                        )
                    if old_schema.columns:
                        ensure_compatible(old_schema, target_schema)
                    else:
                        self._validate_initial_schema(target_schema)

                # 4) 逐记录校验与投影（缺失/多余/类型错误均明确拒绝）
                new_rows = [
                    self._project_record(target_schema, r, idx)
                    for idx, r in enumerate(records)
                ]
            except SchemaIncompatibleError as exc:
                # 在提交边界补充表上下文，便于错误体/日志精确归因
                if exc.table is None:
                    exc.table = self.table
                raise

            # 5) 以前一个提交点为基准做快照前滚：
            #    历史行按新结构投影（新增字段补稳定缺省、类型拓宽透传），
            #    与本批新行组成该版本的完整不可变快照。
            carried = self._carry_forward(state, old_schema, target_schema)
            full_rows = carried + new_rows

            # 6) 生成新版本与不可变快照文件
            version = state.current_version + 1
            data_file = f"data/v-{version:0{_VERSION_WIDTH}d}.jsonl"
            entry = CommitEntry(
                version=version,
                batch_id=batch_id,
                schema=target_schema.to_dict(),
                data_file=data_file,
                record_count=len(full_rows),
                created_at=_now_iso(),
                parent_version=state.current_version or None,
                fingerprint=_fingerprint(new_rows, target_schema),
            )
            self.storage.write_jsonl(f"{self.table}/{data_file}", full_rows)
            self.catalog.append_commit(state, entry)
            self.catalog.store(state)

        log.info(
            "commit accepted table=%s version=%s batch_id=%s added=%d total=%d "
            "columns=%d decision=accept",
            self.table, version, batch_id, len(new_rows), len(full_rows),
            len(target_schema.columns),
        )
        return entry

    # ------------------------------------------------------------------ #
    # 读取 / 时间旅行
    # ------------------------------------------------------------------ #
    def read(
        self,
        version: int | None = None,
        *,
        as_of: str | None = None,
    ) -> dict[str, Any]:
        """读取指定版本（或当前版本）快照。

        - ``version`` 与 ``as_of`` 只能给一个；
        - 版本从未存在 -> :class:`VersionNotFoundError`；
        - 版本已过期 -> :class:`VersionExpiredError`。
        """
        state = self.catalog.load()
        if version is not None and as_of is not None:
            raise LakehouseError("specify either version or as_of, not both", table=self.table)

        if as_of is not None:
            version = self._resolve_as_of(state, as_of)
        elif version is None:
            version = state.current_version

        if version <= 0 or version > state.current_version:
            raise VersionNotFoundError(
                f"version {version!r} does not exist "
                f"(current_version={state.current_version})",
                table=self.table,
            )
        if state.is_expired(version):
            raise VersionExpiredError(
                f"version {version} has expired and its snapshot was removed",
                table=self.table,
            )

        entry = state.find(version)
        assert entry is not None
        rows = self.storage.read_jsonl(f"{self.table}/{entry.data_file}")
        log.info(
            "snapshot read table=%s version=%d batch_id=%s rows=%d decision=snapshot_hit",
            self.table, version, entry.batch_id, len(rows),
        )
        return {
            "table": self.table,
            "version": entry.version,
            "batch_id": entry.batch_id,
            "schema": entry.schema,
            "created_at": entry.created_at,
            "records": rows,
        }

    def current_version(self) -> int:
        return self.catalog.load().current_version

    def history(self) -> list[dict[str, Any]]:
        state = self.catalog.load()
        return [
            {
                "version": c.version,
                "batch_id": c.batch_id,
                "record_count": c.record_count,
                "created_at": c.created_at,
                "parent_version": c.parent_version,
                "expired": c.version in state.expired_versions,
            }
            for c in state.commits
        ]

    def expire_versions(self, retain: int) -> list[int]:
        """仅保留最近 ``retain`` 个快照；更旧快照标记过期并删除数据文件。

        catalog 提交历史与 batch 索引永久保留，保证可追溯与批次幂等。
        """
        if retain < 0:
            raise LakehouseError("retain must be >= 0", table=self.table)
        with self.storage.lock(f"{self.table}.lock"):
            state = self.catalog.load()
            alive = [c for c in state.commits if c.version not in state.expired_versions]
            to_expire = sorted(c.version for c in alive[: max(0, len(alive) - retain)])
            self.catalog.mark_expired(state, to_expire)
            self.catalog.store(state)
        for v in to_expire:
            log.info("snapshot expired table=%s version=%d decision=expire", self.table, v)
        return to_expire

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _schema_of(self, state: CatalogState) -> Schema:
        if not state.commits:
            return Schema(columns=())
        return Schema.from_dict(state.commits[-1].schema)

    def _replay_or_conflict(
        self,
        state: CatalogState,
        batch_id: str,
        records: list[dict[str, Any]],
        new_schema: dict[str, Any] | Schema | None,
    ) -> CommitEntry:
        existing_version = state.batch_index[batch_id]
        entry = state.find(existing_version)
        assert entry is not None
        prior_schema = Schema.from_dict(entry.schema)
        requested_schema = prior_schema if new_schema is None else Schema.from_dict(new_schema)
        same_schema = requested_schema.to_dict() == prior_schema.to_dict()
        if not same_schema:
            log.warning(
                "commit rejected table=%s batch_id=%s existing_version=%d "
                "decision=reject reason=batch_conflict",
                self.table, batch_id, existing_version,
            )
            raise BatchConflictError(
                f"batch_id {batch_id!r} was already committed as version "
                f"{existing_version} with a different schema",
                table=self.table,
            )
        # 用既有结构投影+校验调用方重放的批次：非法记录直接得到明确原因，
        # 再与首次提交时固化在 catalog 中的批次指纹比较（快照过期也可判定）。
        replayed_rows = [
            self._project_record(prior_schema, r, idx) for idx, r in enumerate(records)
        ]
        same_payload = _fingerprint(replayed_rows, prior_schema) == self._entry_fingerprint(
            state, entry, prior_schema
        )
        if not same_payload:
            log.warning(
                "commit rejected table=%s batch_id=%s existing_version=%d "
                "decision=reject reason=batch_conflict",
                self.table, batch_id, existing_version,
            )
            raise BatchConflictError(
                f"batch_id {batch_id!r} was already committed as version "
                f"{existing_version} with different records",
                table=self.table,
            )
        log.info(
            "commit replay table=%s version=%d batch_id=%s records=%d "
            "decision=idempotent_replay",
            self.table, entry.version, batch_id, entry.record_count,
        )
        return entry

    def _entry_fingerprint(
        self, state: CatalogState, entry: CommitEntry, schema: Schema
    ) -> str:
        """返回该提交批次负载的指纹。

        优先使用 catalog 中固化的指纹（快照过期后仍可用）；
        对缺失指纹的旧 catalog，回退为读取快照文件（此时必须未过期）。
        """
        if entry.fingerprint:
            return entry.fingerprint
        if entry.version in state.expired_versions:
            raise LakehouseError(
                f"version {entry.version} expired and its batch fingerprint "
                "is missing in catalog; cannot verify idempotent replay",
                table=self.table,
            )
        rows = self.storage.read_jsonl(f"{self.table}/{entry.data_file}")
        # 旧目录里指纹缺省时数据文件存的是“全量快照”，无法精确回推出批次行；
        # 该回退分支仅用于从未过期、且全表只有一次提交的情形做尽力比对。
        return _fingerprint(rows, schema)

    def _project_record(
        self, schema: Schema, record: dict[str, Any], index: int
    ) -> dict[str, Any]:
        where = f"record[{index}]"
        unknown = set(record) - set(schema.names())
        if unknown:
            name = sorted(unknown)[0]
            raise SchemaIncompatibleError(
                f"{where}: unknown field {name!r} not present in schema",
                table=self.table,
                sub_reason=SchemaIncompatibleError.RECORD_UNKNOWN_FIELD,
                field=name,
            )
        row: dict[str, Any] = {}
        for col in schema.columns:
            if col.name in record:
                value = record[col.name]
            elif col.default_value is not None:
                value = col.default_value
            elif col.nullable:
                value = None
            else:
                raise SchemaIncompatibleError(
                    f"{where}: missing value for non-nullable column {col.name!r}",
                    table=self.table,
                    sub_reason=SchemaIncompatibleError.RECORD_MISSING_FIELD,
                    field=col.name,
                )
            validate_value(col, value)
            row[col.name] = value
        return row

    def _carry_forward(
        self,
        state: CatalogState,
        old_schema: Schema,
        new_schema: Schema,
    ) -> list[dict[str, Any]]:
        """把父版本快照按新结构前滚为新行集合。

        - 既有字段保留取值（long->double 拓宽时规范化为 float）；
        - 新增字段按 nullable 默认 None 或显式 default 稳定补齐；
        - 不删除、不改写任何历史值。
        """
        if not state.commits:
            return []
        parent = state.commits[-1]
        if parent.version in state.expired_versions:
            # 理论不可达：当前版本不会被先于旧版本过期
            raise LakehouseError(
                f"parent version {parent.version} is expired; cannot build snapshot",
                table=self.table,
            )
        prior_rows = self.storage.read_jsonl(f"{self.table}/{parent.data_file}")
        old_names = old_schema.names()
        rows: list[dict[str, Any]] = []
        for raw in prior_rows:
            row: dict[str, Any] = {}
            for col in new_schema.columns:
                if col.name in old_names:
                    value = raw.get(col.name)
                    # long -> double 拓宽：把历史整数规范化为浮点表示
                    old_col = old_schema.get(col.name)
                    if (
                        value is not None
                        and old_col is not None
                        and old_col.logical_type is LogicalType.LONG
                        and col.logical_type is LogicalType.DOUBLE
                    ):
                        value = float(value)
                    validate_value(col, value)
                else:
                    value = new_schema.effective_default(col)
                    validate_value(col, value)
                row[col.name] = value
            rows.append(row)
        return rows

    def _validate_initial_schema(self, schema: Schema) -> None:
        seen: set[str] = set()
        for col in schema.columns:
            if col.name in seen:
                raise SchemaIncompatibleError(
                    f"duplicate column {col.name!r}",
                    table=self.table,
                    sub_reason=SchemaIncompatibleError.DUPLICATE_COLUMN,
                    field=col.name,
                )
            seen.add(col.name)
            if col.default_value is not None:
                validate_value(
                    Column(col.name, col.logical_type, nullable=True), col.default_value
                )

    def _resolve_as_of(self, state: CatalogState, as_of: str) -> int:
        try:
            target = datetime.fromisoformat(as_of)
        except ValueError as exc:
            raise LakehouseError(f"invalid as_of timestamp: {as_of!r}", table=self.table) from exc
        if target.tzinfo is None:
            target = target.replace(tzinfo=UTC)
        chosen = 0
        for c in state.commits:
            created = datetime.fromisoformat(c.created_at)
            if created <= target:
                chosen = c.version
        if chosen == 0:
            raise VersionNotFoundError(
                f"no committed version at or before {as_of!r}", table=self.table
            )
        return chosen


__all__ = ["LakehouseTable", "ConcurrentCommitError"]
