"""错误类型定义。

所有可预期失败都继承 :class:`LakehouseError`，并用稳定的 ``reason`` 码区分，
便于调用方与测试日志精确判定失败原因。
"""

from __future__ import annotations


class LakehouseError(Exception):
    """所有湖仓错误的基类。"""

    reason: str = "lakehouse_error"

    def __init__(self, message: str = "", *, table: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.table = table

    def __str__(self) -> str:
        prefix = f"[{self.reason}]"
        if self.table:
            prefix += f" table={self.table}"
        return f"{prefix} {self.message}".rstrip()


class TableNotFoundError(LakehouseError):
    """请求的表不存在。"""

    reason = "table_not_found"


class VersionNotFoundError(LakehouseError):
    """请求的版本从未存在。"""

    reason = "version_not_found"


class VersionExpiredError(LakehouseError):
    """版本曾经存在但快照已过期（被清理）。"""

    reason = "version_expired"


class BatchConflictError(LakehouseError):
    """批次 ID 冲突：已以不同内容提交过。"""

    reason = "batch_conflict"


class SchemaIncompatibleError(LakehouseError):
    """结构变更不兼容。子原因见 :attr:`sub_reason`。"""

    reason = "schema_incompatible"

    # ------- 稳定的子原因码 -------
    FIELD_REMOVED = "field_removed"
    TYPE_NARROWED = "type_narrowed"
    TYPE_CHANGED = "type_changed"
    RECORD_MISSING_FIELD = "record_missing_field"
    RECORD_UNKNOWN_FIELD = "record_unknown_field"
    RECORD_TYPE_MISMATCH = "record_type_mismatch"
    NULLABLE_NARROWED = "nullable_narrowed"
    NEW_COLUMN_MISSING_DEFAULT = "new_column_missing_default"
    DUPLICATE_COLUMN = "duplicate_column"
    EMPTY_SCHEMA = "empty_schema"

    def __init__(
        self,
        message: str = "",
        *,
        table: str | None = None,
        sub_reason: str = "schema_incompatible",
        field: str | None = None,
    ) -> None:
        super().__init__(message, table=table)
        self.sub_reason = sub_reason
        self.field = field

    def __str__(self) -> str:
        base = super().__str__()
        detail = f"(sub_reason={self.sub_reason}"
        if self.field:
            detail += f", field={self.field}"
        detail += ")"
        return f"{base} {detail}"
