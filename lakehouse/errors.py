"""可区分的失败原因类型定义。"""

from __future__ import annotations

from enum import Enum


class LakehouseError(Exception):
    """所有湖仓摄取错误的基类。"""


class DuplicateBatchError(LakehouseError):
    """批次重复提交：该批次此前已成功提交，本次提交被忽略（幂等）。"""

    def __init__(self, batch_id: str, committed_version: int) -> None:
        self.batch_id = batch_id
        self.committed_version = committed_version
        super().__init__(
            f"批次 {batch_id!r} 已在版本 {committed_version} 提交，重复提交被幂等忽略"
        )


class SchemaRejectionReason(str, Enum):
    """结构变更被拒绝的具体原因。"""

    FIELD_REMOVED = "field_removed"
    TYPE_NARROWED = "type_narrowed"
    TYPE_INCOMPATIBLE = "type_incompatible"
    MISSING_DEFAULT_FOR_REQUIRED = "missing_default_for_required"


class IncompatibleSchemaError(LakehouseError):
    """结构演进不兼容：字段删除、类型收窄或不兼容变更。"""

    def __init__(self, reason: SchemaRejectionReason, field: str, detail: str) -> None:
        self.reason = reason
        self.field = field
        super().__init__(f"字段 {field!r} 结构变更被拒绝（{reason.value}）：{detail}")


class VersionNotFoundError(LakehouseError):
    """请求的版本从未存在。"""

    def __init__(self, version: int) -> None:
        self.version = version
        super().__init__(f"版本 {version} 不存在")


class VersionExpiredError(LakehouseError):
    """请求的版本曾经存在但已过期被清理。"""

    def __init__(self, version: int, min_available: int) -> None:
        self.version = version
        self.min_available = min_available
        super().__init__(
            f"版本 {version} 已过期，当前最早可用版本为 {min_available}"
        )


class RecordValidationError(LakehouseError):
    """记录与表结构不匹配：未知字段或值类型不兼容。"""

    def __init__(self, detail: str) -> None:
        super().__init__(f"记录校验失败：{detail}")
