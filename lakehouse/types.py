"""逻辑数据类型与表结构定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .errors import SchemaIncompatibleError

# bool 是 int 的子类，类型判断时必须显式排除
_INT_TYPES = (int,)
_FLOAT_TYPES = (int, float)


class LogicalType(str, Enum):
    """支持的逻辑数据类型。

    - long: 64 位有符号整数
    - double: 双精度浮点数
    - string: UTF-8 字符串
    - boolean: 布尔
    - timestamp: 以微秒计的 Unix 时间戳（整数承载）
    """

    LONG = "long"
    DOUBLE = "double"
    STRING = "string"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True)
class Column:
    """字段定义。

    新增字段必须 ``nullable=True`` 或提供非 None 的 ``default_value``，
    以便为历史数据确定稳定的缺省取值。
    """

    name: str
    logical_type: LogicalType
    nullable: bool = True
    default_value: object | None = None


@dataclass(frozen=True)
class Schema:
    """有序表结构。字段顺序即投影顺序。"""

    columns: tuple[Column, ...] = field(default_factory=tuple)

    def names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def get(self, name: str) -> Column | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    # ------- 序列化（供 catalog / 快照持久化） -------
    def to_dict(self) -> dict[str, Any]:
        return {
            "columns": [
                {
                    "name": c.name,
                    "type": c.logical_type.value,
                    "nullable": c.nullable,
                    "default": c.default_value,
                }
                for c in self.columns
            ]
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | Schema | None) -> Schema:
        if isinstance(data, Schema):
            return data
        if not data:
            return cls(columns=())
        cols: list[Column] = []
        seen: set[str] = set()
        for raw in data.get("columns", []):
            name = raw["name"]
            if name in seen:
                raise SchemaIncompatibleError(
                    f"duplicate column: {name}",
                    sub_reason=SchemaIncompatibleError.DUPLICATE_COLUMN,
                    field=name,
                )
            seen.add(name)
            default = raw.get("default", None)
            cols.append(
                Column(
                    name=name,
                    logical_type=LogicalType(raw["type"]),
                    nullable=bool(raw.get("nullable", True)),
                    default_value=default,
                )
            )
        return cls(columns=tuple(cols))

    def effective_default(self, col: Column) -> object:
        """返回新增字段对历史数据生效的稳定缺省值。

        nullable 且无显式默认 -> ``None``；否则使用显式 ``default_value``。
        """
        if col.default_value is not None:
            return col.default_value
        if col.nullable:
            return None
        # 不应到达：ensure_compatible 会拒绝无默认值的 required 新字段
        raise SchemaIncompatibleError(
            f"new column {col.name!r} is NOT NULL without default",
            sub_reason=SchemaIncompatibleError.NEW_COLUMN_MISSING_DEFAULT,
            field=col.name,
        )


# 允许的类型拓宽（widening）：仅 long -> double（数值精度不丢失）
WIDENING: dict[LogicalType, frozenset[LogicalType]] = {
    LogicalType.LONG: frozenset({LogicalType.DOUBLE}),
}

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def validate_value(column: Column, value: object) -> None:
    """校验单个值是否符合字段类型；不符合抛 SchemaIncompatibleError。"""
    if value is None:
        if not column.nullable:
            raise SchemaIncompatibleError(
                f"null is not allowed for non-nullable column {column.name!r}",
                sub_reason=SchemaIncompatibleError.RECORD_TYPE_MISMATCH,
                field=column.name,
            )
        return
    t = column.logical_type
    if t is LogicalType.LONG:
        ok = isinstance(value, bool) is False and isinstance(value, _INT_TYPES)
        if ok and not (_INT64_MIN <= value <= _INT64_MAX):
            ok = False
    elif t is LogicalType.TIMESTAMP:
        ok = isinstance(value, bool) is False and isinstance(value, _INT_TYPES)
    elif t is LogicalType.DOUBLE:
        # bool 显式排除；拒绝 float('nan') 以保持 JSON 可序列化
        ok = (
            isinstance(value, bool) is False
            and isinstance(value, _FLOAT_TYPES)
            and value == value
        )
    elif t is LogicalType.STRING:
        ok = isinstance(value, str)
    elif t is LogicalType.BOOLEAN:
        ok = isinstance(value, bool)
    else:  # pragma: no cover - 枚举穷尽
        ok = False
    if not ok:
        raise SchemaIncompatibleError(
            f"value for column {column.name!r} is not {t.value}: {value!r}",
            sub_reason=SchemaIncompatibleError.RECORD_TYPE_MISMATCH,
            field=column.name,
        )


def ensure_compatible(old: Schema, new: Schema) -> None:
    """校验结构演进的兼容性；不兼容抛 SchemaIncompatibleError（带 sub_reason）。

    规则：
    - 允许在尾部新增 nullable 或带默认值的字段；
    - 允许既有字段 long -> double 的类型拓宽；
    - 允许保持 nullable 不变或 required -> nullable（放宽）；
    - 禁止删除字段、禁止字段重排（同名字段必须留在原位）；
    - 禁止类型收窄/非拓宽类型变更；
    - 禁止 nullable -> required 的收窄。
    """
    old_cols = old.columns
    new_cols = new.columns

    if len(new_cols) < len(old_cols):
        # 被删除的字段（按位置+名字精确定位第一个消失的旧字段）
        new_names = {c.name for c in new_cols}
        removed = next(c.name for c in old_cols if c.name not in new_names)
        raise SchemaIncompatibleError(
            f"column {removed!r} removed; dropping columns is not allowed",
            sub_reason=SchemaIncompatibleError.FIELD_REMOVED,
            field=removed,
        )

    for i, old_col in enumerate(old_cols):
        new_col = new_cols[i]
        if new_col.name != old_col.name:
            # 位置上的字段变了：说明发生了重排或在中间插入/删除
            old_names = {c.name for c in old_cols}
            if new_col.name not in old_names:
                raise SchemaIncompatibleError(
                    f"new column {new_col.name!r} must be appended at the tail, "
                    f"but appears at position {i}",
                    sub_reason=SchemaIncompatibleError.FIELD_REMOVED,
                    field=new_col.name,
                )
            raise SchemaIncompatibleError(
                f"column {old_col.name!r} moved/removed at position {i}; "
                "reordering and dropping columns are not allowed",
                sub_reason=SchemaIncompatibleError.FIELD_REMOVED,
                field=old_col.name,
            )

        if new_col.logical_type is not old_col.logical_type:
            allowed = WIDENING.get(old_col.logical_type, frozenset())
            if new_col.logical_type not in allowed:
                # 反向成立（新 -> 旧 是拓宽）说明本次是收窄，其余为不兼容变更
                reverse = WIDENING.get(new_col.logical_type, frozenset())
                sub = (
                    SchemaIncompatibleError.TYPE_NARROWED
                    if old_col.logical_type in reverse
                    else SchemaIncompatibleError.TYPE_CHANGED
                )
                raise SchemaIncompatibleError(
                    f"column {old_col.name!r} type changed "
                    f"{old_col.logical_type.value} -> {new_col.logical_type.value}; "
                    "only long -> double widening is allowed",
                    sub_reason=sub,
                    field=old_col.name,
                )

        if old_col.nullable and not new_col.nullable:
            raise SchemaIncompatibleError(
                f"column {old_col.name!r} changed nullable -> NOT NULL; "
                "tightening nullability is not allowed",
                sub_reason=SchemaIncompatibleError.NULLABLE_NARROWED,
                field=old_col.name,
            )

    # 新增字段只能追加在尾部
    for new_col in new_cols[len(old_cols) :]:
        if new_col.default_value is None and not new_col.nullable:
            raise SchemaIncompatibleError(
                f"new column {new_col.name!r} must be nullable or have a default",
                sub_reason=SchemaIncompatibleError.NEW_COLUMN_MISSING_DEFAULT,
                field=new_col.name,
            )
        # 校验默认值本身类型合法
        if new_col.default_value is not None:
            validate_value(
                Column(
                    name=new_col.name,
                    logical_type=new_col.logical_type,
                    nullable=True,
                    default_value=None,
                ),
                new_col.default_value,
            )


__all__ = [
    "LogicalType",
    "Column",
    "Schema",
    "WIDENING",
    "validate_value",
    "ensure_compatible",
]
