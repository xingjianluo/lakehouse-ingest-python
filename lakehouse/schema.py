"""表结构定义与演进兼容性校验。

兼容判定遵循一条明确、可写进文档的规则：

    只有「不动既有数据语义」的变更才允许。

- 既有字段的类型一律不得变更（连 int->float 这类加宽也不允许：
  声明类型一旦改变，历史行的原值语义就与新声明对不上）；
- 既有字段不得删除；
- 允许追加新字段，但新字段必须携带与声明类型一致的缺省值
  （default=None 表示该字段可空），缺省值对历史快照稳定生效；
- 新字段自身的缺省值类型必须与声明类型匹配。
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import IncompatibleSchemaError, SchemaRejectionReason

# 支持的字段类型；bool 必须排在 int 之前判定（bool 是 int 的子类）。
_KNOWN_TYPES = {"int", "float", "string", "bool"}


@dataclass(frozen=True)
class Field:
    name: str
    type: str  # "int" | "float" | "string" | "bool"
    default: object = None  # 新增字段的缺省值，对历史数据稳定生效


@dataclass(frozen=True)
class Schema:
    fields: tuple[Field, ...] = ()

    def field_map(self) -> dict[str, Field]:
        return {f.name: f for f in self.fields}


def default_matches_type(default: object, ftype: str) -> bool:
    """缺省值是否与字段声明类型兼容（None 永远合法，表示可空）。"""
    if default is None:
        return True
    if ftype == "int":
        return isinstance(default, int) and not isinstance(default, bool)
    if ftype == "float":
        return isinstance(default, (int, float)) and not isinstance(default, bool)
    if ftype == "string":
        return isinstance(default, str)
    if ftype == "bool":
        return isinstance(default, bool)
    return False


def _check_existing_field(old: Field, new: Field) -> None:
    if new.type not in _KNOWN_TYPES:
        raise IncompatibleSchemaError(
            SchemaRejectionReason.TYPE_INCOMPATIBLE,
            old.name,
            f"未知的目标类型 {new.type!r}",
        )
    # 既有字段类型一律不得变更：任何变化都会让历史行的原值与新声明对不上。
    if old.type != new.type:
        raise IncompatibleSchemaError(
            SchemaRejectionReason.TYPE_CHANGED,
            old.name,
            f"既有字段类型不允许变更：{old.type!r} -> {new.type!r}，"
            f"历史数据仍按 {old.type!r} 存储，语义会与新声明冲突",
        )


def check_compatibility(old: Schema, new: Schema) -> Schema:
    """校验 new 相对 old 的演进是否兼容（只做检查，不涉及任何落盘）。

    规则（只允许不动既有数据语义的变更）：
    - 已有字段不得删除（FIELD_REMOVED）；
    - 已有字段类型一律不得变更（TYPE_CHANGED，加宽也不行）；
    - 只允许追加新字段；新字段的 default 必须与声明类型一致
      （DEFAULT_TYPE_MISMATCH；default=None 表示可空，永远合法）。

    兼容时返回合并后的结构（旧字段顺序保持，新字段按 new 中的顺序追加）；
    不兼容时抛出 IncompatibleSchemaError，reason 可区分具体原因。
    """
    old_map = old.field_map()
    new_map = new.field_map()

    for name in old_map:
        if name not in new_map:
            raise IncompatibleSchemaError(
                SchemaRejectionReason.FIELD_REMOVED,
                name,
                "不允许删除既有字段，历史数据必须保留",
            )

    for name, old_field in old_map.items():
        _check_existing_field(old_field, new_map[name])

    for new_field in new.fields:
        if new_field.type not in _KNOWN_TYPES:
            raise IncompatibleSchemaError(
                SchemaRejectionReason.TYPE_INCOMPATIBLE,
                new_field.name,
                f"未知的字段类型 {new_field.type!r}",
            )
        if new_field.name not in old_map and not default_matches_type(
            new_field.default, new_field.type
        ):
            raise IncompatibleSchemaError(
                SchemaRejectionReason.DEFAULT_TYPE_MISMATCH,
                new_field.name,
                f"新字段缺省值 {new_field.default!r} 与声明类型 "
                f"{new_field.type!r} 不匹配（default=None 表示可空）",
            )

    merged = [new_map[old_field.name] for old_field in old.fields]
    for new_field in new.fields:
        if new_field.name not in old_map:
            merged.append(new_field)
    return Schema(fields=tuple(merged))
