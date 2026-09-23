"""表结构定义与演进兼容性校验。"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import IncompatibleSchemaError, SchemaRejectionReason

# 数值/字符串链上的加宽方向：int -> float -> string。
_WIDENING_RANK = {"int": 0, "float": 1, "string": 2}
# bool 独立成族，只允许与自身兼容。
_KNOWN_TYPES = set(_WIDENING_RANK) | {"bool"}


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


def _check_type_evolution(old: Field, new: Field) -> None:
    if old.type == new.type:
        return
    if old.type not in _WIDENING_RANK or new.type not in _WIDENING_RANK:
        raise IncompatibleSchemaError(
            SchemaRejectionReason.TYPE_INCOMPATIBLE,
            old.name,
            f"类型 {old.type!r} 与 {new.type!r} 不属于同一可加宽类型族",
        )
    if _WIDENING_RANK[new.type] < _WIDENING_RANK[old.type]:
        raise IncompatibleSchemaError(
            SchemaRejectionReason.TYPE_NARROWED,
            old.name,
            f"类型 {old.type!r} -> {new.type!r} 属于收窄，会破坏既有数据",
        )
    # 其余为加宽（int->float->string），允许。


def check_compatibility(old: Schema, new: Schema) -> Schema:
    """校验 new 相对 old 的演进是否兼容。

    规则：
    - 已有字段不得删除（FIELD_REMOVED）；
    - 已有字段类型只允许加宽，收窄（TYPE_NARROWED）或跨族变更
      （TYPE_INCOMPATIBLE）被拒绝；
    - 新增字段必须携带确定的缺省语义（default 可为 None，表示可空），
      该缺省值对历史数据稳定生效。

    兼容时返回合并后的结构（旧字段顺序保持，新字段追加）；
    不兼容时抛出 IncompatibleSchemaError。
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
        _check_type_evolution(old_field, new_map[name])

    merged = []
    for old_field in old.fields:
        # 既有字段采用演进后的定义（可能已加宽），保持原有顺序。
        merged.append(new_map[old_field.name])
    for new_field in new.fields:
        if new_field.name not in old_map:
            merged.append(new_field)
    return Schema(fields=tuple(merged))
