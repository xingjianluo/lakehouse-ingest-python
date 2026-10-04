"""表结构定义与演进兼容性校验。

兼容规则（收敛目标：任何被放行的变更，都必须让已存数据在新结构下
语义依然成立，即"历史数据的类型/取值 与 声明的结构 永远对得上"）：

- 新增字段：允许，但字段类型必须已知，且缺省值（非 None 时）必须与
  声明类型匹配；缺省值对历史数据稳定生效，因此历史行在新结构下
  自动成立。default 为 None 表示可空，任何类型都接受 None。
- 类型加宽：仅允许 int -> float（整数是浮点的真子集，历史 int 值
  在新结构 float 下语义不变）。
- 其余一切变更都会被拒绝，且原因可区分：
  - 删除字段            -> FIELD_REMOVED
  - 类型收窄 float->int -> TYPE_NARROWED
  - 跨类型族变更        -> TYPE_INCOMPATIBLE
    （int/float -> string、string -> bool 等；历史值类型与新声明
      对不上，属于破坏既有数据语义）
  - 新字段缺省值与类型不匹配 -> DEFAULT_TYPE_MISMATCH
  - 未知类型名               -> UNKNOWN_TYPE

预检与提交共用同一份判定逻辑（`_evaluate`），保证"预检说不行，
提交一定不行；预检说行，提交一定行"。
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import IncompatibleSchemaError, SchemaRejectionReason

# 已知类型集合。数值族内部只允许 int -> float 加宽；
# string / bool 各自独立成族，不与数值族互通。
_KNOWN_TYPES = {"int", "float", "string", "bool"}
_NUMERIC_WIDENING = {("int", "float")}


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


@dataclass(frozen=True)
class SchemaCheckResult:
    """结构预检结论：不写任何数据，只回答"能不能接受、为什么"。"""

    accepted: bool
    merged_schema: Schema | None = None  # 接受时：演进合并后的结构
    reason: SchemaRejectionReason | None = None  # 拒绝时：可程序识别的原因
    field: str | None = None  # 拒绝时：涉及的字段名
    detail: str | None = None  # 拒绝时：人类可读的判定依据


def _default_matches(field: Field) -> bool:
    """缺省值（非 None）必须与声明类型匹配，否则历史行补齐后即违反结构。"""
    v = field.default
    if v is None:
        return True
    checks = {
        "int": lambda x: isinstance(x, int) and not isinstance(x, bool),
        # float 字段允许 int 缺省值（整数是浮点子集）。
        "float": lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
        "string": lambda x: isinstance(x, str),
        "bool": lambda x: isinstance(x, bool),
    }
    return checks[field.type](v)


def _evaluate(old: Schema, new: Schema) -> SchemaCheckResult:
    """判定核心：返回预检结论，不产生任何副作用。"""
    old_map = old.field_map()
    new_map = new.field_map()

    # 新结构自身的合法性：类型必须已知，缺省值必须匹配声明类型。
    for f in new.fields:
        if f.type not in _KNOWN_TYPES:
            return SchemaCheckResult(
                accepted=False,
                reason=SchemaRejectionReason.UNKNOWN_TYPE,
                field=f.name,
                detail=f"类型 {f.type!r} 不是受支持的类型（{sorted(_KNOWN_TYPES)}）",
            )
        if not _default_matches(f):
            return SchemaCheckResult(
                accepted=False,
                reason=SchemaRejectionReason.DEFAULT_TYPE_MISMATCH,
                field=f.name,
                detail=(
                    f"缺省值 {f.default!r} 与声明类型 {f.type!r} 不匹配，"
                    "历史行补齐后会违反结构"
                ),
            )

    for name in old_map:
        if name not in new_map:
            return SchemaCheckResult(
                accepted=False,
                reason=SchemaRejectionReason.FIELD_REMOVED,
                field=name,
                detail="不允许删除既有字段，历史数据必须保留",
            )

    for name, old_field in old_map.items():
        new_field = new_map[name]
        if old_field.type == new_field.type:
            continue
        if (old_field.type, new_field.type) in _NUMERIC_WIDENING:
            continue  # int -> float：历史 int 值在 float 下语义不变
        if old_field.type in ("int", "float") and new_field.type in ("int", "float"):
            return SchemaCheckResult(
                accepted=False,
                reason=SchemaRejectionReason.TYPE_NARROWED,
                field=name,
                detail=(
                    f"类型 {old_field.type!r} -> {new_field.type!r} 属于收窄，"
                    "会破坏既有数据"
                ),
            )
        return SchemaCheckResult(
            accepted=False,
            reason=SchemaRejectionReason.TYPE_INCOMPATIBLE,
            field=name,
            detail=(
                f"类型 {old_field.type!r} -> {new_field.type!r} 跨类型族，"
                "历史值类型与新声明对不上"
            ),
        )

    merged = []
    for old_field in old.fields:
        # 既有字段采用演进后的定义（可能已加宽），保持原有顺序。
        merged.append(new_map[old_field.name])
    for new_field in new.fields:
        if new_field.name not in old_map:
            merged.append(new_field)
    return SchemaCheckResult(accepted=True, merged_schema=Schema(fields=tuple(merged)))


def precheck_compatibility(old: Schema, new: Schema) -> SchemaCheckResult:
    """只检查不写入：返回 new 相对 old 的演进能否被接受及原因。"""
    return _evaluate(old, new)


def check_compatibility(old: Schema, new: Schema) -> Schema:
    """校验 new 相对 old 的演进是否兼容。

    兼容时返回合并后的结构（旧字段顺序保持，新字段追加）；
    不兼容时抛出 IncompatibleSchemaError（reason 可区分具体原因）。
    与 precheck_compatibility 共用同一份判定逻辑，结论必然一致。
    """
    result = _evaluate(old, new)
    if not result.accepted:
        raise IncompatibleSchemaError(result.reason, result.field, result.detail)
    return result.merged_schema
