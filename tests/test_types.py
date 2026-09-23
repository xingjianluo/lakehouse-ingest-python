"""类型与结构演进规则测试。"""

from __future__ import annotations

import pytest

from lakehouse.errors import SchemaIncompatibleError as E
from lakehouse.types import Column, LogicalType as T, Schema, ensure_compatible, validate_value


# ---------------------------------------------------------------- values
@pytest.mark.parametrize(
    "lt,value",
    [
        (T.LONG, 1),
        (T.LONG, None),
        (T.DOUBLE, 1),
        (T.DOUBLE, 1.5),
        (T.STRING, "x"),
        (T.BOOLEAN, True),
        (T.TIMESTAMP, 1_700_000_000_000_000),
    ],
)
def test_validate_value_ok(lt, value):
    validate_value(Column("c", lt, nullable=True), value)


@pytest.mark.parametrize(
    "lt,value",
    [
        (T.LONG, 1.5),
        (T.LONG, True),
        (T.LONG, "1"),
        (T.LONG, 2**63),
        (T.DOUBLE, "1"),
        (T.DOUBLE, float("nan")),
        (T.STRING, 1),
        (T.BOOLEAN, 1),
        (T.TIMESTAMP, 1.2),
    ],
)
def test_validate_value_mismatch(lt, value):
    with pytest.raises(E) as ei:
        validate_value(Column("c", lt), value)
    assert ei.value.sub_reason == E.RECORD_TYPE_MISMATCH
    assert ei.value.field == "c"


def test_null_rejected_for_required_column():
    with pytest.raises(E) as ei:
        validate_value(Column("c", T.STRING, nullable=False), None)
    assert ei.value.sub_reason == E.RECORD_TYPE_MISMATCH


# ------------------------------------------------------- compatibility
def test_long_to_double_widening_allowed():
    ensure_compatible(
        Schema((Column("id", T.LONG),)),
        Schema((Column("id", T.DOUBLE),)),
    )


def test_double_to_long_is_distinguished_as_narrowing():
    with pytest.raises(E) as ei:
        ensure_compatible(
            Schema((Column("id", T.DOUBLE),)),
            Schema((Column("id", T.LONG),)),
        )
    assert ei.value.sub_reason == E.TYPE_NARROWED
    assert ei.value.field == "id"


def test_unrelated_type_change_is_distinguished_as_changed():
    with pytest.raises(E) as ei:
        ensure_compatible(
            Schema((Column("id", T.LONG),)),
            Schema((Column("id", T.STRING),)),
        )
    assert ei.value.sub_reason == E.TYPE_CHANGED


def test_append_nullable_column_allowed():
    ensure_compatible(
        Schema((Column("id", T.LONG),)),
        Schema((Column("id", T.LONG), Column("tag", T.STRING))),
    )


def test_append_column_with_default_allowed():
    ensure_compatible(
        Schema((Column("id", T.LONG),)),
        Schema(
            (Column("id", T.LONG), Column("n", T.LONG, nullable=False, default_value=0),)
        ),
    )


def test_drop_column_rejected_with_field_name():
    old = Schema((Column("id", T.LONG), Column("tag", T.STRING)))
    with pytest.raises(E) as ei:
        ensure_compatible(old, Schema((Column("id", T.LONG),)))
    assert ei.value.sub_reason == E.FIELD_REMOVED
    assert ei.value.field == "tag"


def test_column_inserted_in_middle_rejected():
    with pytest.raises(E) as ei:
        ensure_compatible(
            Schema((Column("id", T.LONG),)),
            Schema((Column("tag", T.STRING), Column("id", T.LONG))),
        )
    assert ei.value.sub_reason == E.FIELD_REMOVED


def test_new_required_column_without_default_rejected():
    with pytest.raises(E) as ei:
        ensure_compatible(
            Schema((Column("id", T.LONG),)),
            Schema((Column("id", T.LONG), Column("n", T.STRING, nullable=False))),
        )
    assert ei.value.sub_reason == E.NEW_COLUMN_MISSING_DEFAULT
    assert ei.value.field == "n"


def test_nullability_narrowing_rejected():
    with pytest.raises(E) as ei:
        ensure_compatible(
            Schema((Column("id", T.LONG, nullable=True),)),
            Schema((Column("id", T.LONG, nullable=False),)),
        )
    assert ei.value.sub_reason == E.NULLABLE_NARROWED


def test_effective_default_is_null_for_nullable_without_default():
    col = Column("n", T.STRING)
    assert Schema(()).effective_default(col) is None


def test_effective_default_uses_explicit_value():
    col = Column("n", T.LONG, nullable=False, default_value=7)
    assert Schema(()).effective_default(col) == 7


def test_schema_round_trip():
    s = Schema((Column("id", T.LONG, nullable=False, default_value=0),))
    s2 = Schema.from_dict(s.to_dict())
    assert s2.to_dict() == s.to_dict()
