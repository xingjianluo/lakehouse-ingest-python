"""结构演进：新增字段缺省语义稳定；删除/收窄/不兼容变更被可区分地拒绝。"""

import logging

import pytest

from lakehouse import (
    Field,
    IncompatibleSchemaError,
    RecordValidationError,
    Schema,
    SchemaRejectionReason,
    Table,
)

log = logging.getLogger("lakehouse-test")

BASE = Schema((Field("id", "int"), Field("name", "string")))


def test_add_field_with_default_applies_to_history(table_dir):
    t = Table(table_dir, BASE)
    v1 = t.commit_batch("b1", [{"id": 1, "name": "a"}])

    evolved = Schema((
        Field("id", "int"),
        Field("name", "string"),
        Field("score", "float", 0.0),
    ))
    v2 = t.commit_batch("b2", [{"id": 2, "name": "b", "score": 9.5}], evolved)
    log.info("结构演进提交: version=%d batch=b2 判定依据=新增字段 score 缺省值 0.0", v2)

    rows = t.snapshot()
    # 历史数据稳定生效缺省值，新数据保留原值。
    assert rows == [
        {"id": 1, "name": "a", "score": 0.0},
        {"id": 2, "name": "b", "score": 9.5},
    ]
    # 历史快照不受演进影响，可复现。
    assert t.snapshot(v1) == [{"id": 1, "name": "a"}]
    log.info("校验通过: 历史行缺省值=%s 历史快照版本=%d 可复现", rows[0]["score"], v1)


def test_new_field_default_fills_missing_values(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    evolved = Schema((*BASE.fields, Field("tag", "string", "untagged")))
    # 新批次记录不携带新字段 -> 自动补缺省值。
    t.commit_batch("b2", [{"id": 2, "name": "b"}], evolved)
    assert t.snapshot()[1]["tag"] == "untagged"
    log.info("缺省语义: 缺失字段 tag 填充缺省值=untagged 判定依据=字段定义 default")


def test_remove_field_rejected(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    bad = Schema((Field("id", "int"),))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"id": 2}], bad)
    assert exc.value.reason == SchemaRejectionReason.FIELD_REMOVED
    assert exc.value.field == "name"
    log.info("拒绝: batch=b2 原因=%s 字段=%s 判定依据=不允许删除既有字段",
             exc.value.reason.value, exc.value.field)
    # 失败不产生任何写入：版本与数据不变。
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}]


def test_existing_field_type_change_rejected_even_when_widening(table_dir):
    """新规则：既有字段任何类型变更都拒绝，包括 int->float 这类加宽。

    原因：声明类型一变，历史行原值的语义就与新声明对不上。
    """
    t = Table(table_dir, Schema((Field("v", "int"),)))
    t.commit_batch("b1", [{"v": 1}])
    bad = Schema((Field("v", "float"),))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"v": 2.5}], bad)
    assert exc.value.reason == SchemaRejectionReason.TYPE_CHANGED
    assert exc.value.field == "v"
    log.info("拒绝: batch=b2 原因=%s 字段=v 判定依据=既有字段类型一律不得变更(int->float)",
             exc.value.reason.value)
    assert t.current_version() == 1
    assert t.snapshot() == [{"v": 1}]


def test_type_narrowing_rejected(table_dir):
    t = Table(table_dir, Schema((Field("v", "float"),)))
    t.commit_batch("b1", [{"v": 1.5}])
    bad = Schema((Field("v", "int"),))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"v": 2}], bad)
    assert exc.value.reason == SchemaRejectionReason.TYPE_CHANGED
    log.info("拒绝: batch=b2 原因=%s 字段=v 判定依据=float->int 属于既有字段类型变更",
             exc.value.reason.value)
    assert t.current_version() == 1


def test_incompatible_type_change_rejected(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    bad = Schema((Field("id", "int"), Field("name", "bool")))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"id": 2, "name": True}], bad)
    assert exc.value.reason == SchemaRejectionReason.TYPE_CHANGED
    log.info("拒绝: batch=b2 原因=%s 字段=name 判定依据=string->bool 既有字段类型变更",
             exc.value.reason.value)
    assert t.current_version() == 1


def test_new_field_default_must_match_type(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    bad = Schema((*BASE.fields, Field("score", "int", "not-an-int")))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"id": 2, "name": "b"}], bad)
    assert exc.value.reason == SchemaRejectionReason.DEFAULT_TYPE_MISMATCH
    assert exc.value.field == "score"
    log.info("拒绝: batch=b2 原因=%s 字段=score 判定依据=缺省值与声明类型不匹配",
             exc.value.reason.value)
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}]


def test_new_nullable_field_allowed(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    evolved = Schema((*BASE.fields, Field("note", "string", None)))
    v2 = t.commit_batch("b2", [{"id": 2, "name": "b"}], evolved)
    log.info("放行: version=%d batch=b2 判定依据=新增可空字段 default=None", v2)
    assert t.snapshot() == [
        {"id": 1, "name": "a", "note": None},
        {"id": 2, "name": "b", "note": None},
    ]


def test_record_value_type_mismatch_rejected(table_dir):
    t = Table(table_dir, BASE)
    with pytest.raises(RecordValidationError):
        t.commit_batch("b1", [{"id": "not-an-int", "name": "a"}])
    assert t.current_version() == 0
    log.info("拒绝: batch=b1 判定依据=记录值类型与结构不匹配, 未产生提交")


def test_unknown_field_in_record_rejected(table_dir):
    t = Table(table_dir, BASE)
    with pytest.raises(RecordValidationError):
        t.commit_batch("b1", [{"id": 1, "name": "a", "ghost": 1}])
    assert t.current_version() == 0
    log.info("拒绝: batch=b1 判定依据=记录包含未知字段 ghost, 不静默丢弃")
