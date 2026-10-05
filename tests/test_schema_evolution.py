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

    evolved = Schema(
        (
            Field("id", "int"),
            Field("name", "string"),
            Field("score", "float", 0.0),
        )
    )
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
    log.info(
        "拒绝: batch=b2 原因=%s 字段=%s 判定依据=不允许删除既有字段",
        exc.value.reason.value,
        exc.value.field,
    )
    # 失败不产生任何写入：版本与数据不变。
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}]


def test_type_narrowing_rejected(table_dir):
    t = Table(table_dir, Schema((Field("v", "float"),)))
    t.commit_batch("b1", [{"v": 1.5}])
    bad = Schema((Field("v", "int"),))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"v": 2}], bad)
    assert exc.value.reason == SchemaRejectionReason.TYPE_NARROWED
    log.info(
        "拒绝: batch=b2 原因=%s 字段=v 判定依据=float->int 属于类型收窄",
        exc.value.reason.value,
    )
    assert t.current_version() == 1


def test_incompatible_type_change_rejected(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    bad = Schema((Field("id", "int"), Field("name", "bool")))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"id": 2, "name": True}], bad)
    assert exc.value.reason == SchemaRejectionReason.TYPE_INCOMPATIBLE
    log.info(
        "拒绝: batch=b2 原因=%s 字段=name 判定依据=string->bool 跨类型族",
        exc.value.reason.value,
    )
    assert t.current_version() == 1


def test_type_widening_rejected(table_dir):
    """int -> float 也拒绝：历史行读出来仍是整数，与声明的 float 对不上。"""
    t = Table(table_dir, Schema((Field("v", "int"),)))
    t.commit_batch("b1", [{"v": 1}])
    widened = Schema((Field("v", "float"),))
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"v": 2.5}], widened)
    assert exc.value.reason == SchemaRejectionReason.TYPE_WIDENED
    assert exc.value.field == "v"
    log.info(
        "拒绝: batch=b2 原因=%s 字段=v 判定依据=int->float 历史整数行与声明类型对不上",
        exc.value.reason.value,
    )
    # 被拒绝的提交不留任何残留：版本、数据、结构均不变。
    assert t.current_version() == 1
    assert t.snapshot() == [{"v": 1}]
    assert t.schema() == Schema((Field("v", "int"),))
    assert not t.storage.version_path(2).exists()


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


def test_float_field_int_values_normalized_to_float(table_dir):
    """float 字段的整数值/整型缺省值在写入时规范化为浮点。

    保证落盘数据的类型与声明结构永远对得上：历史行补出的缺省值、
    本批写入的整数值，读回来都是浮点。
    """
    t = Table(table_dir, Schema((Field("id", "int"),)))
    t.commit_batch("b1", [{"id": 1}])
    evolved = Schema((Field("id", "int"), Field("ratio", "float", 1)))
    t.commit_batch("b2", [{"id": 2, "ratio": 2}], evolved)

    rows = t.snapshot()
    assert rows[0]["ratio"] == 1.0 and isinstance(rows[0]["ratio"], float)
    assert rows[1]["ratio"] == 2.0 and isinstance(rows[1]["ratio"], float)
    log.info(
        "类型规范化: 历史行缺省值=%r 本批整数值=%r 判定依据=float 字段落盘一律为浮点",
        rows[0]["ratio"],
        rows[1]["ratio"],
    )
