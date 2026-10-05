"""结构预检：预检结论与真实提交一致；被拒绝的变更不留数据、不占版本。"""

import logging

import pytest

from lakehouse import (
    Field,
    IncompatibleSchemaError,
    Schema,
    SchemaRejectionReason,
    Table,
)

log = logging.getLogger("lakehouse-test")

BASE = Schema((Field("id", "int"), Field("name", "string")))


def _seed(t: Table) -> int:
    return t.commit_batch("seed", [{"id": 1, "name": "a"}])


def test_precheck_accept_then_commit_succeeds(table_dir):
    t = Table(table_dir, BASE)
    _seed(t)
    evolved = Schema((*BASE.fields, Field("score", "float", 0.0)))

    pre = t.check_schema(evolved)
    assert pre.accepted
    assert pre.merged_schema is not None
    log.info("预检放行: 新增字段 score 判定依据=缺省值 0.0 与 float 匹配")

    v = t.commit_batch("b2", [{"id": 2, "name": "b", "score": 9.5}], evolved)
    assert t.schema() == pre.merged_schema  # 提交后的结构与预检合并结果一致
    log.info("提交成功: version=%d 判定依据=预检结论与真实提交一致", v)


@pytest.mark.parametrize(
    ("new_schema", "reason"),
    [
        (Schema((Field("id", "int"),)), SchemaRejectionReason.FIELD_REMOVED),
        (
            Schema((Field("id", "int"), Field("name", "bool"))),
            SchemaRejectionReason.TYPE_INCOMPATIBLE,
        ),
        (
            Schema((Field("id", "string"), Field("name", "string"))),
            SchemaRejectionReason.TYPE_INCOMPATIBLE,  # int->string 不再放行
        ),
        (
            Schema((*BASE.fields, Field("tag", "int", "oops"))),
            SchemaRejectionReason.DEFAULT_TYPE_MISMATCH,
        ),
        (
            Schema((*BASE.fields, Field("ts", "date"))),
            SchemaRejectionReason.UNKNOWN_TYPE,
        ),
    ],
    ids=[
        "field_removed",
        "cross_family",
        "int_to_string",
        "bad_default",
        "unknown_type",
    ],
)
def test_precheck_reject_then_commit_rejected_same_reason(
    table_dir, new_schema, reason
):
    """预检拒绝的原因与真实提交抛出的原因一致，且拒绝不留任何残留。"""
    t = Table(table_dir, BASE)
    v1 = _seed(t)

    pre = t.check_schema(new_schema)
    assert not pre.accepted
    assert pre.reason == reason
    log.info(
        "预检拒绝: 原因=%s 字段=%s 判定依据=%s", pre.reason.value, pre.field, pre.detail
    )

    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"id": 2, "name": "b"}], new_schema)
    assert exc.value.reason == pre.reason  # 预检与提交同一判定逻辑
    log.info("提交拒绝: batch=b2 原因=%s 与预检一致", exc.value.reason.value)

    # 不留任何残留：版本不变、无新数据文件、数据不变、批次号可重试。
    assert t.current_version() == v1
    assert not t.storage.version_path(v1 + 1).exists()
    assert t.snapshot() == [{"id": 1, "name": "a"}]
    status = t.batch_status("b2")
    assert status.status == "rejected"
    assert status.reason == reason.value
    log.info(
        "残留检查: version=%d 无 v%d 数据文件 对账状态=rejected(%s)",
        t.current_version(),
        v1 + 1,
        status.reason,
    )


@pytest.mark.parametrize(
    ("stored", "declared", "reason"),
    [
        ("int", "float", SchemaRejectionReason.TYPE_MISMATCH),
        ("float", "int", SchemaRejectionReason.TYPE_NARROWED),
        ("int", "string", SchemaRejectionReason.TYPE_INCOMPATIBLE),
        ("string", "int", SchemaRejectionReason.TYPE_INCOMPATIBLE),
        ("string", "bool", SchemaRejectionReason.TYPE_INCOMPATIBLE),
        ("bool", "string", SchemaRejectionReason.TYPE_INCOMPATIBLE),
    ],
    ids=[
        "int_to_float",
        "float_to_int",
        "int_to_string",
        "string_to_int",
        "string_to_bool",
        "bool_to_string",
    ],
)
def test_any_existing_column_type_change_rejected_with_distinct_reason(
    table_dir, stored, declared, reason
):
    """已存字段的任何类型变更都在写入前拒绝，且原因彼此可区分。"""
    t = Table(table_dir, Schema((Field("v", stored),)))
    t.commit_batch("b1", [{"v": None}])

    pre = t.check_schema(Schema((Field("v", declared),)))
    assert not pre.accepted
    assert pre.reason == reason
    assert pre.field == "v"
    log.info(
        "预检拒绝: 字段类型 %s->%s 原因=%s 判定依据=%s",
        stored,
        declared,
        pre.reason.value,
        pre.detail,
    )

    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"v": None}], Schema((Field("v", declared),)))
    assert exc.value.reason == reason

    # 拒绝不留数据、不占版本；同批次号再来一次不受失败影响。
    assert t.current_version() == 1
    assert not t.storage.version_path(2).exists()
    v = t.commit_batch("b2", [{"v": None}])  # 不带结构，沿用原结构
    assert v == 2
    log.info("拒绝后同号重试: batch=b2 version=%d 判定依据=拒绝记录不参与提交判定", v)


def test_int_to_float_rejected_precheck_matches_commit(table_dir):
    """int -> float 拒绝：历史行读出来仍是 int，与声明 float 对不上；
    预检与提交给出同一个原因。"""
    t = Table(table_dir, Schema((Field("v", "int"),)))
    t.commit_batch("b1", [{"v": 1}])
    widened = Schema((Field("v", "float"),))
    pre = t.check_schema(widened)
    assert not pre.accepted
    assert pre.reason == SchemaRejectionReason.TYPE_MISMATCH
    log.info(
        "预检拒绝: int->float 原因=%s 字段=%s 判定依据=%s",
        pre.reason.value,
        pre.field,
        pre.detail,
    )
    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"v": 2.5}], widened)
    assert exc.value.reason == pre.reason
    # 不留任何残留。
    assert t.current_version() == 1
    assert not t.storage.version_path(2).exists()
    assert t.snapshot() == [{"v": 1}]
    log.info("提交拒绝: batch=b2 原因=%s 与预检一致, 无残留", exc.value.reason.value)


def test_float_to_int_narrowing_rejected(table_dir):
    t = Table(table_dir, Schema((Field("v", "float"),)))
    t.commit_batch("b1", [{"v": 1.5}])
    pre = t.check_schema(Schema((Field("v", "int"),)))
    assert not pre.accepted
    assert pre.reason == SchemaRejectionReason.TYPE_NARROWED
    log.info("收窄拒绝: float->int 原因=%s", pre.reason.value)


def test_precheck_does_not_change_state(table_dir):
    """预检本身纯只读：连续预检后版本、结构、数据完全不变。"""
    t = Table(table_dir, BASE)
    v1 = _seed(t)
    t.check_schema(Schema((Field("id", "int"),)))  # 会被拒绝的预检
    t.check_schema(Schema((*BASE.fields, Field("x", "int", 0))))  # 会放行的预检
    assert t.current_version() == v1
    assert t.schema() == BASE
    assert t.snapshot() == [{"id": 1, "name": "a"}]
    assert len(t.storage.read_log()) == 1
    log.info("预检无副作用: version=%d 结构未变 日志条目=1", v1)
