"""结构预检：只检查不写数据；预检结论与真正提交一致；拒绝无残留。"""

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


def test_precheck_does_not_write_anything(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])

    evolved = Schema((
        Field("id", "int"),
        Field("name", "string"),
        Field("score", "float", 0.0),
    ))
    result = t.precheck_schema(evolved)
    assert result.compatible is True
    assert result.merged_schema is not None
    assert [f.name for f in result.merged_schema.fields] == ["id", "name", "score"]
    log.info("预检放行: batch未提交 判定依据=仅追加新字段 score, 预检不写数据")

    # 预检零副作用。
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}]
    assert t.batch_status("b1").state == "committed"
    assert t.storage.read_rejected() == []
    log.info("预检零副作用: version=1 日志=1 被拒台账=0")


@pytest.mark.parametrize(
    "bad_schema,expected_reason,field",
    [
        (Schema((Field("id", "int"),)), SchemaRejectionReason.FIELD_REMOVED, "name"),
        (Schema((Field("id", "float"), Field("name", "string"))),
         SchemaRejectionReason.TYPE_CHANGED, "id"),
        (Schema((Field("id", "int"), Field("name", "string"),
                 Field("tag", "int", "x"))),
         SchemaRejectionReason.DEFAULT_TYPE_MISMATCH, "tag"),
    ],
)
def test_precheck_rejection_matches_commit(table_dir, bad_schema, expected_reason, field):
    """同一条结构变更：预检结论与真正提交的结果必须一致（同因同字段）。"""
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])

    pre = t.precheck_schema(bad_schema)
    assert pre.compatible is False
    assert pre.reason == expected_reason
    assert pre.field == field
    log.info("预检拒绝: 原因=%s 字段=%s 判定依据=%s", pre.reason.value, pre.field, pre.detail)

    with pytest.raises(IncompatibleSchemaError) as exc:
        t.commit_batch("b2", [{"id": 2}], bad_schema)
    assert exc.value.reason == expected_reason
    assert exc.value.field == field
    log.info("提交拒绝一致: batch=b2 原因=%s 字段=%s 判定依据=预检与提交同一检查函数",
             exc.value.reason.value, exc.value.field)


def test_rejected_schema_change_leaves_no_residue(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    files_before = sorted(p.name for p in t.storage.data_dir.iterdir())

    bad = Schema((Field("id", "int"), Field("name", "bool")))
    for attempt in range(3):
        with pytest.raises(IncompatibleSchemaError):
            t.commit_batch("b2", [{"id": 2, "name": True}], bad)
    log.info("连续拒绝 batch=b2 三次: 原因=type_changed, 数据文件与版本均不得增长")

    # 无数据残留、无版本占用、临时文件也不留下。
    assert t.current_version() == 1
    assert t.snapshot() == [{"id": 1, "name": "a"}]
    files_after = sorted(p.name for p in t.storage.data_dir.iterdir())
    assert files_after == files_before
    assert all(not n.startswith(".tmp-") for n in files_after)
    assert len(t.storage.read_log()) == 1
    log.info("无残留校验: 版本=1 数据文件=%s 无 .tmp- 文件", files_after)

    # 失败不影响判定：同批次随后用兼容结构可以正常提交。
    ok_schema = Schema((
        Field("id", "int"), Field("name", "string"), Field("note", "string", None),
    ))
    v2 = t.commit_batch("b2", [{"id": 2, "name": "b"}], ok_schema)
    assert v2 == 2
    assert t.snapshot() == [
        {"id": 1, "name": "a", "note": None},
        {"id": 2, "name": "b", "note": None},
    ]
    log.info("同批次兼容重试成功: batch=b2 version=2 判定依据=被拒台账不影响提交判定")


def test_precheck_then_commit_consistent_acceptance(table_dir):
    t = Table(table_dir, BASE)
    t.commit_batch("b1", [{"id": 1, "name": "a"}])
    evolved = Schema((*BASE.fields, Field("note", "string", "n/a")))

    pre = t.precheck_schema(evolved)
    assert pre.compatible is True
    v2 = t.commit_batch("b2", [{"id": 2, "name": "b"}], evolved)
    assert v2 == 2
    log.info("预检放行->提交放行: batch=b2 version=%d 判定依据=预检与提交结论一致", v2)
