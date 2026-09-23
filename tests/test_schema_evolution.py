"""提交链路上的结构演进端到端测试。"""

from __future__ import annotations

import pytest

from lakehouse.errors import SchemaIncompatibleError
from lakehouse.table import LakehouseTable

BASE = {"columns": [{"name": "id", "type": "long", "nullable": False}]}


@pytest.fixture
def table(tmp_data_root):
    return LakehouseTable(tmp_data_root, "events")


def test_add_nullable_column_defaults_history_to_null(table):
    table.commit("b1", [{"id": 1}, {"id": 2}], BASE)
    evolved = {"columns": BASE["columns"] + [{"name": "tag", "type": "string"}]}
    table.commit("b2", [{"id": 3, "tag": "new"}], evolved)
    cur = table.read()
    assert cur["records"][0]["tag"] is None
    assert cur["records"][1]["tag"] is None
    assert cur["records"][2]["tag"] == "new"
    # 历史快照不受结构演进影响
    assert "tag" not in table.read(1)["records"][0]


def test_add_column_with_explicit_default_backfills_stable_value(table):
    table.commit("b1", [{"id": 1}], BASE)
    evolved = {
        "columns": BASE["columns"]
        + [{"name": "score", "type": "long", "nullable": False, "default": 100}]
    }
    table.commit("b2", [{"id": 2, "score": 7}], evolved)
    rows = table.read()["records"]
    assert rows[0]["score"] == 100  # 历史行获得稳定缺省
    assert rows[1]["score"] == 7
    # 新批次记录省略该列时也取默认值
    table.commit("b3", [{"id": 3}], evolved)
    assert table.read(3)["records"][-1]["score"] == 100


def test_widening_long_to_double_propagates_to_history(table):
    schema = {"columns": [{"name": "id", "type": "long", "nullable": False},
                          {"name": "v", "type": "long"}]}
    table.commit("b1", [{"id": 1, "v": 5}], schema)
    widened = {"columns": [{"name": "id", "type": "long", "nullable": False},
                           {"name": "v", "type": "double"}]}
    table.commit("b2", [{"id": 2, "v": 6.5}], widened)
    rows = table.read(2)["records"]
    assert isinstance(rows[0]["v"], float) and rows[0]["v"] == 5.0
    assert rows[1]["v"] == 6.5
    # 老快照保留原始整型，未被改写
    assert table.read(1)["records"][0]["v"] == 5
    assert isinstance(table.read(1)["records"][0]["v"], int)


def test_drop_column_rejected_and_data_untouched(table):
    with_extra = {"columns": BASE["columns"] + [{"name": "tag", "type": "string"}]}
    table.commit("b1", [{"id": 1, "tag": "a"}], with_extra)
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b2", [{"id": 2}], BASE)  # 试图删除 tag
    assert ei.value.sub_reason == SchemaIncompatibleError.FIELD_REMOVED
    assert ei.value.field == "tag"
    assert table.current_version() == 1
    assert table.read(1)["records"][0] == {"id": 1, "tag": "a"}


def test_narrowing_rejected_with_distinct_reason(table):
    table.commit("b1", [{"id": 1}], BASE)
    changed = {"columns": [{"name": "id", "type": "string", "nullable": False}]}
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b2", [{"id": "2"}], changed)
    assert ei.value.sub_reason == SchemaIncompatibleError.TYPE_CHANGED
    assert table.current_version() == 1


def test_no_schema_arg_reuses_current_schema(table):
    table.commit("b1", [{"id": 1}], BASE)
    e = table.commit("b2", [{"id": 2}])
    assert e.version == 2
    assert [r["id"] for r in table.read()["records"]] == [1, 2]


def test_incompatible_change_aborts_before_any_data_file_published(table):
    table.commit("b1", [{"id": 1}], BASE)
    # id: NOT NULL -> nullable 是合法放宽；但记录给 None 仍违反“提交结构”，
    # 必须在发布任何版本文件之前按记录类型错误整体拒绝。
    widened_null = {"columns": [{"name": "id", "type": "long", "nullable": True}]}
    with pytest.raises(SchemaIncompatibleError) as ei:
        # 中间插入新列（非尾部追加）必须在发布任何版本前拒绝
        table.commit(
            "b2",
            [{"id": None, "other": 1}],
            {"columns": [
                {"name": "other", "type": "long"},
                {"name": "id", "type": "long", "nullable": False},
            ]},
        )
    assert ei.value.sub_reason == SchemaIncompatibleError.FIELD_REMOVED
    assert table.current_version() == 1
    # 合法放宽可提交
    table.commit("b2", [{"id": 2}], widened_null)
    assert table.current_version() == 2
    # 放宽后的结构允许 None
    table.commit("b3", [{"id": None}], widened_null)
    assert table.current_version() == 3
    # 试图收回 nullable -> 明确拒绝（收窄），不产生新版本
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b4", [{"id": 4}], BASE)
    assert ei.value.sub_reason == SchemaIncompatibleError.NULLABLE_NARROWED
    assert table.current_version() == 3


def test_first_commit_requires_non_empty_schema(table):
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b1", [])
    assert ei.value.sub_reason == SchemaIncompatibleError.EMPTY_SCHEMA
    with pytest.raises(SchemaIncompatibleError) as ei:
        table.commit("b1", [], {"columns": []})
    assert ei.value.sub_reason == SchemaIncompatibleError.EMPTY_SCHEMA
    assert table.current_version() == 0
