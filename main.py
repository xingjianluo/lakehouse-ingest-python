"""湖仓摄取服务的 HTTP 入口：批次提交、对账查询、结构预检、快照读取。"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic import Field as PydanticField

from lakehouse import (
    BatchConflictError,
    DuplicateBatchError,
    Field,
    IncompatibleSchemaError,
    RecordValidationError,
    Schema,
    Table,
    VersionExpiredError,
    VersionNotFoundError,
)

TABLE_ROOT = Path(os.environ.get("LAKEHOUSE_TABLE_ROOT", "./data/orders"))

app = FastAPI(title="lakehouse-ingest")
table = Table(TABLE_ROOT)


class FieldModel(BaseModel):
    name: str
    type: str
    default: object = None


class CommitRequest(BaseModel):
    batch_id: str
    records: list[dict]
    table_schema: list[FieldModel] | None = PydanticField(default=None, alias="schema")


def _schema_from(models: list[FieldModel] | None) -> Schema | None:
    if models is None:
        return None
    return Schema(tuple(Field(f.name, f.type, f.default) for f in models))


@app.post("/batches", status_code=201)
def commit_batch(req: CommitRequest):
    schema = _schema_from(req.table_schema)
    try:
        version = table.commit_batch(req.batch_id, req.records, schema)
    except DuplicateBatchError as e:
        # 幂等重试：内容一致，直接把首次结果还给调用方，未重复写入。
        raise HTTPException(
            409,
            detail={
                "error": "duplicate_batch",
                "batch_id": e.batch_id,
                "committed_version": e.committed_version,
                "record_count": e.record_count,
                "idempotent": True,
            },
        ) from e
    except BatchConflictError as e:
        # 同批次号内容不一致：明确的程序化失败，指出已落版本。
        raise HTTPException(
            409,
            detail={
                "error": "batch_conflict",
                "batch_id": e.batch_id,
                "committed_version": e.committed_version,
            },
        ) from e
    except IncompatibleSchemaError as e:
        raise HTTPException(
            422,
            detail={
                "error": "incompatible_schema",
                "reason": e.reason.value,
                "field": e.field,
            },
        ) from e
    except RecordValidationError as e:
        raise HTTPException(422, detail={"error": "record_validation", "message": str(e)}) from e
    return {"version": version, "batch_id": req.batch_id}


@app.get("/batches/{batch_id}")
def get_batch(batch_id: str):
    """批次对账查询：成功落点（版本、条数）或历次被拒原因；未知批次 404。"""
    status = table.batch_status(batch_id)
    if status.state == "unknown":
        raise HTTPException(
            404, detail={"error": "batch_unknown", "batch_id": batch_id}
        )
    return {
        "batch_id": status.batch_id,
        "state": status.state,
        "version": status.version,
        "record_count": status.batch_size,
        "rejections": [
            {
                "reason": r.reason,
                "detail": r.detail,
                "rejected_at": r.rejected_at,
                "field": r.field,
                "schema_reason": r.schema_reason,
                "committed_version": r.committed_version,
            }
            for r in status.rejections
        ],
    }


@app.post("/schema/precheck")
def schema_precheck(req: CommitRequest):
    """结构预检：只检查不写数据，结论与真正提交时的结构判定一致。"""
    if req.table_schema is None:
        raise HTTPException(
            422, detail={"error": "record_validation", "message": "schema 不能为空"}
        )
    result = table.precheck_schema(_schema_from(req.table_schema))
    return {
        "compatible": result.compatible,
        "reason": result.reason.value if result.reason else None,
        "field": result.field,
        "detail": result.detail,
    }


@app.get("/snapshot")
def read_snapshot(version: int | None = None):
    try:
        rows = table.snapshot(version)
    except VersionNotFoundError as e:
        raise HTTPException(404, detail={"error": "version_not_found", "version": e.version}) from e
    except VersionExpiredError as e:
        raise HTTPException(
            410,
            detail={
                "error": "version_expired",
                "version": e.version,
                "min_available": e.min_available,
            },
        ) from e
    return {"version": version or table.current_version(), "records": rows}


@app.get("/version")
def current_version():
    return {"version": table.current_version()}


@app.get("/")
def read_root():
    return {"service": "lakehouse-ingest", "table_root": str(TABLE_ROOT)}
