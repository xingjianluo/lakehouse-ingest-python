"""湖仓摄取服务的 HTTP 入口：批次提交、批次对账、结构预检、快照读取。"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic import Field as PydanticField

from lakehouse import (
    BatchConflictError,
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


class SchemaCheckRequest(BaseModel):
    table_schema: list[FieldModel] = PydanticField(alias="schema")


def _to_schema(fields: list[FieldModel]) -> Schema:
    return Schema(tuple(Field(f.name, f.type, f.default) for f in fields))


@app.post("/batches", status_code=201)
def commit_batch(req: CommitRequest):
    schema = _to_schema(req.table_schema) if req.table_schema is not None else None
    # 提交前已提交过同号批次 -> 本次为幂等重试（响应里标注，状态码 200）。
    prior = table.batch_status(req.batch_id)
    try:
        version = table.commit_batch(req.batch_id, req.records, schema)
    except BatchConflictError as e:
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
        raise HTTPException(
            422, detail={"error": "record_validation", "message": str(e)}
        ) from e
    deduplicated = prior.status == "committed" and prior.version == version
    return {"version": version, "batch_id": req.batch_id, "deduplicated": deduplicated}


@app.get("/batches/{batch_id}")
def get_batch_status(batch_id: str):
    """批次对账：成功落在哪个版本/多少条，或失败原因；未知批次 404。"""
    status = table.batch_status(batch_id)
    if status.status == "unknown":
        raise HTTPException(
            404, detail={"error": "batch_not_found", "batch_id": batch_id}
        )
    body = {"batch_id": status.batch_id, "status": status.status}
    if status.status == "committed":
        body.update(version=status.version, record_count=status.record_count)
    else:
        body.update(reason=status.reason, detail=status.detail)
    return body


@app.post("/schema/check")
def check_schema(req: SchemaCheckRequest):
    """结构预检：只检查不写入，返回能否接受及可区分的原因。"""
    result = table.check_schema(_to_schema(req.table_schema))
    body = {"accepted": result.accepted}
    if not result.accepted:
        body.update(
            reason=result.reason.value, field=result.field, detail=result.detail
        )
    return body


@app.get("/snapshot")
def read_snapshot(version: int | None = None):
    try:
        rows = table.snapshot(version)
    except VersionNotFoundError as e:
        raise HTTPException(
            404, detail={"error": "version_not_found", "version": e.version}
        ) from e
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
