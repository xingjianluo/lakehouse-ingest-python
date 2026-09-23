"""湖仓摄取服务的 HTTP 入口：批次提交、快照读取与时间旅行。"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field as PydanticField

from lakehouse import (
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


@app.post("/batches", status_code=201)
def commit_batch(req: CommitRequest):
    schema = (
        Schema(tuple(Field(f.name, f.type, f.default) for f in req.table_schema))
        if req.table_schema is not None
        else None
    )
    try:
        version = table.commit_batch(req.batch_id, req.records, schema)
    except DuplicateBatchError as e:
        raise HTTPException(
            409,
            detail={
                "error": "duplicate_batch",
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
