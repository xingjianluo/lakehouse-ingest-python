"""FastAPI HTTP 适配层：提交、快照读取、历史、过期。"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    BatchConflictError,
    LakehouseError,
    SchemaIncompatibleError,
    VersionExpiredError,
    VersionNotFoundError,
)
from .table import ConcurrentCommitError, LakehouseTable

# 各类领域错误对应的 HTTP 状态码（错误体仍带稳定 reason/sub_reason）
_STATUS_BY_REASON: dict[str, int] = {
    "table_not_found": 404,
    "version_not_found": 404,
    "version_expired": 410,
    "batch_conflict": 409,
    "concurrent_modification": 409,
    "schema_incompatible": 422,
    "lakehouse_error": 400,
}


class CommitRequest(BaseModel):
    batch_id: str = Field(..., min_length=1)
    records: list[dict[str, Any]]
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    base_version: int | None = None

    model_config = {"populate_by_name": True}


def create_app(data_root: str = "./.lakehouse-data") -> FastAPI:
    """构造 FastAPI 应用。表在请求时惰性打开。"""
    app = FastAPI(title="local-lakehouse-ingest", version="0.1.0")
    app.state.data_root = data_root

    def open_table(table: str) -> LakehouseTable:
        return LakehouseTable(app.state.data_root, table)

    def error_response(exc: LakehouseError) -> JSONResponse:
        body: dict[str, Any] = {
            "error": exc.reason,
            "message": exc.message,
            "table": exc.table,
        }
        if isinstance(exc, SchemaIncompatibleError):
            body["sub_reason"] = exc.sub_reason
            body["field"] = exc.field
        status = _STATUS_BY_REASON.get(exc.reason, 400)
        return JSONResponse(status_code=status, content=body)

    @app.exception_handler(LakehouseError)
    async def _handle(_request, exc: LakehouseError):  # noqa: ANN202
        return error_response(exc)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tables/{table}/commits")
    def commit(table: str, req: CommitRequest) -> dict[str, Any]:
        entry = open_table(table).commit(
            batch_id=req.batch_id,
            records=req.records,
            new_schema=req.schema_,
            base_version=req.base_version,
        )
        return {
            "table": table,
            "version": entry.version,
            "batch_id": entry.batch_id,
            "record_count": entry.record_count,
            "parent_version": entry.parent_version,
            "created_at": entry.created_at,
            "decision": "accept",
        }

    @app.get("/tables/{table}/snapshots/{version:int}")
    def read_version(table: str, version: int) -> dict[str, Any]:
        return open_table(table).read(version)

    @app.get("/tables/{table}/current")
    def read_current(table: str) -> dict[str, Any]:
        return open_table(table).read()

    @app.get("/tables/{table}/history")
    def history(table: str) -> dict[str, Any]:
        return {"table": table, "commits": open_table(table).history()}

    @app.post("/tables/{table}/expire")
    def expire(table: str, retain: int = 1) -> dict[str, Any]:
        expired = open_table(table).expire_versions(retain)
        return {"table": table, "expired_versions": expired}

    return app


app = create_app()


__all__ = ["create_app", "app"]
