"""服务入口：uvicorn main:app。"""

from lakehouse.api import app  # noqa: F401

__all__ = ["app"]
