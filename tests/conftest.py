"""pytest 公共夹具（骨架）。"""

from __future__ import annotations

import pytest


@pytest.fixture
def tmp_data_root(tmp_path):
    """每个测试独立的本地存储根目录。"""
    return str(tmp_path / "lakehouse")
