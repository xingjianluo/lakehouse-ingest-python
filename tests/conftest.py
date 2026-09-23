import logging
import sys

import pytest

# 测试日志：打印提交版本、批次标识与判定依据。
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("lakehouse-test")


@pytest.fixture()
def table_dir(tmp_path):
    return tmp_path / "table"
