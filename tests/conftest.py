from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from tests.database_support import isolated_database

SRC_DIR = Path(__file__).resolve().parents[1] / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture
def isolated_database_dsn():
    dsn = os.getenv("MCP_GATEWAY_TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("set MCP_GATEWAY_TEST_DATABASE_URL or DATABASE_URL to run Postgres integration tests")
    with isolated_database(dsn) as scoped_dsn:
        yield scoped_dsn
