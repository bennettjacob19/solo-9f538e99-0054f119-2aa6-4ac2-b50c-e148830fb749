"""每个测试使用独立临时目录里的两个 SQLite 文件。"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import get_paths  # noqa: E402
from app.state import AppState, state as global_state  # noqa: E402


@pytest.fixture()
def app_state(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PRIMARY_DB", str(tmp_path / "primary.sqlite3"))
    monkeypatch.setenv("REPLICA_DB", str(tmp_path / "replica.sqlite3"))
    # get_paths 读环境变量；AppState 内部会建表
    st = AppState()
    monkeypatch.setattr("app.state.state", st, raising=False)
    yield st
    st.close()
