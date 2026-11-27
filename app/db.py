"""SQLite 连接与建表。

主库文件与副本文件完全独立：
- 主库 main_state 保存当前生效的检修状态，change_log 保存每个提交的逐设备变更，
  commit_seq 保存单调递增的提交号。
- 副本 read_index 是按提交顺序回放出来的读索引（状态/撤销标记），
  replica_progress 保存最近一次完整落盘的提交号，重启后从它之后接续。
"""
from __future__ import annotations

import sqlite3
from typing import Iterator

from .config import get_paths


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # WAL：查询/回放可以与写入并发；busy_timeout 兜底写锁等待。
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


_PRIMARY_DDL = """
CREATE TABLE IF NOT EXISTS main_state (
    device_id     INTEGER PRIMARY KEY,
    status        TEXT NOT NULL,
    updated_seq   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS change_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    seq           INTEGER NOT NULL,
    device_id     INTEGER NOT NULL,
    op            TEXT NOT NULL CHECK (op IN ('register', 'correct', 'revoke')),
    old_status    TEXT,
    new_status    TEXT
);
CREATE INDEX IF NOT EXISTS idx_change_log_seq ON change_log(seq, id);
CREATE INDEX IF NOT EXISTS idx_change_log_device_seq
    ON change_log(device_id, seq, id);

CREATE TABLE IF NOT EXISTS commit_seq (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    next_seq INTEGER NOT NULL
);
INSERT OR IGNORE INTO commit_seq (id, next_seq) VALUES (1, 1);
"""

_REPLICA_DDL = """
CREATE TABLE IF NOT EXISTS read_index (
    device_id     INTEGER PRIMARY KEY,
    status        TEXT,            -- 撤销标记为 NULL（is_revoked=1）
    is_revoked    INTEGER NOT NULL DEFAULT 0,
    updated_seq   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS replica_progress (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq      INTEGER NOT NULL
);
INSERT OR IGNORE INTO replica_progress (id, last_seq) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS replica_fault (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    fault_seq  INTEGER NOT NULL
);
"""


def init_primary(db_path: str | None = None) -> sqlite3.Connection:
    db_path = db_path or get_paths().primary
    conn = connect(db_path)
    conn.executescript(_PRIMARY_DDL)
    conn.commit()
    return conn


def init_replica(db_path: str | None = None) -> sqlite3.Connection:
    db_path = db_path or get_paths().replica
    conn = connect(db_path)
    conn.executescript(_REPLICA_DDL)
    conn.commit()
    return conn


def get_connections() -> Iterator[tuple[sqlite3.Connection, sqlite3.Connection]]:
    """FastAPI 依赖：每个请求拿到各自的连接（进程内共享，配合 busy_timeout）。"""
    from .state import state

    yield state.primary, state.replica
