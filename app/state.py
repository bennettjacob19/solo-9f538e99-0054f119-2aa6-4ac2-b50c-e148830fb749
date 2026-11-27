"""进程内共享的数据库连接与启停生命周期。"""
from __future__ import annotations

import sqlite3
import threading

from .config import get_paths
from .db import init_primary, init_replica


class AppState:
    def __init__(self) -> None:
        paths = get_paths()
        self.primary: sqlite3.Connection = init_primary(paths.primary)
        self.replica: sqlite3.Connection = init_replica(paths.replica)
        # 同一连接被线程池中的请求共享，用 RLock 把端点级数据库操作串行化，
        # 避免两个请求在同一连接上交错开启事务。
        self.lock = threading.RLock()

    def close(self) -> None:
        self.primary.close()
        self.replica.close()

    def close(self) -> None:
        self.primary.close()
        self.replica.close()


state: AppState | None = None


def init_state() -> AppState:
    global state
    if state is None:
        state = AppState()
    return state


def close_state() -> None:
    global state
    if state is not None:
        state.close()
        state = None
