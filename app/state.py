"""进程内共享的数据库连接与启停生命周期。"""
from __future__ import annotations

import sqlite3
import threading

from .config import get_paths, get_role
from .db import init_primary, init_replica


class AppState:
    def __init__(
        self,
        primary_path: str | None = None,
        replica_path: str | None = None,
        role: str | None = None,
    ) -> None:
        paths = get_paths()
        self.role = role or get_role()
        self.media_dir = paths.media_dir
        self.replica_path = replica_path or paths.replica
        if primary_path is not None:
            self.primary_path: str | None = primary_path
        else:
            self.primary_path = paths.primary

        # 工位角色且未显式提供主库路径时，不连接/创建任何主库。
        if self.role == "station" and primary_path is None and paths.primary is None:
            self.primary: sqlite3.Connection | None = None
        elif self.primary_path:
            self.primary = init_primary(self.primary_path)
        else:
            self.primary = None

        self.replica: sqlite3.Connection = init_replica(self.replica_path)
        # 同一连接被线程池中的请求共享，用 RLock 把端点级数据库操作串行化，
        # 避免两个请求在同一连接上交错开启事务。
        self.lock = threading.RLock()
        # 交接包安装（暂存/回放）串行化，但不长期占用 self.lock，
        # 这样安装期间 /query 仍可在旧副本上读取。
        self.import_lock = threading.RLock()
        # 被原子换出的旧连接不立即关闭：可能有请求已拿到引用正在读旧版，
        # 进程生命周期结束时统一关闭。Linux 下重命名后旧 inode 仍可读。
        self.retired_conns: list[sqlite3.Connection] = []

    def retire_replica(self, old: sqlite3.Connection) -> None:
        self.retired_conns.append(old)

    def close(self) -> None:
        conns: list[sqlite3.Connection | None] = [self.primary, self.replica, *self.retired_conns]
        for conn in conns:
            if conn is None:
                continue
            try:
                conn.close()
            except sqlite3.Error:
                pass


state: AppState | None = None


def init_state(role: str | None = None) -> AppState:
    global state
    if state is None:
        state = AppState(role=role)
    return state


def get_state() -> AppState:
    """请求期取状态；若 lifespan 未运行（如裸 TestClient）则惰性初始化。"""
    global state
    if state is None:
        state = init_state()
    return state


def close_state() -> None:
    global state
    if state is not None:
        state.close()
        state = None
