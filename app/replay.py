"""副本回放：手动推进，按提交顺序逐提交事务回放，状态与进度一起落盘。

失败/重启语义：
- 每个提交在一个事务内完成 read_index 变更 + progress 推进，提交前崩溃
  整个事务回滚，下次从 replica_progress.last_seq 之后接续。
- 故障注入（replica_fault 表）在目标提交的事务开启前抛出 ReplayFault，
  该提交及之后的提交本次都不会应用；清除故障后再次推进即可继续。
"""
from __future__ import annotations

import sqlite3

from .primary import current_primary_seq


class ReplayFault(Exception):
    def __init__(self, seq: int):
        self.seq = seq
        super().__init__(f"注入故障：回放提交 {seq} 前中断")


def replica_last_seq(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute("SELECT last_seq FROM replica_progress WHERE id = 1").fetchone()["last_seq"]
    )


def get_fault_seq(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT fault_seq FROM replica_fault WHERE id = 1").fetchone()
    return None if row is None else int(row["fault_seq"])


def set_fault_seq(conn: sqlite3.Connection, seq: int | None) -> None:
    with conn:
        conn.execute("DELETE FROM replica_fault WHERE id = 1")
        if seq is not None:
            conn.execute(
                "INSERT INTO replica_fault (id, fault_seq) VALUES (1, ?)", (seq,)
            )


def _apply_commit(replica: sqlite3.Connection, rows: list[sqlite3.Row], seq: int) -> None:
    """在调用方事务内回放单个提交。"""
    for row in rows:
        device_id = row["device_id"]
        if row["op"] == "revoke":
            # 撤销标记：删除有效状态，以 NULL + is_revoked=1 留痕
            replica.execute(
                "INSERT INTO read_index (device_id, status, is_revoked, updated_seq) "
                "VALUES (?, NULL, 1, ?) "
                "ON CONFLICT(device_id) DO UPDATE SET "
                "status = NULL, is_revoked = 1, updated_seq = excluded.updated_seq",
                (device_id, seq),
            )
        else:
            replica.execute(
                "INSERT INTO read_index (device_id, status, is_revoked, updated_seq) "
                "VALUES (?, ?, 0, ?) "
                "ON CONFLICT(device_id) DO UPDATE SET "
                "status = excluded.status, is_revoked = 0, "
                "updated_seq = excluded.updated_seq",
                (device_id, row["new_status"], seq),
            )
    replica.execute(
        "UPDATE replica_progress SET last_seq = ? WHERE id = 1", (seq,)
    )


def advance(
    primary: sqlite3.Connection,
    replica: sqlite3.Connection,
    max_commits: int,
) -> list[int]:
    """手动推进副本。返回本次完整应用的提交号列表；遇到注入故障则抛出 ReplayFault。"""
    applied: list[int] = []
    primary_head = current_primary_seq(primary)

    for _ in range(max_commits):
        last = replica_last_seq(replica)
        next_seq = last + 1
        if next_seq > primary_head:
            break

        fault = get_fault_seq(replica)
        if fault is not None and next_seq == fault:
            raise ReplayFault(next_seq)

        rows = primary.execute(
            "SELECT id, seq, device_id, op, old_status, new_status "
            "FROM change_log WHERE seq = ? ORDER BY id",
            (next_seq,),
        ).fetchall()
        if not rows:  # 主库声称有该提交却没有日志行，属于数据损坏，拒绝推进
            raise RuntimeError(f"主库 change_log 缺少提交 {next_seq} 的变更记录")

        with replica:  # 状态与进度同一事务落盘
            _apply_commit(replica, rows, next_seq)
        applied.append(next_seq)

    return applied


def pending_commits(primary: sqlite3.Connection, replica: sqlite3.Connection) -> list[int]:
    head = current_primary_seq(primary)
    last = replica_last_seq(replica)
    return list(range(last + 1, head + 1))
