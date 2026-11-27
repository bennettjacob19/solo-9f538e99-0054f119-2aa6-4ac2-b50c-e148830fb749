"""主库写入：批量登记/更正/撤销，整批一个事务，返回递增提交号。"""
from __future__ import annotations

import sqlite3

from .models import BatchItem, BatchResponse


class StaleOperationError(Exception):
    """设备当前状态与操作前提不符（重复登记、更正/撤销未登记设备等）。"""


def current_primary_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT next_seq FROM commit_seq WHERE id = 1").fetchone()
    return int(row["next_seq"]) - 1


def apply_batch(conn: sqlite3.Connection, items: list[BatchItem]) -> BatchResponse:
    """整批校验并提交：任一条目不合法则整批回滚，提交号不发出去。"""
    with conn:  # 事务：异常自动回滚，正常结束自动提交
        next_seq = int(
            conn.execute("SELECT next_seq FROM commit_seq WHERE id = 1").fetchone()["next_seq"]
        )

        for item in items:
            row = conn.execute(
                "SELECT status, updated_seq FROM main_state WHERE device_id = ?",
                (item.device_id,),
            ).fetchone()

            if item.op == "register":
                if row is not None:
                    raise StaleOperationError(
                        f"设备 {item.device_id} 已登记，不能重复登记（请用 correct/revoke）"
                    )
                if item.new_status is None:
                    raise StaleOperationError(f"设备 {item.device_id} 登记必须提供 new_status")
                conn.execute(
                    "INSERT INTO main_state (device_id, status, updated_seq) VALUES (?, ?, ?)",
                    (item.device_id, item.new_status, next_seq),
                )
                conn.execute(
                    "INSERT INTO change_log (seq, device_id, op, old_status, new_status) "
                    "VALUES (?, ?, 'register', NULL, ?)",
                    (next_seq, item.device_id, item.new_status),
                )

            elif item.op == "correct":
                if row is None:
                    raise StaleOperationError(f"设备 {item.device_id} 未登记，无法更正")
                if item.new_status is None:
                    raise StaleOperationError(f"设备 {item.device_id} 更正必须提供 new_status")
                old_status = row["status"]
                conn.execute(
                    "UPDATE main_state SET status = ?, updated_seq = ? WHERE device_id = ?",
                    (item.new_status, next_seq, item.device_id),
                )
                conn.execute(
                    "INSERT INTO change_log (seq, device_id, op, old_status, new_status) "
                    "VALUES (?, ?, 'correct', ?, ?)",
                    (next_seq, item.device_id, old_status, item.new_status),
                )

            else:  # revoke
                if row is None:
                    raise StaleOperationError(f"设备 {item.device_id} 未登记，无法撤销")
                old_status = row["status"]
                conn.execute(
                    "DELETE FROM main_state WHERE device_id = ?", (item.device_id,)
                )
                conn.execute(
                    "INSERT INTO change_log (seq, device_id, op, old_status, new_status) "
                    "VALUES (?, ?, 'revoke', ?, NULL)",
                    (next_seq, item.device_id, old_status),
                )

        conn.execute(
            "UPDATE commit_seq SET next_seq = ? WHERE id = 1", (next_seq + 1,)
        )

    return BatchResponse(seq=next_seq, item_count=len(items))
