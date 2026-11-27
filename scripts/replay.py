#!/usr/bin/env python3
"""命令行手动推进副本，可模拟进程级崩溃以验证“从最近完成的提交接续”。

用法：
  python -m scripts.replay --max-commits 5
  python -m scripts.replay --crash-at 3      # 回放提交 3 的事务提交后 os._exit
  python -m scripts.replay --status

崩溃模式特意用 os._exit：不执行任何 Python 清理，模拟真实进程被 kill -9 / 断电。
SQLite 事务保证崩溃时未提交的整提交回滚，已提交的 read_index 与 progress 一起保留。
"""
from __future__ import annotations

import argparse
import os
import sys

# 允许直接 `python scripts/replay.py` 运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import get_paths  # noqa: E402
from app.db import init_primary, init_replica  # noqa: E402
from app.primary import current_primary_seq  # noqa: E402
from app import replay  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="手动推进 SQLite 副本")
    parser.add_argument("--max-commits", type=int, default=1)
    parser.add_argument(
        "--crash-at",
        type=int,
        default=None,
        help="成功回放该提交号后立即 os._exit 模拟进程崩溃",
    )
    parser.add_argument("--status", action="store_true", help="只查看主/副本进度")
    args = parser.parse_args()

    paths = get_paths()
    primary = init_primary(paths.primary)
    replica = init_replica(paths.replica)

    if args.status:
        print(
            f"primary_seq={current_primary_seq(primary)} "
            f"replica_seq={replay.replica_last_seq(replica)} "
            f"pending={replay.pending_commits(primary, replica)} "
            f"fault_seq={replay.get_fault_seq(replica)}"
        )
        return 0

    head = current_primary_seq(primary)
    applied: list[int] = []
    for _ in range(args.max_commits):
        last = replay.replica_last_seq(replica)
        nxt = last + 1
        if nxt > head:
            break
        fault = replay.get_fault_seq(replica)
        if fault is not None and nxt == fault:
            print(f"FAULT: 回放提交 {nxt} 前被注入故障中断，进度仍为 {last}", file=sys.stderr)
            return 2

        rows = primary.execute(
            "SELECT id, seq, device_id, op, old_status, new_status "
            "FROM change_log WHERE seq = ? ORDER BY id",
            (nxt,),
        ).fetchall()
        if not rows:
            print(f"ERROR: 主库缺少提交 {nxt} 的日志", file=sys.stderr)
            return 3

        with replica:
            replay._apply_commit(replica, rows, nxt)
        applied.append(nxt)
        print(f"applied seq={nxt} ({len(rows)} 条变更)")

        if args.crash_at is not None and nxt == args.crash_at:
            print(f"CRASH: 提交 {nxt} 已落盘，模拟进程崩溃 os._exit(1)", file=sys.stderr)
            sys.stderr.flush()
            os._exit(1)

    print(
        f"done applied={applied} replica_seq={replay.replica_last_seq(replica)} "
        f"primary_seq={head}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
