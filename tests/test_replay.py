"""副本回放：手动推进、按序落盘、故障中断与接续、重启后从进度继续。"""
from __future__ import annotations

import pytest

from app.config import get_paths
from app.db import init_replica
from app.models import BatchItem
from app.primary import apply_batch
from app import replay


def _b(device_id: int, op: str, status: str | None = None) -> BatchItem:
    return BatchItem(device_id=device_id, op=op, new_status=status)


def _seed_commits(app_state) -> None:
    # seq1: 登记 1/2；seq2: 更正 1，登记 3；seq3: 撤销 2；seq4: 更正 3
    apply_batch(
        app_state.primary,
        [_b(1, "register", "maintenance"), _b(2, "register", "in_service")],
    )
    apply_batch(
        app_state.primary,
        [_b(1, "correct", "overhaul"), _b(3, "register", "maintenance")],
    )
    apply_batch(app_state.primary, [_b(2, "revoke")])
    apply_batch(app_state.primary, [_b(3, "correct", "in_service")])


def test_manual_advance_in_order_with_progress(app_state):
    _seed_commits(app_state)
    assert replay.replica_last_seq(app_state.replica) == 0
    assert replay.pending_commits(app_state.primary, app_state.replica) == [1, 2, 3, 4]

    assert replay.advance(app_state.primary, app_state.replica, 1) == [1]
    assert replay.replica_last_seq(app_state.replica) == 1

    assert replay.advance(app_state.primary, app_state.replica, 5) == [2, 3, 4]
    assert replay.replica_last_seq(app_state.replica) == 4

    idx = {
        r["device_id"]: (r["status"], r["is_revoked"])
        for r in app_state.replica.execute(
            "SELECT device_id, status, is_revoked FROM read_index"
        )
    }
    assert idx == {1: ("overhaul", 0), 2: (None, 1), 3: ("in_service", 0)}


def test_fault_blocks_commit_and_resumes_after_clear(app_state):
    _seed_commits(app_state)
    replay.advance(app_state.primary, app_state.replica, 2)  # 1,2 完成
    replay.set_fault_seq(app_state.replica, 3)

    with pytest.raises(replay.ReplayFault):
        replay.advance(app_state.primary, app_state.replica, 2)
    # 故障提交未应用，进度停在 2
    assert replay.replica_last_seq(app_state.replica) == 2

    # 不清除故障时再次推进仍然失败
    with pytest.raises(replay.ReplayFault):
        replay.advance(app_state.primary, app_state.replica, 1)

    replay.set_fault_seq(app_state.replica, None)
    applied = replay.advance(app_state.primary, app_state.replica, 10)
    assert applied == [3, 4]
    assert replay.replica_last_seq(app_state.replica) == 4


def test_progress_survives_replica_reopen(app_state, monkeypatch):
    _seed_commits(app_state)
    replay.advance(app_state.primary, app_state.replica, 2)
    paths = get_paths()
    app_state.replica.close()

    # 模拟服务重启：重新打开同一个副本文件，从最近完成的提交之后接续
    reopened = init_replica(paths.replica)
    assert replay.replica_last_seq(reopened) == 2
    applied = replay.advance(app_state.primary, reopened, 10)
    assert applied == [3, 4]
    reopened.close()
