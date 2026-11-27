"""主库批量提交：整批事务、递增提交号、操作前提校验、撤销留痕。"""
from __future__ import annotations

import pytest

from app.models import BatchItem
from app.primary import StaleOperationError, apply_batch, current_primary_seq


def _b(device_id: int, op: str, status: str | None = None) -> BatchItem:
    return BatchItem(device_id=device_id, op=op, new_status=status)


def test_batch_returns_monotonic_seq_and_persists(app_state):
    r1 = apply_batch(app_state.primary, [_b(1, "register", "maintenance")])
    r2 = apply_batch(
        app_state.primary,
        [_b(2, "register", "in_service"), _b(1, "correct", "overhaul")],
    )
    assert (r1.seq, r2.seq) == (1, 2)
    assert r2.item_count == 2
    assert current_primary_seq(app_state.primary) == 2

    rows = {
        r["device_id"]: r["status"]
        for r in app_state.primary.execute("SELECT device_id, status FROM main_state")
    }
    assert rows == {1: "overhaul", 2: "in_service"}


def test_revoke_deletes_main_row_but_keeps_log(app_state):
    apply_batch(app_state.primary, [_b(7, "register", "maintenance")])
    apply_batch(app_state.primary, [_b(7, "revoke")])

    found = app_state.primary.execute(
        "SELECT 1 FROM main_state WHERE device_id = 7"
    ).fetchall()
    assert found == []
    ops = [
        r["op"]
        for r in app_state.primary.execute(
            "SELECT op FROM change_log WHERE device_id = 7 ORDER BY id"
        )
    ]
    assert ops == ["register", "revoke"]


def test_invalid_batch_rolls_back_and_consumes_no_seq(app_state):
    apply_batch(app_state.primary, [_b(3, "register", "maintenance")])
    with pytest.raises(StaleOperationError):
        # 设备 3 重复登记 + 设备 4 正常登记：整批回滚，4 也不能留下
        apply_batch(
            app_state.primary,
            [_b(3, "register", "overhaul"), _b(4, "register", "in_service")],
        )
    assert current_primary_seq(app_state.primary) == 1
    assert (
        app_state.primary.execute(
            "SELECT 1 FROM main_state WHERE device_id = 4"
        ).fetchall()
        == []
    )


def test_correct_and_revoke_on_unregistered_rejected(app_state):
    with pytest.raises(StaleOperationError):
        apply_batch(app_state.primary, [_b(9, "correct", "overhaul")])
    with pytest.raises(StaleOperationError):
        apply_batch(app_state.primary, [_b(9, "revoke")])


def test_register_requires_status(app_state):
    with pytest.raises(StaleOperationError):
        apply_batch(app_state.primary, [_b(5, "register")])
