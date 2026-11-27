"""查询：副本快照路径、落后重建路径、精确计量、额度整批报错、不等待不推进。"""
from __future__ import annotations

import pytest

from app.models import BatchItem, QueryRequest
from app.primary import apply_batch
from app import replay
from app.query import PrimaryStaleError, QuotaExceeded, execute_query


def _b(device_id: int, op: str, status: str | None = None) -> BatchItem:
    return BatchItem(device_id=device_id, op=op, new_status=status)


def _seed(app_state) -> None:
    # seq1 登记 1(ms),2(is)；seq2 1->overhaul、登记 3(ms)；seq3 撤销 2；
    # seq4 3->is；seq5 登记 4(ms)
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
    apply_batch(app_state.primary, [_b(4, "register", "maintenance")])


def _req(device_ids, min_commit, main_cap=1000, log_cap=1000) -> QueryRequest:
    return QueryRequest(
        device_ids=device_ids,
        min_commit=min_commit,
        main_read_cap=main_cap,
        log_read_cap=log_cap,
    )


def test_snapshot_path_when_replica_caught_up(app_state):
    _seed(app_state)
    replay.advance(app_state.primary, app_state.replica, 100)
    resp = execute_query(
        app_state.primary, app_state.replica, _req([1, 2, 3, 4, 99], min_commit=5)
    )
    assert resp.served_from == "replica_snapshot"
    assert resp.used_seq == 5
    by_id = {r.device_id: r for r in resp.results}
    assert by_id[1].status == "overhaul" and by_id[1].registered
    assert by_id[2].revoked and not by_id[2].registered
    assert by_id[3].status == "in_service"
    assert by_id[4].status == "maintenance"
    assert not by_id[99].registered and by_id[99].status is None

    # 5 次索引探查；命中里 3 个状态(+3 主表读)、1 个撤销(+1 日志读)、1 个未命中
    assert resp.main_reads == 5 + 3
    assert resp.log_reads == 1


def test_snapshot_quota_failure_is_batch_level(app_state):
    _seed(app_state)
    replay.advance(app_state.primary, app_state.replica, 100)
    # 5 个设备的探查本身就要 5 次主表读，cap=4 整批报错
    with pytest.raises(QuotaExceeded) as ei:
        execute_query(
            app_state.primary, app_state.replica,
            _req([1, 2, 3, 4, 99], min_commit=5, main_cap=4, log_cap=1000),
        )
    assert ei.value.main_reads == 8  # 仍然完成全部计量后再判定
    with pytest.raises(QuotaExceeded) as ei:
        execute_query(
            app_state.primary, app_state.replica,
            _req([1, 2], min_commit=5, main_cap=1000, log_cap=0),
        )
    assert ei.value.log_reads == 1


def test_lagging_reconstruct_uses_log_and_does_not_advance(app_state):
    _seed(app_state)
    replay.advance(app_state.primary, app_state.replica, 2)  # 副本停在 seq2
    assert replay.replica_last_seq(app_state.replica) == 2

    req = _req([1, 2, 3, 4, 99], min_commit=5)  # 副本落后 -> 重建路径
    resp = execute_query(app_state.primary, app_state.replica, req)

    assert resp.served_from == "primary_reconstructed"
    assert resp.used_seq == 5  # 主库快照时点
    by_id = {r.device_id: r for r in resp.results}
    assert by_id[1].status == "overhaul"        # seq4 区间内 1 无变更，取日志/索引
    assert by_id[2].revoked                     # seq3 撤销，日志增量
    assert by_id[3].status == "in_service"      # seq4 更正
    assert by_id[4].status == "maintenance"     # seq5 登记，日志中带 new_status
    assert not by_id[99].registered             # 无增量无索引 -> 主表未命中

    # 计量核对：
    # 索引探查 5 次；增量日志：设备1=0、2=1(revoke)、3=1(correct)、4=1(register)、99=0
    # 设备1 无增量、索引是状态 -> +1 主表读；99 无增量无索引 -> 主表探查 +1
    assert resp.log_reads == 3
    assert resp.main_reads == 5 + 1 + 1
    # 查询没有推进副本
    assert replay.replica_last_seq(app_state.replica) == 2


def test_lagging_replica_never_served_as_snapshot(app_state):
    _seed(app_state)
    replay.advance(app_state.primary, app_state.replica, 3)  # 副本 seq3
    resp = execute_query(
        app_state.primary, app_state.replica, _req([1], min_commit=4)
    )
    assert resp.served_from == "primary_reconstructed"
    assert resp.used_seq == 5


def test_lagging_log_quota_exceeded(app_state):
    _seed(app_state)
    replay.advance(app_state.primary, app_state.replica, 2)
    # 设备 2/3/4 在 (2,5] 各有 1 条增量 -> 需要 3 次日志读，只给 2
    with pytest.raises(QuotaExceeded) as ei:
        execute_query(
            app_state.primary, app_state.replica,
            _req([2, 3, 4], min_commit=5, main_cap=1000, log_cap=2),
        )
    assert ei.value.log_reads == 3


def test_reconstructed_snapshot_matches_eventual_replica_snapshot(app_state):
    """落后期间重建出的结果，与副本追上后同一时点快照结果一致。"""
    _seed(app_state)
    replay.advance(app_state.primary, app_state.replica, 1)
    ids = [1, 2, 3, 4, 99]
    lagged = execute_query(
        app_state.primary, app_state.replica, _req(ids, min_commit=5)
    )
    replay.advance(app_state.primary, app_state.replica, 100)
    fresh = execute_query(
        app_state.primary, app_state.replica, _req(ids, min_commit=5)
    )
    assert [(r.registered, r.status, r.revoked) for r in lagged.results] == [
        (r.registered, r.status, r.revoked) for r in fresh.results
    ]


def test_primary_stale_min_commit_rejected(app_state):
    _seed(app_state)
    with pytest.raises(PrimaryStaleError):
        execute_query(
            app_state.primary, app_state.replica, _req([1], min_commit=6)
        )


def test_empty_store_returns_not_registered(app_state):
    # 主库尚无任何提交时，min_commit 只能取 1 且主库尚未签发 -> 409
    with pytest.raises(PrimaryStaleError):
        execute_query(app_state.primary, app_state.replica, _req([1], min_commit=1))

    # 有一个提交后，未登记设备在两条路径下都应是 registered=false
    apply_batch(app_state.primary, [_b(1, "register", "maintenance")])
    lag = execute_query(
        app_state.primary, app_state.replica, _req([2], min_commit=1)
    )
    assert lag.served_from == "primary_reconstructed"
    assert lag.results[0].registered is False
    assert lag.main_reads == 2  # 索引未命中 1 次 + 主表兜底探查 1 次
    assert lag.log_reads == 0

    replay.advance(app_state.primary, app_state.replica, 1)
    snap = execute_query(
        app_state.primary, app_state.replica, _req([2], min_commit=1)
    )
    assert snap.served_from == "replica_snapshot"
    assert snap.results[0].registered is False
    assert snap.main_reads == 1  # 只有一次索引探查，未命中也计
    assert snap.log_reads == 0
