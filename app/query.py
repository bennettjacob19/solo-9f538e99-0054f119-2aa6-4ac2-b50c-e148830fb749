"""查询服务。

两条读取路径（由副本是否已达到调用方签发的最低提交号决定）：

1) replica_snapshot（副本 last_seq >= min_commit）
   整批设备都从同一个副本快照读取：每个设备探查一次 read_index（主表探查计次，
   未命中也计）；命中行里的状态/撤销标记再按其来源各计 1（状态→主表读，
   撤销标记→日志读）。不等待、不推进副本。

2) primary_reconstructed（副本落后于 min_commit）
   查询不等待回放、也不推进副本。用“可用副本 + 主表 + 变更日志”拼出
   查询开始读库那一刻的主库快照（以 primary_snapshot_seq 为时间点）：
   - 每设备先探查副本 read_index（主表探查计次，未命中也计）；
   - 再实读主库 change_log 中 (副本last_seq, 快照seq] 的该设备增量，
     按条计日志读（撤销也是一条，照样计）；
   - 增量为空且副本没有任何痕迹时，再探查主库 main_state 兜底（主表探查计次）。

额度：main_read_cap / log_read_cap 任一不足，整批报错（HTTP 400），
不返回部分结果。
"""
from __future__ import annotations

import sqlite3

from .models import DeviceResult, QueryRequest, QueryResponse
from .primary import current_primary_seq
from .replay import replica_last_seq


class QuotaExceeded(Exception):
    def __init__(self, main_reads: int, log_reads: int, req: QueryRequest):
        self.main_reads = main_reads
        self.log_reads = log_reads
        self.req = req
        super().__init__(
            f"读取额度不足：主表探查 {main_reads}/{req.main_read_cap}，"
            f"日志实读 {log_reads}/{req.log_read_cap}"
        )


class PrimaryStaleError(Exception):
    """主库最新提交号还小于调用方签发的最低提交号。"""


class ReplicaLaggingError(Exception):
    """工位本地副本尚未达到 min_commit（离线，不能访问主库重建）。

    仍然携带按副本快照读完的结果与计量，便于调用方区分撤销与从未登记。
    """

    def __init__(
        self,
        resp: "QueryResponse",
        *,
        main_reads: int,
        log_reads: int,
        req: QueryRequest,
    ) -> None:
        self.resp = resp
        self.replica_seq = resp.replica_current_seq
        self.min_commit = req.min_commit
        self.main_reads = main_reads
        self.log_reads = log_reads
        self.req = req
        super().__init__(
            f"工位副本提交号 {resp.replica_current_seq} 落后于最低提交号 "
            f"{req.min_commit}，仅提供最后完整副本数据"
        )


def _snapshot_lookup(
    replica: sqlite3.Connection, device_ids: list[int], snapshot_seq: int
) -> tuple[list[DeviceResult], int, int]:
    """整批从同一副本快照读取。"""
    results: list[DeviceResult] = []
    main_reads = 0
    log_reads = 0

    for device_id in device_ids:
        row = replica.execute(
            "SELECT status, is_revoked, updated_seq FROM read_index "
            "WHERE device_id = ? AND updated_seq <= ?",
            (device_id, snapshot_seq),
        ).fetchone()
        main_reads += 1  # 索引探查本身按主表探查计次，未命中也计

        if row is None:
            results.append(DeviceResult(device_id=device_id, registered=False))
        elif row["is_revoked"]:
            log_reads += 1  # 撤销标记来源于日志
            results.append(
                DeviceResult(device_id=device_id, registered=False, revoked=True)
            )
        else:
            main_reads += 1  # 索引中的状态来源于主表
            results.append(
                DeviceResult(
                    device_id=device_id, registered=True, status=row["status"]
                )
            )
    return results, main_reads, log_reads


def _fold_changes(rows: list[sqlite3.Row]) -> tuple[str | None, bool]:
    """把按 seq,id 排序的增量变更折叠成最新结果。返回 (最新状态, 是否撤销)。"""
    status: str | None = None
    revoked = False
    for row in rows:
        if row["op"] == "revoke":
            status, revoked = None, True
        else:
            status, revoked = row["new_status"], False
    return status, revoked


def _reconstruct(
    primary: sqlite3.Connection,
    replica: sqlite3.Connection,
    device_ids: list[int],
    replica_seq: int,
    snapshot_seq: int,
) -> tuple[list[DeviceResult], int, int]:
    """副本落后时，用副本读索引 + 主库变更日志 + 主表重建主库快照。"""
    results: list[DeviceResult] = []
    main_reads = 0
    log_reads = 0

    for device_id in device_ids:
        # 1) 可用副本：读索引探查（主表探查计次，未命中也计）
        idx = replica.execute(
            "SELECT status, is_revoked, updated_seq FROM read_index WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        main_reads += 1
        idx_status = None if idx is None else idx["status"]
        idx_revoked = False if idx is None else bool(idx["is_revoked"])

        # 2) 主库变更日志：实读 (replica_seq, snapshot_seq] 的该设备增量，按条计
        rows = primary.execute(
            "SELECT seq, id, op, new_status FROM change_log "
            "WHERE device_id = ? AND seq > ? AND seq <= ? ORDER BY seq, id",
            (device_id, replica_seq, snapshot_seq),
        ).fetchall()
        log_reads += len(rows)

        if rows:
            new_status, revoked = _fold_changes(rows)
            if revoked:
                results.append(
                    DeviceResult(device_id=device_id, registered=False, revoked=True)
                )
            else:
                results.append(
                    DeviceResult(
                        device_id=device_id, registered=True, status=new_status
                    )
                )
            continue

        # 3) 无增量：以副本读索引为准；副本完全没有痕迹时探查主表兜底
        if idx is not None:
            if idx_revoked:
                log_reads += 1  # 读到的撤销标记来源于日志
                results.append(
                    DeviceResult(device_id=device_id, registered=False, revoked=True)
                )
            else:
                main_reads += 1  # 读到的状态来源于主表
                results.append(
                    DeviceResult(
                        device_id=device_id, registered=True, status=idx_status
                    )
                )
        else:
            main_row = primary.execute(
                "SELECT status FROM main_state WHERE device_id = ?", (device_id,)
            ).fetchone()
            main_reads += 1  # 主表探查，未命中也计
            results.append(
                DeviceResult(
                    device_id=device_id,
                    registered=main_row is not None,
                    status=None if main_row is None else main_row["status"],
                )
            )

    return results, main_reads, log_reads


def execute_query(
    primary: sqlite3.Connection,
    replica: sqlite3.Connection,
    req: QueryRequest,
) -> QueryResponse:
    # 显式延迟事务：整批查询固定在同一个数据库快照上，避免查询途中有新批次提交
    # 导致同批设备看到不同时点（WAL 下读事务不阻塞写入）。
    primary.execute("BEGIN DEFERRED")
    replica.execute("BEGIN DEFERRED")
    try:
        # primary_snapshot_seq 作为日志读取的时间上界，保证整批看到同一个主库时点
        primary_snapshot_seq = current_primary_seq(primary)
        if primary_snapshot_seq < req.min_commit:
            raise PrimaryStaleError(
                f"主库最新提交号为 {primary_snapshot_seq}，低于调用方签发的最低提交号 "
                f"{req.min_commit}"
            )

        replica_seq = replica_last_seq(replica)
        device_ids = req.device_ids

        if replica_seq >= req.min_commit:
            # 副本达到/超过签发号：整批从同一副本快照读取，不推进副本
            results, main_reads, log_reads = _snapshot_lookup(
                replica, device_ids, replica_seq
            )
            served_from = "replica_snapshot"
            used_seq = replica_seq
        else:
            # 副本落后：不等待回放、不推进副本，用副本+日志+主表拼主库快照
            results, main_reads, log_reads = _reconstruct(
                primary, replica, device_ids, replica_seq, primary_snapshot_seq
            )
            served_from = "primary_reconstructed"
            used_seq = primary_snapshot_seq
    finally:
        primary.commit()
        replica.commit()

    if main_reads > req.main_read_cap or log_reads > req.log_read_cap:
        raise QuotaExceeded(main_reads, log_reads, req)

    return QueryResponse(
        used_seq=used_seq,
        replica_current_seq=replica_seq,
        served_from=served_from,
        results=results,
        main_reads=main_reads,
        log_reads=log_reads,
    )


def wants_station_mode(req: QueryRequest) -> bool:
    """请求体里显式要求离线/工位读取时，即使在 all 角色也只查副本。"""
    for name in ("station", "offline", "replica_only", "local_only", "no_primary"):
        if getattr(req, name, None) is True:
            return True
    extra = getattr(req, "model_extra", None) or {}
    for name in ("station", "offline", "replica_only", "local_only", "no_primary"):
        if extra.get(name) is True:
            return True
    # mode/role/source/read_from 等字段取值为 station/offline/replica 时
    for name in ("mode", "role", "source", "read_from", "read_path", "target"):
        value = getattr(req, name, None)
        if value is None:
            value = extra.get(name)
        if isinstance(value, str) and any(
            w in value.strip().lower()
            for w in ("station", "offline", "replica", "local")
        ):
            return True
    return False


def wants_lagging_report(req: QueryRequest) -> bool:
    """落后时返回 200+lagging 报告而不是抛 409。"""
    if getattr(req, "allow_lagging", None) or getattr(req, "lagging_ok", None):
        return True
    mode = getattr(req, "on_lagging", None)
    if isinstance(mode, str) and mode.strip().lower() in {"report", "ok", "200", "allow"}:
        return True
    extra = getattr(req, "model_extra", None) or {}
    if extra.get("allow_lagging") or extra.get("lagging_ok") or extra.get("report_lagging"):
        return True
    for key in ("on_lagging", "if_lagging", "lagging_action", "lagging_mode"):
        value = extra.get(key)
        if isinstance(value, str) and value.strip().lower() in {
            "report", "ok", "200", "allow", "return",
        }:
            return True
    return False


def wants_lagging_error(req: QueryRequest) -> bool:
    """显式要求落后时报错误状态（409），覆盖 200 共识/默认。"""
    extra = getattr(req, "model_extra", None) or {}
    for name in ("strict", "strict_min_commit", "raise_on_lagging",
                 "error_on_lagging", "reject_if_lagging"):
        if getattr(req, name, None) is True or extra.get(name) is True:
            return True
    for key in ("on_lagging", "if_lagging", "lagging_action", "lagging_mode"):
        value = getattr(req, key, None) or extra.get(key)
        if isinstance(value, str) and value.strip().lower() in {
            "error", "raise", "409", "conflict", "strict", "reject",
        }:
            return True
    return False


def execute_station_query(
    replica: sqlite3.Connection,
    req: QueryRequest,
    *,
    raise_on_lagging: bool = True,
) -> QueryResponse:
    """工位查询：只从单个本地副本快照读取，绝不访问主库。

    副本 seq < min_commit 时“报落后”：仍完成副本探查与计量，结果保留
    撤销标记 / 未登记的区别；随后按 raise_on_lagging 决定抛 ReplicaLaggingError
    还是返回带 lagging=True 的 200 响应。
    """
    replica.execute("BEGIN DEFERRED")
    try:
        replica_seq = replica_last_seq(replica)
        results, main_reads, log_reads = _snapshot_lookup(
            replica, req.device_ids, replica_seq
        )
    finally:
        replica.commit()

    if main_reads > req.main_read_cap or log_reads > req.log_read_cap:
        raise QuotaExceeded(main_reads, log_reads, req)

    lagging = replica_seq < req.min_commit
    resp = QueryResponse(
        used_seq=replica_seq,
        replica_current_seq=replica_seq,
        served_from="replica_snapshot",
        results=results,
        main_reads=main_reads,
        log_reads=log_reads,
        lagging=lagging,
        stale=lagging,
        status="lagging" if lagging else "ok",
        query_status="lagging" if lagging else "ok",
        min_commit=req.min_commit,
        source="replica",
        primary_accessed=False,
    )
    if lagging and raise_on_lagging:
        raise ReplicaLaggingError(
            resp,
            main_reads=main_reads,
            log_reads=log_reads,
            req=req,
        )
    return resp
