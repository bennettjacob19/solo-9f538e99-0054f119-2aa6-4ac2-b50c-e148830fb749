"""FastAPI 入口：/docs 可交互试跑全部接口。"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager

from fastapi import Depends, FastAPI, HTTPException

from . import replay
from .models import (
    AdvanceRequest,
    AdvanceResponse,
    BatchRequest,
    BatchResponse,
    FaultRequest,
    FaultResponse,
    QueryRequest,
    QueryResponse,
    ReplicaStatus,
)
from .primary import StaleOperationError, apply_batch, current_primary_seq
from .query import PrimaryStaleError, QuotaExceeded, execute_query
from .state import close_state, init_state


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_state()
    try:
        yield
    finally:
        close_state()


app = FastAPI(
    title="检修状态批量更正演练（主库 + 手动副本）",
    version="1.0.0",
    description=(
        "主库/副本各一个 SQLite 文件。批量登记、更正、撤销整批提交返回递增提交号；"
        "副本手动按序回放并落盘进度；查询按额度从副本快照或主库重建快照读取。"
    ),
    lifespan=lifespan,
)


@contextmanager
def _locked() -> Iterator[None]:
    from .state import state as app_state

    assert app_state is not None
    with app_state.lock:
        yield


def _dbs() -> tuple[sqlite3.Connection, sqlite3.Connection]:
    from .state import state as app_state

    assert app_state is not None
    return app_state.primary, app_state.replica


@app.post("/maintenance/batch", response_model=BatchResponse, tags=["主库写入"])
def maintenance_batch(
    req: BatchRequest,
    dbs: tuple[sqlite3.Connection, sqlite3.Connection] = Depends(_dbs),
) -> BatchResponse:
    """整批登记/更正/撤销检修状态，单事务提交，返回本批提交号。"""
    primary, _ = dbs
    with _locked():
        try:
            return apply_batch(primary, req.items)
        except StaleOperationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/replica/advance", response_model=AdvanceResponse, tags=["副本回放"])
def replica_advance(
    req: AdvanceRequest,
    dbs: tuple[sqlite3.Connection, sqlite3.Connection] = Depends(_dbs),
) -> AdvanceResponse:
    """手动推进副本：按提交顺序回放，状态与进度一起落盘。"""
    primary, replica = dbs
    with _locked():
        before = replay.replica_last_seq(replica)
        try:
            applied = replay.advance(primary, replica, req.max_commits)
        except replay.ReplayFault as exc:
            # 故障前已完整应用的提交保留；故障提交未开启事务，进度不变
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        after = replay.replica_last_seq(replica)
    return AdvanceResponse(
        applied_commits=applied,
        replica_seq_before=before,
        replica_seq_after=after,
    )


@app.get("/replica/status", response_model=ReplicaStatus, tags=["副本回放"])
def replica_status(
    dbs: tuple[sqlite3.Connection, sqlite3.Connection] = Depends(_dbs),
) -> ReplicaStatus:
    primary, replica = dbs
    with _locked():
        return ReplicaStatus(
            replica_seq=replay.replica_last_seq(replica),
            primary_seq=current_primary_seq(primary),
            pending_commits=replay.pending_commits(primary, replica),
            fault_seq=replay.get_fault_seq(replica),
        )


@app.post("/replica/fault", response_model=FaultResponse, tags=["故障注入"])
def replica_fault(
    req: FaultRequest,
    dbs: tuple[sqlite3.Connection, sqlite3.Connection] = Depends(_dbs),
) -> FaultResponse:
    """注入回放故障：副本推进到该提交号前中断（清除后可接续）。"""
    _, replica = dbs
    with _locked():
        replay.set_fault_seq(replica, req.seq)
    return FaultResponse(fault_seq=req.seq, cleared=False)


@app.delete("/replica/fault", response_model=FaultResponse, tags=["故障注入"])
def replica_fault_clear(
    dbs: tuple[sqlite3.Connection, sqlite3.Connection] = Depends(_dbs),
) -> FaultResponse:
    _, replica = dbs
    with _locked():
        old = replay.get_fault_seq(replica)
        replay.set_fault_seq(replica, None)
    return FaultResponse(fault_seq=old or 0, cleared=True)


@app.post("/query", response_model=QueryResponse, tags=["查询"])
def query(
    req: QueryRequest,
    dbs: tuple[sqlite3.Connection, sqlite3.Connection] = Depends(_dbs),
) -> QueryResponse:
    """按设备号（1～32）、已签发最低提交号和两项读取上限查询检修状态。"""
    primary, replica = dbs
    with _locked():
        try:
            return execute_query(primary, replica, req)
        except PrimaryStaleError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except QuotaExceeded as exc:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "quota_exceeded",
                    "message": str(exc),
                    "main_reads": exc.main_reads,
                    "log_reads": exc.log_reads,
                    "main_read_cap": exc.req.main_read_cap,
                    "log_read_cap": exc.req.log_read_cap,
                },
            ) from exc
