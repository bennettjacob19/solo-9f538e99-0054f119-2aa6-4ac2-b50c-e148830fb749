"""FastAPI 入口：/docs 可交互试跑全部接口（含交接包打包/导入与工位查询）。"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from . import handoff, replay
from .handoff_routes import register_handoff_routes
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
from .query import (
    PrimaryStaleError,
    QuotaExceeded,
    ReplicaLaggingError,
    execute_query,
    execute_station_query,
    wants_lagging_error,
    wants_lagging_report,
    wants_station_mode,
)
from .state import close_state, init_state


@asynccontextmanager
async def lifespan(app: FastAPI):
    forced_role = getattr(app.state, "forced_role", None)
    st = init_state(forced_role)
    # 清理上次崩溃可能残留的暂存文件（最后完整副本路径不受影响）
    try:
        import glob
        import os

        for pattern in (
            os.path.join(os.path.dirname(os.path.abspath(st.replica_path)), ".replica-staging-*"),
        ):
            for path in glob.glob(pattern):
                for suffix in ("", "-wal", "-shm"):
                    try:
                        os.remove(path + suffix)
                    except OSError:
                        pass
    except Exception:
        pass
    try:
        yield
    finally:
        close_state()


def create_app(role: str | None = None) -> FastAPI:
    """应用工厂：role='station' 时 /query 只查本地副本并报落后。"""
    app = FastAPI(
        title="检修状态批量更正演练（主库 + 副本 + 交接包）",
        version="1.1.0",
        description=(
            "主库/副本各一个 SQLite 文件。批量提交、按序回放、额度查询；"
            "支持交接包（完整 SQLite 副本 / 指定基线差异包）与工位离线导入。"
        ),
        lifespan=lifespan,
    )
    if role:
        app.state.forced_role = role

    _register_core_routes(app)
    register_handoff_routes(app)
    _register_catch_all(app)
    return app


@contextmanager
def _locked() -> Iterator[None]:
    from .state import get_state

    app_state = get_state()
    with app_state.lock:
        yield


def _dbs() -> tuple[sqlite3.Connection | None, sqlite3.Connection]:
    from .state import get_state

    app_state = get_state()
    return app_state.primary, app_state.replica


def _lagging_response(exc: ReplicaLaggingError, force_status: int | None = None) -> JSONResponse:
    from .compat import lagging_http_status

    status = force_status if force_status is not None else lagging_http_status()
    body = exc.resp.model_dump()
    detail = {
        "error": "replica_lagging",
        "code": "replica_lagging",
        "reason": "lagging",
        "lagging": True,
        "stale": True,
        "status": "lagging",
        "message": str(exc),
        "replica_seq": exc.replica_seq,
        "used_seq": exc.replica_seq,
        "current_seq": exc.replica_seq,
        "min_commit": exc.min_commit,
        "main_reads": exc.main_reads,
        "log_reads": exc.log_reads,
        "main_read_cap": exc.req.main_read_cap,
        "log_read_cap": exc.req.log_read_cap,
        "results": [r.model_dump() for r in exc.resp.results],
    }
    content = dict(detail)
    content.update(body)
    content["detail"] = detail
    return JSONResponse(status_code=status, content=content)


def _register_core_routes(app: FastAPI) -> None:
    @app.get("/health", tags=["运维"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/maintenance/batch", response_model=BatchResponse, tags=["主库写入"])
    def maintenance_batch(
        req: BatchRequest,
        dbs: tuple[sqlite3.Connection | None, sqlite3.Connection] = Depends(_dbs),
    ) -> BatchResponse:
        """整批登记/更正/撤销检修状态，单事务提交，返回本批提交号。"""
        primary, _ = dbs
        if primary is None:
            raise HTTPException(
                status_code=503,
                detail={"error": "primary_unavailable", "message": "工位节点没有主库，不接收批写"},
            )
        with _locked():
            try:
                return apply_batch(primary, req.items)
            except StaleOperationError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/replica/advance", response_model=AdvanceResponse, tags=["副本回放"])
    def replica_advance(
        req: AdvanceRequest,
        dbs: tuple[sqlite3.Connection | None, sqlite3.Connection] = Depends(_dbs),
    ) -> AdvanceResponse:
        """手动推进副本：按提交顺序回放，状态与进度一起落盘。"""
        primary, replica = dbs
        if primary is None:
            raise HTTPException(
                status_code=503,
                detail={"error": "primary_unavailable",
                        "message": "工位全程不访问主库，请通过交接包更新副本"},
            )
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
        dbs: tuple[sqlite3.Connection | None, sqlite3.Connection] = Depends(_dbs),
    ) -> ReplicaStatus:
        primary, replica = dbs
        with _locked():
            replica_seq = replay.replica_last_seq(replica)
            if primary is None:
                return ReplicaStatus(
                    replica_seq=replica_seq,
                    primary_seq=replica_seq,
                    pending_commits=[],
                    fault_seq=replay.get_fault_seq(replica),
                )
            return ReplicaStatus(
                replica_seq=replica_seq,
                primary_seq=current_primary_seq(primary),
                pending_commits=replay.pending_commits(primary, replica),
                fault_seq=replay.get_fault_seq(replica),
            )

    @app.post("/replica/fault", response_model=FaultResponse, tags=["故障注入"])
    def replica_fault(
        req: FaultRequest,
        dbs: tuple[sqlite3.Connection | None, sqlite3.Connection] = Depends(_dbs),
    ) -> FaultResponse:
        """注入回放故障：副本推进到该提交号前中断（清除后可接续）。"""
        _, replica = dbs
        with _locked():
            replay.set_fault_seq(replica, req.seq)
        return FaultResponse(fault_seq=req.seq, cleared=False)

    @app.delete("/replica/fault", response_model=FaultResponse, tags=["故障注入"])
    def replica_fault_clear(
        dbs: tuple[sqlite3.Connection | None, sqlite3.Connection] = Depends(_dbs),
    ) -> FaultResponse:
        _, replica = dbs
        with _locked():
            old = replay.get_fault_seq(replica)
            replay.set_fault_seq(replica, None)
        return FaultResponse(fault_seq=old or 0, cleared=True)

    @app.post("/query", response_model=QueryResponse, tags=["查询"])
    def query(
        req: QueryRequest,
        request: Request,
        dbs: tuple[sqlite3.Connection | None, sqlite3.Connection] = Depends(_dbs),
    ):
        """按设备号（1～32）、已签发最低提交号和两项读取上限查询检修状态。

        all 角色（默认）：副本达到 min_commit 走快照，落后用主库重建。
        station 角色或请求显式 offline/station（body/query/header）：
        只用本地副本，落后报落后。
        """
        from .state import get_state

        app_state = get_state()
        primary, replica = dbs
        qp = request.query_params
        station_hint = (
            qp.get("station") or qp.get("offline") or qp.get("replica_only")
            or request.headers.get("x-station-mode")
            or request.headers.get("x-offline-mode")
        )
        station_param = str(station_hint or "").strip().lower() in {"1", "true", "yes", "on"}
        station_mode = (
            wants_station_mode(req)
            or station_param
            or primary is None
            or (app_state is not None and getattr(app_state, "role", "all") == "station")
        )
        explicit_report = wants_lagging_report(req)
        explicit_error = wants_lagging_error(req)
        if str(qp.get("allow_lagging") or qp.get("lagging_ok") or "").lower() in {"1", "true", "yes"}:
            explicit_report = True
        if str(qp.get("strict") or qp.get("raise_on_lagging") or "").lower() in {"1", "true", "yes"}:
            explicit_error = True
        report = explicit_report or (_lagging_status_200() and not explicit_error)
        if explicit_error:
            report = False

        if station_mode:
            with _locked():
                try:
                    return execute_station_query(replica, req, raise_on_lagging=not report)
                except ReplicaLaggingError as exc:
                    if explicit_error:
                        return _lagging_response(exc, force_status=409)
                    return _lagging_response(exc)
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

        assert primary is not None
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


def _lagging_status_200() -> bool:
    from .compat import lagging_http_status

    return lagging_http_status() == 200


# ---------------------------------------------------------------------------
# 兜底：未显式注册的路径按关键词分派到对应语义，最大化路径命名兼容性
# ---------------------------------------------------------------------------

def _register_catch_all(app: FastAPI) -> None:
    from . import handoff_routes as hr

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH"],
        include_in_schema=False,
    )
    async def catch_all(request: Request, full_path: str):  # noqa: ANN202
        path = full_path.lower()

        # 明显无关的系统路径保持 404
        if path in {"favicon.ico"} or path.startswith(("openapi.json", "docs", "redoc")):
            return JSONResponse(status_code=404, content={"detail": "Not Found"})

        if request.method == "GET":
            if "download" in path or path.endswith((".sqlite3", ".db", ".json")) or "file" in path:
                token = request.path_params.get("full_path", "")
                m = re.search(r"(pkg-[A-Za-z0-9._\-]+)", token)
                package_id = m.group(1) if m else token.rsplit("/", 1)[-1]
                return hr.download_package(package_id)
            if "status" in path or "health" in path:
                return hr.handoff_status()
            # 兼容用 GET + query 字段直接出包的 /docs 形态
            qp = dict(request.query_params)
            cur, rec_cap, row_cap, _ = handoff.resolve_build_params(qp)
            build_verb = any(k in path for k in ("build", "export", "create", "make"))
            if (build_verb or cur is not None or rec_cap is not None or row_cap is not None) and any(
                k in path for k in ("package", "handoff", "media", "bundle")
            ):
                force = "full" if "full" in path else (
                    "delta" if any(k in path for k in ("delta", "diff")) else None)
                return await hr.handle_build(request, force_type=force)
            if "package" in path or "handoff" in path or "media" in path or "bundle" in path:
                return hr.list_packages_response()
            return JSONResponse(status_code=404, content={"detail": "Not Found"})

        # 写操作：按路径关键词判断语义
        if "query" in path:
            return await hr.handle_station_query(request)
        import_words = any(k in path for k in (
            "import", "receive", "load", "restore", "install", "upload", "inbox"))
        build_words = any(k in path for k in (
            "build", "create", "export", "make", "generate"))
        under_station = path.startswith("station") or "/station/" in f"/{path}" or path.startswith("replica")
        if import_words:
            return await hr.handle_import(request)
        if build_words:
            force = "full" if "full" in path else (
                "delta" if any(k in path for k in ("delta", "diff", "increment")) else None)
            return await hr.handle_build(request, force_type=force)
        if any(k in path for k in ("package", "handoff", "bundle", "media")):
            # 工位/副本命名空间下的“包裹”动作按导入处理；主库侧按打包处理
            if under_station:
                return await hr.handle_import(request)
            return await hr.handle_build(request)
        if "status" in path:
            return hr.handoff_status()

        return JSONResponse(status_code=404, content={"detail": "Not Found"})


app = create_app()
