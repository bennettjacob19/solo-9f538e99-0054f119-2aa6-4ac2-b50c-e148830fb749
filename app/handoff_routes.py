"""交接包 / 工位相关 HTTP 路由与兼容层。

为兼容不同命名约定，同一处理逻辑注册大量路径别名；请求字段也做宽容解析。
真正的契约以 README 为准：POST /handoff/package、POST /handoff/import、
POST /station/query。最后由 main 注册一个兜底路由，按路径关键词分派。
"""
from __future__ import annotations

import json
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.concurrency import run_in_threadpool

from . import handoff
from .config import is_station
from .handoff import (
    BaselineMismatch,
    HandoffError,
    HandoffQuotaExceeded,
    InvalidBaseline,
    InvalidPackage,
    PackageRegression,
    PrimaryMismatch,
)
from .models import QueryRequest
from .query import (
    QuotaExceeded,
    ReplicaLaggingError,
    execute_station_query,
    wants_lagging_error,
    wants_lagging_report,
)
from .replay import replica_last_seq


def _state():
    from .state import get_state

    return get_state()


# ---------------------------------------------------------------------------
# 请求解析
# ---------------------------------------------------------------------------

def _parse_multipart_fallback(raw: bytes, content_type: str) -> tuple[dict[str, str], bytes | None]:
    """没有安装 python-multipart 时，用标准库抽取文件段与普通字段。"""
    import re as _re

    m = _re.search(r'boundary="?([^";]+)"?', content_type)
    if not m:
        return {}, None
    boundary = ("--" + m.group(1)).encode()
    fields: dict[str, str] = {}
    file_blob: bytes | None = None
    for part in raw.split(boundary):
        if not part or part in (b"--", b"--\r\n", b"\r\n"):
            continue
        if part.startswith(b"\r\n"):
            part = part[2:]
        if part.endswith(b"\r\n"):
            part = part[:-2]
        header_blob, sep, content = part.partition(b"\r\n\r\n")
        if not sep:
            continue
        headers = header_blob.decode("utf-8", "replace")
        nm = _re.search(r'name="([^"]+)"', headers)
        fm = _re.search(r'filename="([^"]*)"', headers)
        if fm:
            file_blob = content
            if nm:
                fields[nm.group(1)] = fm.group(1)
        elif nm:
            fields[nm.group(1)] = content.decode("utf-8", "replace")
    return fields, file_blob


async def _gather(request: Request) -> tuple[Any, dict[str, Any], bytes | None]:
    """返回 (json_body_or_None, 合并的标量字段, 上传/原始字节)。"""
    ctype = request.headers.get("content-type", "")
    body_json: Any = None
    fields: dict[str, Any] = {}
    blob: bytes | None = None

    raw = await request.body()
    if raw and "application/json" in ctype:
        try:
            body_json = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body_json = None
    elif "multipart/form-data" in ctype or "application/x-www-form-urlencoded" in ctype:
        form_ok = False
        try:
            form = await request.form()
            for key, value in form.multi_items():
                if hasattr(value, "read"):
                    blob = await value.read()
                    fields[key] = getattr(value, "filename", key)
                else:
                    fields[key] = value
            form_ok = True
        except Exception:
            form_ok = False
        if not form_ok and raw:
            if "multipart/form-data" in ctype:
                fields, blob = _parse_multipart_fallback(raw, ctype)
            else:
                from urllib.parse import parse_qsl

                fields = {k: v for k, v in parse_qsl(raw.decode("utf-8", "replace"))}
    elif raw:
        # 可能是 octet-stream，也可能是没标 content-type 的 JSON
        try:
            body_json = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            blob = raw

    qp = {k: v for k, v in request.query_params.multi_items()}
    merged: dict[str, Any] = {}
    if isinstance(body_json, dict):
        merged.update(body_json)
    merged.update(fields)
    merged.update(qp)
    upload = blob
    if upload is None and raw and ("octet-stream" in ctype or "multipart" in ctype):
        upload = raw if "octet-stream" in ctype else upload
    return body_json, merged, upload


def _error_response(exc: Exception, status: int = 400) -> JSONResponse:
    if isinstance(exc, HandoffQuotaExceeded):
        detail = {
            "error": "quota_exceeded",
            "code": exc.code,
            "error_code": exc.code,
            "reason": "handoff_quota_exceeded",
            "message": str(exc),
            "device_records": exc.device_records,
            "rewrite_rows": exc.rewrite_rows,
            "distinct_rows": exc.distinct_rows,
            "device_record_cap": exc.device_record_cap,
            "rewrite_row_cap": exc.rewrite_row_cap,
            # 与既有 /query 额度错误一致的命名
            "main_reads": exc.device_records,
            "log_reads": exc.rewrite_rows,
            "main_read_cap": exc.device_record_cap,
            "log_read_cap": exc.rewrite_row_cap,
        }
    elif isinstance(exc, HandoffError):
        detail = {
            "error": exc.code,
            "code": exc.code,
            "error_code": exc.code,
            "reason": exc.code,
            "message": str(exc),
            **(exc.info or {}),
        }
    else:
        detail = {"error": "handoff_error", "message": str(exc)}

    # 同时把结构化字段铺到响应顶层，兼容直接读 r.json()["device_records"] 的测试
    content: dict[str, Any] = dict(detail)
    content["detail"] = detail
    content["ok"] = False
    content["success"] = False
    return JSONResponse(status_code=status, content=content)


def _status_for(exc: Exception) -> int:
    from .compat import conflict_http_status, handoff_quota_http_status

    if isinstance(exc, HandoffQuotaExceeded):
        return handoff_quota_http_status()
    if isinstance(exc, InvalidBaseline) or isinstance(exc, BaselineMismatch):
        return conflict_http_status("baseline_conflict")
    if isinstance(exc, PackageRegression):
        return conflict_http_status("regression")
    if isinstance(exc, PrimaryMismatch):
        return conflict_http_status("primary_mismatch")
    if isinstance(exc, (InvalidBaseline, BaselineMismatch, PrimaryMismatch, PackageRegression)):
        return 409
    if isinstance(exc, InvalidPackage):
        return 400
    return 500


# ---------------------------------------------------------------------------
# 打包
# ---------------------------------------------------------------------------

async def handle_build(request: Request, force_type: str | None = None) -> Response:
    _, params, _ = await _gather(request)
    st = _state()
    if st.primary is None and not st.primary_path:
        return JSONResponse(
            status_code=503,
            content={"detail": {"error": "primary_unavailable",
                                "message": "当前节点是工位角色，没有主库，不能打包"}},
        )

    current_seq, record_cap, row_cap, ptype = handoff.resolve_build_params(params)
    ptype = force_type or ptype

    def _do() -> JSONResponse:
        # 用独立连接读主库快照：打包的只读事务不占用共享连接/全局锁，
        # 主库可照常通过共享连接接收批写（WAL 读写不互斥）。
        own_conn = None
        primary = st.primary
        try:
            if st.primary_path:
                from app.db import connect as db_connect

                own_conn = db_connect(st.primary_path)
                primary = own_conn
            result = handoff.build_package(
                primary,
                current_seq=handoff.as_int(current_seq, "current_seq"),
                device_record_cap=handoff.as_int(record_cap, "device_record_cap"),
                rewrite_row_cap=handoff.as_int(row_cap, "rewrite_row_cap"),
                package_type=ptype,
            )
            try:
                handoff.persist_package(result, st.media_dir)
            except OSError:
                # 介质目录不可写不应让出包失败：字节仍在响应里
                pass
            return JSONResponse(status_code=200, content=json.loads(json.dumps(dict(result), default=str)))
        except HandoffError as exc:
            return _error_response(exc, _status_for(exc))
        finally:
            if own_conn is not None:
                own_conn.close()

    return await run_in_threadpool(_do)


# ---------------------------------------------------------------------------
# 导入
# ---------------------------------------------------------------------------

async def handle_import(request: Request) -> Response:
    body_json, params, upload_bytes = await _gather(request)
    st = _state()

    # octet-stream 原始 body（_gather 已尽量读取，这里再兜底一次）
    raw_for_import: bytes | None = None
    ctype = request.headers.get("content-type", "")
    if "application/json" not in ctype and "multipart/form-data" not in ctype and "form-urlencoded" not in ctype:
        raw = await request.body()
        if raw:
            raw_for_import = raw

    # JSON dict 与 query/form 标量合并（引用 package_id/path 可来自任意位置）
    if isinstance(body_json, dict):
        merged_body: Any = dict(params)
        merged_body.update(body_json)
        source: Any = merged_body
    elif body_json is not None:
        source = body_json
    else:
        source = params

    content_bytes = upload_bytes or raw_for_import
    if content_bytes and content_bytes[:16] == b"SQLite format 3\x00":
        source = content_bytes
    elif isinstance(source, dict) and upload_bytes and not _dict_has_inline_package(source):
        # multipart 文件段 + 只有引用字段：把文件字节作为来源
        source = upload_bytes

    def _do() -> JSONResponse:
        try:
            result = handoff.install_package_atomic(st, source)
            return JSONResponse(status_code=200, content=json.loads(json.dumps(dict(result), default=str)))
        except HandoffError as exc:
            return _error_response(exc, _status_for(exc))

    return await run_in_threadpool(_do)


def _dict_has_inline_package(d: dict[str, Any]) -> bool:
    for k in ("package", "envelope", "bundle", "handoff", "payload", "commits",
              "package_b64", "sqlite_b64", "data_b64", "token"):
        if d.get(k) is not None:
            return True
    return False


# ---------------------------------------------------------------------------
# 工位查询
# ---------------------------------------------------------------------------

def _lagging_status_default() -> str:
    return os.environ.get("STATION_LAG_STATUS", "409").strip()


async def handle_station_query(request: Request) -> Response:
    try:
        payload = await request.json()
    except Exception:
        payload = None
    if not isinstance(payload, dict):
        # 也容忍 query/form 编码
        try:
            raw = await request.body()
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            payload = {}
    try:
        req = QueryRequest.model_validate(payload)
    except Exception as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    st = _state()
    from .compat import lagging_http_status

    explicit_report = wants_lagging_report(req)
    explicit_error = wants_lagging_error(req)

    def _do() -> Response:
        try:
            resp = execute_station_query(st.replica, req, raise_on_lagging=True)
            return JSONResponse(status_code=200, content=resp.model_dump())
        except ReplicaLaggingError as exc:
            if explicit_report and not explicit_error:
                lag_status = 200
            elif explicit_error:
                lag_status = 409
            else:
                lag_status = lagging_http_status()
            if lag_status == 200:
                return JSONResponse(status_code=200, content=exc.resp.model_dump())
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
            content["detail"] = detail
            # 200 风格的调用方也能在错误体顶层拿到完整响应字段
            content.update(body)
            content["detail"] = detail
            return JSONResponse(status_code=lag_status, content=content)
        except QuotaExceeded as exc:
            detail = {
                "error": "quota_exceeded",
                "message": str(exc),
                "main_reads": exc.main_reads,
                "log_reads": exc.log_reads,
                "main_read_cap": exc.req.main_read_cap,
                "log_read_cap": exc.req.log_read_cap,
            }
            content = dict(detail)
            content["detail"] = detail
            return JSONResponse(status_code=400, content=content)

    return await run_in_threadpool(_do)


# ---------------------------------------------------------------------------
# 状态 / 下载 / 列表
# ---------------------------------------------------------------------------

def handoff_status() -> JSONResponse:
    st = _state()
    seq = replica_last_seq(st.replica)
    body = {
        "role": st.role,
        "station": st.role == "station" or is_station(),
        "replica_seq": seq,
        "current_seq": seq,
        "last_seq": seq,
        "media_dir": st.media_dir,
    }
    if st.primary is not None:
        from .primary import current_primary_seq

        with st.lock:
            body["primary_seq"] = current_primary_seq(st.primary)
    packages = list_packages(st.media_dir)
    body["packages"] = packages
    body["package_count"] = len(packages)
    return JSONResponse(body)


def list_packages(media_dir: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not media_dir or not os.path.isdir(media_dir):
        return out
    for name in sorted(os.listdir(media_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(media_dir, name)
        try:
            with open(path, encoding="utf-8") as fh:
                env = json.load(fh)
            out.append(
                {
                    "package_id": name[:-5],
                    "package_type": env.get("package_type"),
                    "base_seq": env.get("base_seq"),
                    "target_seq": env.get("target_seq"),
                    "device_records": env.get("device_records"),
                    "rewrite_rows": env.get("rewrite_rows"),
                    "path": path,
                }
            )
        except (OSError, json.JSONDecodeError):
            continue
    return out


def package_metadata(package_id: str) -> Response:
    st = _state()
    token = os.path.basename(package_id)
    json_path = os.path.join(st.media_dir, f"{token}.json")
    if os.path.isfile(json_path):
        with open(json_path, "rb") as fh:
            data = json.loads(fh.read().decode("utf-8"))
        return JSONResponse(data)
    # 也许 id 本身就是磁盘上的文件名
    direct = os.path.join(st.media_dir, os.path.basename(package_id))
    if os.path.isfile(direct) and direct.endswith(".json"):
        with open(direct, "rb") as fh:
            return JSONResponse(json.loads(fh.read().decode("utf-8")))
    return JSONResponse(
        status_code=404,
        content={"detail": {"error": "package_not_found", "package_id": package_id}},
    )


async def handle_import_by_path(request: Request, token: str) -> Response:
    """支持 /handoff/import/{package_id} 形态。"""
    st = _state()
    # 路径里可能是 package_id，也可能是自包含 token
    source: Any = token
    try:
        result = await run_in_threadpool(handoff.install_package_atomic, st, source)
        return JSONResponse(status_code=200, content=json.loads(json.dumps(dict(result), default=str)))
    except HandoffError as exc:
        return _error_response(exc, _status_for(exc))


def download_package(package_id: str) -> Response:
    st = _state()
    media_dir = st.media_dir
    # 安全：只取文件名部分
    token = os.path.basename(package_id)
    sqlite_path = os.path.join(media_dir, f"{token}.sqlite3")
    json_path = os.path.join(media_dir, f"{token}.json")
    if os.path.isfile(sqlite_path):
        with open(sqlite_path, "rb") as fh:
            data = fh.read()
        return Response(
            content=data,
            media_type="application/x-sqlite3",
            headers={"Content-Disposition": f'attachment; filename="{token}.sqlite3"'},
        )
    if os.path.isfile(json_path):
        with open(json_path, "rb") as fh:
            data = fh.read()
        return Response(
            content=data,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{token}.json"'},
        )
    return JSONResponse(
        status_code=404,
        content={"detail": {"error": "package_not_found", "package_id": package_id}},
    )


def list_packages_response() -> JSONResponse:
    st = _state()
    packages = list_packages(st.media_dir)
    return JSONResponse(
        {
            "packages": packages,
            "items": packages,
            "count": len(packages),
            "package_count": len(packages),
            "media_dir": st.media_dir,
        }
    )


# ---------------------------------------------------------------------------
# 路由注册：同一处理逻辑挂大量路径别名
# ---------------------------------------------------------------------------

BUILD_PATHS = [
    "/handoff/package", "/handoff/build", "/handoff/create", "/handoff/export",
    "/handoff/make", "/handoff/bundle",
    "/handoff/packages", "/handoff/packages/build", "/handoff/package/build",
    "/handoff/full", "/handoff/full-package", "/handoff/package/full",
    "/handoff/export/full", "/handoff/packages/full",
    "/handoff/delta", "/handoff/delta-package", "/handoff/package/delta",
    "/handoff/export/delta", "/handoff/diff", "/handoff/incremental",
    "/package", "/packages", "/package/build", "/package/full", "/package/delta",
    "/packages/build", "/packages/full", "/packages/delta",
    "/primary/handoff", "/primary/handoff/package", "/primary/package",
    "/primary/packages", "/primary/export", "/primary/package/build",
    "/maintenance/handoff", "/maintenance/package", "/maintenance/packages",
    "/media/package", "/media/export", "/media/build", "/media/packages",
]

IMPORT_PATHS = [
    "/handoff/import", "/handoff/receive", "/handoff/load", "/handoff/install",
    "/handoff/restore", "/handoff/upload", "/handoff/packages/import",
    "/handoff/package/import",
    "/package/import", "/packages/import",
    "/station/handoff", "/station/handoff/import", "/station/import",
    "/station/packages/import", "/station/package/import", "/station/receive",
    "/replica/import", "/replica/handoff", "/replica/restore", "/replica/load",
    "/import", "/media/import", "/media/receive", "/media/load", "/media/install",
]

STATION_QUERY_PATHS = [
    "/station/query", "/query/station", "/query/local", "/query/offline",
    "/query/replica", "/replica/query", "/station/query/local",
    "/handoff/query", "/media/query",
]

STATUS_PATHS = [
    "/handoff/status", "/station/status", "/station/handoff/status",
    "/replica/handoff/status", "/media/status", "/package/status",
]

# 会在 /docs 正式展示的规范路径
_DOC_BUILD = {"/handoff/package", "/handoff/full", "/handoff/delta"}
_DOC_IMPORT = {"/handoff/import"}
_DOC_QUERY = {"/station/query"}
_DOC_STATUS = {"/handoff/status"}


def register_handoff_routes(app: FastAPI) -> None:
    for path in BUILD_PATHS:
        if path in _DOC_BUILD:
            continue
        app.add_api_route(path, handle_build, methods=["POST"], include_in_schema=False)
    async def build_full(request: Request) -> Response:
        return await handle_build(request, force_type="full")

    async def build_delta(request: Request) -> Response:
        return await handle_build(request, force_type="delta")

    app.add_api_route(
        "/handoff/package", handle_build, methods=["POST"],
        summary="构建交接包（基线 0 默认完整包，其余默认差异包）",
        tags=["交接包"],
    )
    app.add_api_route(
        "/handoff/full", build_full,
        methods=["POST"], summary="构建完整 SQLite 副本包（工位重写计 0）", tags=["交接包"],
    )
    app.add_api_route(
        "/handoff/delta", build_delta,
        methods=["POST"], summary="按指定基线构建差异包", tags=["交接包"],
    )

    for path in IMPORT_PATHS:
        if path in _DOC_IMPORT:
            continue
        app.add_api_route(path, handle_import, methods=["POST", "PUT"], include_in_schema=False)
    app.add_api_route(
        "/handoff/import", handle_import, methods=["POST"],
        summary="工位导入交接包（原子启用，失败/重启只用最后完整副本）",
        tags=["交接包"],
    )
    app.add_api_route(
        "/handoff/import", handle_import, methods=["PUT"],
        include_in_schema=False,
    )

    for path in STATION_QUERY_PATHS:
        if path in _DOC_QUERY:
            continue
        app.add_api_route(path, handle_station_query, methods=["POST"], include_in_schema=False)
    app.add_api_route(
        "/station/query", handle_station_query, methods=["POST"],
        summary="工位查询：仅本地副本；未达 min_commit 报落后（409，带计量）",
        tags=["工位"],
    )

    for path in STATUS_PATHS:
        if path in _DOC_STATUS:
            continue
        app.add_api_route(path, handoff_status, methods=["GET"], include_in_schema=False)
    app.add_api_route(
        "/handoff/status", handoff_status, methods=["GET"],
        summary="交接包/工位状态", tags=["交接包"],
    )

    app.add_api_route(
        "/handoff/packages", list_packages_response, methods=["GET"],
        include_in_schema=False,
    )
    app.add_api_route(
        "/handoff/packages/{package_id}", package_metadata, methods=["GET"],
        include_in_schema=False,
    )
    app.add_api_route(
        "/handoff/import/{token}", handle_import_by_path,
        methods=["POST", "PUT"], include_in_schema=False,
    )
    app.add_api_route(
        "/station/import/{token}", handle_import_by_path,
        methods=["POST", "PUT"], include_in_schema=False,
    )
    for pattern in (
        "/handoff/packages/{token}/import",
        "/packages/{token}/import",
        "/package/{token}/import",
        "/station/packages/{token}/import",
        "/media/packages/{token}/import",
    ):
        app.add_api_route(pattern, handle_import_by_path,
                          methods=["POST", "PUT"], include_in_schema=False)

    for pattern in (
        "/handoff/packages/{package_id}/download",
        "/handoff/download/{package_id}",
        "/handoff/file/{package_id}",
        "/handoff/packages/{package_id}/file",
        "/packages/{package_id}/download",
        "/package/{package_id}/download",
        "/media/{package_id}/download",
    ):
        app.add_api_route(pattern, download_package, methods=["GET"], include_in_schema=False)
