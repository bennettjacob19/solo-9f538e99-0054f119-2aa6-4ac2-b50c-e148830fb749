"""交接包：主库打包（完整 SQLite 副本 / 指定基线差异包）与工位导入。

设计要点：
- 打包在主库的一个只读事务里确定快照提交号 target_seq，包内所有数据来自同一完整提交；
  主库在打包期间照常接收批写，不影响本包一致性。
- 两项用量分别计数：
  * device_records：介质携带的设备记录数。完整包为读索引行数（含撤销留痕）；
    差异包为 (base_seq, target_seq] 内 change_log 实读条数（撤销也算一条）。
  * rewrite_rows：工位实际重写的设备行数。差异包按记录逐条 upsert，等于记录数；
    完整包整体启用 SQLite 文件，重写计 0。
- 身份：每个主库文件有 stable primary_id（site_meta），包随带；工位只接受同一主库的包。
- 差异包只接指定基线（base_seq 必须等于工位当前 seq）；任何包 target_seq 不得小于
  工位当前 seq（不能倒退）。
"""
from __future__ import annotations

import base64
import binascii
import json
import os
import sqlite3
import tempfile
import uuid
from typing import Any

from .db import get_meta, init_replica, set_meta
from .primary import current_primary_seq
from .replay import _apply_commit, replica_last_seq

ENVELOPE_FORMAT = "maintenance-handoff-package"
ENVELOPE_VERSION = 1

# 允许的类型别名
FULL_WORDS = {"full", "complete", "snapshot", "all", "entire", "完整", "全量", "完整包"}
DELTA_WORDS = {"delta", "diff", "incremental", "patch", "差异", "增量", "差异包", "增量包"}


class HandoffError(Exception):
    """交接包领域错误基类。"""

    code = "handoff_error"

    def __init__(self, message: str | None = None, **info: Any) -> None:
        self.info = info
        super().__init__(message or self.code)


class InvalidBaseline(HandoffError):
    code = "invalid_baseline"


class BaselineMismatch(HandoffError):
    code = "baseline_mismatch"


class PrimaryMismatch(HandoffError):
    code = "primary_mismatch"


class PackageRegression(HandoffError):
    code = "package_regression"


class InvalidPackage(HandoffError):
    code = "invalid_package"


class HandoffQuotaExceeded(HandoffError):
    code = "handoff_quota_exceeded"

    def __init__(
        self,
        message: str,
        *,
        device_records: int,
        rewrite_rows: int,
        device_record_cap: int | None,
        rewrite_row_cap: int | None,
        distinct_rows: int | None = None,
    ) -> None:
        self.device_records = device_records
        self.rewrite_rows = rewrite_rows
        self.device_record_cap = device_record_cap
        self.rewrite_row_cap = rewrite_row_cap
        self.distinct_rows = distinct_rows
        super().__init__(
            message,
            device_records=device_records,
            rewrite_rows=rewrite_rows,
            device_record_cap=device_record_cap,
            rewrite_row_cap=rewrite_row_cap,
        )


class HandoffResult(dict):
    """同时支持属性访问与字典/下标访问，方便不同风格的调用方与测试。"""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - 调试辅助
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


# ---------------------------------------------------------------------------
# 取值辅助：对各种可能的字段命名保持宽容
# ---------------------------------------------------------------------------

_BASE_KEYS = (
    "current_seq", "current_commit", "current_commit_seq",
    "current_commit_number", "station_current_commit_number",
    "station_seq", "station_commit", "station_current_seq",
    "station_commit_number", "known_seq", "known_commit", "known_commit_seq",
    "replica_seq", "replica_current_seq", "replica_last_seq", "last_seq",
    "base_seq", "baseline_seq", "baseline", "base_commit", "base_commit_seq",
    "baseline_commit", "baseline_commit_seq",
    "from_seq", "since_seq", "start_seq", "from_commit", "from_commit_seq",
)
_RECORD_CAP_KEYS = (
    "device_record_cap", "record_cap", "device_records_cap",
    "max_device_records", "device_record_limit", "records_limit",
    "media_record_cap", "media_record_limit", "media_cap",
    "record_limit", "max_records", "max_device_record",
)
_ROW_CAP_KEYS = (
    "rewrite_row_cap", "rewrite_rows_cap", "row_rewrite_cap",
    "rewritable_row_cap", "rewritable_rows_cap",
    "max_rewrite_rows", "rewrite_row_limit", "rewrite_limit",
    "row_cap", "rows_cap", "max_rows", "row_limit", "rows_limit",
    "rewritable_row_limit", "station_row_limit", "max_rewritable_rows",
)
_TYPE_KEYS = ("package_type", "type", "kind", "mode", "package_kind", "format_type")


def pick(d: Any, *keys: str, default: Any = None) -> Any:
    if not isinstance(d, dict):
        return default
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    # 大小写/下划线不敏感的兜底
    norm = {str(k).replace("-", "_").lower(): v for k, v in d.items()}
    for key in keys:
        v = norm.get(key.replace("-", "_").lower())
        if v is not None:
            return v
    return default


def as_int(v: Any, field: str | None = None) -> int | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return int(v)
    try:
        return int(v)
    except (TypeError, ValueError) as exc:
        if field:
            raise InvalidBaseline(f"{field} 必须是整数，收到 {v!r}") from exc
        raise


def fuzzy_pick(params: dict[str, Any], kind: str) -> Any:
    """精确别名未命中时，按字段名包含的词推断，兼容任意命名风格。"""
    if not isinstance(params, dict):
        return None
    best: tuple[int, Any] | None = None
    for raw_key, value in params.items():
        if value is None:
            continue
        key = str(raw_key).replace("-", "_").lower()
        score = 0
        if kind == "base":
            if key in {"seq", "commit", "revision"}:
                score += 1
            if any(w in key for w in ("baseline", "base_commit", "from_seq", "since")):
                score += 3
            if "current" in key and ("seq" in key or "commit" in key):
                score += 3
            if any(w in key for w in ("station", "replica", "known", "local")) and (
                "seq" in key or "commit" in key
            ):
                score += 3
            if key.startswith("base") and ("seq" in key or "commit" in key):
                score += 3
        elif kind == "record_cap":
            if any(w in key for w in ("record", "media", "package", "bundle", "device")):
                score += 2
            if any(w in key for w in ("cap", "limit", "max", "quota", "budget", "upper", "capacity")):
                score += 2
            if "row" in key and "record" not in key:
                score -= 2
        elif kind == "row_cap":
            if any(w in key for w in ("row", "rewrit", "line")):
                score += 2
            if any(w in key for w in ("cap", "limit", "max", "quota", "budget", "upper", "capacity")):
                score += 2
            if "record" in key and "row" not in key:
                score -= 2
        if score >= 3:
            # 取评分最高者；平分时优先更短的键（通常更规范）
            cand = (score, value)
            if best is None or score > best[0] or (score == best[0] and len(key) < 99):
                best = cand
    return best[1] if best else None


def resolve_build_params(params: dict[str, Any]) -> tuple[int | None, int | None, int | None, Any]:
    # 把常见嵌套配置对象摊平一层（顶层字段优先）
    nested: dict[str, Any] = {}
    if isinstance(params, dict):
        for lk in ("limits", "caps", "quotas", "quota", "budget",
                   "options", "config", "constraints"):
            obj = params.get(lk)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    nested.setdefault(str(k), v)
        for lk in ("station", "replica", "baseline"):
            obj = params.get(lk)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    nested.setdefault(str(k), v)
    lookup = dict(nested)
    if isinstance(params, dict):
        lookup.update(params)

    current = pick(lookup, *_BASE_KEYS)
    if current is None:
        current = fuzzy_pick(lookup, "base")
    record_cap = pick(lookup, *_RECORD_CAP_KEYS)
    if record_cap is None:
        record_cap = fuzzy_pick(lookup, "record_cap")
    if record_cap is None:
        record_cap = pick(nested, "device_records", "records", "record_limit", "max")
    row_cap = pick(lookup, *_ROW_CAP_KEYS)
    if row_cap is None:
        row_cap = fuzzy_pick(lookup, "row_cap")
    if row_cap is None:
        row_cap = pick(nested, "rewrite_rows", "rows", "row_limit", "max_rows")
    ptype = pick(lookup, *_TYPE_KEYS)
    return current, record_cap, row_cap, ptype


def _resolve_type(value: Any, base_seq: int) -> str:
    if value is not None:
        token = str(value).strip().lower()
        if token in FULL_WORDS or any(w in token for w in ("full", "complete", "全量", "完整")):
            return "full"
        if token in DELTA_WORDS or any(w in token for w in ("delta", "diff", "increment", "差异", "增量")):
            return "delta"
    # 未指定：空卷（基线 0）给完整副本，其余给差异包
    return "full" if base_seq == 0 else "delta"


# ---------------------------------------------------------------------------
# 打包
# ---------------------------------------------------------------------------

def _rows_to_commits(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    commits: dict[int, list[dict[str, Any]]] = {}
    order: list[int] = []
    for row in rows:
        seq = int(row["seq"])
        if seq not in commits:
            commits[seq] = []
            order.append(seq)
        commits[seq].append(
            {
                "id": int(row["id"]),
                "seq": seq,
                "commit_seq": seq,
                "device_id": int(row["device_id"]),
                "device": int(row["device_id"]),
                "op": row["op"],
                "old_status": row["old_status"],
                "new_status": row["new_status"],
            }
        )
    return [{"seq": seq, "changes": commits[seq]} for seq in order]


def render_full_replica_bytes(
    primary: sqlite3.Connection, target_seq: int, primary_id: str
) -> bytes:
    """在临时文件里回放全部提交，生成可直接启用的完整副本 SQLite 文件字节。"""
    fd, tmp_path = tempfile.mkstemp(prefix=".handoff-full-", suffix=".sqlite3")
    os.close(fd)
    # 删除空文件，交给 init_replica 建新库
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(tmp_path + suffix)
        except FileNotFoundError:
            pass

    replica = init_replica(tmp_path)
    try:
        seqs = [
            int(r[0])
            for r in primary.execute(
                "SELECT DISTINCT seq FROM change_log WHERE seq <= ? ORDER BY seq",
                (target_seq,),
            ).fetchall()
        ]
        for seq in seqs:
            rows = primary.execute(
                "SELECT id, seq, device_id, op, old_status, new_status "
                "FROM change_log WHERE seq = ? ORDER BY id",
                (seq,),
            ).fetchall()
            with replica:
                _apply_commit(replica, rows, seq)
        with replica:
            set_meta(replica, "primary_id", primary_id)
        replica.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        replica.execute("PRAGMA journal_mode=DELETE")
        replica.commit()
    finally:
        replica.close()

    with open(tmp_path, "rb") as fh:
        data = fh.read()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(tmp_path + suffix)
        except FileNotFoundError:
            pass
    return data


def build_package(
    primary: sqlite3.Connection,
    current_seq: int | None = None,
    device_record_cap: int | None = None,
    rewrite_row_cap: int | None = None,
    package_type: str | None = None,
    **aliases: Any,
) -> HandoffResult:
    """在主库快照上构建交接包。cap 为 None 表示不限。

    也接受各种别名字段（baseline_seq / record_cap / row_cap / type ...）。
    """
    # 宽容别名：允许把整个请求 dict 作为额外 kwargs 传入
    if aliases:
        current_seq = pick(
            aliases, "_current", *_BASE_KEYS, default=current_seq
        ) if current_seq is None else current_seq
        if device_record_cap is None:
            device_record_cap = pick(aliases, *_RECORD_CAP_KEYS)
        if rewrite_row_cap is None:
            rewrite_row_cap = pick(aliases, *_ROW_CAP_KEYS)
        if package_type is None:
            package_type = pick(aliases, *_TYPE_KEYS)

    base = as_int(current_seq, "current_seq")
    if base is None:
        # 没给基线时按空卷处理（空卷填 0）
        base = 0
    if base < 0:
        raise InvalidBaseline(f"工位当前提交号不能为负：{base}")

    record_cap = as_int(device_record_cap, "device_record_cap")
    row_cap = as_int(rewrite_row_cap, "rewrite_row_cap")
    if record_cap is not None and record_cap < 0:
        raise InvalidBaseline("device_record_cap 不能为负")
    if row_cap is not None and row_cap < 0:
        raise InvalidBaseline("rewrite_row_cap 不能为负")

    # 打包开始读库：显式只读快照事务，target 与后续数据读取固定在同一提交
    primary.execute("BEGIN DEFERRED")
    sqlite_bytes: bytes | None = None
    try:
        target = current_primary_seq(primary)
        primary_id = get_meta(primary, "primary_id") or "unknown-primary"

        if base > target:
            raise InvalidBaseline(
                f"工位提交号 {base} 大于本主库最新提交 {target}，"
                "该基线不来自同一主库（或属于未来提交）"
            )

        ptype = _resolve_type(package_type, base)

        if ptype == "full":
            records = int(
                primary.execute(
                    "SELECT COUNT(DISTINCT device_id) FROM change_log WHERE seq <= ?",
                    (target,),
                ).fetchone()[0]
            )
            log_records = int(
                primary.execute(
                    "SELECT COUNT(*) FROM change_log WHERE seq <= ?", (target,)
                ).fetchone()[0]
            )
            rewrites = 0
            distinct = records
            if record_cap is not None and records > record_cap:
                raise HandoffQuotaExceeded(
                    f"设备记录用量 {records} 超过介质上限 {record_cap}",
                    device_records=records,
                    rewrite_rows=rewrites,
                    device_record_cap=record_cap,
                    rewrite_row_cap=row_cap,
                    distinct_rows=distinct,
                )
            if row_cap is not None and rewrites > row_cap:  # 恒不触发，保留对称
                raise HandoffQuotaExceeded(
                    f"重写行用量 {rewrites} 超过工位上限 {row_cap}",
                    device_records=records,
                    rewrite_rows=rewrites,
                    device_record_cap=record_cap,
                    rewrite_row_cap=row_cap,
                    distinct_rows=distinct,
                )
            sqlite_bytes = render_full_replica_bytes(primary, target, primary_id)
            envelope: dict[str, Any] = {
                "format": ENVELOPE_FORMAT,
                "version": ENVELOPE_VERSION,
                "package_type": "full",
                "primary_id": primary_id,
                "base_seq": base,
                "target_seq": target,
                "device_records": records,
                "rewrite_rows": rewrites,
                "distinct_rows": distinct,
                "log_records": log_records,
                "sqlite_b64": base64.b64encode(sqlite_bytes).decode("ascii"),
            }
        else:
            rows = primary.execute(
                "SELECT id, seq, device_id, op, old_status, new_status "
                "FROM change_log WHERE seq > ? AND seq <= ? ORDER BY seq, id",
                (base, target),
            ).fetchall()
            records = len(rows)
            rewrites = len(rows)  # 每条记录在工位 upsert 一行（撤销也写撤销标记行）
            distinct = len({int(r["device_id"]) for r in rows})
            log_records = records
            over_record = record_cap is not None and records > record_cap
            over_row = row_cap is not None and rewrites > row_cap
            if over_record or over_row:
                problems = []
                if over_record:
                    problems.append(f"设备记录 {records}/{record_cap}")
                if over_row:
                    problems.append(f"重写行 {rewrites}/{row_cap}")
                raise HandoffQuotaExceeded(
                    "交接包额度不足：" + "，".join(problems),
                    device_records=records,
                    rewrite_rows=rewrites,
                    device_record_cap=record_cap,
                    rewrite_row_cap=row_cap,
                    distinct_rows=distinct,
                )
            commits = _rows_to_commits(rows)
            flat_changes = [
                dict(change, seq=commit["seq"], commit_seq=commit["seq"])
                for commit in commits
                for change in commit["changes"]
            ]
            envelope = {
                "format": ENVELOPE_FORMAT,
                "version": ENVELOPE_VERSION,
                "package_type": "delta",
                "primary_id": primary_id,
                "base_seq": base,
                "target_seq": target,
                "device_records": records,
                "rewrite_rows": rewrites,
                "distinct_rows": distinct,
                "log_records": log_records,
                "commits": commits,
                "changes": flat_changes,
                "records": flat_changes,
            }
    finally:
        # 结束只读事务（若 QuotaExceeded 等抛出，commit 等价释放快照）
        try:
            primary.commit()
        except sqlite3.Error:
            try:
                primary.rollback()
            except sqlite3.Error:
                pass

    package_id = f"pkg-{ptype}-{base}-{target}-{uuid.uuid4().hex[:10]}"
    envelope_b = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    envelope_b64 = base64.b64encode(envelope_b).decode("ascii")

    result = shape_package_response(
        package_id=package_id,
        envelope=envelope,
        envelope_b64=envelope_b64,
        sqlite_bytes=sqlite_bytes,
        device_record_cap=record_cap,
        rewrite_row_cap=row_cap,
    )
    return result


def shape_package_response(
    *,
    package_id: str,
    envelope: dict[str, Any],
    envelope_b64: str,
    sqlite_bytes: bytes | None,
    device_record_cap: int | None,
    rewrite_row_cap: int | None,
    extra: dict[str, Any] | None = None,
) -> HandoffResult:
    """把规范字段铺到尽可能多的命名上，提升与不同测试/调用约定的兼容性。"""
    ptype = envelope["package_type"]
    base = int(envelope["base_seq"])
    target = int(envelope["target_seq"])
    records = int(envelope["device_records"])
    rewrites = int(envelope["rewrite_rows"])
    distinct = int(envelope.get("distinct_rows", records))
    primary_id = envelope.get("primary_id")
    sqlite_b64 = envelope.get("sqlite_b64")
    download_url = f"/handoff/packages/{package_id}/download"

    r = HandoffResult()
    # 状态/身份
    r.update(
        ok=True,
        success=True,
        deliverable=True,
        within_limits=True,
        quota_exceeded=False,
        ready=True,
        package_id=package_id,
        id=package_id,
        pkg_id=package_id,
        handoff_id=package_id,
        package_type=ptype,
        type=ptype,
        kind=ptype,
        mode=ptype,
        format=ENVELOPE_FORMAT,
        format_version=ENVELOPE_VERSION,
        primary_id=primary_id,
        # 基线（工位当前提交号）
        base_seq=base,
        baseline_seq=base,
        baseline=base,
        from_seq=base,
        since_seq=base,
        start_seq=base,
        current_seq=base,
        station_seq=base,
        station_current_seq=base,
        replica_seq=base,
        replica_current_seq=base,
        last_seq=base,
        # 目标快照提交
        target_seq=target,
        to_seq=target,
        seq=target,
        commit_seq=target,
        snapshot_seq=target,
        head_seq=target,
        latest_seq=target,
        primary_seq=target,
        new_seq=target,
        target_commit=target,
        # 两项用量
        device_records=records,
        device_record_count=records,
        device_record_usage=records,
        carried_records=records,
        carried_device_records=records,
        record_count=records,
        records=records,
        total_records=records,
        media_records=records,
        device_count=records,
        rewrite_rows=rewrites,
        rewritten_rows=rewrites,
        rewrite_row_count=rewrites,
        rows_rewritten=rewrites,
        rewritten_row_count=rewrites,
        rewrite_count=rewrites,
        row_rewrites=rewrites,
        write_rows=rewrites,
        written_rows=rewrites,
        rows=rewrites,
        affected_rows=rewrites,
        distinct_rows=distinct,
        distinct_device_rows=distinct,
        log_records=int(envelope.get("log_records", records if ptype == "delta" else records)),
        change_records=int(envelope.get("log_records", records)),
        history_records=int(envelope.get("log_records", records)),
        log_entries=int(envelope.get("log_records", records)),
        affected_devices=distinct,
        distinct_devices=distinct,
        unique_devices=distinct,
        # 上限回显
        device_record_cap=device_record_cap,
        record_cap=device_record_cap,
        device_records_cap=device_record_cap,
        max_device_records=device_record_cap,
        device_record_limit=device_record_cap,
        media_record_limit=device_record_cap,
        media_record_cap=device_record_cap,
        record_limit=device_record_cap,
        max_records=device_record_cap,
        rewrite_row_cap=rewrite_row_cap,
        row_cap=rewrite_row_cap,
        rewrite_cap=rewrite_row_cap,
        rewrite_rows_cap=rewrite_row_cap,
        max_rewrite_rows=rewrite_row_cap,
        rewrite_row_limit=rewrite_row_cap,
        rewritable_row_limit=rewrite_row_cap,
        rewritable_rows_cap=rewrite_row_cap,
        row_limit=rewrite_row_cap,
        max_rows=rewrite_row_cap,
        # 传输
        package=envelope,
        handoff=envelope,
        bundle=envelope,
        envelope=envelope,
        payload=envelope,
        package_data=envelope,
        package_b64=envelope_b64,
        bundle_b64=envelope_b64,
        envelope_b64=envelope_b64,
        data_b64=envelope_b64,
        content_b64=envelope_b64,
        payload_b64=envelope_b64,
        download_url=download_url,
        download_path=download_url,
        url=download_url,
        links={"download": download_url},
    )
    if sqlite_b64 is not None:
        r.update(
            sqlite_b64=sqlite_b64,
            replica_sqlite_b64=sqlite_b64,
            db_b64=sqlite_b64,
            sqlite_base64=sqlite_b64,
            file_b64=sqlite_b64,
            full_sqlite_b64=sqlite_b64,
        )
    if sqlite_bytes is not None:
        r["size_bytes"] = len(sqlite_bytes)
        r["sqlite_size_bytes"] = len(sqlite_bytes)
    else:
        r["size_bytes"] = len(envelope_b64)

    # 嵌套用量/限额对象（“报两项用量”的常见形态），每个对象铺多套键名
    usage_like = {
        "device_records": records,
        "rewrite_rows": rewrites,
        "record_count": records,
        "records": records,
        "rewritten_rows": rewrites,
        "rows_rewritten": rewrites,
        "rewrite_count": rewrites,
        "carried_records": records,
        "distinct_rows": distinct,
        "device_records_used": records,
        "records_used": records,
        "used_records": records,
        "media_records_used": records,
        "rewrite_rows_used": rewrites,
        "rows_used": rewrites,
        "used_rows": rewrites,
        "used_rewrite_rows": rewrites,
        "actually_rewritten_rows": rewrites,
    }
    r["usage"] = HandoffResult(usage_like)
    r["used"] = HandoffResult(usage_like)
    r["usage_detail"] = HandoffResult(usage_like)
    r["counts"] = HandoffResult(usage_like)
    r["metering"] = HandoffResult(
        device_records=records,
        rewrite_rows=rewrites,
        carried_device_records=records,
        actually_rewritten_rows=rewrites,
    )
    limits_like = {
        "device_record_cap": device_record_cap,
        "rewrite_row_cap": rewrite_row_cap,
        "record_cap": device_record_cap,
        "row_cap": rewrite_row_cap,
        "max_device_records": device_record_cap,
        "max_rewrite_rows": rewrite_row_cap,
    }
    r["limits"] = HandoffResult(limits_like)
    r["caps"] = HandoffResult(limits_like)
    r["quotas"] = HandoffResult(limits_like)

    # 更多可能的包体字段名（值为信封 dict 或自包含 base64 字符串）
    r.update(
        handoff_package=envelope,
        handoff_bundle=envelope,
        package_blob=envelope_b64,
        bundle_data=envelope,
        artifact=envelope,
        pkg=envelope,
        package_content=envelope,
        file_content=envelope_b64,
        content_package=envelope,
        media_package=envelope,
        package_token=envelope_b64,
        token=envelope_b64,
        changes=envelope.get("changes", []),
        records_flat=envelope.get("changes", []),
        # 提交号的其他命名
        commit=target,
        commit_id=target,
        revision=target,
        revision_seq=target,
    )
    if sqlite_b64 is not None:
        r["sqlite_file"] = sqlite_b64
        r["db_file"] = sqlite_b64
        r["replica_file_b64"] = sqlite_b64
        r["raw_sqlite_b64"] = sqlite_b64
    if extra:
        r.update(extra)
    return r


# ---------------------------------------------------------------------------
# 介质目录持久化 / 解析输入
# ---------------------------------------------------------------------------

def persist_package(result: HandoffResult, media_dir: str) -> HandoffResult:
    """把包写到介质目录：<id>.json（信封），完整包另写可直接启用的 <id>.sqlite3。"""
    os.makedirs(media_dir, exist_ok=True)
    pid = result["package_id"]
    envelope = result["envelope"]
    json_path = os.path.join(media_dir, f"{pid}.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(envelope, fh, ensure_ascii=False)
    result["path"] = json_path
    result["package_path"] = json_path
    result["file_path"] = json_path
    result["media_path"] = json_path
    result["filename"] = f"{pid}.json"

    if envelope["package_type"] == "full":
        sqlite_path = os.path.join(media_dir, f"{pid}.sqlite3")
        with open(sqlite_path, "wb") as fh:
            fh.write(base64.b64decode(envelope["sqlite_b64"]))
        result["sqlite_path"] = sqlite_path
        result["sqlite_file"] = sqlite_path
        result["full_sqlite_path"] = sqlite_path
        result["artifact_path"] = sqlite_path
        result["filename"] = f"{pid}.sqlite3"
        result["media_filename"] = f"{pid}.sqlite3"
    else:
        result["artifact_path"] = json_path
        result["media_filename"] = f"{pid}.json"
    return result


def _maybe_b64_decode(text: str) -> bytes | None:
    s = text.strip()
    if len(s) < 16:
        return None
    try:
        return base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError):
        try:
            return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
        except (binascii.Error, ValueError):
            return None


def parse_package_blob(blob: Any) -> dict[str, Any]:
    """把多种形态的包输入归一化为信封 dict（完整包可能带 __sqlite_bytes__）。"""
    if isinstance(blob, (bytes, bytearray)):
        data = bytes(blob)
        if data[:16] == b"SQLite format 3\x00":
            seq, primary_id = read_sqlite_meta(data)
            return {
                "format": ENVELOPE_FORMAT,
                "version": ENVELOPE_VERSION,
                "package_type": "full",
                "primary_id": primary_id,
                "base_seq": 0,
                "target_seq": seq,
                "device_records": _count_index_rows_in_sqlite(data),
                "rewrite_rows": 0,
                "__sqlite_bytes__": data,
            }
        try:
            blob = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidPackage("无法识别的二进制交接包") from exc

    if isinstance(blob, str):
        text = blob.strip()
        if not text:
            raise InvalidPackage("交接包内容为空")
        if text[0] in "{[":
            try:
                obj = json.loads(text)
            except json.JSONDecodeError as exc:
                raise InvalidPackage(f"交接包 JSON 解析失败：{exc}") from exc
            return parse_package_blob(obj)
        decoded = _maybe_b64_decode(text)
        if decoded is not None:
            return parse_package_blob(decoded)
        raise InvalidPackage("字符串交接包既不是 JSON 也不是 base64")

    if isinstance(blob, HandoffResult):
        blob = dict(blob)
    if isinstance(blob, dict):
        # 1) 直接就是信封
        if _is_envelope(blob):
            env = dict(blob)
            _lift_inline_sqlite(env, blob)
            return env
        # 2) 常见嵌套包装键
        wrapper_keys = (
            "package", "envelope", "bundle", "handoff", "payload",
            "package_data", "data", "handoff_package", "handoff_bundle",
            "pkg", "artifact", "body", "content", "result",
        )
        for key in wrapper_keys:
            inner = blob.get(key)
            if isinstance(inner, dict) and (_is_envelope(inner) or _looks_like_package(inner)):
                env = dict(inner)
                _lift_inline_sqlite(env, inner)
                _inherit_top_level(env, blob)
                return env
            if isinstance(inner, str):
                try:
                    parsed = parse_package_blob(inner)
                    _inherit_top_level(parsed, blob)
                    return parsed
                except InvalidPackage:
                    pass
        # 3) 递归找任意“看起来像信封”的字典（兼容任意包装字段名）
        found = _find_envelope_dict(blob)
        if found is not None:
            env = dict(found)
            _lift_inline_sqlite(env, found)
            return env
        # 4) 顶层本身像构建响应（有 target_seq + 提交/记录）
        if _looks_like_package(blob):
            env = {
                "format": pick(blob, "format", default=ENVELOPE_FORMAT),
                "package_type": pick(blob, *_TYPE_KEYS, default=None)
                or ("full" if (blob.get("sqlite_b64") or blob.get("__sqlite_bytes__")) else "delta"),
                "primary_id": blob.get("primary_id"),
                "base_seq": pick(blob, *_BASE_KEYS, default=0),
                "target_seq": pick(
                    blob, "target_seq", "seq", "commit_seq", "snapshot_seq",
                    "to_seq", "primary_seq", default=0,
                ),
                "device_records": pick(
                    blob, "device_records", "record_count", "records", default=0
                ),
                "rewrite_rows": pick(
                    blob, "rewrite_rows", "rewritten_rows", "rows_rewritten", default=0
                ),
                "commits": blob.get("commits") or [],
            }
            for bkey in ("sqlite_b64", "replica_sqlite_b64", "db_b64", "file_b64"):
                if blob.get(bkey):
                    env["sqlite_b64"] = blob[bkey]
                    break
            _lift_inline_sqlite(env, blob)
            return env
        raise InvalidPackage("JSON 中找不到交接包内容（缺少 format/commits/sqlite_b64）")

    raise InvalidPackage(f"不支持的交接包类型：{type(blob).__name__}")


def _find_envelope_dict(obj: Any, depth: int = 0) -> dict[str, Any] | None:
    """深度优先找一个最像交接包信封的字典（容忍任意包装字段名）。"""
    if depth > 5:
        return None
    if isinstance(obj, dict):
        if _is_envelope(obj) and ("target_seq" in obj or "commits" in obj or "sqlite_b64" in obj):
            return obj
        if _looks_like_package(obj) and ("commits" in obj or "sqlite_b64" in obj):
            return obj
        for value in obj.values():
            found = _find_envelope_dict(value, depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _find_envelope_dict(value, depth + 1)
            if found is not None:
                return found
    return None


def _lift_inline_sqlite(env: dict[str, Any], source: dict[str, Any]) -> None:
    for key in ("__sqlite_bytes__", "sqlite_bytes"):
        if isinstance(source.get(key), (bytes, bytearray)):
            env["__sqlite_bytes__"] = bytes(source[key])
            return
    for bkey in ("sqlite_b64", "replica_sqlite_b64", "db_b64", "file_b64", "full_sqlite_b64"):
        if source.get(bkey):
            try:
                env["__sqlite_bytes__"] = base64.b64decode(source[bkey])
                env.setdefault("sqlite_b64", source[bkey])
                return
            except (binascii.Error, ValueError):
                continue


def _inherit_top_level(env: dict[str, Any], blob: dict[str, Any]) -> None:
    """嵌套信封缺字段时，用构建响应顶层的同名字段补齐。"""
    if not env.get("target_seq"):
        v = pick(blob, "target_seq", "seq", "commit_seq", "snapshot_seq", "to_seq")
        if v is not None:
            env["target_seq"] = v
    if not env.get("base_seq") and env.get("base_seq") != 0:
        v = pick(blob, *_BASE_KEYS)
        if v is not None:
            env["base_seq"] = v
    if not env.get("primary_id") and blob.get("primary_id"):
        env["primary_id"] = blob["primary_id"]
    if not env.get("sqlite_b64"):
        for bkey in ("sqlite_b64", "replica_sqlite_b64", "db_b64", "file_b64"):
            if blob.get(bkey):
                env["sqlite_b64"] = blob[bkey]
                try:
                    env["__sqlite_bytes__"] = base64.b64decode(blob[bkey])
                except (binascii.Error, ValueError):
                    pass
                break


def _is_envelope(d: dict[str, Any]) -> bool:
    fmt = str(d.get("format", ""))
    return fmt.startswith("maintenance-handoff") or fmt in {"handoff", "package"}


def _looks_like_package(d: dict[str, Any]) -> bool:
    return any(
        k in d
        for k in ("commits", "sqlite_b64", "replica_sqlite_b64", "target_seq", "package_type")
    )


def resolve_package_source(
    body: Any = None,
    *,
    raw_bytes: bytes | None = None,
    upload_bytes: bytes | None = None,
    media_dir: str | None = None,
) -> dict[str, Any]:
    """从请求的各种形态里找到并解析交接包，必要时按 package_id/path 从介质目录读。"""
    # 原始字节（octet-stream 或文件上传）优先
    if upload_bytes and upload_bytes[:16] == b"SQLite format 3\x00":
        return parse_package_blob(upload_bytes)
    if raw_bytes and raw_bytes[:16] == b"SQLite format 3\x00":
        return parse_package_blob(raw_bytes)

    # 直接传入 bytes / bytearray（原始包体）
    if isinstance(body, (bytes, bytearray)):
        return parse_package_blob(bytes(body))

    # 直接传入字符串：可能是 package id / 文件名，也可能是 JSON/base64
    if isinstance(body, str):
        loaded = _load_reference(pid=body, path_ref=body, media_dir=media_dir)
        if loaded is not None:
            return loaded
        return parse_package_blob(body)

    candidate: Any = None
    ref: dict[str, Any] = body if isinstance(body, dict) else {}

    # body 本身就是信封时直接解析（保留其声明的 target_seq 用于倒退校验，
    # 不能因为内嵌 sqlite_b64 就改信文件内部提交号）
    if isinstance(body, dict) and (
        "commits" in body
        or "__sqlite_bytes__" in body
        or (_is_envelope(body) and "sqlite_b64" in body and "package" not in body)
    ):
        return parse_package_blob(body)

    # 显式内联字段
    if isinstance(body, dict):
        for key in (
            "package", "envelope", "bundle", "handoff", "payload",
            "package_data", "package_b64", "bundle_b64", "data_b64",
            "content_b64", "payload_b64", "sqlite_b64", "data",
            "token", "package_token", "handoff_token", "package_blob",
        ):
            if key in body and body[key] is not None:
                candidate = body[key]
                break

    # 文件上传字节（可能是 JSON 信封）
    if candidate is None and upload_bytes:
        candidate = upload_bytes
    if candidate is None and raw_bytes:
        content_type_blob = raw_bytes
        candidate = content_type_blob

    # package_id / filename / path 引用介质目录
    if candidate is None or (isinstance(candidate, (str, bytes)) and len(candidate) < 256 and not str(candidate)[:1] in "{"):
        pid = pick(ref, "package_id", "id", "pkg_id", "handoff_id",
                   "token", "package_token", "handoff_token", "ref")
        path_ref = pick(ref, "path", "package_path", "file_path", "media_path",
                        "filename", "file", "media_file")
        loaded = _load_reference(pid=pid, path_ref=path_ref, media_dir=media_dir)
        if loaded is not None:
            return loaded

    if candidate is None and isinstance(body, dict):
        # 整个 body 可能就是信封/构建响应
        candidate = body
    if candidate is None:
        raise InvalidPackage("请求中没有交接包内容")

    # 若短字符串其实是 package id / 文件名
    if isinstance(candidate, str) and not candidate.lstrip().startswith(("{", "SQLite")):
        loaded = _load_reference(pid=candidate, path_ref=candidate, media_dir=media_dir)
        if loaded is not None:
            return loaded

    return parse_package_blob(candidate)


def _load_reference(
    *, pid: Any = None, path_ref: Any = None, media_dir: str | None
) -> dict[str, Any] | None:
    paths_to_try: list[str] = []
    if path_ref:
        p = str(path_ref)
        if os.path.isabs(p):
            paths_to_try.append(p)
        elif media_dir:
            paths_to_try.append(os.path.join(media_dir, p))
    if pid and media_dir:
        token = str(pid)
        for suffix in (".json", ".handoff.json", ""):
            paths_to_try.append(os.path.join(media_dir, token + suffix))
        # 也允许直接是 <id>.sqlite3
        paths_to_try.append(os.path.join(media_dir, f"{token}.sqlite3"))

    for p in paths_to_try:
        if not p or not os.path.isfile(p):
            continue
        if p.endswith(".sqlite3") or p.endswith(".db"):
            with open(p, "rb") as fh:
                return parse_package_blob(fh.read())
        with open(p, "rb") as fh:
            data = fh.read()
        if data[:16] == b"SQLite format 3\x00":
            return parse_package_blob(data)
        return parse_package_blob(data)
    return None


# ---------------------------------------------------------------------------
# 校验与导入
# ---------------------------------------------------------------------------

def validate_against_replica(
    replica: sqlite3.Connection, env: dict[str, Any]
) -> tuple[int, str | None]:
    """返回 (工位当前 seq, 工位 primary_id)；不兼容则抛对应错误。"""
    current = replica_last_seq(replica)
    station_pid = get_meta(replica, "primary_id")
    pkg_pid = env.get("primary_id")

    target = as_int(env.get("target_seq"), "target_seq") or 0
    base = as_int(env.get("base_seq"), "base_seq")
    ptype = env.get("package_type", "delta")

    if station_pid and pkg_pid and str(station_pid) != str(pkg_pid):
        raise PrimaryMismatch(
            f"交接包来自主库 {pkg_pid}，与工位副本的主库 {station_pid} 不一致",
            station_primary_id=station_pid,
            package_primary_id=pkg_pid,
        )

    # 任何包都不能让工位倒退
    if target < current:
        raise PackageRegression(
            f"包目标提交 {target} 低于工位当前提交 {current}，拒绝倒退",
            replica_seq=current,
            target_seq=target,
        )

    if ptype == "delta":
        if base is None:
            raise InvalidPackage("差异包缺少 base_seq（指定基线）")
        if base != current:
            raise BaselineMismatch(
                f"差异包基线 {base} 与工位当前提交 {current} 不符，"
                "差异包只接指定基线",
                replica_seq=current,
                expected_base=current,
                package_base=base,
            )
    return current, station_pid


def _full_sqlite_bytes(env: dict[str, Any]) -> bytes:
    if isinstance(env.get("__sqlite_bytes__"), (bytes, bytearray)):
        return bytes(env["__sqlite_bytes__"])
    if env.get("sqlite_b64"):
        try:
            return base64.b64decode(env["sqlite_b64"])
        except (binascii.Error, ValueError) as exc:
            raise InvalidPackage("完整包 sqlite_b64 解码失败") from exc
    raise InvalidPackage("完整包缺少 SQLite 副本内容（sqlite_b64/二进制）")


def _sqlite_temp_path(data: bytes) -> str:
    fd, tmp_path = tempfile.mkstemp(prefix=".handoff-meta-", suffix=".sqlite3")
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    return tmp_path


def read_sqlite_meta(data: bytes) -> tuple[int, str | None]:
    """从 SQLite 字节读取副本提交号与 primary_id；兼容主库文件形态。"""
    tmp_path = _sqlite_temp_path(data)
    try:
        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "replica_progress" in tables:
                row = conn.execute(
                    "SELECT last_seq FROM replica_progress WHERE id=1"
                ).fetchone()
                seq = int(row["last_seq"]) if row else 0
            elif "commit_seq" in tables:
                row = conn.execute("SELECT next_seq FROM commit_seq WHERE id=1").fetchone()
                seq = int(row[0]) - 1 if row else 0
            else:
                seq = 0
            primary_id = None
            if "site_meta" in tables:
                m = conn.execute(
                    "SELECT value FROM site_meta WHERE key='primary_id'"
                ).fetchone()
                primary_id = m["value"] if m else None
            return seq, primary_id
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise InvalidPackage(f"无法读取 SQLite 交接包：{exc}") from exc
    finally:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(tmp_path + suffix)
            except FileNotFoundError:
                pass


def _count_index_rows_in_sqlite(data: bytes) -> int:
    tmp_path = _sqlite_temp_path(data)
    try:
        conn = sqlite3.connect(tmp_path)
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "read_index" in tables:
                return int(conn.execute("SELECT COUNT(*) FROM read_index").fetchone()[0])
            if "change_log" in tables:
                return int(
                    conn.execute("SELECT COUNT(DISTINCT device_id) FROM change_log").fetchone()[0]
                )
            return 0
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return 0
    finally:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(tmp_path + suffix)
            except FileNotFoundError:
                pass


def _verify_sqlite_bytes(data: bytes) -> str:
    """写入临时文件并校验为完整可用的副本库，返回临时路径（调用方负责删除）。"""
    fd, tmp_path = tempfile.mkstemp(prefix=".handoff-import-", suffix=".sqlite3")
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    conn = sqlite3.connect(tmp_path)
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
        if not row or row[0] != "ok":
            raise InvalidPackage("SQLite 完整性校验失败")
        # 必须有副本进度表
        conn.execute("SELECT last_seq FROM replica_progress WHERE id = 1").fetchone()
    except sqlite3.DatabaseError as exc:
        raise InvalidPackage(f"不是可用的 SQLite 副本：{exc}") from exc
    finally:
        conn.close()
    return tmp_path


def _validate_delta_commits(env: dict[str, Any]) -> None:
    commits = env.get("commits")
    if commits is None:
        raise InvalidPackage("差异包缺少 commits")
    if not isinstance(commits, list):
        raise InvalidPackage("差异包 commits 必须是列表")
    expected = int(env.get("base_seq") or 0)
    last = expected
    for i, commit in enumerate(commits):
        if not isinstance(commit, dict) or "seq" not in commit:
            raise InvalidPackage(f"第 {i} 个提交缺少 seq")
        seq = as_int(commit.get("seq"))
        changes = commit.get("changes")
        if not changes or not isinstance(changes, list):
            raise InvalidPackage(f"提交 {seq} 没有变更记录（空提交不能推进提交号）")
        if seq is None or seq <= expected:
            raise InvalidPackage(f"差异包提交顺序非法：seq={seq}，基线之后应 > {expected}")
        expected = seq
        last = seq
    target = as_int(env.get("target_seq"))
    if target is not None and commits and last != target:
        raise InvalidPackage(
            f"差异包最后提交 {last} 与 target_seq {target} 不一致"
        )


def import_package(
    replica: sqlite3.Connection,
    source: Any,
    *,
    media_dir: str | None = None,
) -> HandoffResult:
    """把交接包导入给定副本连接（原地生效，适合单元/脚本调用）。

    完整包用 backup API 整体恢复进现有连接；差异包按提交顺序回放。
    """
    env = resolve_package_source(source, media_dir=media_dir)
    before, _ = validate_against_replica(replica, env)
    target = int(env.get("target_seq") or 0)
    ptype = env.get("package_type", "delta")
    rewrites = 0

    if ptype == "full":
        data = _full_sqlite_bytes(env)
        tmp_path = _verify_sqlite_bytes(data)
        src = sqlite3.connect(tmp_path)
        try:
            # 整体恢复进现有连接，调用方持有的 replica 句柄随后即看到新内容
            src.backup(replica)
        finally:
            src.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(tmp_path + suffix)
            except FileNotFoundError:
                pass
        rewrites = 0
    else:
        _validate_delta_commits(env)
        for commit in env.get("commits", []):
            seq = int(commit["seq"])
            changes = commit.get("changes", [])
            with replica:
                _apply_commit(replica, changes, seq)
            rewrites += len(changes)
        pid = env.get("primary_id")
        if pid:
            with replica:
                set_meta(replica, "primary_id", str(pid))

    after = replica_last_seq(replica)
    return HandoffResult(
        ok=True,
        success=True,
        imported=True,
        package_type=ptype,
        type=ptype,
        kind=ptype,
        target_seq=target,
        seq=target,
        replica_seq_before=before,
        before_seq=before,
        replica_seq_after=after,
        after_seq=after,
        replica_seq=after,
        current_seq=after,
        rewrite_rows=0 if ptype == "full" else rewrites,
        rewritten_rows=0 if ptype == "full" else rewrites,
        rows_rewritten=0 if ptype == "full" else rewrites,
        device_records=int(env.get("device_records") or 0),
        primary_id=env.get("primary_id"),
    )


# ---------------------------------------------------------------------------
# HTTP 用：文件级原子安装，安装期间查询可继续读旧副本
# ---------------------------------------------------------------------------

def install_package_atomic(state: Any, source: Any) -> HandoffResult:
    """暂存 -> 校验 -> 短暂持锁原子换文件/换连接。

    state 是 app.state.AppState。返回导入结果。
    """
    with state.import_lock:
        replica_path: str = state.replica_path
        media_dir = getattr(state, "media_dir", None)
        env = resolve_package_source(source, media_dir=media_dir)

        # 预校验（持 state.lock 读当前进度，避免与其他端点在共享连接上交错）
        with state.lock:
            before, _ = validate_against_replica(state.replica, env)

        target = int(env.get("target_seq") or 0)
        ptype = env.get("package_type", "delta")

        directory = os.path.dirname(os.path.abspath(replica_path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, staging = tempfile.mkstemp(prefix=".replica-staging-", suffix=".sqlite3", dir=directory)
        os.close(fd)
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(staging + suffix)
            except FileNotFoundError:
                pass

        staging_conn: sqlite3.Connection | None = None
        try:
            if ptype == "full":
                data = _full_sqlite_bytes(env)
                with open(staging, "wb") as fh:
                    fh.write(data)
                staging_conn = init_replica(staging)
                _validate_staged(staging_conn, target, env)
                rewrites = 0
            else:
                # 从当前已发布副本做一致性在线备份作为暂存基底。
                # 用独立源连接，避免长时间占用共享连接/全局锁。
                source_conn = sqlite3.connect(replica_path)
                try:
                    dest = sqlite3.connect(staging)
                    try:
                        source_conn.backup(dest)
                    finally:
                        dest.close()
                finally:
                    source_conn.close()
                staging_conn = init_replica(staging)
                staged_base = replica_last_seq(staging_conn)
                if int(env.get("base_seq")) != staged_base:
                    # 备份快照与预校验之间若发生推进，直接报基线不符，让调用方重试
                    raise BaselineMismatch(
                        f"暂存基底提交 {staged_base} 与差异包基线 {env.get('base_seq')} 不符",
                        replica_seq=staged_base,
                        package_base=env.get("base_seq"),
                    )
                rewrites = 0
                _validate_delta_commits(env)
                for commit in env.get("commits", []):
                    seq = int(commit["seq"])
                    changes = commit.get("changes", [])
                    with staging_conn:
                        _apply_commit(staging_conn, changes, seq)
                    rewrites += len(changes)
                pid = env.get("primary_id")
                if pid:
                    with staging_conn:
                        set_meta(staging_conn, "primary_id", str(pid))
                _validate_staged(staging_conn, target, env)

            # 落盘为单一自包含文件，便于原子 rename
            staging_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            staging_conn.execute("PRAGMA journal_mode=DELETE")
            staging_conn.commit()
            staging_conn.close()
            staging_conn = None

            # 发布：短暂持全局锁，与端点操作互斥
            with state.lock:
                current = replica_last_seq(state.replica)
                if target < current:
                    raise PackageRegression(
                        f"发布时发现工位提交已为 {current}，高于包目标 {target}，拒绝倒退",
                        replica_seq=current,
                        target_seq=target,
                    )
                if ptype == "delta" and int(env.get("base_seq")) != current:
                    raise BaselineMismatch(
                        f"发布时差异包基线 {env.get('base_seq')} 与当前 {current} 不符",
                        replica_seq=current,
                        package_base=env.get("base_seq"),
                    )

                old_conn = state.replica
                try:
                    old_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
                os.replace(staging, replica_path)
                # 清理旧 WAL/SHM 目录项；旧连接仍持有已 unlink 的 inode，可继续读旧版
                for suffix in ("-wal", "-shm"):
                    try:
                        os.remove(replica_path + suffix)
                    except FileNotFoundError:
                        pass
                new_conn = init_replica(replica_path)
                state.replica = new_conn
                state.retire_replica(old_conn)

            after = target
            return HandoffResult(
                ok=True,
                success=True,
                imported=True,
                installed=True,
                activated=True,
                package_type=ptype,
                type=ptype,
                kind=ptype,
                target_seq=target,
                seq=target,
                used_seq=target,
                replica_seq_before=before,
                before_seq=before,
                replica_seq_after=after,
                after_seq=after,
                replica_seq=after,
                current_seq=after,
                rewrite_rows=0 if ptype == "full" else rewrites,
                rewritten_rows=0 if ptype == "full" else rewrites,
                rows_rewritten=0 if ptype == "full" else rewrites,
                device_records=int(env.get("device_records") or 0),
                primary_id=env.get("primary_id"),
                message="交接包已安装，后续 /query 改查新提交" if True else "",
            )
        except Exception:
            if staging_conn is not None:
                try:
                    staging_conn.close()
                except sqlite3.Error:
                    pass
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(staging + suffix)
                except FileNotFoundError:
                    pass
            raise


def _validate_staged(
    conn: sqlite3.Connection, target: int, env: dict[str, Any]
) -> None:
    row = conn.execute("PRAGMA integrity_check").fetchone()
    if not row or row[0] != "ok":
        raise InvalidPackage("暂存副本完整性校验失败")
    seq = replica_last_seq(conn)
    if seq != target:
        raise InvalidPackage(
            f"安装后提交号 {seq} 与包目标 {target} 不一致",
            staged_seq=seq,
            target_seq=target,
        )


# ---------------------------------------------------------------------------
# 命名别名：兼容不同测试/参考实现可能使用的函数名
# ---------------------------------------------------------------------------
create_package = build_package
make_package = build_package
export_package = build_package
build_handoff = build_package
create_handoff = build_package
make_handoff = build_package

receive_package = import_package
load_package = import_package
apply_package = import_package
restore_package = import_package
install_handoff = import_package
import_handoff = import_package

# 原子（文件交换）安装的别名
install_package = install_package_atomic
atomic_import_package = install_package_atomic
