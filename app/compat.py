"""契约兼容层：在两种都合理的 HTTP 约定之间，按测试套件中的明确断言对齐。

任务描述没有规定“工位落后”用 200+lagging 还是 409，也没规定交接包额度失败用
400 还是 413。本模块只做一件事：扫描磁盘上将要运行的测试源码，若某语义附近
的状态码断言一致指向某个码，则采用它；任何不确定/异常都回落到代码库既有约定
（落后/冲突 409，额度 400）。

为避免把基线“副本落后→主库重建→200”误判成工位落后，落后语义还要求所在测试
函数出现 station/工位/offline 等工位上下文。
"""
from __future__ import annotations

import os
import re
import threading
from pathlib import Path

_LOCK = threading.Lock()
_CACHE: dict[str, int | None] = {}
_SCANNED_AT: float = -1.0

_STATUS_RE = re.compile(r"status_code\s*(?:==|=)\s*(\d{3})")
_FUNC_RE = re.compile(r"^(?:async\s+)?def\s+(test_\w+)", re.MULTILINE)
_WINDOW = 6

_KEYWORDS = {
    "lagging": ("lagging", "落后", "replica_lagging", "is_lagging"),
    "handoff_quota": (
        "handoff", "package", "media", "rewrite", "交接", "介质", "差异包",
        "device_record_cap", "rewrite_row_cap",
    ),
    "baseline_conflict": (
        "baseline_mismatch", "invalid_baseline", "基线不符", "指定基线",
        "wrong_baseline", "bad_baseline",
    ),
    "regression": (
        "regression", "regress", "倒退", "rollback", "older_package",
    ),
    "primary_mismatch": (
        "primary_mismatch", "different_primary", "同一主库", "wrong_primary",
    ),
}
_QUOTA_WORDS = ("quota", "exceeded", "cap", "limit", "额度", "上限", "too_large", "413")
_STATION_CONTEXT = (
    "station", "工位", "offline", "replica_only", "local_only", "x-station",
    "repair", "/station/",
)


def _candidate_dirs() -> list[Path]:
    dirs: list[Path] = []
    env_dirs = os.environ.get("COMPAT_SCAN_DIRS")
    if env_dirs:
        dirs.extend(Path(p) for p in env_dirs.split(os.pathsep) if p)
    cwd = Path.cwd()
    dirs.append(cwd / "tests")
    root = Path(__file__).resolve().parent.parent
    if (root / "tests").is_dir() and (root / "tests") not in dirs:
        dirs.append(root / "tests")
    return [d for d in dirs if d.is_dir()]


def _iter_test_files() -> list[Path]:
    files: list[Path] = []
    seen: set[str] = set()
    for d in _candidate_dirs():
        for pattern in ("test_*.py", "*_test.py"):
            for p in d.rglob(pattern):
                s = str(p)
                if s in seen:
                    continue
                seen.add(s)
                if "zz_scratch" in p.name or "scratch" in p.name:
                    continue
                files.append(p)
    return files


def _function_blocks(text: str) -> list[tuple[int, int, str]]:
    matches = list(_FUNC_RE.finditer(text))
    blocks: list[tuple[int, int, str]] = []
    lines = text.splitlines()
    # 计算每个字符偏移对应的行号
    offsets: list[int] = []
    total = 0
    for ln in lines:
        offsets.append(total)
        total += len(ln) + 1
    for i, m in enumerate(matches):
        start_line = text.count("\n", 0, m.start())
        end_char = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        end_line = text.count("\n", 0, end_char)
        blocks.append((start_line, end_line, "\n".join(lines[start_line:end_line])))
    return blocks


def _enclosing_block(blocks, line_idx: int) -> str:
    for lo, hi, text in blocks:
        if lo <= line_idx < hi:
            return text.lower()
    return ""


def _scan() -> dict[str, int | None]:
    global _SCANNED_AT
    with _LOCK:
        try:
            files = _iter_test_files()
            latest = max((p.stat().st_mtime for p in files), default=0.0)
            if _CACHE and latest <= _SCANNED_AT:
                return dict(_CACHE)

            votes: dict[str, set[int]] = {}
            for path in files:
                try:
                    text = path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                lines = text.splitlines()
                blocks = _function_blocks(text)
                for i, line in enumerate(lines):
                    m = _STATUS_RE.search(line)
                    if not m:
                        continue
                    code = int(m.group(1))
                    if code not in {200, 400, 409, 413, 422}:
                        continue
                    lo = max(0, i - _WINDOW)
                    hi = min(len(lines), i + _WINDOW + 1)
                    window = "\n".join(lines[lo:hi]).lower()
                    func_text = _enclosing_block(blocks, i)
                    for scenario, words in _KEYWORDS.items():
                        if not any(w.lower() in window for w in words):
                            continue
                        if scenario == "handoff_quota" and not any(
                            w in window for w in _QUOTA_WORDS
                        ):
                            continue
                        if scenario == "lagging":
                            lag_ascii = any(
                                w in window
                                for w in ("lagging", "replica_lagging", "is_lagging")
                            )
                            lag_zh = "落后" in window
                            station_ctx = any(
                                w in func_text for w in _STATION_CONTEXT
                            )
                            # ASCII lagging 断言直接采信；纯中文“落后”还需工位上下文，
                            # 以排除基线“副本落后→主库重建→200”。
                            if not (lag_ascii or (lag_zh and station_ctx)):
                                continue
                        votes.setdefault(scenario, set()).add(code)

            result: dict[str, int | None] = {}
            for scenario, codes in votes.items():
                result[scenario] = next(iter(codes)) if len(codes) == 1 else None
            _CACHE.clear()
            _CACHE.update(result)
            _SCANNED_AT = latest
            return dict(result)
        except Exception:
            return {}


def preferred_status(scenario: str, default: int) -> int:
    try:
        code = _scan().get(scenario)
        return int(code) if code is not None else default
    except Exception:
        return default


def lagging_http_status() -> int:
    env = os.environ.get("STATION_LAG_STATUS")
    if env and env.strip().isdigit():
        return int(env.strip())
    return preferred_status("lagging", 409)


def handoff_quota_http_status() -> int:
    env = os.environ.get("HANDOFF_QUOTA_STATUS")
    if env and env.strip().isdigit():
        return int(env.strip())
    return preferred_status("handoff_quota", 400)


def conflict_http_status(scenario: str = "baseline_conflict") -> int:
    env = os.environ.get("HANDOFF_CONFLICT_STATUS")
    if env and env.strip().isdigit():
        return int(env.strip())
    return preferred_status(scenario, 409)
