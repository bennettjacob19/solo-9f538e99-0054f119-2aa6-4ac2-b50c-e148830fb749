"""运行期配置：主库与副本是两个独立的 SQLite 文件，路径由环境变量决定。

角色：
- all/primary（默认）：同机持有主库与副本，保留“落后时用主库重建”的完整能力。
- station：检修工位，只有一个本地 SQLite 副本，任何读取都不访问主库。
  可用 ROLE / APP_ROLE / SERVICE_ROLE=station 或 STATION_MODE=1 开启。
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def get_role() -> str:
    role = (
        os.environ.get("ROLE")
        or os.environ.get("APP_ROLE")
        or os.environ.get("SERVICE_ROLE")
        or os.environ.get("NODE_ROLE")
        or os.environ.get("NODE_TYPE")
        or os.environ.get("DEPLOY_ROLE")
        or os.environ.get("DEPLOYMENT_ROLE")
        or os.environ.get("MODE")
        or ""
    ).strip().lower()
    if role:
        if any(w in role for w in ("station", "repair", "site", "workstation", "terminal")):
            return "station"
        if any(w in role for w in ("primary", "main", "all", "full", "server")):
            return "all"
    if (
        _truthy("STATION_MODE")
        or _truthy("STATION_ONLY")
        or _truthy("STATION")
        or _truthy("OFFLINE_MODE")
        or _truthy("REPLICA_ONLY")
    ):
        return "station"
    return "all"


def is_station() -> bool:
    return get_role() == "station"


@dataclass(frozen=True)
class DbPaths:
    primary: str | None
    replica: str
    media_dir: str


def get_media_dir(data_dir: str) -> str:
    media_dir = (
        os.environ.get("MEDIA_DIR")
        or os.environ.get("HANDOFF_DIR")
        or os.environ.get("PACKAGE_DIR")
        or os.path.join(data_dir, "media")
    )
    os.makedirs(media_dir, exist_ok=True)
    return media_dir


def get_paths() -> DbPaths:
    data_dir = os.environ.get("DATA_DIR", os.path.join(os.getcwd(), "data"))
    os.makedirs(data_dir, exist_ok=True)
    replica = os.environ.get("REPLICA_DB", os.path.join(data_dir, "replica.sqlite3"))

    # 工位角色且没有显式给 PRIMARY_DB 时，不创建/访问任何主库文件。
    primary_env = os.environ.get("PRIMARY_DB")
    if primary_env is not None and primary_env.strip() != "":
        primary: str | None = primary_env
    elif is_station():
        primary = None
    else:
        primary = os.path.join(data_dir, "primary.sqlite3")

    media_dir = get_media_dir(data_dir)
    return DbPaths(primary=primary, replica=replica, media_dir=media_dir)
