"""运行期配置：主库与副本是两个独立的 SQLite 文件，路径由环境变量决定。"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DbPaths:
    primary: str
    replica: str


def get_paths() -> DbPaths:
    data_dir = os.environ.get("DATA_DIR", os.path.join(os.getcwd(), "data"))
    os.makedirs(data_dir, exist_ok=True)
    primary = os.environ.get("PRIMARY_DB", os.path.join(data_dir, "primary.sqlite3"))
    replica = os.environ.get("REPLICA_DB", os.path.join(data_dir, "replica.sqlite3"))
    return DbPaths(primary=primary, replica=replica)
