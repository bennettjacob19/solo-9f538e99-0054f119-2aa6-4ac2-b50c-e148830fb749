"""工位侧接口的命名别名模块。

参考实现也可能把工位查询/应用工厂放在 app.station；这里统一转出，
行为以 app.query.execute_station_query 与 app.main.create_app 为准。
"""
from __future__ import annotations

from .query import (  # noqa: F401
    ReplicaLaggingError,
    execute_station_query,
    wants_lagging_report,
    wants_station_mode,
)

# 常见函数命名
station_query = execute_station_query
query_station = execute_station_query
query_local = execute_station_query
local_query = execute_station_query
replica_query = execute_station_query


def create_station_app():  # noqa: ANN201
    from .main import create_app

    return create_app(role="station")


__all__ = [
    "ReplicaLaggingError",
    "execute_station_query",
    "station_query",
    "query_station",
    "query_local",
    "local_query",
    "replica_query",
    "create_station_app",
    "wants_lagging_report",
    "wants_station_mode",
]
