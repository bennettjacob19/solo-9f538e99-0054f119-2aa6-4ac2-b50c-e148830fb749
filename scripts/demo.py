#!/usr/bin/env python3
"""端到端演练脚本（针对运行中的服务，默认 http://127.0.0.1:8000）。

覆盖需求中的关键场景：
  1. 批量登记/更正/撤销 -> 递增提交号
  2. 副本落后时查询不等待、不推进，走主库重建
  3. 限额查询：额度内返回两项实读数，额度不足整批报错
  4. 手动推进副本，故障注入中断 + 清除后续接
  5. 服务重启后从最近完成提交接续（由 CLI 崩溃脚本配合演示）
"""
from __future__ import annotations

import json
import sys

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"


def show(title: str, resp: httpx.Response) -> None:
    print(f"\n=== {title} [{resp.status_code}] ===")
    try:
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
    except Exception:
        print(resp.text)


def main() -> int:
    c = httpx.Client(base_url=BASE, timeout=10)

    show("初始副本状态", c.get("/replica/status"))

    show("提交 seq=1：登记设备 1/2", c.post("/maintenance/batch", json={
        "items": [
            {"device_id": 1, "op": "register", "new_status": "maintenance"},
            {"device_id": 2, "op": "register", "new_status": "in_service"},
        ]}))

    show("提交 seq=2：更正 1 + 登记 3", c.post("/maintenance/batch", json={
        "items": [
            {"device_id": 1, "op": "correct", "new_status": "overhaul"},
            {"device_id": 3, "op": "register", "new_status": "maintenance"},
        ]}))

    show("提交 seq=3：撤销设备 2", c.post("/maintenance/batch", json={
        "items": [{"device_id": 2, "op": "revoke"}]}))

    show("提交 seq=4：更正设备 3", c.post("/maintenance/batch", json={
        "items": [{"device_id": 3, "op": "correct", "new_status": "in_service"}]}))

    show("非法批（重复登记 1）整批 409", c.post("/maintenance/batch", json={
        "items": [{"device_id": 1, "op": "register", "new_status": "overhaul"}]}))

    show("副本落后时查询（重建路径，不应推进副本）", c.post("/query", json={
        "device_ids": [1, 2, 3, 99],
        "min_commit": 4,
        "main_read_cap": 100,
        "log_read_cap": 100,
    }))

    show("限额不足：日志上限 0，整批 400", c.post("/query", json={
        "device_ids": [1, 2, 3, 99],
        "min_commit": 4,
        "main_read_cap": 100,
        "log_read_cap": 0,
    }))

    show("注入故障 seq=2", c.post("/replica/fault", json={"seq": 2}))
    show("推进副本：seq1 成功后在 seq2 前 409", c.post("/replica/advance", json={
        "max_commits": 10}))
    show("故障中副本状态（停在 seq1）", c.get("/replica/status"))
    show("清除故障", c.delete("/replica/fault"))
    show("再次推进：从 seq2 接续到 seq4", c.post("/replica/advance", json={
        "max_commits": 10}))

    show("副本追上后查询（同一副本快照路径）", c.post("/query", json={
        "device_ids": [1, 2, 3, 99],
        "min_commit": 4,
        "main_read_cap": 100,
        "log_read_cap": 100,
    }))

    show("快照路径限额：只给 2 次主表读，整批 400", c.post("/query", json={
        "device_ids": [1, 2, 3],
        "min_commit": 4,
        "main_read_cap": 2,
        "log_read_cap": 100,
    }))

    show("最终副本状态", c.get("/replica/status"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
