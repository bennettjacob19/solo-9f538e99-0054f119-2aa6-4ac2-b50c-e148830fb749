"""并发冒烟：混合写入/推进/查询并发执行，结果与计量必须始终自洽。"""
from __future__ import annotations

import threading

from fastapi.testclient import TestClient

from app.main import app
from app import replay


def test_concurrent_mixed_requests(app_state):
    client = TestClient(app)
    errors: list[Exception] = []

    # 先落一个提交，保证后续查询的 min_commit=1 始终有主库快照可读
    r = client.post(
        "/maintenance/batch",
        json={"items": [{
            "device_id": 100, "op": "register", "new_status": "maintenance",
        }]},
    )
    assert r.status_code == 200

    def writer() -> None:
        try:
            for seq in range(1, 41):
                r = client.post(
                    "/maintenance/batch",
                    json={"items": [{
                        "device_id": 100 + (seq % 10),
                        "op": "register" if seq % 3 else "correct",
                        "new_status": "maintenance",
                    }]},
                )
                # 已登记设备的 register 会 409，属预期；correct 在未登记时也可能 409
                assert r.status_code in (200, 409)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def advancer() -> None:
        try:
            for _ in range(20):
                r = client.post("/replica/advance", json={"max_commits": 2})
                assert r.status_code in (200, 409)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def reader() -> None:
        try:
            for _ in range(40):
                r = client.post(
                    "/query",
                    json={
                        "device_ids": [1, 2, 100, 101, 109],
                        "min_commit": 1,
                        "main_read_cap": 1000,
                        "log_read_cap": 1000,
                    },
                )
                assert r.status_code == 200
                body = r.json()
                # 计量恒等式：每设备至少一次索引探查（主表读 >= 设备数）
                assert body["main_reads"] >= 5
                assert len(body["results"]) == 5
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=t) for t in (writer, advancer, reader, reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    # 收尾：把副本追平，最终快照路径应能读出全部已登记设备
    client.post("/replica/advance", json={"max_commits": 1000})
    r = client.post(
        "/query",
        json={
            "device_ids": [100 + i for i in range(10)],
            "min_commit": 1,
            "main_read_cap": 1000,
            "log_read_cap": 1000,
        },
    )
    assert r.status_code == 200
    assert r.json()["served_from"] == "replica_snapshot"
