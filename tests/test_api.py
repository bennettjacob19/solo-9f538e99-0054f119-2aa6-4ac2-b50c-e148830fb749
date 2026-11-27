"""HTTP 端到端：走完整 FastAPI 路由，验证 /docs 暴露的全部接口行为。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app import replay


@pytest.fixture()
def client(app_state):
    with TestClient(app) as c:
        yield c


def test_full_flow_http(client):
    # 1) 两批写入
    r = client.post(
        "/maintenance/batch",
        json={"items": [
            {"device_id": 1, "op": "register", "new_status": "maintenance"},
            {"device_id": 2, "op": "register", "new_status": "in_service"},
        ]},
    )
    assert r.status_code == 200 and r.json()["seq"] == 1
    r = client.post(
        "/maintenance/batch",
        json={"items": [{"device_id": 1, "op": "correct", "new_status": "overhaul"}]},
    )
    assert r.status_code == 200 and r.json()["seq"] == 2

    # 2) 副本落后：重建路径，且不推进
    r = client.post(
        "/query",
        json={"device_ids": [1, 2, 99], "min_commit": 2,
              "main_read_cap": 100, "log_read_cap": 100},
    )
    body = r.json()
    assert r.status_code == 200
    assert body["served_from"] == "primary_reconstructed"
    assert body["used_seq"] == 2
    assert body["replica_current_seq"] == 0

    # 3) 手动推进到 seq2
    r = client.post("/replica/advance", json={"max_commits": 5})
    assert r.status_code == 200
    assert r.json()["applied_commits"] == [1, 2]

    # 4) 副本追上：快照路径
    r = client.post(
        "/query",
        json={"device_ids": [1, 2], "min_commit": 2,
              "main_read_cap": 100, "log_read_cap": 100},
    )
    body = r.json()
    assert body["served_from"] == "replica_snapshot"
    assert body["used_seq"] == 2

    # 5) 额度不足整批 400
    r = client.post(
        "/query",
        json={"device_ids": [1, 2], "min_commit": 2,
              "main_read_cap": 1, "log_read_cap": 100},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "quota_exceeded"

    # 6) 非法操作 409，且不发提交号
    r = client.post(
        "/maintenance/batch",
        json={"items": [{"device_id": 1, "op": "register", "new_status": "overhaul"}]},
    )
    assert r.status_code == 409
    r = client.get("/replica/status")
    assert r.json()["primary_seq"] == 2


def test_fault_endpoint_blocks_and_clears(client):
    client.post(
        "/maintenance/batch",
        json={"items": [
            {"device_id": 1, "op": "register", "new_status": "maintenance"},
            {"device_id": 2, "op": "register", "new_status": "in_service"},
        ]},
    )
    client.post(
        "/maintenance/batch",
        json={"items": [{"device_id": 1, "op": "revoke"}]},
    )
    client.post(
        "/maintenance/batch",
        json={"items": [{"device_id": 2, "op": "correct", "new_status": "overhaul"}]},
    )
    client.post("/replica/fault", json={"seq": 2})

    r = client.post("/replica/advance", json={"max_commits": 10})
    assert r.status_code == 409 and "2" in r.json()["detail"]
    assert client.get("/replica/status").json()["replica_seq"] == 1

    r = client.delete("/replica/fault")
    assert r.status_code == 200 and r.json()["cleared"] is True
    r = client.post("/replica/advance", json={"max_commits": 10})
    assert r.status_code == 200
    assert r.json()["applied_commits"] == [2, 3]


def test_validation_rejects_over_32_devices(client):
    ids = list(range(1, 34))
    r = client.post(
        "/query",
        json={"device_ids": ids, "min_commit": 1,
              "main_read_cap": 1000, "log_read_cap": 1000},
    )
    assert r.status_code == 422
