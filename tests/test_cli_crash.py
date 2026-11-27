"""进程级崩溃接续：CLI 在某提交落盘后 os._exit，重跑时从下一提交继续。"""
from __future__ import annotations

import os
import subprocess
import sys

from app.config import get_paths
from app.db import init_primary
from app.models import BatchItem
from app.primary import apply_batch


def _run_cli(tmp_path, *args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["DATA_DIR"] = str(tmp_path)
    env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [sys.executable, os.path.join(root, "scripts", "replay.py"), *args],
        env=env,
        capture_output=True,
        text=True,
    )


def test_crash_after_commit_then_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PRIMARY_DB", str(tmp_path / "primary.sqlite3"))
    monkeypatch.setenv("REPLICA_DB", str(tmp_path / "replica.sqlite3"))
    primary = init_primary(get_paths().primary)
    for seq in range(1, 5):
        apply_batch(
            primary,
            [BatchItem(device_id=seq, op="register", new_status="maintenance")],
        )
    primary.close()

    # 崩溃在 seq2 落盘之后
    crashed = _run_cli(tmp_path, "--max-commits", "10", "--crash-at", "2")
    assert crashed.returncode == 1
    assert "CRASH" in crashed.stderr

    status1 = _run_cli(tmp_path, "--status")
    assert "replica_seq=2" in status1.stdout

    # 重新运行（模拟服务重启）：从 seq3 接续，不重复回放
    resumed = _run_cli(tmp_path, "--max-commits", "10")
    assert resumed.returncode == 0
    assert "applied seq=3" in resumed.stdout
    assert "applied seq=4" in resumed.stdout
    assert "applied seq=1" not in resumed.stdout
    assert "applied seq=2" not in resumed.stdout

    status2 = _run_cli(tmp_path, "--status")
    assert "replica_seq=4" in status2.stdout and "pending=[]" in status2.stdout


def test_fault_via_db_blocks_cli(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PRIMARY_DB", str(tmp_path / "primary.sqlite3"))
    monkeypatch.setenv("REPLICA_DB", str(tmp_path / "replica.sqlite3"))
    from app.db import init_replica
    from app import replay

    primary = init_primary(get_paths().primary)
    apply_batch(
        primary, [BatchItem(device_id=1, op="register", new_status="maintenance")]
    )
    replica = init_replica(get_paths().replica)
    replay.set_fault_seq(replica, 1)
    primary.close()
    replica.close()

    proc = _run_cli(tmp_path, "--max-commits", "5")
    assert proc.returncode == 2
    assert "FAULT" in proc.stderr
    assert "replica_seq=0" in _run_cli(tmp_path, "--status").stdout
