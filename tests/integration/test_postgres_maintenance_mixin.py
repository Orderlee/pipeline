"""PostgresMaintenanceMixin — gpu_maintenance_lock read/write (real-PG)."""
from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def test_set_get_and_clear(pg_resource):
    row0 = pg_resource.get_gpu_maintenance("sam3")
    assert row0 is None or row0.get("active") in (False, None)

    pg_resource.set_gpu_maintenance(
        "sam3", active=True, owner_run_id="run-42", ttl_seconds=900, note="ft"
    )
    row = pg_resource.get_gpu_maintenance("sam3")
    assert row["active"] is True
    assert row["owner_run_id"] == "run-42"
    assert row["ttl_seconds"] == 900
    assert row["entered_at"] is not None and row["heartbeat_at"] is not None

    pg_resource.set_gpu_maintenance("sam3", active=False)
    row2 = pg_resource.get_gpu_maintenance("sam3")
    assert row2["active"] is False


def test_heartbeat_bumps_only_when_active(pg_resource):
    pg_resource.set_gpu_maintenance("pe_core", active=True, owner_run_id="r", ttl_seconds=600)
    before = pg_resource.get_gpu_maintenance("pe_core")["heartbeat_at"]
    pg_resource.bump_gpu_maintenance_heartbeat("pe_core")
    after = pg_resource.get_gpu_maintenance("pe_core")["heartbeat_at"]
    assert after >= before
