"""lib.maintenance_flag — 순수 정비락 판단 로직 (no PG/dagster)."""

from __future__ import annotations

from vlm_pipeline.lib.maintenance_flag import (
    CLEAR_FLAG,
    MaintenanceFlag,
    decide_auto_release,
    flag_from_pg_row,
    is_heartbeat_stale,
)


def _active(**kw) -> MaintenanceFlag:
    base = dict(
        active=True,
        target="sam3",
        owner_run_id="run-1",
        entered_at=1000.0,
        heartbeat_at=1000.0,
        ttl_seconds=600,
        note=None,
    )
    base.update(kw)
    return MaintenanceFlag(**base)


def test_flag_from_none_row_is_clear():
    f = flag_from_pg_row(None, target="sam3")
    assert f.active is False
    assert f.target == "sam3"


def test_flag_from_pg_row_parses_timestamps():
    import datetime as dt

    ts = dt.datetime(2026, 6, 29, tzinfo=dt.timezone.utc)
    row = {
        "active": True,
        "owner_run_id": "run-9",
        "entered_at": ts,
        "heartbeat_at": ts,
        "ttl_seconds": 900,
        "note": "training",
    }
    f = flag_from_pg_row(row, target="pe_core")
    assert f.active is True and f.owner_run_id == "run-9"
    assert f.ttl_seconds == 900
    assert f.heartbeat_at == ts.timestamp()


def test_heartbeat_stale_when_past_ttl():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    assert is_heartbeat_stale(f, now_ts=1700.0) is True  # 700s > 600 ttl
    assert is_heartbeat_stale(f, now_ts=1500.0) is False  # 500s < 600 ttl


def test_clear_flag_never_stale():
    assert is_heartbeat_stale(CLEAR_FLAG, now_ts=10**12) is False


def test_auto_release_on_dead_heartbeat():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    release, reason = decide_auto_release(f, owner_run_is_running=True, now_ts=1700.0)
    assert release is True and reason == "heartbeat_ttl_expired"


def test_auto_release_on_dead_owner_run():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    release, reason = decide_auto_release(f, owner_run_is_running=False, now_ts=1100.0)
    assert release is True and reason == "owner_run_not_running"


def test_no_release_when_healthy():
    f = _active(heartbeat_at=1000.0, ttl_seconds=600)
    release, reason = decide_auto_release(f, owner_run_is_running=True, now_ts=1100.0)
    assert release is False and reason is None


def test_no_release_when_inactive():
    release, reason = decide_auto_release(CLEAR_FLAG, owner_run_is_running=False, now_ts=1100.0)
    assert release is False and reason is None
