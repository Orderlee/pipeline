"""maintenance_guard_sensor 순수 판단 로직 + settings 검증 (no Dagster instance)."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from vlm_pipeline.defs.train.sensor_maintenance_guard import resolve_release_actions  # noqa: E402
from vlm_pipeline.lib.maintenance_flag import MaintenanceFlag  # noqa: E402


def _flag(**kw) -> MaintenanceFlag:
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


def test_release_on_dead_heartbeat():
    release, reason = resolve_release_actions(_flag(), owner_run_is_running=True, now_ts=1700.0)
    assert release is True and reason == "heartbeat_ttl_expired"


def test_release_on_dead_owner_run():
    release, reason = resolve_release_actions(_flag(), owner_run_is_running=False, now_ts=1100.0)
    assert release is True and reason == "owner_run_not_running"


def test_release_when_owner_run_id_missing():
    release, reason = resolve_release_actions(_flag(owner_run_id=None), owner_run_is_running=False, now_ts=1100.0)
    assert release is True and reason == "owner_run_id_missing"


def test_no_release_when_healthy():
    release, reason = resolve_release_actions(_flag(), owner_run_is_running=True, now_ts=1100.0)
    assert release is False and reason is None


def test_no_release_when_inactive():
    release, reason = resolve_release_actions(_flag(active=False), owner_run_is_running=False, now_ts=1100.0)
    assert release is False and reason is None


def test_settings_loader_defaults(monkeypatch):
    from vlm_pipeline.resources.runtime_settings import load_maintenance_guard_settings

    monkeypatch.delenv("MAINTENANCE_GUARD_ENABLED", raising=False)
    s = load_maintenance_guard_settings()
    assert s.enabled is True  # fail-safe default ON
    assert "sam3" in s.targets and "pe_core" in s.targets
    assert s.interval_sec >= 30
