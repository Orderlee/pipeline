"""maintenance_guard_sensor — GPU 정비락 fail-safe 자동해제 (stuck_run_guard 패턴).

정비 owner run 이 죽었거나(heartbeat TTL 초과 / run 비RUNNING) 정비락이 stale 이면:
  1) PG 플래그 clear  2) 서빙 /maintenance/exit + /warmup  3) 경고 로그.
'락은 fail-safe(자동해제), fail-stuck 아님' — 설계 요구사항(§9).
"""
from __future__ import annotations

import time

import requests
from dagster import DefaultSensorStatus, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus

from vlm_pipeline.lib.maintenance_flag import (
    MaintenanceFlag,
    decide_auto_release,
    flag_from_pg_row,
)
from vlm_pipeline.resources.runtime_settings import load_maintenance_guard_settings

_RUNNING_STATUSES = {DagsterRunStatus.STARTED, DagsterRunStatus.STARTING}


def resolve_release_actions(
    flag: MaintenanceFlag,
    *,
    owner_run_is_running: bool,
    now_ts: float,
) -> tuple[bool, str | None]:
    """순수 판단: 정비락을 자동해제해야 하는가. (sensor body 에서 분리해 단위테스트)"""
    if not flag.active:
        return (False, None)
    if not flag.owner_run_id:
        return (True, "owner_run_id_missing")
    return decide_auto_release(flag, owner_run_is_running=owner_run_is_running, now_ts=now_ts)


def _owner_run_is_running(context, owner_run_id: str | None) -> bool:
    if not owner_run_id:
        return False
    try:
        run = context.instance.get_run_by_id(owner_run_id)
    except Exception:  # noqa: BLE001
        return False
    if run is None:
        return False
    return run.status in _RUNNING_STATUSES


def _release_serving(base_url: str, *, timeout: float = 10.0) -> None:
    requests.post(f"{base_url.rstrip('/')}/maintenance/exit", timeout=timeout)
    requests.post(f"{base_url.rstrip('/')}/warmup", timeout=timeout)


@sensor(
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="GPU 정비락 stale 자동해제 (fail-safe) + 서빙 re-warm",
    required_resource_keys={"db"},
)
def maintenance_guard_sensor(context):
    settings = load_maintenance_guard_settings()
    if not settings.enabled:
        return SkipReason("maintenance guard 비활성화됨")

    now_ts = time.time()
    db = getattr(context.resources, "db", None)
    if db is None:
        return SkipReason("db resource 없음")

    released: list[str] = []
    for target, base_url in settings.targets.items():
        try:
            row = db.get_gpu_maintenance(target)
        except Exception as exc:  # noqa: BLE001
            context.log.warning("maintenance_guard %s 조회 실패: %s", target, exc)
            continue
        flag = flag_from_pg_row(row, target=target)
        if not flag.active:
            continue

        owner_running = _owner_run_is_running(context, flag.owner_run_id)
        should_release, reason = resolve_release_actions(
            flag, owner_run_is_running=owner_running, now_ts=now_ts
        )
        if not should_release:
            continue

        try:
            db.set_gpu_maintenance(target, active=False)
            _release_serving(base_url)
            released.append(f"{target}:{reason}")
            context.log.warning(
                "maintenance 자동해제: target=%s reason=%s owner_run_id=%s base_url=%s",
                target, reason, flag.owner_run_id, base_url,
            )
        except Exception as exc:  # noqa: BLE001
            context.log.warning("maintenance 자동해제 실패 target=%s: %s", target, exc)

    if not released:
        return SkipReason(f"stale 정비락 없음 (targets={sorted(settings.targets)})")
    return SkipReason(f"maintenance 자동해제 완료: {released}")
