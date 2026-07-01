"""stale_state_reaper_sensor — TTL 초과 orphan 상태 행 정리.

크래시(hard SIGKILL / 컨테이너 재시작)로 non-terminal 상태에 고아로 남은 행을
주기적으로 마감한다. 감사(2026-07-01 core) 발견 INGEST-1 / DISPATCH-2 / BUILD-4 를
하나의 리퍼로 흡수:

  - raw_files.ingest_status='uploading' + TTL 초과 → 'failed'(재수집 가능)  [INGEST-1]
      ('pending' 은 제외 — auto_bootstrap 이 dispatch 대기용 장기 pending 을 둔다)
  - dispatch_requests.status IN('running','archive_moved') + TTL 초과 + 활성 run 없음
      → canceled  [DISPATCH-2]
  - datasets.build_status='building' + TTL 초과 → 'failed'  [BUILD-4]

기본 **STOPPED** — 배포 후 운영자가 Dagster UI 에서 검토 뒤 활성화. TTL 은 정상 실행이
절대 넘지 않는 값(기본 120분)으로 잡아 진행 중 작업을 오인 reap 하지 않는다.
side-effect 전용 센서(RunRequest 미발행) — stuck_run_guard 와 동일 패턴.

Layer 4: Dagster sensor. DB write 는 ``context.resources.db`` (required_resource_keys).
"""

from __future__ import annotations

from dagster import DefaultSensorStatus, RunsFilter, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus

from vlm_pipeline.lib.env_utils import int_env

_ACTIVE_RUN_STATUSES = [DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]


def _has_active_run_for_folder(context, folder_name: str) -> bool:
    """folder 태그로 QUEUED/STARTED run 존재 확인. 조회 실패 시 보수적으로 True(=reap 안 함)."""
    if not folder_name:
        return False
    try:
        runs = context.instance.get_runs(
            filters=RunsFilter(statuses=_ACTIVE_RUN_STATUSES, tags={"folder_name": folder_name}),
            limit=1,
        )
    except Exception:  # noqa: BLE001
        return True
    return bool(runs)


@sensor(
    name="stale_state_reaper_sensor",
    minimum_interval_seconds=int_env("STALE_REAPER_INTERVAL_SEC", 300, 60),
    default_status=DefaultSensorStatus.STOPPED,
    description="TTL 초과 orphan 상태(uploading/running/building) 정리 (INGEST-1/DISPATCH-2/BUILD-4)",
    required_resource_keys={"db"},
)
def stale_state_reaper_sensor(context):
    ttl_min = int_env("STALE_REAPER_TTL_MINUTES", 120, 10)
    db = context.resources.db
    parts: list[str] = []

    # 1) raw_files: stale 'uploading' → failed(재수집 가능)
    try:
        reaped_uploads = db.reap_stale_uploading_raw_files(ttl_min)
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"stale_reaper: raw_files reap 실패: {exc}")
        reaped_uploads = []
    if reaped_uploads:
        context.log.info(f"stale_reaper: uploading→failed {len(reaped_uploads)}건 (재수집 가능): {reaped_uploads[:20]}")
        parts.append(f"uploading={len(reaped_uploads)}")

    # 2) dispatch_requests: stale running/archive_moved + 활성 run 없음 → canceled
    try:
        stale_dispatch = db.find_stale_running_dispatch_requests(ttl_min)
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"stale_reaper: dispatch 조회 실패: {exc}")
        stale_dispatch = []
    closed = 0
    for row in stale_dispatch:
        if _has_active_run_for_folder(context, str(row.get("folder_name") or "")):
            continue
        try:
            db.close_dispatch_request(
                str(row["request_id"]),
                status="canceled",
                error_message="reaped_stale_dispatch_no_active_run",
            )
            closed += 1
        except Exception as exc:  # noqa: BLE001
            context.log.warning(f"stale_reaper: dispatch {row.get('request_id')} close 실패: {exc}")
    if closed:
        context.log.info(f"stale_reaper: orphan dispatch_requests {closed}건 canceled")
        parts.append(f"dispatch={closed}")

    # 3) datasets: stale 'building' → failed
    try:
        reaped_builds = db.reap_stale_building_datasets(ttl_min)
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"stale_reaper: datasets reap 실패: {exc}")
        reaped_builds = []
    if reaped_builds:
        context.log.info(f"stale_reaper: building→failed datasets {len(reaped_builds)}건: {reaped_builds[:20]}")
        parts.append(f"building={len(reaped_builds)}")

    if not parts:
        return SkipReason(f"stale_reaper: TTL({ttl_min}m) 초과 orphan 없음")
    return SkipReason("stale_reaper: reaped " + ", ".join(parts))
