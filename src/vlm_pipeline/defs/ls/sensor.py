"""LS task 자동 생성 sensor + presigned URL 갱신 schedule.

dispatch_stage_job이 완료된 후 (dispatch_requests.status='completed'),
아직 LS task가 생성되지 않은 요청을 감지하여 ls_task_create_job을 트리거합니다.

처리 흐름:
  dispatch_requests
      WHERE status='completed'
        AND COALESCE(ls_task_status, 'pending') = 'pending'
    → ls_task_create_job (folder_name 기준으로 ls_tasks.py create 실행)
    → ls_task_status = 'created' 업데이트

presigned URL 갱신:
  매일 05:00 KST — ls_tasks.py renew --all-projects 실행
  만료 1일 이내 URL을 자동 갱신하여 라벨링 작업 중단 방지
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    job,
    op,
    sensor,
)

from vlm_pipeline.lib.env_utils import default_duckdb_path, int_env
from vlm_pipeline.lib.minio_cross_sync import (
    is_cross_sync_needed,
    ls_minio_endpoint,
    sync_folder_for_ls,
)

LS_TASKS_SCRIPT = Path(
    os.environ.get("LS_TASKS_SCRIPT", Path(__file__).parents[3] / "gemini" / "ls_tasks.py")
)


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------

def _fetch_pending_dispatch_requests(db_path: str) -> list[dict]:
    """ls_task_status='pending'이고 dispatch가 완료된 요청 목록."""
    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT request_id, folder_name
            FROM dispatch_requests
            WHERE status = 'completed'
              AND COALESCE(ls_task_status, 'pending') = 'pending'
            ORDER BY completed_at
            """
        ).fetchall()
        return [{"request_id": r[0], "folder_name": r[1]} for r in rows if r[1]]
    finally:
        conn.close()


def _update_ls_task_status(db_path: str, request_id: str, status: str) -> None:
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            "UPDATE dispatch_requests SET ls_task_status = ? WHERE request_id = ?",
            [status, request_id],
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Op
# ---------------------------------------------------------------------------

@op
def create_ls_tasks(context) -> None:
    """dispatch request 별로 ls_tasks.py create 실행."""
    db_path = default_duckdb_path()
    requests = _fetch_pending_dispatch_requests(db_path)

    if not requests:
        context.log.info("ls task 생성 대상 없음")
        return

    api_key = os.environ.get("LS_API_KEY", "")
    if not api_key:
        raise RuntimeError("LS_API_KEY 환경변수가 필요합니다.")

    need_sync = is_cross_sync_needed()
    target_ep = ls_minio_endpoint()

    for req in requests:
        request_id = req["request_id"]
        folder_name = req["folder_name"].rstrip("/")
        clip_prefix = f"{folder_name}/clips"

        context.log.info(f"ls task 생성 시작: request_id={request_id}, prefix={clip_prefix}")
        try:
            # (A/C) staging이면 클립·라벨을 production MinIO로 복사
            if need_sync:
                n = sync_folder_for_ls(
                    folder_name,
                    target_endpoint=target_ep,
                    log_fn=lambda msg: context.log.info(msg),
                )
                context.log.info(f"staging→production 동기화: {n}건 복사")

            # (B) --minio-endpoint를 명시적으로 전달하여 LS가 production 버킷 참조
            result = subprocess.run(
                [
                    sys.executable,
                    str(LS_TASKS_SCRIPT),
                    "--minio-endpoint", target_ep,
                    "create",
                    "--prefix", clip_prefix,
                    "--api-key", api_key,
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                raise RuntimeError(f"ls_tasks.py 실패:\n{result.stderr}")

            context.log.info(result.stdout)
            _update_ls_task_status(db_path, request_id, "created")
            context.log.info(f"ls_task_status='created' 업데이트: request_id={request_id}")

        except Exception as exc:
            context.log.error(f"ls task 생성 실패: request_id={request_id} — {exc}")
            _update_ls_task_status(db_path, request_id, "failed")


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

@job(name="ls_task_create_job", description="dispatch 완료 후 LS task 자동 생성")
def ls_task_create_job():
    create_ls_tasks()


# ---------------------------------------------------------------------------
# Sensor
# ---------------------------------------------------------------------------

@sensor(
    job=ls_task_create_job,
    name="ls_task_create_sensor",
    minimum_interval_seconds=int_env("LS_TASK_SENSOR_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.STOPPED,
    description="dispatch 완료 후 LS task 미생성 요청 감지 → ls_task_create_job 트리거",
)
def ls_task_create_sensor(context):
    db_path = default_duckdb_path()
    if not Path(db_path).exists():
        yield SkipReason(f"DuckDB not found: {db_path}")
        return

    try:
        pending = _fetch_pending_dispatch_requests(db_path)
    except Exception as exc:
        yield SkipReason(f"DB 조회 실패: {exc}")
        return

    if not pending:
        yield SkipReason("ls task 생성 대기 중인 dispatch 없음")
        return

    request_ids = [r["request_id"] for r in pending]
    context.log.info(f"ls task 생성 대상 {len(pending)}건: {request_ids}")

    yield RunRequest(
        run_key=f"ls-task-create-{int(time.time())}",
        tags={"trigger": "ls_task_create_sensor", "pending_count": str(len(pending))},
    )


# ---------------------------------------------------------------------------
# Presigned URL 갱신 Job + Schedule
# ---------------------------------------------------------------------------

@op
def renew_ls_presigned_urls(context) -> None:
    """모든 LS project의 만료 임박 presigned URL 갱신."""
    api_key = os.environ.get("LS_API_KEY", "")
    if not api_key:
        context.log.warning("LS_API_KEY 미설정 — presigned URL 갱신 건너뜀")
        return

    target_ep = ls_minio_endpoint()
    result = subprocess.run(
        [
            sys.executable,
            str(LS_TASKS_SCRIPT),
            "--minio-endpoint", target_ep,
            "renew",
            "--all-projects",
            "--api-key", api_key,
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(f"presigned URL 갱신 실패:\n{result.stderr}")
        raise RuntimeError(f"ls_tasks.py renew 실패 (exit={result.returncode})")


@job(name="ls_presign_renew_job", description="LS presigned URL 만료 임박 자동 갱신")
def ls_presign_renew_job():
    renew_ls_presigned_urls()


ls_presign_renew_schedule = ScheduleDefinition(
    name="ls_presign_renew_schedule",
    job=ls_presign_renew_job,
    cron_schedule="0 5 * * *",
    execution_timezone="Asia/Seoul",
)
