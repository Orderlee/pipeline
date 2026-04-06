"""LS task 자동 생성 sensor.

dispatch_stage_job이 완료된 후 (staging_dispatch_requests.status='completed'),
아직 LS task가 생성되지 않은 요청을 감지하여 ls_task_create_job을 트리거합니다.

처리 흐름:
  staging_dispatch_requests
      WHERE status='completed'
        AND COALESCE(ls_task_status, 'pending') = 'pending'
    → ls_task_create_job (folder_name 기준으로 ls_tasks.py create 실행)
    → ls_task_status = 'created' 업데이트
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    job,
    op,
    sensor,
)

from vlm_pipeline.lib.env_utils import default_duckdb_path, int_env

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
            FROM staging_dispatch_requests
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
            "UPDATE staging_dispatch_requests SET ls_task_status = ? WHERE request_id = ?",
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

    for req in requests:
        request_id = req["request_id"]
        folder_name = req["folder_name"].rstrip("/")
        clip_prefix = f"{folder_name}/clips"

        context.log.info(f"ls task 생성 시작: request_id={request_id}, prefix={clip_prefix}")
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(LS_TASKS_SCRIPT),
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
