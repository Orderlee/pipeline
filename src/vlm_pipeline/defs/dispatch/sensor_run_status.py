"""Dispatch run status finalizers.

dispatch_stage_job / archive-only ingest_job 종료 시
dispatch_requests, dispatch_pipeline_runs 상태를 마감한다.
"""

from __future__ import annotations

import os

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from vlm_pipeline.defs.dispatch.service import resolve_dispatch_request_id_from_tags
from vlm_pipeline.lib.env_utils import (
    DB_BACKEND_DUCKDB,
    db_backend_mode,
    default_postgres_dsn,
)
from vlm_pipeline.resources.duckdb import DuckDBResource


_TARGET_JOBS = {"dispatch_stage_job", "ingest_job"}


def _build_runtime_db_resource():
    """run_status sensor 안에서 사용할 db resource 인스턴스.

    PG primary 모드(``postgres`` / ``dual_pg_primary``) + DSN 설정 → PostgresResource.
    그 외 → 기존 DuckDBResource (legacy).

    Note: ``definitions_production._build_db_resource()`` 와 달리 EnvVar 미사용
    (sensor context 외부 호출이라 EnvVar resolve 안 됨). 대신 환경변수 직접 read.
    """
    mode = db_backend_mode()
    dsn = default_postgres_dsn()
    if mode != DB_BACKEND_DUCKDB and dsn:
        from vlm_pipeline.resources.postgres import PostgresResource  # noqa: PLC0415

        return PostgresResource(dsn=dsn)
    return DuckDBResource(
        db_path=os.getenv("DATAOPS_DUCKDB_PATH", "/data/pipeline.duckdb")
    )


def _resolve_dispatch_request_id(context: RunStatusSensorContext) -> str | None:
    run = context.dagster_run
    tags = getattr(run, "tags", {}) or {}
    request_id = resolve_dispatch_request_id_from_tags(tags)
    if not request_id:
        return None
    if run.job_name not in _TARGET_JOBS:
        return None
    if run.job_name == "ingest_job" and str(tags.get("dispatch_archive_only") or "").strip().lower() not in {
        "1",
        "true",
        "yes",
    }:
        return None
    return request_id


def _finalize_dispatch_request(
    context: RunStatusSensorContext,
    *,
    status: str,
    error_message: str | None = None,
) -> None:
    request_id = _resolve_dispatch_request_id(context)
    if not request_id:
        return

    db_resource = _build_runtime_db_resource()

    try:
        db_resource.close_dispatch_request(
            request_id,
            status=status,
            error_message=error_message,
        )
    except Exception:
        context.log.exception(
            "dispatch run finalizer: close_dispatch_request FAILED "
            f"request_id={request_id} run_id={context.dagster_run.run_id} "
            f"job={context.dagster_run.job_name} status={status}"
        )
        return

    aborted_raw = 0
    if status in {"canceled", "failed"}:
        try:
            aborted_raw = db_resource.abort_in_progress_raw_files_for_dispatch(
                request_id,
                error_message=f"dispatch_{status}:run_id={context.dagster_run.run_id}",
            )
        except Exception:
            context.log.exception(
                "dispatch run finalizer: abort_in_progress_raw_files FAILED "
                f"request_id={request_id} run_id={context.dagster_run.run_id}"
            )

    context.log.info(
        "dispatch run finalizer: "
        f"request_id={request_id} run_id={context.dagster_run.run_id} "
        f"job={context.dagster_run.job_name} status={status} "
        f"aborted_raw_files={aborted_raw}"
    )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    monitor_all_code_locations=False,
)
def dispatch_run_success_sensor(context: RunStatusSensorContext):
    _finalize_dispatch_request(context, status="completed")


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
    monitor_all_code_locations=False,
)
def dispatch_run_failure_sensor(context: RunStatusSensorContext):
    _finalize_dispatch_request(
        context,
        status="failed",
        error_message="run_status:failure",
    )


@run_status_sensor(
    run_status=DagsterRunStatus.CANCELED,
    default_status=DefaultSensorStatus.RUNNING,
    monitor_all_code_locations=False,
)
def dispatch_run_canceled_sensor(context: RunStatusSensorContext):
    _finalize_dispatch_request(
        context,
        status="canceled",
        error_message="run_status:canceled",
    )
