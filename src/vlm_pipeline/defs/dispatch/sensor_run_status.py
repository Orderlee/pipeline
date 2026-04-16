"""Dispatch run status finalizers.

dispatch_stage_job / archive-only ingest_job 종료 시
staging_dispatch_requests, staging_pipeline_runs 상태를 마감한다.
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
from vlm_pipeline.resources.duckdb import DuckDBResource


_TARGET_JOBS = {"dispatch_stage_job", "ingest_job"}


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

    db_resource = DuckDBResource(
        db_path=os.getenv("DATAOPS_DUCKDB_PATH", "/data/pipeline.duckdb")
    )

    db_resource.close_dispatch_request(
        request_id,
        status=status,
        error_message=error_message,
    )
    context.log.info(
        "dispatch run finalizer: "
        f"request_id={request_id} run_id={context.dagster_run.run_id} "
        f"job={context.dagster_run.job_name} status={status}"
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
