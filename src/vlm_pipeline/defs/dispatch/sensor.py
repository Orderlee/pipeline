"""Dispatch JSON ingress sensor.

production 기본 경로이며, staging에서는 파일 기반 레거시/호환 경로로 유지한다.
"""

from __future__ import annotations

import json
from pathlib import Path

from dagster import SensorDefinition, SensorEvaluationContext, sensor

from vlm_pipeline.defs.dispatch.service import (
    DispatchIngressRequest,
    move_dispatch_file,
    process_dispatch_ingress_request,
    record_failed_dispatch_request,
)
from vlm_pipeline.lib.env_utils import is_duckdb_lock_conflict
from vlm_pipeline.resources.config import PipelineConfig


def _record_failed_and_move(
    *,
    db_resource,
    request_id: str,
    request_payload: dict,
    reason: str,
    request_file: Path,
    failed_dir: Path,
    context: SensorEvaluationContext,
) -> None:
    record_failed_dispatch_request(
        db_resource,
        request_id,
        request_payload,
        reason,
    )
    move_dispatch_file(request_file, failed_dir, context=context)


def build_dispatch_sensor(*, jobs) -> SensorDefinition:
    """Builder — definitions.py에서 job 객체를 주입받아 SensorDefinition 생성."""
    return sensor(
        name="dispatch_sensor",
        description="dispatch JSON 처리 후 dispatch 트리거 (dispatch_stage_job / ingest_job)",
        minimum_interval_seconds=30,
        required_resource_keys={"db"},
        jobs=jobs,
    )(_dispatch_sensor_fn)


def _dispatch_sensor_fn(context: SensorEvaluationContext):
    config = PipelineConfig()
    incoming_dir = Path(config.incoming_dir)
    pending_dir = incoming_dir / ".dispatch" / "pending"
    processed_dir = incoming_dir / ".dispatch" / "processed"
    failed_dir = incoming_dir / ".dispatch" / "failed"

    if not pending_dir.exists():
        return

    db_resource = getattr(context.resources, "db", None)
    if db_resource is None:
        return

    try:
        db_resource.ensure_runtime_schema()
    except Exception as exc:
        if is_duckdb_lock_conflict(exc):
            context.log.warning(f"DuckDB lock 충돌 — 다음 tick에서 재시도: {exc}")
            return
        raise

    requests = sorted(fpath for fpath in pending_dir.glob("*.json") if fpath.is_file())

    for req_file in requests:
        try:
            req_data = json.loads(req_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        request_id = str(req_data.get("request_id") or req_file.stem).strip() or req_file.stem
        outcome = process_dispatch_ingress_request(
            context,
            db_resource=db_resource,
            config=config,
            ingress_request=DispatchIngressRequest(
                payload=req_data,
                fallback_request_id=request_id,
                duplicate_policy="reject",
                in_flight_policy="reject",
            ),
        )
        if outcome.status != "run_request":
            _record_failed_and_move(
                db_resource=db_resource,
                request_id=outcome.request_id,
                request_payload=req_data,
                reason=outcome.reason,
                request_file=req_file,
                failed_dir=failed_dir,
                context=context,
            )
            continue

        prepared = outcome.prepared
        if prepared is None or outcome.run_request is None:
            _record_failed_and_move(
                db_resource=db_resource,
                request_id=outcome.request_id,
                request_payload=req_data,
                reason="invalid_dispatch_outcome",
                request_file=req_file,
                failed_dir=failed_dir,
                context=context,
            )
            continue

        move_dispatch_file(req_file, processed_dir, context=context)

        if prepared.archive_only:
            context.log.info(
                "dispatch archive-only 요청 감지: request_id=%s folder=%s -> ingest_job만 실행",
                prepared.request_id,
                prepared.folder_name,
            )

        yield outcome.run_request
