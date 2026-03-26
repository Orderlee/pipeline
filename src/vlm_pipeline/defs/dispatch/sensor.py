"""Dispatch JSON 센서 — `.dispatch/pending` 처리 후 dispatch_stage_job 트리거."""

from __future__ import annotations

import json
from pathlib import Path

from dagster import SensorEvaluationContext, sensor

from vlm_pipeline.defs.dispatch.service import (
    build_dispatch_pipeline_rows,
    build_dispatch_request_record,
    build_dispatch_run_request,
    has_active_dispatch_run,
    move_dispatch_file,
    prepare_dispatch_request,
    record_failed_dispatch_request,
    resolve_dispatch_applied_params,
    write_dispatch_manifest,
)
from vlm_pipeline.resources.config import PipelineConfig


@sensor(
    name="dispatch_sensor",
    description="dispatch JSON 처리 후 dispatch_stage_job 트리거 (archive 이동은 raw_ingest에서 수행)",
    minimum_interval_seconds=30,
    required_resource_keys={"db"},
    job_name="dispatch_stage_job",
)
def dispatch_sensor(context: SensorEvaluationContext):
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

    db_resource.ensure_runtime_schema()
    db_resource.ensure_dispatch_tracking_tables()

    requests = sorted(fpath for fpath in pending_dir.glob("*.json") if fpath.is_file())

    for req_file in requests:
        try:
            req_data = json.loads(req_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        request_id = str(req_data.get("request_id") or req_file.stem).strip()

        try:
            prepared = prepare_dispatch_request(req_data, incoming_dir=incoming_dir)
        except ValueError as exc:
            record_failed_dispatch_request(db_resource, request_id, req_data, str(exc))
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        if not prepared.incoming_folder_path.is_dir():
            record_failed_dispatch_request(
                db_resource,
                prepared.request_id,
                req_data,
                "folder_not_in_incoming",
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        existing_rows = db_resource.get_in_flight_dispatch_requests(prepared.folder_name)
        existing_status = db_resource.get_dispatch_request_status(prepared.request_id)

        if existing_rows and has_active_dispatch_run(context, prepared.folder_name):
            record_failed_dispatch_request(
                db_resource,
                prepared.request_id,
                req_data,
                "folder_dispatch_in_flight",
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        if existing_rows:
            for row in existing_rows:
                stale_request_id = str(row.get("request_id") or "").strip()
                if not stale_request_id:
                    continue
                db_resource.close_dispatch_request(
                    stale_request_id,
                    status="canceled",
                    error_message="stale_dispatch_request_without_active_run",
                )

        if existing_status:
            record_failed_dispatch_request(
                db_resource,
                prepared.request_id,
                req_data,
                "duplicate_request_id",
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        try:
            manifest_path, archive_dest = write_dispatch_manifest(config, prepared)
        except Exception as exc:  # noqa: BLE001
            record_failed_dispatch_request(
                db_resource,
                prepared.request_id,
                req_data,
                f"failed_to_write_manifest:{exc}",
            )
            move_dispatch_file(req_file, failed_dir, context=context)
            continue

        context.log.info(
            "dispatch manifest 생성 완료(archive 이동/파일 인덱싱은 raw_ingest에서 수행): "
            f"request_id={prepared.request_id} folder={prepared.folder_name} manifest={manifest_path.name}"
        )

        model_defaults = db_resource.get_active_staging_model_configs(prepared.labeling_method)
        applied_params = resolve_dispatch_applied_params(req_data, model_defaults)

        db_resource.insert_dispatch_request(
            build_dispatch_request_record(
                prepared,
                archive_dest=archive_dest,
                applied_params=applied_params,
            )
        )
        db_resource.insert_dispatch_pipeline_runs(
            build_dispatch_pipeline_rows(
                prepared,
                model_defaults=model_defaults,
                applied_params=applied_params,
            )
        )

        move_dispatch_file(req_file, processed_dir, context=context)

        if prepared.archive_only:
            context.log.info(
                "dispatch archive-only 요청 감지: request_id=%s folder=%s -> ingest_job만 실행",
                prepared.request_id,
                prepared.folder_name,
            )

        yield build_dispatch_run_request(
            prepared,
            manifest_path=manifest_path,
            applied_params=applied_params,
        )
