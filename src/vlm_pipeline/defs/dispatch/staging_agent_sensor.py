"""Staging-only dispatch ingress sensor backed by piaspace-agent polling API."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import requests
from dagster import SensorEvaluationContext, SkipReason, sensor

from vlm_pipeline.defs.dispatch.service import (
    build_dispatch_pipeline_rows,
    build_dispatch_request_record,
    build_dispatch_run_request,
    has_active_dispatch_run,
    prepare_dispatch_request,
    record_failed_dispatch_request,
    resolve_dispatch_applied_params,
    write_dispatch_manifest,
)
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.runtime_settings import load_staging_agent_polling_settings


def _build_agent_request_id(delivery_id: str) -> str:
    rendered = sanitize_path_component(delivery_id) or "unknown"
    return f"agent_{rendered}"


def _payload_field_has_value(value: Any) -> bool:
    if isinstance(value, list):
        return any(str(item or "").strip() for item in value)
    return bool(str(value or "").strip())


def should_wait_for_dispatch_params(request_payload: Mapping[str, Any]) -> bool:
    """Return True when dispatch-driving metadata is entirely empty."""
    watched_fields = (
        request_payload.get("labeling_method"),
        request_payload.get("outputs"),
        request_payload.get("run_mode"),
        request_payload.get("categories"),
        request_payload.get("classes"),
    )
    return not any(_payload_field_has_value(value) for value in watched_fields)


def normalize_agent_dispatch_request(
    request_payload: Mapping[str, Any],
    *,
    delivery_id: str,
) -> dict[str, Any]:
    """Convert agent payload into canonical staging dispatch request payload."""
    normalized = dict(request_payload)
    source_unit_name = str(normalized.get("source_unit_name") or normalized.get("folder_name") or "").strip()
    if not source_unit_name:
        raise ValueError("missing_source_unit_name")

    request_id = str(normalized.get("request_id") or "").strip()
    if not request_id:
        request_id = _build_agent_request_id(delivery_id)

    normalized["request_id"] = request_id
    normalized["folder_name"] = source_unit_name
    if not str(normalized.get("image_profile") or "").strip():
        normalized["image_profile"] = "current"
    if not str(normalized.get("requested_by") or "").strip():
        normalized["requested_by"] = "piaspace-agent"
    return normalized


def build_agent_ack_payload(
    *,
    delivery_id: str,
    status: str,
    request_id: str | None,
    message: str,
) -> dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "status": status,
        "request_id": request_id,
        "message": message,
    }


def _fetch_pending_dispatch_items(
    session: requests.Session,
    *,
    base_url: str,
    limit: int,
    timeout: tuple[int, int],
) -> list[dict[str, Any]]:
    response = session.get(
        f"{base_url}/api/staging/dispatch/pending",
        params={"limit": limit},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("invalid_pending_items")
    return [item for item in items if isinstance(item, dict)]


def _post_dispatch_ack(
    context,
    session: requests.Session,
    *,
    base_url: str,
    timeout: tuple[int, int],
    delivery_id: str,
    status: str,
    request_id: str | None,
    message: str,
) -> None:
    try:
        response = session.post(
            f"{base_url}/api/staging/dispatch/ack",
            json=build_agent_ack_payload(
                delivery_id=delivery_id,
                status=status,
                request_id=request_id,
                message=message,
            ),
            timeout=timeout,
        )
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        context.log.warning(
            "staging agent ack 전송 실패: "
            f"delivery_id={delivery_id} status={status} request_id={request_id or ''} error={exc}"
        )


@sensor(
    name="staging_agent_dispatch_sensor",
    description="staging 전용 agent API polling -> dispatch_stage_job ingress",
    minimum_interval_seconds=load_staging_agent_polling_settings().interval_sec,
    required_resource_keys={"db"},
    job_name="dispatch_stage_job",
)
def staging_agent_dispatch_sensor(context: SensorEvaluationContext):
    settings = load_staging_agent_polling_settings()
    if not settings.enabled:
        yield SkipReason("STAGING_AGENT_POLLING_ENABLED=false")
        return

    db_resource = getattr(context.resources, "db", None)
    if db_resource is None:
        yield SkipReason("db resource unavailable")
        return

    db_resource.ensure_runtime_schema()
    db_resource.ensure_dispatch_tracking_tables()

    config = PipelineConfig()
    incoming_dir = Path(config.incoming_dir)
    timeout = (settings.connect_timeout_sec, settings.read_timeout_sec)

    with requests.Session() as session:
        try:
            items = _fetch_pending_dispatch_items(
                session,
                base_url=settings.base_url,
                limit=settings.poll_limit,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            yield SkipReason(f"staging agent polling unavailable: {exc}")
            return
        except ValueError as exc:
            yield SkipReason(f"staging agent pending payload invalid: {exc}")
            return

        if not items:
            yield SkipReason("staging agent pending queue empty")
            return

        yielded = False

        for item in items:
            delivery_id = str(item.get("delivery_id") or "").strip()
            raw_request = item.get("request")
            if not delivery_id or not isinstance(raw_request, Mapping):
                context.log.warning(f"staging agent payload 무시: invalid item shape={item}")
                continue

            request_id: str | None = None

            try:
                canonical_request = normalize_agent_dispatch_request(raw_request, delivery_id=delivery_id)
                request_id = str(canonical_request.get("request_id") or "").strip() or None
            except ValueError as exc:
                record_failed_dispatch_request(
                    db_resource,
                    _build_agent_request_id(delivery_id),
                    {
                        "request_id": _build_agent_request_id(delivery_id),
                        "folder_name": raw_request.get("source_unit_name") if isinstance(raw_request, Mapping) else None,
                        "image_profile": raw_request.get("image_profile") if isinstance(raw_request, Mapping) else None,
                    },
                    str(exc),
                )
                _post_dispatch_ack(
                    context,
                    session,
                    base_url=settings.base_url,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="rejected",
                    request_id=_build_agent_request_id(delivery_id),
                    message=str(exc),
                )
                continue

            folder_name = str(canonical_request.get("folder_name") or "").strip()
            incoming_folder_path = incoming_dir / folder_name
            if not incoming_folder_path.is_dir():
                record_failed_dispatch_request(
                    db_resource,
                    request_id or _build_agent_request_id(delivery_id),
                    canonical_request,
                    "folder_not_in_incoming",
                )
                _post_dispatch_ack(
                    context,
                    session,
                    base_url=settings.base_url,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="rejected",
                    request_id=request_id,
                    message="folder_not_in_incoming",
                )
                continue

            existing_status = db_resource.get_dispatch_request_status(request_id or "")
            if existing_status:
                context.log.info(
                    "staging agent duplicate request_id 감지: "
                    f"delivery_id={delivery_id} request_id={request_id} status={existing_status}"
                )
                _post_dispatch_ack(
                    context,
                    session,
                    base_url=settings.base_url,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="accepted",
                    request_id=request_id,
                    message="duplicate_request_id_noop",
                )
                continue

            if should_wait_for_dispatch_params(canonical_request):
                context.log.info(
                    "staging agent dispatch waiting: "
                    f"delivery_id={delivery_id} request_id={request_id} "
                    f"folder={folder_name} reason=waiting_for_dispatch_params"
                )
                _post_dispatch_ack(
                    context,
                    session,
                    base_url=settings.base_url,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="accepted",
                    request_id=request_id,
                    message="waiting_for_dispatch_params",
                )
                continue

            try:
                prepared = prepare_dispatch_request(canonical_request, incoming_dir=incoming_dir)
            except ValueError as exc:
                record_failed_dispatch_request(
                    db_resource,
                    request_id or _build_agent_request_id(delivery_id),
                    canonical_request,
                    str(exc),
                )
                _post_dispatch_ack(
                    context,
                    session,
                    base_url=settings.base_url,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="rejected",
                    request_id=request_id,
                    message=str(exc),
                )
                continue

            existing_rows = db_resource.get_in_flight_dispatch_requests(prepared.folder_name)
            if existing_rows and has_active_dispatch_run(context, prepared.folder_name):
                context.log.info(
                    "staging agent folder in flight -> ack 보류: "
                    f"delivery_id={delivery_id} folder={prepared.folder_name}"
                )
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

            try:
                manifest_path, archive_dest = write_dispatch_manifest(config, prepared)
                model_defaults = db_resource.get_active_staging_model_configs(prepared.labeling_method)
                applied_params = resolve_dispatch_applied_params(canonical_request, model_defaults)
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
                run_request = build_dispatch_run_request(
                    prepared,
                    manifest_path=manifest_path,
                    applied_params=applied_params,
                )
            except Exception as exc:  # noqa: BLE001
                context.log.error(
                    "staging agent dispatch 처리 실패(ack 보류): "
                    f"delivery_id={delivery_id} request_id={prepared.request_id} error={exc}"
                )
                continue

            context.log.info(
                "staging agent dispatch accepted: "
                f"delivery_id={delivery_id} request_id={prepared.request_id} folder={prepared.folder_name}"
            )
            _post_dispatch_ack(
                context,
                session,
                base_url=settings.base_url,
                timeout=timeout,
                delivery_id=delivery_id,
                status="accepted",
                request_id=prepared.request_id,
                message="dispatch manifest created and run requested",
            )
            yielded = True
            yield run_request

        if not yielded:
            yield SkipReason("staging agent pending items handled without new run request")
