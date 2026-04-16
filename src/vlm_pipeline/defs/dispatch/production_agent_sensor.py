"""Production optional dispatch ingress sensor backed by piaspace-agent polling API.

기본 운영 ingress는 file-based dispatch_sensor를 유지한다.
이 sensor는 PROD_AGENT_POLLING_ENABLED=true일 때만 활성 동작한다.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests
from dagster import SensorDefinition, SensorEvaluationContext, SkipReason, sensor

from vlm_pipeline.defs.dispatch.service import (
    DispatchIngressRequest,
    process_dispatch_ingress_request,
    record_failed_dispatch_request,
)
from vlm_pipeline.lib.env_utils import is_duckdb_lock_conflict
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.runtime_settings import load_production_agent_polling_settings

_PROD_PENDING_PATH = "/api/production/dispatch/pending"
_PROD_ACK_PATH = "/api/production/dispatch/ack"


# ---------------------------------------------------------------------------
# Agent payload helpers (module-private — originally in agent_sensor_common.py)
# ---------------------------------------------------------------------------


def _build_agent_request_id(delivery_id: str) -> str:
    rendered = sanitize_path_component(delivery_id) or "unknown"
    return f"agent_{rendered}"


def _payload_field_has_value(value: Any) -> bool:
    if isinstance(value, list):
        return any(str(item or "").strip() for item in value)
    return bool(str(value or "").strip())


def _should_wait_for_dispatch_params(request_payload: Mapping[str, Any]) -> bool:
    """Return True when dispatch-driving metadata is entirely empty."""
    watched_fields = (
        request_payload.get("labeling_method"),
        request_payload.get("outputs"),
        request_payload.get("run_mode"),
        request_payload.get("categories"),
        request_payload.get("classes"),
    )
    return not any(_payload_field_has_value(value) for value in watched_fields)


def _normalize_agent_dispatch_request(
    request_payload: Mapping[str, Any],
    *,
    delivery_id: str,
) -> dict[str, Any]:
    """Convert agent payload into canonical dispatch request payload."""
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


def _build_agent_ack_payload(
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
        f"{base_url}{_PROD_PENDING_PATH}",
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
            f"{base_url}{_PROD_ACK_PATH}",
            json=_build_agent_ack_payload(
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
            "production agent ack 전송 실패: "
            f"delivery_id={delivery_id} status={status} request_id={request_id or ''} error={exc}"
        )


def _ack_dispatch_item(
    context,
    session: requests.Session,
    *,
    settings,
    timeout: tuple[int, int],
    delivery_id: str,
    status: str,
    request_id: str | None,
    message: str,
) -> None:
    _post_dispatch_ack(
        context,
        session,
        base_url=settings.base_url,
        timeout=timeout,
        delivery_id=delivery_id,
        status=status,
        request_id=request_id,
        message=message,
    )


def _reject_dispatch_item(
    *,
    db_resource,
    canonical_request: Mapping[str, Any],
    request_id: str,
    reason: str,
    context,
    session: requests.Session,
    settings,
    timeout: tuple[int, int],
    delivery_id: str,
) -> None:
    record_failed_dispatch_request(
        db_resource,
        request_id,
        canonical_request,
        reason,
    )
    _ack_dispatch_item(
        context,
        session,
        settings=settings,
        timeout=timeout,
        delivery_id=delivery_id,
        status="rejected",
        request_id=request_id,
        message=reason,
    )


def build_production_agent_dispatch_sensor(*, jobs) -> SensorDefinition:
    """Builder — definitions.py에서 job 객체를 주입받아 SensorDefinition 생성."""
    return sensor(
        name="production_agent_dispatch_sensor",
        description="production optional agent API polling -> dispatch ingress (dispatch_stage_job / ingest_job)",
        minimum_interval_seconds=load_production_agent_polling_settings().interval_sec,
        required_resource_keys={"db"},
        jobs=jobs,
    )(_production_agent_dispatch_sensor_fn)


def _production_agent_dispatch_sensor_fn(context: SensorEvaluationContext):
    settings = load_production_agent_polling_settings()
    if not settings.enabled:
        yield SkipReason("PROD_AGENT_POLLING_ENABLED=false")
        return

    db_resource = getattr(context.resources, "db", None)
    if db_resource is None:
        yield SkipReason("db resource unavailable")
        return

    try:
        db_resource.ensure_runtime_schema()
        db_resource.ensure_dispatch_tracking_tables()
    except Exception as exc:
        if is_duckdb_lock_conflict(exc):
            yield SkipReason(f"DuckDB lock 충돌 — 다음 tick에서 재시도: {exc}")
            return
        raise

    config = PipelineConfig()
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
            yield SkipReason(f"production agent polling unavailable: {exc}")
            return
        except ValueError as exc:
            yield SkipReason(f"production agent pending payload invalid: {exc}")
            return

        if not items:
            yield SkipReason("production agent pending queue empty")
            return

        yielded = False

        for item in items:
            delivery_id = str(item.get("delivery_id") or "").strip()
            raw_request = item.get("request")
            if not delivery_id or not isinstance(raw_request, Mapping):
                context.log.warning(f"production agent payload 무시: invalid item shape={item}")
                continue

            request_id: str | None = None

            try:
                canonical_request = _normalize_agent_dispatch_request(raw_request, delivery_id=delivery_id)
                request_id = str(canonical_request.get("request_id") or "").strip() or None
            except ValueError as exc:
                fallback_request_id = _build_agent_request_id(delivery_id)
                _reject_dispatch_item(
                    db_resource=db_resource,
                    canonical_request={
                        "request_id": fallback_request_id,
                        "folder_name": raw_request.get("source_unit_name") if isinstance(raw_request, Mapping) else None,
                        "image_profile": raw_request.get("image_profile") if isinstance(raw_request, Mapping) else None,
                    },
                    request_id=fallback_request_id,
                    reason=str(exc),
                    context=context,
                    session=session,
                    settings=settings,
                    timeout=timeout,
                    delivery_id=delivery_id,
                )
                continue

            if _should_wait_for_dispatch_params(canonical_request):
                folder_name = str(canonical_request.get("folder_name") or "").strip()
                context.log.info(
                    "production agent dispatch waiting: "
                    f"delivery_id={delivery_id} request_id={request_id} "
                    f"folder={folder_name} reason=waiting_for_dispatch_params"
                )
                _ack_dispatch_item(
                    context,
                    session,
                    settings=settings,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="accepted",
                    request_id=request_id,
                    message="waiting_for_dispatch_params",
                )
                continue

            try:
                outcome = process_dispatch_ingress_request(
                    context,
                    db_resource=db_resource,
                    config=config,
                    ingress_request=DispatchIngressRequest(
                        payload=canonical_request,
                        fallback_request_id=request_id or _build_agent_request_id(delivery_id),
                        duplicate_policy="accept_noop",
                        in_flight_policy="defer",
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                context.log.error(
                    "production agent dispatch 처리 실패(ack 보류): "
                    f"delivery_id={delivery_id} request_id={request_id or ''} error={exc}"
                )
                continue

            if outcome.status == "deferred":
                prepared = outcome.prepared
                folder_name = prepared.folder_name if prepared is not None else ""
                context.log.info(
                    "production agent folder in flight -> ack 보류: "
                    f"delivery_id={delivery_id} folder={folder_name}"
                )
                continue

            if outcome.status == "duplicate_noop":
                context.log.info(
                    "production agent duplicate request_id 감지: "
                    f"delivery_id={delivery_id} request_id={outcome.request_id}"
                )
                _ack_dispatch_item(
                    context,
                    session,
                    settings=settings,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="accepted",
                    request_id=outcome.request_id,
                    message=outcome.reason,
                )
                continue

            if outcome.status == "rejected":
                _reject_dispatch_item(
                    db_resource=db_resource,
                    canonical_request=canonical_request,
                    request_id=outcome.request_id,
                    reason=outcome.reason,
                    context=context,
                    session=session,
                    settings=settings,
                    timeout=timeout,
                    delivery_id=delivery_id,
                )
                continue

            if outcome.status != "run_request" or outcome.run_request is None or outcome.prepared is None:
                context.log.error(
                    "production agent dispatch unexpected outcome(ack 보류): "
                    f"delivery_id={delivery_id} status={outcome.status} request_id={outcome.request_id}"
                )
                continue

            prepared = outcome.prepared
            context.log.info(
                "production agent dispatch accepted: "
                f"delivery_id={delivery_id} request_id={prepared.request_id} folder={prepared.folder_name}"
            )
            _ack_dispatch_item(
                context,
                session,
                settings=settings,
                timeout=timeout,
                delivery_id=delivery_id,
                status="accepted",
                request_id=prepared.request_id,
                message="dispatch manifest created and run requested",
            )
            yielded = True
            yield outcome.run_request

        if not yielded:
            yield SkipReason("production agent pending items handled without new run request")
