"""Environment-driven dispatch ingress sensor backed by agent polling API."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests
from dagster import SensorEvaluationContext, SkipReason, sensor

from vlm_pipeline.defs.dispatch.agent_sensor_common import (
    build_agent_ack_payload,
    build_agent_request_id,
    normalize_agent_dispatch_request,
    should_wait_for_dispatch_params,
)
from vlm_pipeline.defs.dispatch.service import (
    DispatchIngressRequest,
    process_dispatch_ingress_request,
    record_failed_dispatch_request,
)
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.runtime_settings import load_dispatch_agent_polling_settings


def _fetch_pending_dispatch_items(
    session: requests.Session,
    *,
    base_url: str,
    pending_path: str,
    limit: int,
    timeout: tuple[int, int],
) -> list[dict[str, Any]]:
    response = session.get(
        f"{base_url}{pending_path}",
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
    ack_path: str,
    timeout: tuple[int, int],
    delivery_id: str,
    status: str,
    request_id: str | None,
    message: str,
) -> None:
    try:
        response = session.post(
            f"{base_url}{ack_path}",
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
            "dispatch agent ack 전송 실패: "
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
        ack_path=settings.ack_path,
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


@sensor(
    name="dispatch_agent_sensor",
    description="environment-driven agent API polling -> dispatch_stage_job ingress",
    minimum_interval_seconds=load_dispatch_agent_polling_settings().interval_sec,
    required_resource_keys={"db"},
    job_name="dispatch_stage_job",
)
def dispatch_agent_sensor(context: SensorEvaluationContext):
    settings = load_dispatch_agent_polling_settings()
    if not settings.enabled:
        yield SkipReason("DISPATCH_AGENT_POLLING_ENABLED=false")
        return

    db_resource = getattr(context.resources, "db", None)
    if db_resource is None:
        yield SkipReason("db resource unavailable")
        return

    db_resource.ensure_runtime_schema()
    db_resource.ensure_dispatch_tracking_tables()

    config = PipelineConfig()
    timeout = (settings.connect_timeout_sec, settings.read_timeout_sec)

    with requests.Session() as session:
        try:
            items = _fetch_pending_dispatch_items(
                session,
                base_url=settings.base_url,
                pending_path=settings.pending_path,
                limit=settings.poll_limit,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            yield SkipReason(f"dispatch agent polling unavailable: {exc}")
            return
        except ValueError as exc:
            yield SkipReason(f"dispatch agent pending payload invalid: {exc}")
            return

        if not items:
            yield SkipReason("dispatch agent pending queue empty")
            return

        yielded = False

        for item in items:
            delivery_id = str(item.get("delivery_id") or "").strip()
            raw_request = item.get("request")
            if not delivery_id or not isinstance(raw_request, Mapping):
                context.log.warning(f"dispatch agent payload 무시: invalid item shape={item}")
                continue

            request_id: str | None = None

            try:
                canonical_request = normalize_agent_dispatch_request(raw_request, delivery_id=delivery_id)
                request_id = str(canonical_request.get("request_id") or "").strip() or None
            except ValueError as exc:
                fallback_request_id = build_agent_request_id(delivery_id)
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

            if should_wait_for_dispatch_params(canonical_request):
                folder_name = str(canonical_request.get("folder_name") or "").strip()
                context.log.info(
                    "dispatch agent waiting: "
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
                        fallback_request_id=request_id or build_agent_request_id(delivery_id),
                        duplicate_policy="accept_noop",
                        in_flight_policy="defer",
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                context.log.error(
                    "dispatch agent 처리 실패(ack 보류): "
                    f"delivery_id={delivery_id} request_id={request_id or ''} error={exc}"
                )
                continue

            if outcome.status == "deferred":
                prepared = outcome.prepared
                folder_name = prepared.folder_name if prepared is not None else ""
                context.log.info(
                    "dispatch agent folder in flight -> ack 보류: "
                    f"delivery_id={delivery_id} folder={folder_name}"
                )
                continue

            if outcome.status == "duplicate_noop":
                context.log.info(
                    "dispatch agent duplicate request_id 감지: "
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
                _ack_dispatch_item(
                    context,
                    session,
                    settings=settings,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="rejected",
                    request_id=outcome.request_id,
                    message=outcome.reason,
                )
                continue

            if outcome.run_request is not None:
                yielded = True
                yield outcome.run_request
                _ack_dispatch_item(
                    context,
                    session,
                    settings=settings,
                    timeout=timeout,
                    delivery_id=delivery_id,
                    status="accepted",
                    request_id=outcome.request_id,
                    message="run_request_created",
                )

        if not yielded:
            yield SkipReason("dispatch agent request handled without launching run")
