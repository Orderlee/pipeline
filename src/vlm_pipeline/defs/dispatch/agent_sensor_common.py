from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from vlm_pipeline.lib.sanitizer import sanitize_path_component


def build_agent_request_id(delivery_id: str) -> str:
    rendered = sanitize_path_component(delivery_id) or "unknown"
    return f"agent_{rendered}"


def payload_field_has_value(value: Any) -> bool:
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
    return not any(payload_field_has_value(value) for value in watched_fields)


def normalize_agent_dispatch_request(
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
        request_id = build_agent_request_id(delivery_id)

    normalized["request_id"] = request_id
    normalized["folder_name"] = source_unit_name
    if not str(normalized.get("image_profile") or "").strip():
        normalized["image_profile"] = "current"
    if not str(normalized.get("requested_by") or "").strip():
        normalized["requested_by"] = " -agent"
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
