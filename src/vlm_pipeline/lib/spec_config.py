"""Helpers for legacy spec-flow run tags and spec config resolution.

Tag-parsing utilities (pure) and DB-dependent config resolution helpers
are co-located here since the DB functions only accept ``db: Any``
without importing from resources/ or defs/.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from vlm_pipeline.lib.env_utils import (
    is_dispatch_yolo_only_requested,
    parse_outputs_raw,
)


# ---------------------------------------------------------------------------
# DB-dependent spec config resolution
# ---------------------------------------------------------------------------


def resolve_and_persist_spec_config(db: Any, spec_id: str) -> dict[str, Any]:
    """Resolve config for a spec at entry point and persist the resolved config id."""
    spec = db.get_labeling_spec_by_id(spec_id)
    if not spec:
        raise RuntimeError(f"spec_not_found:{spec_id}")

    config_id, scope = db.resolve_config_for_requester(
        spec.get("requester_id"),
        spec.get("team_id"),
    )
    if not config_id:
        raise RuntimeError(
            f"config_not_found:spec_id={spec_id}:requester_id={spec.get('requester_id')}:"
            f"team_id={spec.get('team_id')}"
        )

    config = db.get_labeling_config(config_id)
    if not config or not config.get("is_active"):
        raise RuntimeError(f"config_payload_not_found:spec_id={spec_id}:config_id={config_id}")

    resolved_scope = str(scope or "fallback")
    db.update_spec_resolved_config(spec_id, config_id, resolved_scope)
    return {
        "spec": spec,
        "resolved_config_id": config_id,
        "resolved_config_scope": resolved_scope,
        "config_json": config.get("config_json") or {},
    }


def load_persisted_spec_config(db: Any, spec_id: str) -> dict[str, Any]:
    """Load config for downstream legacy spec assets using persisted resolved_config_id."""
    spec = db.get_labeling_spec_by_id(spec_id)
    if not spec:
        raise RuntimeError(f"spec_not_found:{spec_id}")

    resolved_config_id = str(spec.get("resolved_config_id") or "").strip()
    if not resolved_config_id:
        raise RuntimeError(f"resolved_config_id_missing:{spec_id}")

    config = db.get_labeling_config(resolved_config_id)
    if not config or not config.get("is_active"):
        raise RuntimeError(
            f"resolved_config_payload_not_found:spec_id={spec_id}:config_id={resolved_config_id}"
        )

    return {
        "spec": spec,
        "resolved_config_id": resolved_config_id,
        "resolved_config_scope": str(spec.get("resolved_config_scope") or ""),
        "config_json": config.get("config_json") or {},
    }


def is_unscoped_mvp_autolabel_run(tags: Mapping[str, str] | None) -> bool:
    """spec·dispatch(folder/outputs/run_mode) 태그 없이 MVP 일직선 job으로 실행되는 경우."""
    if not tags:
        return True
    if str(tags.get("spec_id") or "").strip():
        return False
    if str(tags.get("dispatch_archive_only") or "").lower() in ("1", "true", "yes"):
        return False
    if is_dispatch_yolo_only_requested(tags):
        return False
    if str(tags.get("folder_name") or "").strip():
        return False
    outputs_raw = (
        tags.get("requested_outputs")
        or tags.get("outputs")
        or tags.get("labeling_method")
        or ""
    )
    if parse_outputs_raw(outputs_raw):
        return False
    if str(tags.get("run_mode") or "").strip():
        return False
    return True


def parse_requested_outputs(tags: Mapping[str, str] | None) -> list[str]:
    """Parse requested outputs from run tags for routed legacy spec jobs."""
    if not tags:
        return []
    outputs_raw = (
        tags.get("requested_outputs")
        or tags.get("outputs")
        or tags.get("labeling_method")
        or ""
    )
    return parse_outputs_raw(outputs_raw)


def is_standard_spec_run(tags: Mapping[str, str] | None) -> bool:
    """True when the routed run was triggered for a labeling spec."""
    if not tags:
        return False
    return bool(str(tags.get("spec_id") or "").strip())
