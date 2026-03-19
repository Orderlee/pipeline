"""Helpers for staging spec-flow run tags and config resolution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from vlm_pipeline.resources.duckdb import DuckDBResource


def parse_requested_outputs(tags: Mapping[str, str] | None) -> list[str]:
    """Parse requested outputs from run tags for routed staging jobs."""
    if not tags:
        return []
    outputs_raw = tags.get("requested_outputs") or tags.get("outputs") or ""
    return [
        output.strip().lower()
        for output in outputs_raw.replace("_", ",").split(",")
        if output.strip()
    ]


def is_standard_spec_run(tags: Mapping[str, str] | None) -> bool:
    """True when the routed run was triggered for a labeling spec."""
    if not tags:
        return False
    return bool(str(tags.get("spec_id") or "").strip())


def resolve_and_persist_spec_config(db: DuckDBResource, spec_id: str) -> dict[str, Any]:
    """Resolve config for a spec at 3-5 entry point and persist the resolved config id."""
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


def load_persisted_spec_config(db: DuckDBResource, spec_id: str) -> dict[str, Any]:
    """Load config for downstream staged assets using persisted resolved_config_id."""
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
