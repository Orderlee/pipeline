"""Spec config resolution — DB-dependent helpers for labeling spec config lookup.

Separated from lib/spec_config (pure tag-parsing) to respect the layer boundary:
lib/ must not depend on resources/.
"""

from __future__ import annotations

from typing import Any


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
