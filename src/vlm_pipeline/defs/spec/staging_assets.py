"""Legacy test-era spec flow assets — config_sync(version/삭제 반영), ingest_router(config 미조회), activate_labeling_spec.

명세: 라우터는 pending/ready 분기만, config는 3-5(clip_timestamp)에서 조회.
현재 기본 prod/test runtime에는 연결하지 않고, 과거 spec 호환 경로로만 남겨둔다.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path

from dagster import AssetKey, asset

from vlm_pipeline.resources.duckdb import DuckDBResource

VALID_LABELING_METHODS = frozenset(["timestamp", "captioning", "bbox"])
CONFIG_PARAMETERS_DIR = os.getenv("LABELING_CONFIG_DIR", "config/parameters")


def _normalize_labeling_method(method: list[str] | None) -> list[str]:
    if not method:
        return []
    return [m for m in method if str(m).strip().lower() in VALID_LABELING_METHODS]


def _canonicalize_config_json(config_json: object) -> str:
    return json.dumps(config_json, ensure_ascii=False, sort_keys=True)


@asset(
    name="config_sync",
    description="[Legacy] config/parameters/*.json → labeling_configs 동기화. version+1, 삭제 시 is_active=false.",
    group_name="spec",
    deps=[],
)
def config_sync(
    context,
    db: DuckDBResource,
) -> dict:
    """Legacy spec flow: 신규 INSERT, 변경 시 version+1, 삭제 시 is_active=false."""
    db.ensure_runtime_schema()
    config_dir = Path(CONFIG_PARAMETERS_DIR)
    if not config_dir.is_dir():
        context.log.warning(f"config_sync: 디렉터리 없음 {config_dir}. 스킵.")
        return {"synced": 0, "errors": ["config_dir_not_found"]}

    files = list(config_dir.glob("*.json"))
    if not files:
        context.log.warning("config_sync: JSON 파일 없음.")
        return {"synced": 0}

    config_ids = {f.stem for f in files}
    if "_fallback" not in config_ids:
        context.log.error("config_sync: _fallback.json 필수.")
        return {"synced": 0, "errors": ["_fallback_required"]}

    existing_configs = {
        cfg["config_id"]: cfg for cfg in db.list_labeling_configs(include_inactive=True)
    }
    synced = 0
    inserted = 0
    updated = 0
    reactivated = 0
    unchanged = 0
    deactivated = 0
    errors: list[str] = []

    for f in files:
        try:
            raw = f.read_text(encoding="utf-8")
            data = json.loads(raw)
            config_id = f.stem
            existing = existing_configs.get(config_id)
            if not existing:
                db.upsert_labeling_config(config_id, data, version=1, is_active=True)
                inserted += 1
                synced += 1
                continue

            same_content = _canonicalize_config_json(data) == _canonicalize_config_json(
                existing.get("config_json") or {}
            )
            version = int(existing.get("version") or 1)
            is_active = bool(existing.get("is_active"))

            if same_content:
                if not is_active:
                    db.upsert_labeling_config(config_id, data, version=version, is_active=True)
                    reactivated += 1
                else:
                    unchanged += 1
                synced += 1
                continue

            db.upsert_labeling_config(config_id, data, version=version + 1, is_active=True)
            updated += 1
            synced += 1
        except Exception as e:
            context.log.error(f"config_sync: {f.name} 실패: {e}")
            errors.append(f"{f.name}: {e}")

    for config_id, existing in existing_configs.items():
        if config_id in config_ids or not bool(existing.get("is_active")):
            continue
        db.set_labeling_config_active(config_id, False)
        deactivated += 1

    out = {
        "synced": synced,
        "inserted": inserted,
        "updated": updated,
        "reactivated": reactivated,
        "unchanged": unchanged,
        "deactivated": deactivated,
    }
    if errors:
        out["errors"] = errors
    return out


@asset(
    name="ingest_router",
    description="[Legacy Test] completed raw_files + labeling_specs 매칭 → pending_spec | ready_for_labeling (config 조회 없음)",
    group_name="spec",
    deps=[AssetKey("raw_ingest")],
)
def ingest_router(
    context,
    db: DuckDBResource,
) -> dict:
    """Legacy test flow: 라우터는 분기만. config는 clip_timestamp(3-5)에서 조회."""
    db.ensure_runtime_schema()
    if not getattr(db, "_table_exists", None):
        return {"routed": 0, "pending_spec": 0, "ready": 0, "failed": 0}

    candidates = db.list_completed_videos_for_spec_router(limit=1000)
    if not candidates:
        return {"routed": 0, "pending_spec": 0, "ready": 0, "failed": 0}

    by_unit: dict[str, list[dict]] = defaultdict(list)
    for c in candidates:
        by_unit[c["source_unit_name"] or "_unknown_"].append(c)

    routed = 0
    pending_spec = 0
    ready = 0

    for source_unit_name, rows in by_unit.items():
        if not source_unit_name or source_unit_name == "_unknown_":
            updates = [{"asset_id": r["asset_id"], "ingest_status": "pending_spec"} for r in rows]
            db.batch_update_spec_and_status(updates)
            pending_spec += len(rows)
            routed += len(rows)
            continue

        spec = db.get_spec_by_source_unit_name(source_unit_name)
        if not spec:
            updates = [{"asset_id": r["asset_id"], "ingest_status": "pending_spec"} for r in rows]
            db.batch_update_spec_and_status(updates)
            pending_spec += len(rows)
            routed += len(rows)
            continue

        method = spec.get("labeling_method") or []
        if not isinstance(method, list):
            method = []
        method = _normalize_labeling_method(method)
        classes = spec.get("classes") or []
        if not isinstance(classes, list):
            classes = []

        if not method:
            updates = [{"asset_id": r["asset_id"], "ingest_status": "pending_spec"} for r in rows]
            db.batch_update_spec_and_status(updates)
            pending_spec += len(rows)
            routed += len(rows)
            continue

        if "bbox" in method and not classes:
            updates = [{"asset_id": r["asset_id"], "ingest_status": "pending_spec"} for r in rows]
            db.batch_update_spec_and_status(updates)
            pending_spec += len(rows)
            routed += len(rows)
            continue

        updates = [
            {
                "asset_id": r["asset_id"],
                "ingest_status": "ready_for_labeling",
                "spec_id": spec["spec_id"],
            }
            for r in rows
        ]
        db.batch_update_spec_and_status(updates)
        ready += len(rows)
        routed += len(rows)

    return {"routed": routed, "pending_spec": pending_spec, "ready": ready, "failed": 0}


@asset(
    name="activate_labeling_spec",
    description="[Legacy Test] routed job 전체 성공 시 spec_status를 active로 전환",
    group_name="spec",
    deps=[AssetKey("bbox_labeling")],
)
def activate_labeling_spec(
    context,
    db: DuckDBResource,
) -> dict:
    """auto_labeling_routed_job 성공 완료 시 spec_status='active' 반영."""
    tags = context.run.tags if context.run else {}
    spec_id = str(tags.get("spec_id") or "").strip()
    if not spec_id:
        return {"updated": False, "skipped": True}

    spec = db.get_labeling_spec_by_id(spec_id)
    if not spec:
        context.log.warning(f"activate_labeling_spec: spec_id={spec_id} 없음")
        return {"updated": False, "skipped": True}

    db.update_spec_status(spec_id, "active", clear_last_error=True)
    context.log.info(f"activate_labeling_spec: spec_id={spec_id} -> active")
    return {"updated": True, "spec_id": spec_id}
