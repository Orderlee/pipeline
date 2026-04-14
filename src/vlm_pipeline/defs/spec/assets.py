"""Spec flow assets — labeling_spec_ingest, config_sync, ingest_router, pending_ingest.

Legacy spec runtime 전용. ingest_router는 raw_ingest 이후에 실행한다.
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


@asset(
    name="labeling_spec_ingest",
    description="외부 spec 수신 → labeling_specs upsert (legacy spec flow)",
    group_name="spec",
    deps=[],
)
def labeling_spec_ingest(
    context,
    db: DuckDBResource,
) -> dict:
    """수동 또는 외부 트리거로 spec JSON 수신 시 upsert. UI/테스트용 — 실제 수신은 Flex/Slack 등."""
    # 실제 구현에서는 run_config 또는 외부 페이로드에서 spec을 받음.
    # 여기서는 빈 결과 반환 (수동 materialize 시 config만 동기화 등).
    context.log.info("labeling_spec_ingest: spec 수신 없음(수동 materialize). spec은 외부에서 upsert.")
    return {"specs_upserted": 0}


@asset(
    name="config_sync",
    description="config/parameters/*.json → labeling_configs 동기화. _fallback.json 필수.",
    group_name="spec",
    deps=[],
)
def config_sync(
    context,
    db: DuckDBResource,
) -> dict:
    """Dagster UI 수동 materialize. config 디렉터리에서 JSON 로드 후 DB 반영."""
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

    synced = 0
    for f in files:
        try:
            raw = f.read_text(encoding="utf-8")
            data = json.loads(raw)
            db.upsert_labeling_config(f.stem, data, version=1, is_active=True)
            synced += 1
        except Exception as e:
            context.log.error(f"config_sync: {f.name} 실패: {e}")
    return {"synced": synced}


@asset(
    name="ingest_router",
    description="completed raw_files + labeling_specs 매칭 → pending_spec | ready_for_labeling",
    group_name="spec",
    deps=[AssetKey("raw_ingest")],
)
def ingest_router(
    context,
    db: DuckDBResource,
) -> dict:
    """source_unit_name 기준 spec 매칭, config resolve, raw_files.ingest_status 전이 (운영: config 조회 포함)."""
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
    failed = 0

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

        config_id, scope = db.resolve_config_for_requester(
            spec.get("requester_id"), spec.get("team_id")
        )
        if not config_id:
            db.update_spec_status(spec["spec_id"], "failed", last_error="config_not_found")
            failed += len(rows)
            continue

        db.update_spec_resolved_config(spec["spec_id"], config_id, scope or "fallback")
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

    return {"routed": routed, "pending_spec": pending_spec, "ready": ready, "failed": failed}


@asset(
    name="pending_ingest",
    description="ingest_status=pending_spec 집계",
    group_name="spec",
    deps=[AssetKey("ingest_router")],
)
def pending_ingest(
    context,
    db: DuckDBResource,
) -> dict:
    """pending_spec 건수·파일 수 출력."""
    with db.connect() as conn:
        if not db._table_exists(conn, "raw_files"):
            return {"pending_spec_count": 0, "pending_file_count": 0}
        cols = db._table_columns(conn, "raw_files")
        if "ingest_status" not in cols:
            return {"pending_spec_count": 0, "pending_file_count": 0}
        row = conn.execute(
            "SELECT COUNT(*) FROM raw_files WHERE ingest_status = 'pending_spec'"
        ).fetchone()
        file_count = int(row[0]) if row else 0
        # spec 단위 집계는 labeling_specs + source_unit_name 매칭으로 가능하나 단순화
        spec_row = conn.execute(
            """
            SELECT COUNT(DISTINCT COALESCE(source_unit_name, ''))
            FROM raw_files
            WHERE ingest_status = 'pending_spec' AND COALESCE(source_unit_name, '') <> ''
            """
        ).fetchone() if "source_unit_name" in cols else (0,)
        spec_count = int(spec_row[0]) if spec_row else 0
    out = {"pending_spec_count": spec_count, "pending_file_count": file_count}
    context.log.info(f"pending_ingest: {out}")
    return out
