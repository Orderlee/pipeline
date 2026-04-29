"""Phase 2b: upload_archived asset — archive 경로의 파일을 MinIO 로 업로드.

`archive_dispatch_sensor` 가 만든 manifest 를 받아 (`from_archived=True`):
- archive_path 의 파일을 vlm-raw 로 업로드
- raw_files.ingest_status: 'archived' → 'completed'

별도 register/dedup 없음 (이미 raw_files row 있음). 라벨링 단계는
follow-up PR 에서 upload_label_job selection 에 통합 예정.
"""

from __future__ import annotations

import json
from pathlib import Path

from dagster import asset

from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .ops_upload_archived import upload_archived_files


@asset(
    name="upload_archived",
    description=(
        "archive 경로의 파일을 MinIO(vlm-raw) 로 업로드 + raw_files 상태 "
        "'archived' → 'completed'. archive_dispatch_sensor 가 트리거."
    ),
    group_name="ingest",
)
def upload_archived(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """archive_dispatch manifest 기반 MinIO 업로드.

    Sensor 가 RunRequest tags 로 `manifest_path` 를 전달.
    """
    manifest_path = str(context.run.tags.get("manifest_path") or "").strip()
    request_id = str(context.run.tags.get("dispatch_request_id") or "").strip()
    folder_name = str(context.run.tags.get("folder_name") or "").strip()

    context.log.info(
        f"upload_archived entered: run_id={context.run.run_id}, request_id={request_id}, "
        f"folder={folder_name}, manifest_path={manifest_path}"
    )

    if not manifest_path:
        context.log.warning("upload_archived: manifest_path tag missing — skip")
        return {"skipped": True, "reason": "no_manifest_path"}

    try:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        context.log.error(f"upload_archived: manifest 로드 실패 {manifest_path}: {exc}")
        return {"failed": True, "reason": f"manifest_load_failed:{exc}"}

    if manifest.get("from_archived") is not True:
        context.log.info("upload_archived: from_archived=False — skip")
        return {"skipped": True, "reason": "not_from_archived"}

    summary = upload_archived_files(context, db, minio, manifest)
    context.add_output_metadata(summary)
    return summary
