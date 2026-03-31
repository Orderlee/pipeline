"""Staging manual import path for already-labeled datasets.

이 모듈의 artifact 처리 로직은 `artifact_import_support` 단일 구현으로 위임한다.
private helper 이름은 테스트/호환성을 위해 얇은 래퍼로 유지한다.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from dagster import Field, StringSource, asset

from vlm_pipeline.defs.ingest.assets import RawIngestState, _run_raw_ingest_pipeline
from vlm_pipeline.defs.label.artifact_import_support import (
    _import_bbox_json_files as _support_import_bbox_json_files,
    _import_image_caption_json_files as _support_import_image_caption_json_files,
    _scan_artifact_json_paths as _support_scan_artifact_json_paths,
    import_local_label_artifacts,
)
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


def _now() -> datetime:
    return datetime.now()


def _build_manifest_path(config: PipelineConfig, source_unit_name: str) -> Path:
    manifest_dir = Path(config.manifest_dir) / "prelabeled"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    return manifest_dir / f"prelabeled_import_{source_unit_name}_{_now():%Y%m%d_%H%M%S}.json"


def _write_prelabeled_manifest(
    config: PipelineConfig,
    *,
    source_unit_name: str,
    source_unit_dir: Path,
    request_json_path: str | None,
) -> Path:
    manifest_path = _build_manifest_path(config, source_unit_name)
    manifest = {
        "manifest_id": f"prelabeled_import_{source_unit_name}_{_now():%Y%m%d_%H%M%S}",
        "generated_at": _now().isoformat(),
        "source_dir": str(Path(config.incoming_dir)),
        "source_unit_type": "directory",
        "source_unit_path": str(source_unit_dir),
        "source_unit_name": source_unit_name,
        "source_unit_total_file_count": 0,
        "file_count": 0,
        "transfer_tool": "prelabeled_import_job",
        "archive_requested": False,
        "request_json_path": request_json_path,
        "files": [],
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def _scan_artifact_json_paths(
    *,
    incoming_dir: Path,
    source_unit_dir: Path,
    dir_names: tuple[str, ...],
    scan_global_dirs: bool,
    scan_local_dirs: bool,
) -> list[Path]:
    return _support_scan_artifact_json_paths(
        incoming_dir=incoming_dir,
        source_unit_dir=source_unit_dir,
        dir_names=dir_names,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
    )


def _import_bbox_json_files(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    config: PipelineConfig,
    source_unit_name: str,
    source_unit_dir: Path,
    json_paths: list[Path],
    failures: list[dict[str, Any]],
):
    del config
    return _support_import_bbox_json_files(
        context,
        db,
        minio,
        source_unit_name=source_unit_name,
        source_unit_dir=source_unit_dir,
        json_paths=json_paths,
        failures=failures,
    )


def _import_image_caption_json_files(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    config: PipelineConfig,
    source_unit_name: str,
    source_unit_dir: Path,
    json_paths: list[Path],
    failures: list[dict[str, Any]],
):
    del config
    return _support_import_image_caption_json_files(
        context,
        db,
        minio,
        source_unit_name=source_unit_name,
        source_unit_dir=source_unit_dir,
        json_paths=json_paths,
        failures=failures,
    )


@asset(
    name="prelabeled_import",
    description="Staging 전용: 이미 라벨링 완료된 데이터(raw + labels/bbox/image_caption)를 수동 적재",
    group_name="label",
    config_schema={
        "source_unit_name": Field(StringSource),
        "request_json_path": Field(StringSource, is_required=False, default_value=""),
        "scan_global_dirs": Field(bool, default_value=True),
        "scan_local_dirs": Field(bool, default_value=True),
    },
)
def prelabeled_import(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict[str, Any]:
    config = PipelineConfig()
    source_unit_name = str(context.op_config["source_unit_name"]).strip()
    request_json_path = str(context.op_config.get("request_json_path") or "").strip() or None
    scan_global_dirs = bool(context.op_config.get("scan_global_dirs", True))
    scan_local_dirs = bool(context.op_config.get("scan_local_dirs", True))

    if not source_unit_name:
        raise ValueError("source_unit_name is required")

    source_unit_dir = Path(config.incoming_dir) / source_unit_name
    if not source_unit_dir.is_dir():
        raise FileNotFoundError(f"source_unit_not_found:{source_unit_dir}")

    db.ensure_runtime_schema()
    manifest_path = _write_prelabeled_manifest(
        config,
        source_unit_name=source_unit_name,
        source_unit_dir=source_unit_dir,
        request_json_path=request_json_path,
    )
    ingest_summary = _run_raw_ingest_pipeline(
        context,
        db,
        minio,
        config=config,
        state=RawIngestState(
            manifest_path=str(manifest_path),
            request_id=None,
            folder_name=source_unit_name,
            archive_only=False,
        ),
    )

    artifact_summary = import_local_label_artifacts(
        context,
        db,
        minio,
        config=config,
        source_unit_name=source_unit_name,
        source_unit_dir=source_unit_dir,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
        failure_log_prefix="prelabeled_import",
        update_timestamp_status=True,
    )

    summary = {
        "source_unit_name": source_unit_name,
        "request_json_path": request_json_path,
        "raw_ingest": ingest_summary,
        **artifact_summary,
    }
    context.add_output_metadata(
        {
            "source_unit_name": source_unit_name,
            "request_json_path": request_json_path or "",
            "raw_ingest_success": int(ingest_summary.get("success", 0)),
            "raw_ingest_failed": int(ingest_summary.get("failed", 0)),
            "raw_ingest_skipped": int(ingest_summary.get("skipped", 0)),
            "event_labels_inserted": int(artifact_summary.get("event_labels_inserted", 0)),
            "bbox_inserted": int(artifact_summary.get("bbox_inserted", 0)),
            "image_captions_inserted": int(artifact_summary.get("image_captions_inserted", 0)),
            "failure_count": int(artifact_summary.get("failure_count", 0)),
            **(
                {"failure_log_path": str(artifact_summary["failure_log_path"])}
                if artifact_summary.get("failure_log_path")
                else {}
            ),
        }
    )
    context.log.info(f"PRELABELED IMPORT 완료: {summary}")
    return summary

