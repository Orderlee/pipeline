"""LABEL @asset — 수동 라벨 JSON 임포트 (기존 clip_timestamp 로직 보존)."""

from __future__ import annotations

from pathlib import Path

from dagster import Field, asset

from vlm_pipeline.defs.label.import_support import (
    collect_event_label_json_paths,
    import_event_label_files,
    summarize_event_label_import,
)
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="manual_label_import",
    description="수동 라벨 JSON 임포트 — incoming/{auto_labels,reviewed_labels,labels,annotations}",
    group_name="label",
    config_schema={"limit": Field(int, default_value=5000)},
)
def manual_label_import(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """로컬 JSON 파일을 vlm-labels + labels 테이블에 등록."""
    config = PipelineConfig()
    json_paths = collect_event_label_json_paths(Path(config.incoming_dir))

    result = import_event_label_files(
        context,
        db,
        minio,
        json_paths,
    )
    summary = summarize_event_label_import(result)
    context.add_output_metadata(summary)
    context.log.info(f"MANUAL LABEL IMPORT 완료: {summary}")
    return summary
