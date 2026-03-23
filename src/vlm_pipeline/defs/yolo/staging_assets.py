"""Staging 전용 YOLO assets.

dispatch_stage_job에서는 raw_ingest 완료 후 실제 frame 추출이 끝난 다음에만
YOLO가 실행되도록 staging 전용 asset dependency를 분리한다.
"""

from __future__ import annotations

from dagster import Field, asset

from vlm_pipeline.defs.yolo.assets import _run_yolo_image_detection
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="yolo_image_detection",
    deps=["clip_to_frame_routed", "raw_video_to_frame"],
    description="Staging dispatch: frame 추출 완료 후 YOLO-World-L object detection 실행",
    group_name="yolo",
    config_schema={
        "limit": Field(int, default_value=500),
        "confidence_threshold": Field(float, default_value=0.25),
        "iou_threshold": Field(float, default_value=0.45),
        "batch_size": Field(int, default_value=4),
    },
)
def staging_yolo_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    return _run_yolo_image_detection(context, db, minio)
