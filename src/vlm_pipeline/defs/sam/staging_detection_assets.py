"""Legacy test-era SAM3 detection assets.

dispatch_stage_job에서는 raw_ingest 완료 후 실제 frame 추출이 끝난 다음에만
SAM3가 실행되도록 legacy test asset dependency를 분리한다.
"""

from __future__ import annotations

from dagster import Field, asset

from vlm_pipeline.defs.sam.detection_assets import _run_sam3_image_detection
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="staging_sam3_image_detection",
    deps=["clip_to_frame", "raw_video_to_frame"],
    description="Legacy test dispatch 라인: frame 추출 완료 후 SAM3.1 text-prompted bbox detection (YOLO 비교용)",
    group_name="sam3",
    config_schema={
        "limit": Field(int, default_value=200),
        "score_threshold": Field(float, default_value=0.0),
        "max_masks_per_prompt": Field(int, default_value=50),
    },
)
def staging_sam3_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    return _run_sam3_image_detection(context, db, minio)
