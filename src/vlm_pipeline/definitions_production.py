"""Production Definitions 조립 요소.

`definitions.py`가 호출하는 단일 production canonical 조립 모듈.
어셈블리(asset/job/schedule/sensor builder) 함수들이 모여 있다.
"""

from __future__ import annotations

from dagster import EnvVar, ScheduleDefinition, define_asset_job

from vlm_pipeline.defs.build.assets import build_dataset
from vlm_pipeline.defs.build.classification import build_classification
from vlm_pipeline.defs.build.sensor import build_dataset_on_finalize_sensor
from vlm_pipeline.defs.dispatch.production_agent_sensor import build_production_agent_dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor import build_dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor_run_status import (
    dispatch_run_canceled_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_success_sensor,
)
from vlm_pipeline.defs.gcp.assets import DEFAULT_GCP_BUCKETS, gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.ingest.sensor import (
    auto_bootstrap_manifest_sensor,
    incoming_manifest_sensor,
    nas_health_sensor,
    stuck_run_guard_sensor,
)
from vlm_pipeline.defs.label.assets import classification_video, clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.ls.sensor import (
    ls_presign_renew_job,
    ls_presign_renew_schedule,
    ls_task_create_job,
    ls_task_create_sensor,
)
from vlm_pipeline.defs.process.assets import clip_captioning, clip_to_frame, raw_video_to_frame
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sam.detection_assets import (
    dispatch_sam3_image_detection,
    sam3_image_detection,
)
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.lib.env_utils import (
    DUCKDB_LABEL_WRITER_TAG,
    DUCKDB_LEGACY_WRITER_TAG,
    DUCKDB_RAW_WRITER_TAG,
    DUCKDB_SAM3_WRITER_TAG,
    DUCKDB_YOLO_WRITER_TAG,
    build_duckdb_writer_tags,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

CLIP_AUTO_LABEL_ASSETS = (
    clip_timestamp,
    clip_captioning,
    clip_to_frame,
)

# dispatch_stage_job에서는 clip_to_frame 제외 — Gemini 초벌(JSON) 까지만 실행.
# clip 분할은 LS 검수 확정(webhook → /sync-approve) 후 post_review_clip_job 에서 수행.
# clip_captioning은 JSON → labels 테이블 upsert만 하므로 유지.
DISPATCH_STAGE_AUTO_LABEL_ASSETS = (
    clip_timestamp,
    clip_captioning,
)

def build_dispatch_stage_selection(*, enable_yolo_detection: bool) -> list[object]:
    """Dispatch 단계 asset selection. YOLO flag=False 면 YOLO asset 미포함 + import 안 함."""
    selection: list[object] = [
        raw_ingest,
        *DISPATCH_STAGE_AUTO_LABEL_ASSETS,
        classification_video,
        raw_video_to_frame,
        dispatch_sam3_image_detection,
    ]
    if enable_yolo_detection:
        from vlm_pipeline.defs.yolo.assets import dispatch_yolo_image_detection

        selection.insert(-1, dispatch_yolo_image_detection)
    return selection

# 사람 검수 확정 후 실행되는 clip 생성 단계.
POST_REVIEW_CLIP_ASSETS = [clip_to_frame]

COMMON_INGEST_SENSORS = (
    incoming_manifest_sensor,
    auto_bootstrap_manifest_sensor,
    stuck_run_guard_sensor,
    nas_health_sensor,
)

COMMON_DISPATCH_STATUS_SENSORS = (
    dispatch_run_success_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_canceled_sensor,
)


def build_common_resources() -> dict[str, object]:
    return {
        "db": DuckDBResource(db_path=EnvVar("DATAOPS_DUCKDB_PATH")),
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
        ),
    }


def build_asset_job(
    *,
    name: str,
    selection: list[object],
    description: str,
    writer_tag: str | None = None,
    tags: dict[str, str] | None = None,
):
    """Asset job builder — DuckDB writer 태그를 일관 적용."""
    if writer_tag is not None:
        merged_tags = build_duckdb_writer_tags(writer_tag)
        if tags:
            merged_tags = {**merged_tags, **tags}
    else:
        merged_tags = tags
    return define_asset_job(
        name,
        selection=selection,
        tags=merged_tags,
        description=description,
    )


def build_gcs_download_schedule(job) -> ScheduleDefinition:
    return ScheduleDefinition(
        name="gcs_download_schedule",
        job=job,
        cron_schedule="0 4 * * *",
        execution_timezone="Asia/Seoul",
        run_config={
            "ops": {
                "pipeline__incoming_nas": {
                    "config": {
                        "mode": "date-folders",
                        "download_dir": "/nas/incoming/gcp",
                        "backend": "gcloud",
                        "skip_existing": True,
                        "dry_run": False,
                        "buckets": DEFAULT_GCP_BUCKETS,
                        "bucket_subdir": True,
                    }
                }
            }
        },
    )


def build_motherduck_daily_schedule(job) -> ScheduleDefinition:
    return ScheduleDefinition(
        name="motherduck_daily_schedule",
        job=job,
        cron_schedule="0 5 * * *",
        execution_timezone="Asia/Seoul",
        run_config={
            "ops": {
                "pipeline__motherduck_sync": {
                    "config": {
                        "enabled": True,
                        "tables": [],
                    }
                }
            }
        },
    )


def build_production_assets(
    *,
    enable_manual_label_import: bool,
    enable_yolo_detection: bool,
    enable_sam3_detection: bool = False,
) -> list[object]:
    assets: list[object] = [
        raw_ingest,
        gcs_download_to_incoming,
        *CLIP_AUTO_LABEL_ASSETS,
        raw_video_to_frame,
        build_dataset,
        build_classification,
        motherduck_sync,
        classification_video,
        dispatch_sam3_image_detection,
        sam3_shadow_compare,
    ]
    if enable_manual_label_import:
        assets.append(manual_label_import)
    if enable_yolo_detection:
        from vlm_pipeline.defs.yolo.assets import dispatch_yolo_image_detection, yolo_image_detection

        assets.extend([dispatch_yolo_image_detection, yolo_image_detection])
    if enable_sam3_detection:
        assets.append(sam3_image_detection)
    return assets


def build_production_sensors(
    motherduck_table_sensors: list[object] | tuple[object, ...],
    *,
    dispatch_target_jobs: list[object],
) -> list[object]:
    return [
        *COMMON_INGEST_SENSORS,
        build_dispatch_sensor(jobs=dispatch_target_jobs),
        build_production_agent_dispatch_sensor(jobs=dispatch_target_jobs),
        *COMMON_DISPATCH_STATUS_SENSORS,
        *motherduck_table_sensors,
        ls_task_create_sensor,
        build_dataset_on_finalize_sensor,
    ]
