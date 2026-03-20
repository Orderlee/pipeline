"""Dagster Definitions — Staging 전용 진입점.

Production definitions.py와 완전 분리된 staging 파이프라인.
IS_STAGING 분기 없이 staging 전용 asset/job/sensor만 등록.

파이프라인 흐름 (staging):
  incoming → dispatch_sensor → raw_ingest
    → spec_resolve_sensor
    → clip_timestamp_routed → clip_captioning_routed → clip_to_frame_routed
    → bbox_labeling (YOLO) → activate_labeling_spec

운영과 동일한 NAS incoming 보조:
  `incoming_manifest_sensor` + `auto_bootstrap_manifest_sensor` (ingest_job 연계).
  NAS 지연 시 `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES` 등은 운영과 동일 env로 조정.

프레임 추출 시점은 운영과 동일하게 `vlm_pipeline.lib.video_frames.plan_frame_timestamps` 를 사용한다
(`raw_video_to_frame` 등).
"""

from __future__ import annotations

from dagster import Definitions, EnvVar, define_asset_job

from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.ingest.sensor import (
    auto_bootstrap_manifest_sensor,
    incoming_manifest_sensor,
    stuck_run_guard_sensor,
)
from vlm_pipeline.defs.label.assets import clip_timestamp_routed
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.dispatch.sensor import dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor_incoming_mover import incoming_to_pending_sensor
from vlm_pipeline.defs.process.assets import (
    clip_captioning_routed,
    clip_to_frame_routed,
    raw_video_to_frame,
)
from vlm_pipeline.defs.spec.assets import labeling_spec_ingest, pending_ingest
from vlm_pipeline.defs.spec.staging_assets import (
    activate_labeling_spec,
    config_sync,
    ingest_router,
)
from vlm_pipeline.defs.spec.staging_sensor import spec_resolve_sensor
from vlm_pipeline.defs.yolo.assets import bbox_labeling, yolo_image_detection
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

# ── Assets ──

assets = [
    raw_ingest,
    gcs_download_to_incoming,
    raw_video_to_frame,
    manual_label_import,
    yolo_image_detection,
    labeling_spec_ingest,
    config_sync,
    ingest_router,
    pending_ingest,
    activate_labeling_spec,
    clip_timestamp_routed,
    clip_captioning_routed,
    clip_to_frame_routed,
    bbox_labeling,
]

# ── Jobs ──

ingest_job = define_asset_job(
    "ingest_job",
    selection=[raw_ingest],
    tags={"duckdb_writer": "true"},
    description="원본 미디어 수집 + inline 중복 검출",
)

gcs_download_job = define_asset_job(
    "gcs_download_job",
    selection=[gcs_download_to_incoming],
    tags={"duckdb_writer": "true"},
    description="GCS 외부 데이터 수집",
)

dispatch_stage_job = define_asset_job(
    "dispatch_stage_job",
    selection=[
        raw_ingest,
        clip_timestamp_routed,
        clip_captioning_routed,
        clip_to_frame_routed,
        raw_video_to_frame,
        yolo_image_detection,
    ],
    tags={"duckdb_writer": "true"},
    description="Staging dispatch — run_mode에 따라 처리 분기",
)

auto_labeling_routed_job = define_asset_job(
    "auto_labeling_routed_job",
    selection=[
        clip_timestamp_routed,
        clip_captioning_routed,
        clip_to_frame_routed,
        bbox_labeling,
        activate_labeling_spec,
    ],
    tags={"duckdb_writer": "true"},
    description="Staging spec: spec_resolve_sensor에서 트리거, requested_outputs에 따라 단계 실행",
)

yolo_detection_job = define_asset_job(
    "yolo_detection_job",
    selection=[yolo_image_detection],
    tags={"duckdb_writer": "true"},
    description="YOLO-World-L object detection (processed_clip_frame → image_labels)",
)

manual_label_import_job = define_asset_job(
    "manual_label_import_job",
    selection=[manual_label_import],
    tags={"duckdb_writer": "true"},
    description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
)

jobs = [
    ingest_job,
    gcs_download_job,
    dispatch_stage_job,
    auto_labeling_routed_job,
    yolo_detection_job,
    manual_label_import_job,
]

# ── Sensors ──

sensors = [
    stuck_run_guard_sensor,
    incoming_manifest_sensor,
    auto_bootstrap_manifest_sensor,
    incoming_to_pending_sensor,
    dispatch_sensor,
    spec_resolve_sensor,
]

# ── Definitions ──

defs = Definitions(
    assets=assets,
    jobs=jobs,
    sensors=sensors,
    resources={
        "db": DuckDBResource(db_path=EnvVar("DATAOPS_DUCKDB_PATH")),
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
        ),
    },
)
