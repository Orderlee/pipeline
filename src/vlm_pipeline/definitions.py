"""Dagster Definitions — Production 전용 진입점.

Layer 5: 모든 assets + resources + sensors를 import하여 조립.
docker/app/dagster_defs.py의 MVP 파이프라인과 동일 구조.

파이프라인 흐름 (일직선):
  incoming_nas → raw_ingest(DEDUP 내장)
    → [auto_labeling_sensor] clip_timestamp → clip_captioning → clip_to_frame → build_dataset
    → motherduck_sync

data_pipeline_job 제거됨 — mvp_stage_job으로 통합.

`clip_to_frame` 프레임 시점은 `vlm_pipeline.lib.video_frames.plan_frame_timestamps` (스테이징과 공유).

NAS 지연 시 `auto_bootstrap_manifest_sensor`: `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES`(권장 20),
`DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`(권장 300) — 스테이징과 동일 env 키.
"""

from __future__ import annotations

from dagster import Definitions, EnvVar, ScheduleDefinition, define_asset_job

from vlm_pipeline.defs.build.assets import build_dataset
from vlm_pipeline.defs.gcp.assets import DEFAULT_GCP_BUCKETS, gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.ingest.sensor import (
    auto_bootstrap_manifest_sensor,
    incoming_manifest_sensor,
    stuck_run_guard_sensor,
)
from vlm_pipeline.defs.label.assets import clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.label.sensor import auto_labeling_sensor
from vlm_pipeline.defs.process.assets import (
    clip_captioning,
    clip_to_frame,
    raw_video_to_frame,
)
from vlm_pipeline.defs.process.sensor import video_frame_extract_sensor
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS
from vlm_pipeline.defs.yolo.assets import yolo_image_detection
from vlm_pipeline.defs.yolo.sensor import yolo_detection_sensor
from vlm_pipeline.lib.env_utils import bool_env
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

ENABLE_MANUAL_LABEL_IMPORT = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
ENABLE_YOLO_DETECTION = bool_env("ENABLE_YOLO_DETECTION", False)

# ── Jobs ──

mvp_stage_job = define_asset_job(
    "mvp_stage_job",
    selection=[
        raw_ingest,
        clip_timestamp,
        clip_captioning,
        clip_to_frame,
        build_dataset,
    ],
    tags={"duckdb_writer": "true"},
    description="전체 파이프라인 — 수집 → timestamp → captioning → clip절단 → 데이터셋 조립",
)

ingest_job = define_asset_job(
    "ingest_job",
    selection=[raw_ingest],
    tags={"duckdb_writer": "true"},
    description="원본 미디어 수집 + inline 중복 검출",
)

label_job = define_asset_job(
    "label_job",
    selection=[clip_timestamp],
    tags={"duckdb_writer": "true"},
    description="이벤트 구간 timestamp 식별 + 라벨 등록",
)

auto_labeling_job = define_asset_job(
    "auto_labeling_job",
    selection=[clip_timestamp, clip_captioning, clip_to_frame],
    tags={"duckdb_writer": "true"},
    description="timestamp → captioning → clip 절단",
)

process_build_job = define_asset_job(
    "process_build_job",
    selection=[clip_to_frame, build_dataset],
    tags={"duckdb_writer": "true"},
    description="clip 절단 → 학습 데이터셋 조립",
)

video_frame_extract_job = define_asset_job(
    "video_frame_extract_job",
    selection=[clip_captioning],
    tags={"duckdb_writer": "true"},
    description="이벤트 구간 captioning (단독)",
)

# Future activation checklist for alternative frame extraction paths:
# 1. Uncomment extracted_processed_clip_frames import in this file.
# 2. Uncomment processed_clip_frame_extract_sensor import in this file.
# 3. Register the job/sensor below only when processed_clips should become the auto-extraction trigger.
# 4. If that switch happens, disable or remove video_frame_extract_sensor to avoid dual-trigger behavior.
#
# from vlm_pipeline.defs.process.assets import extracted_processed_clip_frames
# from vlm_pipeline.defs.process.sensor import processed_clip_frame_extract_sensor
#
# processed_clip_frame_extract_job = define_asset_job(
#     "processed_clip_frame_extract_job",
#     selection=[extracted_processed_clip_frames],
#     tags={"duckdb_writer": "true"},
#     description="PROCESS — processed_clips completed video → frame extraction",
# )

if ENABLE_YOLO_DETECTION:
    yolo_detection_job = define_asset_job(
        "yolo_detection_job",
        selection=[yolo_image_detection],
        tags={"duckdb_writer": "true"},
        description="YOLO-World-L object detection (processed_clip_frame → image_labels)",
    )

if ENABLE_MANUAL_LABEL_IMPORT:
    manual_label_import_job = define_asset_job(
        "manual_label_import_job",
        selection=[manual_label_import],
        tags={"duckdb_writer": "true"},
        description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
    )

gcs_download_job = define_asset_job(
    "gcs_download_job",
    selection=[gcs_download_to_incoming],
    description="GCS 외부 데이터 수집",
)

motherduck_sync_job = define_asset_job(
    "motherduck_sync_job",
    selection=[motherduck_sync],
    tags={"duckdb_writer": "true"},
    description="클라우드 동기화 — DuckDB → MotherDuck",
)

gcs_download_schedule = ScheduleDefinition(
    name="gcs_download_schedule",
    job=gcs_download_job,
    cron_schedule="0 4 * * *",
    execution_timezone="Asia/Seoul",
    run_config={
        "ops": {
            "pipeline__incoming_nas": {
                "config": {
                    "mode": "date-folders",
                    "download_dir": "/nas/incoming",
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

# ── Definitions ──

assets = [
    raw_ingest,
    gcs_download_to_incoming,
    clip_timestamp,
    clip_captioning,
    clip_to_frame,
    raw_video_to_frame,
    build_dataset,
    motherduck_sync,
]
if ENABLE_MANUAL_LABEL_IMPORT:
    assets.append(manual_label_import)
if ENABLE_YOLO_DETECTION:
    assets.append(yolo_image_detection)

jobs = [
    mvp_stage_job,
    ingest_job,
    gcs_download_job,
    label_job,
    auto_labeling_job,
    process_build_job,
    video_frame_extract_job,
    motherduck_sync_job,
]
if ENABLE_MANUAL_LABEL_IMPORT:
    jobs.append(manual_label_import_job)
if ENABLE_YOLO_DETECTION:
    jobs.append(yolo_detection_job)

sensors = [
    incoming_manifest_sensor,
    auto_bootstrap_manifest_sensor,
    stuck_run_guard_sensor,
    auto_labeling_sensor,
    video_frame_extract_sensor,
    *MOTHERDUCK_TABLE_SENSORS,
]
if ENABLE_YOLO_DETECTION:
    sensors.append(yolo_detection_sensor)

defs = Definitions(
    assets=assets,
    jobs=jobs,
    schedules=[gcs_download_schedule],
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
