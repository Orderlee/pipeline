"""Dagster Definitions — Production 진입점 (`docker/app/dagster_defs.py`).

운영 라벨링 정책:
  - **자동 라벨링(Gemini·clip·dispatch YOLO)** 은 `.dispatch/pending` 트리거 JSON만 허용
    → `dispatch_sensor` → `dispatch_stage_job` (run 태그에 folder/outputs 등 전달).
  - **manifest / auto_bootstrap** → `incoming_manifest_sensor` → `ingest_job` (**수집만**, 라벨링 없음).
    단, auto_bootstrap은 운영에서 `incoming/gcp/**` 만 허용되고 일반 `incoming/<folder>` 는
    `.dispatch/pending` 트리거 JSON이 있어야 처리된다.
  - `mvp_stage_job` 은 동일하게 수집만(구 스케줄·수동 실행 호환용).

`clip_*` 에셋은 `dispatch_stage_job` 없이는 **자동** 실행되지 않는다.
예외: UI에서 에셋/잡을 직접 머티리얼라이즈하면 실행 가능하며,
`ENABLE_MANUAL_LABEL_IMPORT` / `ENABLE_YOLO_DETECTION` 을 켠 경우 해당 잡은 정책에서 제외된다(기본 false).

spec 자동 해석(`spec_resolve_sensor`)·`auto_labeling_routed_job` 등은 staging(`definitions_staging.py`) 전용이다.
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
from vlm_pipeline.defs.dispatch.sensor import dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor_incoming_mover import incoming_to_pending_sensor
from vlm_pipeline.defs.label.assets import clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.process.assets import (
    clip_captioning,
    clip_to_frame,
    raw_video_to_frame,
)
from vlm_pipeline.defs.spec.assets import labeling_spec_ingest, pending_ingest
from vlm_pipeline.defs.spec.staging_assets import (
    activate_labeling_spec,
    config_sync,
    ingest_router,
)
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS
from vlm_pipeline.defs.yolo.assets import bbox_labeling, yolo_image_detection
from vlm_pipeline.defs.yolo.staging_assets import staging_yolo_image_detection
from vlm_pipeline.lib.env_utils import bool_env
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

ENABLE_MANUAL_LABEL_IMPORT = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
ENABLE_YOLO_DETECTION = bool_env("ENABLE_YOLO_DETECTION", False)

# Gemini 이벤트 → labels → clip/프레임 (여러 job에서 동일 순서로 재사용)
CLIP_AUTO_LABEL_ASSETS = (
    clip_timestamp,
    clip_captioning,
    clip_to_frame,
)

# ── Jobs: MVP · 부분 실행 ──

mvp_stage_job = define_asset_job(
    "mvp_stage_job",
    selection=[raw_ingest],
    tags={"duckdb_writer": "true"},
    description="[운영] 수집만 — 라벨링은 dispatch 트리거 JSON + dispatch_stage_job",
)

ingest_job = define_asset_job(
    "ingest_job",
    selection=[raw_ingest],
    tags={"duckdb_writer": "true"},
    description="원본 미디어 수집 + inline 중복 검출",
)

if ENABLE_YOLO_DETECTION:
    yolo_standard_detection_job = define_asset_job(
        "yolo_standard_detection_job",
        selection=[yolo_image_detection],
        tags={"duckdb_writer": "true"},
        description="YOLO (clip_to_frame deps) — ENABLE_YOLO_DETECTION 시에만 등록",
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

# ── Jobs: dispatch 트리거(JSON) 전용 라벨링 파이프라인 ──

dispatch_stage_job = define_asset_job(
    "dispatch_stage_job",
    selection=[
        raw_ingest,
        *CLIP_AUTO_LABEL_ASSETS,
        raw_video_to_frame,
        staging_yolo_image_detection,
    ],
    tags={"duckdb_writer": "true"},
    description="[운영 유일 자동 라벨링] `.dispatch/pending` 트리거 JSON → ingest + clip_* + YOLO",
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

# ── Definitions ──

assets = [
    raw_ingest,
    gcs_download_to_incoming,
    *CLIP_AUTO_LABEL_ASSETS,
    raw_video_to_frame,
    build_dataset,
    motherduck_sync,
    labeling_spec_ingest,
    config_sync,
    ingest_router,
    pending_ingest,
    activate_labeling_spec,
    bbox_labeling,
    staging_yolo_image_detection,
]
if ENABLE_MANUAL_LABEL_IMPORT:
    assets.append(manual_label_import)
if ENABLE_YOLO_DETECTION:
    assets.append(yolo_image_detection)

jobs = [
    mvp_stage_job,
    ingest_job,
    gcs_download_job,
    motherduck_sync_job,
    dispatch_stage_job,
]
if ENABLE_MANUAL_LABEL_IMPORT:
    jobs.append(manual_label_import_job)
if ENABLE_YOLO_DETECTION:
    jobs.append(yolo_standard_detection_job)

sensors = [
    incoming_manifest_sensor,
    auto_bootstrap_manifest_sensor,
    stuck_run_guard_sensor,
    incoming_to_pending_sensor,
    dispatch_sensor,
    *MOTHERDUCK_TABLE_SENSORS,
]

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
