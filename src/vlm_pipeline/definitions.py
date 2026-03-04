"""Dagster Definitions — 단일 진입점.

Layer 5: 모든 assets + resources + sensors를 import하여 조립.
docker/app/dagster_defs.py의 MVP 파이프라인과 동일 구조.

파이프라인 단계:
  ingested_raw_files → dedup_results → labeled_files → processed_clips → built_dataset

data_pipeline_job 제거됨 — mvp_stage_job으로 통합.
"""

from dagster import AssetKey, Definitions, EnvVar, ScheduleDefinition, define_asset_job

from vlm_pipeline.defs.build.assets import built_dataset
from vlm_pipeline.defs.dedup.assets import dedup_results
from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import ingested_raw_files
from vlm_pipeline.defs.ingest.sensor import auto_bootstrap_manifest_sensor, incoming_manifest_sensor
from vlm_pipeline.defs.label.assets import labeled_files
from vlm_pipeline.defs.process.assets import processed_clips
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

# ── Jobs ──

# 단계형 MVP Job (전체 파이프라인 실행)
mvp_stage_job = define_asset_job(
    "mvp_stage_job",
    selection=[
        ingested_raw_files,
        dedup_results,
        labeled_files,
        processed_clips,
        built_dataset,
    ],
    tags={"duckdb_writer": "true"},
    description="MVP 전체 파이프라인 — INGEST → DEDUP → LABEL → PROCESS → BUILD",
)

# 분리형 Jobs (운영 전환 대비)
ingest_job = define_asset_job(
    "ingest_job",
    selection=[ingested_raw_files],
    tags={"duckdb_writer": "true"},
    description="INGEST — NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw",
)

dedup_job = define_asset_job(
    "dedup_job",
    selection=[dedup_results],
    tags={"duckdb_writer": "true"},
    description="DEDUP — 비동기 pHash 유사도 검출",
)

label_job = define_asset_job(
    "label_job",
    selection=[labeled_files],
    tags={"duckdb_writer": "true"},
    description="LABEL — 라벨 등록 (가공 툴 완료 후 수동/sensor)",
)

process_build_job = define_asset_job(
    "process_build_job",
    selection=[processed_clips, built_dataset],
    tags={"duckdb_writer": "true"},
    description="PROCESS → BUILD — 가공 + 데이터셋 조립",
)

gcs_download_job = define_asset_job(
    "gcs_download_job",
    selection=[gcs_download_to_incoming],
    description="GCS 다운로드 → /nas/incoming/ 적재",
)

motherduck_sync_job = define_asset_job(
    "motherduck_sync_job",
    selection=[motherduck_sync],
    # MotherDuck sync도 local DuckDB 파일 lock 영향을 받으므로 writer job과 직렬화한다.
    tags={"duckdb_writer": "true"},
    description="SYNC — local DuckDB MVP tables → MotherDuck",
)

gcs_download_schedule = ScheduleDefinition(
    name="gcs_download_schedule",
    job=gcs_download_job,
    cron_schedule="* * * * *",
    execution_timezone="Asia/Seoul",
    run_config={
        "ops": {
            "pipeline__gcs_download_to_incoming": {
                "config": {
                    "mode": "date-folders",
                    "download_dir": "/nas/incoming",
                    "backend": "gcloud",
                    "skip_existing": True,
                    "dry_run": False,
                    "buckets": ["khon-kaen-rtsp-bucket", "adlib-hotel-202512"],
                    "bucket_subdir": True,
                }
            }
        }
    },
)

# ── Definitions ──
defs = Definitions(
    assets=[
        ingested_raw_files,
        dedup_results,
        gcs_download_to_incoming,
        labeled_files,
        processed_clips,
        built_dataset,
        motherduck_sync,
    ],
    jobs=[
        mvp_stage_job,
        ingest_job,
        dedup_job,
        gcs_download_job,
        label_job,
        process_build_job,
        motherduck_sync_job,
    ],
    schedules=[gcs_download_schedule],
    sensors=[incoming_manifest_sensor, auto_bootstrap_manifest_sensor, *MOTHERDUCK_TABLE_SENSORS],
    resources={
        "db": DuckDBResource(db_path=EnvVar("DATAOPS_DUCKDB_PATH")),
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
        ),
    },
)
