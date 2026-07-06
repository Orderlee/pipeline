"""Production Definitions 조립 요소.

`definitions.py`가 호출하는 단일 production canonical 조립 모듈.
어셈블리(asset/job/schedule/sensor builder) 함수들이 모여 있다.
"""

from __future__ import annotations

from dagster import DefaultScheduleStatus, EnvVar, ScheduleDefinition, define_asset_job

from vlm_pipeline.defs.build.assets import build_dataset
from vlm_pipeline.defs.build.classification import build_classification
from vlm_pipeline.defs.build.sensor import build_dataset_on_finalize_sensor
from vlm_pipeline.defs.genai import genai_poll_sensor
from vlm_pipeline.defs.dispatch.archive_dispatch_sensor import build_archive_dispatch_sensor
from vlm_pipeline.defs.dispatch.production_agent_sensor import build_production_agent_dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor import build_dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor_run_status import (
    dispatch_run_canceled_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_success_sensor,
)
from vlm_pipeline.defs.gcp.assets import DEFAULT_GCP_BUCKETS, DEFAULT_GCP_DOWNLOAD_DIR, gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.ingest.sourcea_download import sourcea_site_download
from vlm_pipeline.defs.ingest.upload_archived_asset import upload_archived
from vlm_pipeline.defs.ingest.sensor import (
    auto_bootstrap_manifest_sensor,
    cross_table_consistency_sensor,
    incoming_manifest_sensor,
    nas_health_sensor,
    stale_state_reaper_sensor,
    stuck_run_guard_sensor,
)
from vlm_pipeline.defs.train.sensor_maintenance_guard import maintenance_guard_sensor
from vlm_pipeline.defs.train.catalog_ingest import dataset_catalog_reconciliation_sensor
from vlm_pipeline.defs.train.dataset import build_trainset
from vlm_pipeline.defs.train.eval import train_eval_gate
from vlm_pipeline.defs.label.assets import classification_video, clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.label.sensor import auto_labeling_sensor
from vlm_pipeline.defs.ls.sensor import (
    ls_task_create_sensor,
)
from vlm_pipeline.defs.process.assets import clip_captioning, clip_to_frame, raw_video_to_frame
from vlm_pipeline.defs.embed.assets import caption_embedding, frame_embedding
from vlm_pipeline.defs.embed.sensor import (
    caption_embedding_backlog_sensor,
    frame_embedding_backlog_sensor,
    video_embedding_backlog_sensor,
)
from vlm_pipeline.defs.embed.video import video_embedding
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sam.detection_assets import (
    dispatch_sam3_image_detection,
    sam3_image_detection,
)

from vlm_pipeline.lib.env_utils import default_postgres_dsn
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

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
    stale_state_reaper_sensor,  # default STOPPED — 운영자가 UI 에서 활성화
    nas_health_sensor,
    cross_table_consistency_sensor,
    maintenance_guard_sensor,
)

COMMON_DISPATCH_STATUS_SENSORS = (
    dispatch_run_success_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_canceled_sensor,
)


def _build_db_resource() -> PostgresResource:
    """PostgresResource 인스턴스 생성 — PG-only (2026-05-19 cutover 이후).

    file-based DuckDB / DualDBResource 모드는 모두 제거. DuckDB 는 ``pg_duckdb``
    extension 경유로만 사용된다 (analytics 쿼리 한정, write path 무관).
    """
    dsn = default_postgres_dsn()
    if not dsn:
        raise RuntimeError(
            "DATAOPS_POSTGRES_DSN required — file-based DuckDB is deprecated. "
            "Set DATAOPS_POSTGRES_DSN to a Postgres connection string."
        )
    return PostgresResource(dsn=dsn)


def build_common_resources() -> dict[str, object]:
    return {
        "db": _build_db_resource(),
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
    writer_tag: str | None = None,  # noqa: ARG001 - signature kept for backward compat
    tags: dict[str, str] | None = None,
):
    """Asset job builder. `writer_tag` 는 deprecated (DuckDB single-file lock 제거됨)."""
    return define_asset_job(
        name,
        selection=selection,
        tags=tags,
        description=description,
    )


def build_video_env_backfill_schedule(job) -> ScheduleDefinition:
    """평일 19:00 KST Places365 환경 분류 백필 스케줄.

    default_status=STOPPED — 운영자가 Dagster UI 에서 수동으로 활성화해야 한다.
    백로그 소진 후에는 다시 STOPPED 로 복귀 권장.
    """
    return ScheduleDefinition(
        name="video_env_backfill_schedule",
        job=job,
        cron_schedule="0 19 * * 1-5",
        execution_timezone="Asia/Seoul",
        default_status=DefaultScheduleStatus.STOPPED,
        run_config={
            "ops": {
                "video_env_backfill": {
                    "config": {
                        "limit": 1000,
                    }
                }
            }
        },
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
                        "download_dir": DEFAULT_GCP_DOWNLOAD_DIR,
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


def build_sourcea_download_schedule(job) -> ScheduleDefinition:
    """매일 06:00 KST SourceA 사이트 수집 (tailscale 창구 06:00-07:00).

    07:00 종료는 asset 내부 자체 마감(기본 06:55, env SOURCEA_DEADLINE)이 보장 —
    이 배포에는 run monitoring 이 없어 dagster/max_runtime 태그가 동작하지 않는다.
    env/NAS/tailscale 부재 시 asset 이 graceful skip 하므로 RUNNING 이 안전하다.
    """
    return ScheduleDefinition(
        name="sourcea_download_schedule",
        job=job,
        cron_schedule="0 6 * * *",
        execution_timezone="Asia/Seoul",
        default_status=DefaultScheduleStatus.RUNNING,
    )


def build_production_assets(
    *,
    enable_manual_label_import: bool,
    enable_yolo_detection: bool,
    enable_sam3_detection: bool = False,
    enable_embedding: bool = False,
) -> list[object]:
    assets: list[object] = [
        raw_ingest,
        upload_archived,  # Phase 2b: archive 경로 → MinIO 업로드
        gcs_download_to_incoming,
        sourcea_site_download,
        *CLIP_AUTO_LABEL_ASSETS,
        raw_video_to_frame,
        build_dataset,
        build_classification,
        classification_video,
        dispatch_sam3_image_detection,
        sam3_shadow_compare,
        build_trainset,
        train_eval_gate,
    ]
    if enable_manual_label_import:
        assets.append(manual_label_import)
    if enable_yolo_detection:
        from vlm_pipeline.defs.yolo.assets import dispatch_yolo_image_detection, yolo_image_detection

        assets.extend([dispatch_yolo_image_detection, yolo_image_detection])
    if enable_sam3_detection:
        assets.append(sam3_image_detection)
    if enable_embedding:
        assets.append(frame_embedding)
        assets.append(caption_embedding)
        assets.append(video_embedding)
        from vlm_pipeline.defs.embed.reembed import reembed_under_version

        assets.append(reembed_under_version)  # PE-Core 승격 재임베딩 (gated; design §8.1-B)
    return assets


def build_production_sensors(
    *,
    dispatch_target_jobs: list[object],
    archive_dispatch_jobs: list[object] | None = None,
    enable_embedding: bool = False,
) -> list[object]:
    sensors: list[object] = [
        *COMMON_INGEST_SENSORS,
        build_dispatch_sensor(jobs=dispatch_target_jobs),
        build_production_agent_dispatch_sensor(jobs=dispatch_target_jobs),
        *COMMON_DISPATCH_STATUS_SENSORS,
        ls_task_create_sensor,
        build_dataset_on_finalize_sensor,
        genai_poll_sensor,
        # 2026-05-20: dispatch_stage_job 종료 후 limit 초과분/누락분을 picking up.
        auto_labeling_sensor,
    ]
    if archive_dispatch_jobs:
        # Phase 2b: from_archived=True dispatch JSON 만 처리
        sensors.append(build_archive_dispatch_sensor(jobs=archive_dispatch_jobs))
    if enable_embedding:
        sensors.append(frame_embedding_backlog_sensor)
        sensors.append(caption_embedding_backlog_sensor)
        sensors.append(video_embedding_backlog_sensor)
    # DVC 큐레이션 카탈로그 reconciliation (STOPPED 기본; 런타임에 DVC_DATA_REPO_PATH 미설정이면 self-skip).
    sensors.append(dataset_catalog_reconciliation_sensor)
    return sensors
