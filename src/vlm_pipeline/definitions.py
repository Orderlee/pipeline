"""Dagster Definitions — single canonical entrypoint."""

from __future__ import annotations

from dagster import Definitions

from vlm_pipeline.definitions_production import (
    POST_REVIEW_CLIP_ASSETS,
    build_asset_job,
    build_common_resources,
    build_dispatch_stage_selection,
    build_gcs_download_schedule,
    build_production_assets,
    build_production_sensors,
)
from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.asset_checks import PHASE_3C_ASSET_CHECKS
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.ingest.upload_archived_asset import upload_archived
from vlm_pipeline.defs.label.assets import classification_video, clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.ls.sensor import ls_presign_renew_job, ls_presign_renew_schedule
from vlm_pipeline.defs.process.assets import clip_captioning
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sam.detection_assets import sam3_image_detection
from vlm_pipeline.lib.env_utils import bool_env

_enable_manual_label_import = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
_enable_yolo_detection = bool_env("ENABLE_YOLO_DETECTION", False)
_enable_sam3_detection = bool_env("ENABLE_SAM3_DETECTION", False)

_mvp_stage_job = build_asset_job(
    name="mvp_stage_job",
    selection=[raw_ingest],
    description="[운영] 수집만 — 라벨링은 dispatch 트리거 JSON + dispatch_stage_job",
)
_ingest_job = build_asset_job(
    name="ingest_job",
    selection=[raw_ingest],
    description="원본 미디어 수집 + inline 중복 검출",
)
_gcs_download_job = build_asset_job(
    name="gcs_download_job",
    selection=[gcs_download_to_incoming],
    description="GCS 외부 데이터 수집",
)
# DISABLED 2026-05-19: motherduck sync 일시 비활성화 (사용자 요청)
# _motherduck_sync_job = build_asset_job(
#     name="motherduck_sync_job",
#     selection=[motherduck_sync],
#     writer_tag=DUCKDB_LABEL_WRITER_TAG,
#     description="클라우드 동기화 — DuckDB → MotherDuck",
# )
_dispatch_stage_job = build_asset_job(
    name="dispatch_stage_job",
    selection=build_dispatch_stage_selection(enable_yolo_detection=_enable_yolo_detection),
    description="[운영 유일 자동 라벨링] `.dispatch/pending` 트리거 JSON → ingest + clip_* + (YOLO opt) + SAM3",
)
# 2026-05-20 추가: dispatch_stage_job 종료 후 잔여 (limit 초과분 / 누락분) 을 sensor 가 picking up.
# auto_labeling_sensor 가 backlog 감지 시 이 job 트리거 — clip_timestamp + clip_captioning + classification_video 만 (raw_ingest 제외 — 이미 archive 후).
_auto_labeling_job = build_asset_job(
    name="auto_labeling_job",
    selection=[clip_timestamp, clip_captioning, classification_video],
    description="[backlog 처리] auto_labeling_sensor 트리거 — Gemini 라벨링 잔여분 batch 처리",
)
# Phase 2b — archive 경로 파일을 MinIO 로 업로드하는 단일 step 잡.
# (라벨링 assets 통합은 follow-up PR 에서 selection 확장 예정)
_upload_label_job = build_asset_job(
    name="upload_label_job",
    selection=[upload_archived],
    description="[Phase 2b] from_archived=True dispatch JSON → archive 파일 MinIO 업로드",
)
# LS 검수 확정(/sync-approve) 후 호출되는 clip 분할 전용 job.
# ls_webhook.py finalize_project 에서 GraphQL launchPipelineExecution 으로 트리거.
_post_review_clip_job = build_asset_job(
    name="post_review_clip_job",
    selection=POST_REVIEW_CLIP_ASSETS,
    description="[LS 확정 후] 사람 검수된 labels timestamp 기반 clip 분할 + 프레임 추출",
)
_sam3_shadow_compare_job = build_asset_job(
    name="sam3_shadow_compare_job",
    selection=[sam3_shadow_compare],
    description="[운영 shadow benchmark] YOLO bbox 결과와 SAM3 segmentation/bbox agreement 비교",
)

_jobs: list[object] = [
    _mvp_stage_job,
    _ingest_job,
    _gcs_download_job,
    # DISABLED 2026-05-19: motherduck sync 일시 비활성화 (사용자 요청)
    # _motherduck_sync_job,
    _dispatch_stage_job,
    _auto_labeling_job,
    _upload_label_job,
    _post_review_clip_job,
    _sam3_shadow_compare_job,
]
if _enable_manual_label_import:
    _jobs.append(
        build_asset_job(
            name="manual_label_import_job",
            selection=[manual_label_import],
            description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
        )
    )
if _enable_yolo_detection:
    from vlm_pipeline.defs.yolo.assets import yolo_image_detection

    _jobs.append(
        build_asset_job(
            name="yolo_standard_detection_job",
            selection=[yolo_image_detection],
            description="YOLO (clip_to_frame deps) — ENABLE_YOLO_DETECTION 시에만 등록",
        )
    )
if _enable_sam3_detection:
    _jobs.append(
        build_asset_job(
            name="sam3_standard_detection_job",
            selection=[sam3_image_detection],
            description="SAM3 bbox detection (clip_to_frame deps) — ENABLE_SAM3_DETECTION 시에만 등록",
        )
    )

_jobs.append(ls_presign_renew_job)

defs = Definitions(
    assets=build_production_assets(
        enable_manual_label_import=_enable_manual_label_import,
        enable_yolo_detection=_enable_yolo_detection,
        enable_sam3_detection=_enable_sam3_detection,
    ),
    asset_checks=PHASE_3C_ASSET_CHECKS,
    jobs=_jobs,
    schedules=[
        build_gcs_download_schedule(_gcs_download_job),
        # DISABLED 2026-05-19: motherduck sync 일시 비활성화 (사용자 요청)
        # build_motherduck_daily_schedule(_motherduck_sync_job),
        ls_presign_renew_schedule,
    ],
    sensors=build_production_sensors(
        # DISABLED 2026-05-19: motherduck table sensors 일시 비활성화. 재활성화시 MOTHERDUCK_TABLE_SENSORS 복원.
        [],
        dispatch_target_jobs=[_dispatch_stage_job, _ingest_job],
        archive_dispatch_jobs=[_upload_label_job],
    ),
    resources=build_common_resources(),
)
