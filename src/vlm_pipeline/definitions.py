"""Dagster Definitions — single canonical entrypoint."""

from __future__ import annotations

from dagster import Definitions

from vlm_pipeline.definitions_production import (
    PRODUCTION_DISPATCH_STAGE_SELECTION,
    build_asset_job,
    build_common_resources,
    build_gcs_download_schedule,
    build_motherduck_daily_schedule,
    build_production_assets,
    build_production_sensors,
    ls_presign_renew_job,
    ls_presign_renew_schedule,
)
from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sam.detection_assets import sam3_image_detection
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS
from vlm_pipeline.defs.yolo.assets import yolo_image_detection
from vlm_pipeline.lib.env_utils import (
    DUCKDB_LABEL_WRITER_TAG,
    DUCKDB_LEGACY_WRITER_TAG,
    DUCKDB_RAW_WRITER_TAG,
    DUCKDB_SAM3_WRITER_TAG,
    DUCKDB_YOLO_WRITER_TAG,
    bool_env,
)

_enable_manual_label_import = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
_enable_yolo_detection = bool_env("ENABLE_YOLO_DETECTION", False)
_enable_sam3_detection = bool_env("ENABLE_SAM3_DETECTION", False)

_mvp_stage_job = build_asset_job(
    name="mvp_stage_job",
    selection=[raw_ingest],
    writer_tag=DUCKDB_RAW_WRITER_TAG,
    description="[운영] 수집만 — 라벨링은 dispatch 트리거 JSON + dispatch_stage_job",
)
_ingest_job = build_asset_job(
    name="ingest_job",
    selection=[raw_ingest],
    writer_tag=DUCKDB_RAW_WRITER_TAG,
    description="원본 미디어 수집 + inline 중복 검출",
)
_gcs_download_job = build_asset_job(
    name="gcs_download_job",
    selection=[gcs_download_to_incoming],
    description="GCS 외부 데이터 수집",
)
_motherduck_sync_job = build_asset_job(
    name="motherduck_sync_job",
    selection=[motherduck_sync],
    writer_tag=DUCKDB_LABEL_WRITER_TAG,
    description="클라우드 동기화 — DuckDB → MotherDuck",
)
_dispatch_stage_job = build_asset_job(
    name="dispatch_stage_job",
    selection=PRODUCTION_DISPATCH_STAGE_SELECTION,
    writer_tag=DUCKDB_LEGACY_WRITER_TAG,
    description="[운영 유일 자동 라벨링] `.dispatch/pending` 트리거 JSON → ingest + clip_* + YOLO + SAM3",
)
_sam3_shadow_compare_job = build_asset_job(
    name="sam3_shadow_compare_job",
    selection=[sam3_shadow_compare],
    writer_tag=DUCKDB_LABEL_WRITER_TAG,
    description="[운영 shadow benchmark] YOLO bbox 결과와 SAM3 segmentation/bbox agreement 비교",
)

_jobs: list[object] = [
    _mvp_stage_job,
    _ingest_job,
    _gcs_download_job,
    _motherduck_sync_job,
    _dispatch_stage_job,
    _sam3_shadow_compare_job,
]
if _enable_manual_label_import:
    _jobs.append(
        build_asset_job(
            name="manual_label_import_job",
            selection=[manual_label_import],
            writer_tag=DUCKDB_LABEL_WRITER_TAG,
            description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
        )
    )
if _enable_yolo_detection:
    _jobs.append(
        build_asset_job(
            name="yolo_standard_detection_job",
            selection=[yolo_image_detection],
            writer_tag=DUCKDB_YOLO_WRITER_TAG,
            description="YOLO (clip_to_frame deps) — ENABLE_YOLO_DETECTION 시에만 등록",
        )
    )
if _enable_sam3_detection:
    _jobs.append(
        build_asset_job(
            name="sam3_standard_detection_job",
            selection=[sam3_image_detection],
            writer_tag=DUCKDB_SAM3_WRITER_TAG,
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
    jobs=_jobs,
    schedules=[
        build_gcs_download_schedule(_gcs_download_job),
        build_motherduck_daily_schedule(_motherduck_sync_job),
        ls_presign_renew_schedule,
    ],
    sensors=build_production_sensors(
        MOTHERDUCK_TABLE_SENSORS,
        dispatch_target_jobs=[_dispatch_stage_job, _ingest_job],
    ),
    resources=build_common_resources(),
)
