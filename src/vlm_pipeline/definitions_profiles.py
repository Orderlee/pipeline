"""Profile-based Dagster definitions assembly."""

from __future__ import annotations

from dagster import Definitions

from vlm_pipeline.lib.env_utils import bool_env
from vlm_pipeline.lib.runtime_profile import RuntimeProfileName


def build_definitions_for_profile(profile: RuntimeProfileName) -> Definitions:
    if profile == "production":
        return _build_runtime_definitions(runtime_name="production")
    if profile == "test":
        return _build_runtime_definitions(runtime_name="test")
    raise ValueError(f"unsupported runtime profile: {profile}")


def _build_runtime_definitions(*, runtime_name: str) -> Definitions:
    from vlm_pipeline.definitions_common import (
        build_common_resources,
        build_dispatch_stage_job,
        build_gcs_download_job,
        build_gcs_download_schedule,
        build_ingest_job,
        build_manual_label_import_job,
        build_motherduck_daily_schedule,
        build_motherduck_sync_job,
        build_mvp_stage_job,
        build_runtime_assets,
        build_runtime_sensors,
        build_sam3_shadow_compare_job,
        build_sam3_standard_detection_job,
        build_yolo_standard_detection_job,
        ls_presign_renew_job,
        ls_presign_renew_schedule,
    )
    from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS

    enable_manual_label_import = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
    enable_yolo_detection = bool_env("ENABLE_YOLO_DETECTION", False)
    enable_sam3_detection = bool_env("ENABLE_SAM3_DETECTION", False)

    mvp_stage_job = build_mvp_stage_job(
        description=f"[{runtime_name}] 수집만 — 라벨링은 dispatch 트리거 JSON + dispatch_stage_job",
    )
    ingest_job = build_ingest_job(
        description="원본 미디어 수집 + inline 중복 검출",
    )
    gcs_download_job = build_gcs_download_job(
        description="GCS 외부 데이터 수집",
    )
    motherduck_sync_job = build_motherduck_sync_job(
        description="클라우드 동기화 — DuckDB → MotherDuck",
    )
    dispatch_stage_job = build_dispatch_stage_job(
        description=f"[{runtime_name}] `.dispatch/pending` 트리거 JSON → ingest + clip_* + YOLO + SAM3",
    )
    sam3_shadow_compare_job = build_sam3_shadow_compare_job(
        description=f"[{runtime_name}] YOLO bbox 결과와 SAM3 segmentation/bbox agreement 비교",
    )

    jobs = [
        mvp_stage_job,
        ingest_job,
        gcs_download_job,
        motherduck_sync_job,
        dispatch_stage_job,
        sam3_shadow_compare_job,
    ]
    if enable_manual_label_import:
        jobs.append(
            build_manual_label_import_job(
                description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
            )
        )
    if enable_yolo_detection:
        jobs.append(
            build_yolo_standard_detection_job(
                description="YOLO (clip_to_frame deps) — ENABLE_YOLO_DETECTION 시에만 등록",
            )
        )
    if enable_sam3_detection:
        jobs.append(
            build_sam3_standard_detection_job(
                description="SAM3 bbox detection (clip_to_frame deps) — ENABLE_SAM3_DETECTION 시에만 등록",
            )
        )

    jobs.append(ls_presign_renew_job)

    return Definitions(
        assets=build_runtime_assets(
            enable_manual_label_import=enable_manual_label_import,
            enable_yolo_detection=enable_yolo_detection,
            enable_sam3_detection=enable_sam3_detection,
        ),
        jobs=jobs,
        schedules=[
            build_gcs_download_schedule(gcs_download_job),
            build_motherduck_daily_schedule(motherduck_sync_job),
            ls_presign_renew_schedule,
        ],
        sensors=build_runtime_sensors(MOTHERDUCK_TABLE_SENSORS),
        resources=build_common_resources(),
    )
