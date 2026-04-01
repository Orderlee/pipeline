"""Profile-based Dagster definitions assembly."""

from __future__ import annotations

from dagster import Definitions

from vlm_pipeline.lib.env_utils import bool_env
from vlm_pipeline.lib.runtime_profile import RuntimeProfileName


def build_definitions_for_profile(profile: RuntimeProfileName) -> Definitions:
    if profile == "production":
        return _build_production_definitions()
    return _build_staging_definitions()


def _build_production_definitions() -> Definitions:
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
        build_production_assets,
        build_production_sensors,
        build_sam3_shadow_compare_job,
        build_yolo_standard_detection_job,
    )
    from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS

    enable_manual_label_import = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
    enable_yolo_detection = bool_env("ENABLE_YOLO_DETECTION", False)

    mvp_stage_job = build_mvp_stage_job(
        description="[운영] 수집만 — 라벨링은 dispatch 트리거 JSON + dispatch_stage_job",
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
        description="[운영 유일 자동 라벨링] `.dispatch/pending` 트리거 JSON → ingest + clip_* + YOLO",
        staging=False,
    )
    sam3_shadow_compare_job = build_sam3_shadow_compare_job(
        description="[운영 shadow benchmark] YOLO bbox 결과와 SAM3 segmentation/bbox agreement 비교",
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

    return Definitions(
        assets=build_production_assets(
            enable_manual_label_import=enable_manual_label_import,
            enable_yolo_detection=enable_yolo_detection,
        ),
        jobs=jobs,
        schedules=[
            build_gcs_download_schedule(gcs_download_job),
            build_motherduck_daily_schedule(motherduck_sync_job),
        ],
        sensors=build_production_sensors(MOTHERDUCK_TABLE_SENSORS),
        resources=build_common_resources(),
    )


def _build_staging_definitions() -> Definitions:
    from vlm_pipeline.definitions_common import (
        build_auto_labeling_routed_job,
        build_common_resources,
        build_dispatch_stage_job,
        build_gcs_download_job,
        build_ingest_job,
        build_manual_label_import_job,
        build_prelabeled_import_job,
        build_sam3_shadow_compare_job,
        build_staging_assets,
        build_staging_sensors,
        build_staging_yolo_detection_job,
    )
    from vlm_pipeline.defs.dispatch.sensor import dispatch_sensor
    from vlm_pipeline.defs.dispatch.staging_agent_sensor import staging_agent_dispatch_sensor
    from vlm_pipeline.defs.spec.staging_sensor import spec_resolve_sensor

    ingest_job = build_ingest_job(
        description="원본 미디어 수집 + inline 중복 검출",
    )
    gcs_download_job = build_gcs_download_job(
        tags={"duckdb_writer": "true"},
        description="GCS 외부 데이터 수집",
    )
    dispatch_stage_job = build_dispatch_stage_job(
        description="Staging dispatch — run_mode에 따라 처리 분기",
        staging=True,
    )
    auto_labeling_routed_job = build_auto_labeling_routed_job(
        description="Staging spec: spec_resolve_sensor에서 트리거, requested_outputs에 따라 단계 실행",
    )
    yolo_detection_job = build_staging_yolo_detection_job(
        description="YOLO-World-L object detection (processed_clip_frame → image_labels)",
    )
    manual_label_import_job = build_manual_label_import_job(
        description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
    )
    prelabeled_import_job = build_prelabeled_import_job(
        description="Staging 수동 완료데이터 적재: raw ingest + 기존 이벤트/bbox/image caption import",
    )
    sam3_shadow_compare_job = build_sam3_shadow_compare_job(
        description="[Staging shadow benchmark] YOLO bbox 결과와 SAM3 segmentation/bbox agreement 비교",
    )

    return Definitions(
        assets=build_staging_assets(),
        jobs=[
            ingest_job,
            gcs_download_job,
            dispatch_stage_job,
            auto_labeling_routed_job,
            yolo_detection_job,
            sam3_shadow_compare_job,
            manual_label_import_job,
            prelabeled_import_job,
        ],
        sensors=build_staging_sensors(
            spec_resolve_sensor=spec_resolve_sensor,
            dispatch_ingress_sensor=staging_agent_dispatch_sensor,
            dispatch_json_sensor=dispatch_sensor,
        ),
        resources=build_common_resources(),
    )
