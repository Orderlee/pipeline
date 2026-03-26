"""Dagster Definitions — Staging 전용 진입점.

Production definitions.py와 완전 분리된 staging 파이프라인.
IS_STAGING 분기 없이 staging 전용 asset/job/sensor만 등록.

파이프라인 흐름 (staging):
  piaspace-agent polling → staging_agent_dispatch_sensor → raw_ingest
    → spec_resolve_sensor
    → clip_timestamp → clip_captioning → clip_to_frame (run 태그별 routed 분기)
    → bbox_labeling (YOLO) → activate_labeling_spec

운영과 동일한 NAS incoming 보조:
  `incoming_manifest_sensor` + `auto_bootstrap_manifest_sensor` (ingest_job 연계).
  NAS 지연 시 `AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES` 등은 운영과 동일 env로 조정.

프레임 추출 시점은 운영과 동일하게 `vlm_pipeline.lib.video_frames.plan_frame_timestamps` 를 사용한다
(`raw_video_to_frame` 등).
"""

from __future__ import annotations

from dagster import Definitions

from vlm_pipeline.definitions_common import (
    build_auto_labeling_routed_job,
    build_common_resources,
    build_dispatch_stage_job,
    build_gcs_download_job,
    build_ingest_job,
    build_manual_label_import_job,
    build_prelabeled_import_job,
    build_staging_assets,
    build_staging_sensors,
    build_staging_yolo_detection_job,
)
from vlm_pipeline.defs.dispatch.staging_agent_sensor import staging_agent_dispatch_sensor
from vlm_pipeline.defs.spec.staging_sensor import spec_resolve_sensor

# ── Assets ──

assets = build_staging_assets()

# ── Jobs ──

ingest_job = build_ingest_job(
    description="원본 미디어 수집 + inline 중복 검출",
)

gcs_download_job = build_gcs_download_job(
    tags={"duckdb_writer": "true"},
    description="GCS 외부 데이터 수집",
)

dispatch_stage_job = build_dispatch_stage_job(
    description="Staging dispatch — run_mode에 따라 처리 분기",
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

jobs = [
    ingest_job,
    gcs_download_job,
    dispatch_stage_job,
    auto_labeling_routed_job,
    yolo_detection_job,
    manual_label_import_job,
    prelabeled_import_job,
]

# ── Sensors ──

sensors = build_staging_sensors(
    spec_resolve_sensor=spec_resolve_sensor,
    dispatch_ingress_sensor=staging_agent_dispatch_sensor,
)

# ── Definitions ──

defs = Definitions(
    assets=assets,
    jobs=jobs,
    sensors=sensors,
    resources=build_common_resources(),
)
