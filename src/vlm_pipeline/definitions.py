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

from dagster import Definitions

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
    build_yolo_standard_detection_job,
)
from vlm_pipeline.defs.sync.sensor import MOTHERDUCK_TABLE_SENSORS
from vlm_pipeline.lib.env_utils import bool_env

ENABLE_MANUAL_LABEL_IMPORT = bool_env("ENABLE_MANUAL_LABEL_IMPORT", False)
ENABLE_YOLO_DETECTION = bool_env("ENABLE_YOLO_DETECTION", False)

# ── Jobs: MVP · 부분 실행 ──

mvp_stage_job = build_mvp_stage_job(
    description="[운영] 수집만 — 라벨링은 dispatch 트리거 JSON + dispatch_stage_job",
)

ingest_job = build_ingest_job(
    description="원본 미디어 수집 + inline 중복 검출",
)

if ENABLE_YOLO_DETECTION:
    yolo_standard_detection_job = build_yolo_standard_detection_job(
        description="YOLO (clip_to_frame deps) — ENABLE_YOLO_DETECTION 시에만 등록",
    )

if ENABLE_MANUAL_LABEL_IMPORT:
    manual_label_import_job = build_manual_label_import_job(
        description="수동 라벨 JSON 임포트 (incoming 디렉터리)",
    )

gcs_download_job = build_gcs_download_job(
    description="GCS 외부 데이터 수집",
)

motherduck_sync_job = build_motherduck_sync_job(
    description="클라우드 동기화 — DuckDB → MotherDuck",
)

# ── Jobs: dispatch 트리거(JSON) 전용 라벨링 파이프라인 ──

dispatch_stage_job = build_dispatch_stage_job(
    description="[운영 유일 자동 라벨링] `.dispatch/pending` 트리거 JSON → ingest + clip_* + YOLO",
)

gcs_download_schedule = build_gcs_download_schedule(gcs_download_job)
motherduck_daily_schedule = build_motherduck_daily_schedule(motherduck_sync_job)

# ── Definitions ──

assets = build_production_assets(
    enable_manual_label_import=ENABLE_MANUAL_LABEL_IMPORT,
    enable_yolo_detection=ENABLE_YOLO_DETECTION,
)

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

sensors = build_production_sensors(MOTHERDUCK_TABLE_SENSORS)

defs = Definitions(
    assets=assets,
    jobs=jobs,
    schedules=[gcs_download_schedule, motherduck_daily_schedule],
    sensors=sensors,
    resources=build_common_resources(),
)
