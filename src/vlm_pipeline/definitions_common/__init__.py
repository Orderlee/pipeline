"""Production/Staging definitions 공통 조립 요소.

하위 모듈(assets, jobs, schedules, sensors, resources)로 분할되어 있으며,
기존 import 경로 호환을 위해 모든 public 심볼을 여기서 재수출한다.
"""

from vlm_pipeline.definitions_common.assets import (
    CLIP_AUTO_LABEL_ASSETS,
    PRODUCTION_DISPATCH_STAGE_SELECTION,
    STAGING_DISPATCH_STAGE_SELECTION,
    build_production_assets,
    build_staging_assets,
)
from vlm_pipeline.definitions_common.jobs import (
    build_auto_labeling_routed_job,
    build_dispatch_stage_job,
    build_gcs_download_job,
    build_ingest_job,
    build_manual_label_import_job,
    build_motherduck_sync_job,
    build_mvp_stage_job,
    build_prelabeled_import_job,
    build_sam3_shadow_compare_job,
    build_staging_yolo_detection_job,
    build_yolo_standard_detection_job,
)
from vlm_pipeline.definitions_common.resources import build_common_resources
from vlm_pipeline.definitions_common.schedules import (
    build_gcs_download_schedule,
    build_motherduck_daily_schedule,
)
from vlm_pipeline.definitions_common.sensors import (
    COMMON_DISPATCH_STATUS_SENSORS,
    COMMON_INGEST_SENSORS,
    build_production_sensors,
    build_staging_sensors,
)

__all__ = [
    "CLIP_AUTO_LABEL_ASSETS",
    "COMMON_DISPATCH_STATUS_SENSORS",
    "COMMON_INGEST_SENSORS",
    "PRODUCTION_DISPATCH_STAGE_SELECTION",
    "STAGING_DISPATCH_STAGE_SELECTION",
    "build_auto_labeling_routed_job",
    "build_common_resources",
    "build_dispatch_stage_job",
    "build_gcs_download_job",
    "build_gcs_download_schedule",
    "build_ingest_job",
    "build_manual_label_import_job",
    "build_motherduck_daily_schedule",
    "build_motherduck_sync_job",
    "build_mvp_stage_job",
    "build_prelabeled_import_job",
    "build_production_assets",
    "build_production_sensors",
    "build_sam3_shadow_compare_job",
    "build_staging_assets",
    "build_staging_sensors",
    "build_staging_yolo_detection_job",
    "build_yolo_standard_detection_job",
]
