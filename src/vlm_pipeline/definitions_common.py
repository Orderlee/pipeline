"""Production/Test definitions 공통 조립 요소."""

from __future__ import annotations

from dagster import EnvVar, ScheduleDefinition, define_asset_job

from vlm_pipeline.defs.build.assets import build_dataset
from vlm_pipeline.defs.dispatch.agent_sensor import dispatch_agent_sensor
from vlm_pipeline.defs.dispatch.sensor import dispatch_sensor
from vlm_pipeline.defs.dispatch.sensor_run_status import (
    dispatch_run_canceled_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_success_sensor,
)
from vlm_pipeline.defs.gcp.assets import DEFAULT_GCP_BUCKETS, gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.ingest.sensor import (
    auto_bootstrap_manifest_sensor,
    incoming_manifest_sensor,
    nas_health_sensor,
    stuck_run_guard_sensor,
)
from vlm_pipeline.defs.label.assets import classification_video, clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.ls.sensor import (
    ls_presign_renew_job,
    ls_presign_renew_schedule,
    ls_task_create_job,
    ls_task_create_sensor,
)
from vlm_pipeline.defs.process.assets import clip_captioning, clip_to_frame, raw_video_to_frame
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sam.detection_assets import (
    dispatch_sam3_image_detection,
    sam3_bbox_labeling,
    sam3_image_detection,
)
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.yolo.assets import bbox_labeling, dispatch_yolo_image_detection, yolo_image_detection
from vlm_pipeline.lib.env_utils import (
    DUCKDB_LABEL_WRITER_TAG,
    DUCKDB_LEGACY_WRITER_TAG,
    DUCKDB_RAW_WRITER_TAG,
    DUCKDB_SAM3_WRITER_TAG,
    DUCKDB_YOLO_WRITER_TAG,
    build_duckdb_writer_tags,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

CLIP_AUTO_LABEL_ASSETS = (
    clip_timestamp,
    clip_captioning,
    clip_to_frame,
)

RUNTIME_DISPATCH_STAGE_SELECTION = [
    raw_ingest,
    *CLIP_AUTO_LABEL_ASSETS,
    classification_video,
    raw_video_to_frame,
    dispatch_yolo_image_detection,
    dispatch_sam3_image_detection,
]

COMMON_INGEST_SENSORS = (
    incoming_manifest_sensor,
    auto_bootstrap_manifest_sensor,
    stuck_run_guard_sensor,
    nas_health_sensor,
)

COMMON_DISPATCH_STATUS_SENSORS = (
    dispatch_run_success_sensor,
    dispatch_run_failure_sensor,
    dispatch_run_canceled_sensor,
)


def build_common_resources() -> dict[str, object]:
    return {
        "db": DuckDBResource(db_path=EnvVar("DATAOPS_DUCKDB_PATH")),
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
        ),
    }


def build_ingest_job(*, description: str):
    return define_asset_job(
        "ingest_job",
        selection=[raw_ingest],
        tags=build_duckdb_writer_tags(DUCKDB_RAW_WRITER_TAG),
        description=description,
    )


def build_mvp_stage_job(*, description: str):
    return define_asset_job(
        "mvp_stage_job",
        selection=[raw_ingest],
        tags=build_duckdb_writer_tags(DUCKDB_RAW_WRITER_TAG),
        description=description,
    )


def build_gcs_download_job(*, description: str, tags: dict[str, str] | None = None):
    return define_asset_job(
        "gcs_download_job",
        selection=[gcs_download_to_incoming],
        tags=tags,
        description=description,
    )


def build_dispatch_stage_job(*, description: str):
    return define_asset_job(
        "dispatch_stage_job",
        selection=RUNTIME_DISPATCH_STAGE_SELECTION,
        tags=build_duckdb_writer_tags(DUCKDB_LEGACY_WRITER_TAG),
        description=description,
    )


def build_motherduck_sync_job(*, description: str):
    return define_asset_job(
        "motherduck_sync_job",
        selection=[motherduck_sync],
        tags=build_duckdb_writer_tags(DUCKDB_LABEL_WRITER_TAG),
        description=description,
    )


def build_manual_label_import_job(*, description: str):
    return define_asset_job(
        "manual_label_import_job",
        selection=[manual_label_import],
        tags=build_duckdb_writer_tags(DUCKDB_LABEL_WRITER_TAG),
        description=description,
    )


def build_prelabeled_import_job(*, description: str):
    from vlm_pipeline.defs.label.prelabeled_import import prelabeled_import

    return define_asset_job(
        "prelabeled_import_job",
        selection=[prelabeled_import],
        tags=build_duckdb_writer_tags(DUCKDB_LABEL_WRITER_TAG),
        description=description,
    )


def build_sam3_shadow_compare_job(*, description: str):
    return define_asset_job(
        "sam3_shadow_compare_job",
        selection=[sam3_shadow_compare],
        tags=build_duckdb_writer_tags(DUCKDB_LABEL_WRITER_TAG),
        description=description,
    )


def build_yolo_standard_detection_job(*, description: str):
    return define_asset_job(
        "yolo_standard_detection_job",
        selection=[yolo_image_detection],
        tags=build_duckdb_writer_tags(DUCKDB_YOLO_WRITER_TAG),
        description=description,
    )


def build_legacy_test_yolo_detection_job(*, description: str):
    from vlm_pipeline.defs.yolo.staging_assets import staging_yolo_image_detection

    return define_asset_job(
        "yolo_detection_job",
        selection=[staging_yolo_image_detection],
        tags=build_duckdb_writer_tags(DUCKDB_YOLO_WRITER_TAG),
        description=description,
    )


def build_staging_yolo_detection_job(*, description: str):
    """Backward-compatible alias for legacy staging-named callers."""
    return build_legacy_test_yolo_detection_job(description=description)


def build_sam3_standard_detection_job(*, description: str):
    return define_asset_job(
        "sam3_standard_detection_job",
        selection=[sam3_image_detection],
        tags=build_duckdb_writer_tags(DUCKDB_SAM3_WRITER_TAG),
        description=description,
    )


def build_legacy_test_sam3_detection_job(*, description: str):
    from vlm_pipeline.defs.sam.staging_detection_assets import staging_sam3_image_detection

    return define_asset_job(
        "sam3_detection_job",
        selection=[staging_sam3_image_detection],
        tags=build_duckdb_writer_tags(DUCKDB_SAM3_WRITER_TAG),
        description=description,
    )


def build_staging_sam3_detection_job(*, description: str):
    """Backward-compatible alias for legacy staging-named callers."""
    return build_legacy_test_sam3_detection_job(description=description)


def build_auto_labeling_routed_job(*, description: str):
    from vlm_pipeline.defs.spec.staging_assets import activate_labeling_spec

    return define_asset_job(
        "auto_labeling_routed_job",
        selection=[
            clip_timestamp,
            clip_captioning,
            clip_to_frame,
            bbox_labeling,
            sam3_bbox_labeling,
            activate_labeling_spec,
        ],
        tags=build_duckdb_writer_tags(DUCKDB_LEGACY_WRITER_TAG),
        description=description,
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


def build_motherduck_daily_schedule(job) -> ScheduleDefinition:
    return ScheduleDefinition(
        name="motherduck_daily_schedule",
        job=job,
        cron_schedule="0 5 * * *",
        execution_timezone="Asia/Seoul",
        run_config={
            "ops": {
                "pipeline__motherduck_sync": {
                    "config": {
                        "enabled": True,
                        "tables": [],
                    }
                }
            }
        },
    )


def build_runtime_assets(
    *,
    enable_manual_label_import: bool,
    enable_yolo_detection: bool,
    enable_sam3_detection: bool = False,
) -> list[object]:
    assets = [
        raw_ingest,
        gcs_download_to_incoming,
        *CLIP_AUTO_LABEL_ASSETS,
        raw_video_to_frame,
        build_dataset,
        motherduck_sync,
        classification_video,
        dispatch_yolo_image_detection,
        dispatch_sam3_image_detection,
        sam3_shadow_compare,
    ]
    if enable_manual_label_import:
        assets.append(manual_label_import)
    if enable_yolo_detection:
        assets.append(yolo_image_detection)
    if enable_sam3_detection:
        assets.append(sam3_image_detection)
    return assets


def build_production_assets(
    *,
    enable_manual_label_import: bool,
    enable_yolo_detection: bool,
    enable_sam3_detection: bool = False,
) -> list[object]:
    """Backward-compatible alias for callers not yet renamed."""
    return build_runtime_assets(
        enable_manual_label_import=enable_manual_label_import,
        enable_yolo_detection=enable_yolo_detection,
        enable_sam3_detection=enable_sam3_detection,
    )


def build_legacy_test_assets() -> list[object]:
    from vlm_pipeline.defs.label.prelabeled_import import prelabeled_import
    from vlm_pipeline.defs.sam.staging_detection_assets import staging_sam3_image_detection
    from vlm_pipeline.defs.spec.assets import labeling_spec_ingest, pending_ingest
    from vlm_pipeline.defs.spec.staging_assets import activate_labeling_spec, config_sync, ingest_router
    from vlm_pipeline.defs.yolo.staging_assets import staging_yolo_image_detection

    return [
        raw_ingest,
        gcs_download_to_incoming,
        raw_video_to_frame,
        manual_label_import,
        prelabeled_import,
        staging_yolo_image_detection,
        staging_sam3_image_detection,
        labeling_spec_ingest,
        config_sync,
        ingest_router,
        pending_ingest,
        activate_labeling_spec,
        *CLIP_AUTO_LABEL_ASSETS,
        classification_video,
        bbox_labeling,
        sam3_bbox_labeling,
        sam3_shadow_compare,
    ]


def build_staging_assets() -> list[object]:
    """Backward-compatible alias for legacy staging-named callers."""
    return build_legacy_test_assets()


def build_runtime_sensors(motherduck_table_sensors: list[object] | tuple[object, ...]) -> list[object]:
    return [
        *COMMON_INGEST_SENSORS,
        dispatch_sensor,
        dispatch_agent_sensor,
        *COMMON_DISPATCH_STATUS_SENSORS,
        *motherduck_table_sensors,
        ls_task_create_sensor,
    ]


def build_production_sensors(motherduck_table_sensors: list[object] | tuple[object, ...]) -> list[object]:
    """Backward-compatible alias for callers not yet renamed."""
    return build_runtime_sensors(motherduck_table_sensors)


def build_legacy_test_sensors(
    *,
    spec_resolve_sensor,
    dispatch_ingress_sensor,
    dispatch_json_sensor,
    sam3_detection_sensor=None,
) -> list[object]:
    sensors = [
        *COMMON_INGEST_SENSORS,
        dispatch_json_sensor,
        dispatch_ingress_sensor,
        *COMMON_DISPATCH_STATUS_SENSORS,
        spec_resolve_sensor,
    ]
    if sam3_detection_sensor is not None:
        sensors.append(sam3_detection_sensor)
    return sensors


def build_staging_sensors(
    *,
    spec_resolve_sensor,
    dispatch_ingress_sensor,
    dispatch_json_sensor,
    sam3_detection_sensor=None,
) -> list[object]:
    """Backward-compatible alias for legacy staging-named callers."""
    return build_legacy_test_sensors(
        spec_resolve_sensor=spec_resolve_sensor,
        dispatch_ingress_sensor=dispatch_ingress_sensor,
        dispatch_json_sensor=dispatch_json_sensor,
        sam3_detection_sensor=sam3_detection_sensor,
    )
