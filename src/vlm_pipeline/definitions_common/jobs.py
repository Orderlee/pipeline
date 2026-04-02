"""공통 Dagster job 빌더."""

from __future__ import annotations

from dagster import define_asset_job

from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.yolo.assets import bbox_labeling, yolo_image_detection
from vlm_pipeline.defs.yolo.staging_assets import staging_yolo_image_detection

from .assets import PRODUCTION_DISPATCH_STAGE_SELECTION, STAGING_DISPATCH_STAGE_SELECTION


def build_ingest_job(*, description: str):
    return define_asset_job(
        "ingest_job",
        selection=[raw_ingest],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_mvp_stage_job(*, description: str):
    # 레거시 이름 유지 — dedup/label/process 센서가 이 잡 이름을 참조.
    # ingest_job과 selection 동일하나, 센서 트리거 체인이 다름.
    return define_asset_job(
        "mvp_stage_job",
        selection=[raw_ingest],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_gcs_download_job(*, description: str, tags: dict[str, str] | None = None):
    return define_asset_job(
        "gcs_download_job",
        selection=[gcs_download_to_incoming],
        tags=tags,
        description=description,
    )


def build_dispatch_stage_job(*, description: str, staging: bool):
    return define_asset_job(
        "dispatch_stage_job",
        selection=STAGING_DISPATCH_STAGE_SELECTION if staging else PRODUCTION_DISPATCH_STAGE_SELECTION,
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_motherduck_sync_job(*, description: str):
    return define_asset_job(
        "motherduck_sync_job",
        selection=[motherduck_sync],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_manual_label_import_job(*, description: str):
    return define_asset_job(
        "manual_label_import_job",
        selection=[manual_label_import],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_prelabeled_import_job(*, description: str):
    # staging 전용 — 순환 참조 회피를 위해 지연 import
    from vlm_pipeline.defs.label.prelabeled_import import prelabeled_import

    return define_asset_job(
        "prelabeled_import_job",
        selection=[prelabeled_import],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_sam3_shadow_compare_job(*, description: str):
    return define_asset_job(
        "sam3_shadow_compare_job",
        selection=[sam3_shadow_compare],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_yolo_standard_detection_job(*, description: str):
    return define_asset_job(
        "yolo_standard_detection_job",
        selection=[yolo_image_detection],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_staging_yolo_detection_job(*, description: str):
    return define_asset_job(
        "yolo_detection_job",
        selection=[staging_yolo_image_detection],
        tags={"duckdb_writer": "true"},
        description=description,
    )


def build_auto_labeling_routed_job(*, description: str):
    # staging 전용 — 순환 참조 회피를 위해 지연 import
    from vlm_pipeline.defs.spec.staging_assets import activate_labeling_spec

    from .assets import CLIP_AUTO_LABEL_ASSETS

    return define_asset_job(
        "auto_labeling_routed_job",
        selection=[
            *CLIP_AUTO_LABEL_ASSETS,
            bbox_labeling,
            activate_labeling_spec,
        ],
        tags={"duckdb_writer": "true"},
        description=description,
    )
