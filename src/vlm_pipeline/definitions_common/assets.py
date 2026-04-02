"""공통 에셋 목록 상수 및 빌더."""

from __future__ import annotations

from vlm_pipeline.defs.build.assets import build_dataset
from vlm_pipeline.defs.gcp.assets import gcs_download_to_incoming
from vlm_pipeline.defs.ingest.assets import raw_ingest
from vlm_pipeline.defs.label.assets import classification_video, clip_timestamp
from vlm_pipeline.defs.label.manual_import import manual_label_import
from vlm_pipeline.defs.process.assets import clip_captioning, clip_to_frame, raw_video_to_frame
from vlm_pipeline.defs.sam.assets import sam3_shadow_compare
from vlm_pipeline.defs.sync.assets import motherduck_sync
from vlm_pipeline.defs.yolo.assets import bbox_labeling, dispatch_yolo_image_detection, yolo_image_detection
from vlm_pipeline.defs.yolo.staging_assets import staging_yolo_image_detection

CLIP_AUTO_LABEL_ASSETS = (
    clip_timestamp,
    clip_captioning,
    clip_to_frame,
)

PRODUCTION_DISPATCH_STAGE_SELECTION = [
    raw_ingest,
    *CLIP_AUTO_LABEL_ASSETS,
    classification_video,
    raw_video_to_frame,
    dispatch_yolo_image_detection,
]

STAGING_DISPATCH_STAGE_SELECTION = [
    raw_ingest,
    *CLIP_AUTO_LABEL_ASSETS,
    classification_video,
    raw_video_to_frame,
    staging_yolo_image_detection,
]


def build_production_assets(*, enable_manual_label_import: bool, enable_yolo_detection: bool) -> list[object]:
    assets = [
        raw_ingest,
        gcs_download_to_incoming,
        *CLIP_AUTO_LABEL_ASSETS,
        raw_video_to_frame,
        build_dataset,
        motherduck_sync,
        classification_video,
        dispatch_yolo_image_detection,
        sam3_shadow_compare,
    ]
    if enable_manual_label_import:
        assets.append(manual_label_import)
    if enable_yolo_detection:
        assets.append(yolo_image_detection)
    return assets


def build_staging_assets() -> list[object]:
    # staging 전용 에셋 — 순환 참조 회피를 위해 지연 import
    from vlm_pipeline.defs.label.prelabeled_import import prelabeled_import
    from vlm_pipeline.defs.spec.assets import labeling_spec_ingest, pending_ingest
    from vlm_pipeline.defs.spec.staging_assets import activate_labeling_spec, config_sync, ingest_router

    return [
        raw_ingest,
        gcs_download_to_incoming,
        raw_video_to_frame,
        manual_label_import,
        prelabeled_import,
        staging_yolo_image_detection,
        labeling_spec_ingest,
        config_sync,
        ingest_router,
        pending_ingest,
        activate_labeling_spec,
        *CLIP_AUTO_LABEL_ASSETS,
        classification_video,
        bbox_labeling,
        sam3_shadow_compare,
    ]
