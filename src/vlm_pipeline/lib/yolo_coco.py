"""Backward-compatibility re-exports — canonical location is ``vlm_pipeline.lib.detection_coco``."""

from vlm_pipeline.lib.detection_coco import (  # noqa: F401
    build_coco_detection_payload,
    convert_detection_payload_to_coco,
    convert_sam3_detections_for_coco,
    is_coco_detection_payload,
)
