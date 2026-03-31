"""Utilities for YOLO detection payloads and single-image COCO conversion."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
from typing import Any


def _coerce_positive_float(value: object) -> float | None:
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    if rendered <= 0:
        return None
    return rendered


def _coerce_float(value: object, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _normalize_requested_classes(values: object) -> list[str]:
    if not isinstance(values, list):
        return []

    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _parse_detected_at(value: object) -> datetime:
    if isinstance(value, datetime):
        return value

    rendered = str(value or "").strip()
    if not rendered:
        return datetime.now()

    try:
        return datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now()


def _resolve_coco_category_names(detections: list[dict[str, Any]], requested_classes: list[str]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for class_name in requested_classes:
        rendered = str(class_name or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        names.append(rendered)

    for detection in detections:
        rendered = str(detection.get("class") or detection.get("class_name") or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        names.append(rendered)

    return names


def is_coco_detection_payload(payload: Mapping[str, Any] | None) -> bool:
    if not isinstance(payload, Mapping):
        return False

    return (
        isinstance(payload.get("images"), list)
        and isinstance(payload.get("annotations"), list)
        and isinstance(payload.get("categories"), list)
    )


def build_coco_detection_payload(
    *,
    image_id: str,
    source_clip_id: str | None,
    image_key: str,
    image_width: object,
    image_height: object,
    detections: list[dict[str, Any]],
    requested_classes: list[str],
    class_source: str,
    resolved_config_id: str | None,
    confidence_threshold: float,
    iou_threshold: float,
    detected_at: datetime,
    model_name: str = "yolov8l-worldv2",
) -> dict[str, Any]:
    width = int(_coerce_positive_float(image_width) or 0)
    height = int(_coerce_positive_float(image_height) or 0)

    category_names = _resolve_coco_category_names(detections, requested_classes)
    category_id_map = {name: idx + 1 for idx, name in enumerate(category_names)}
    categories = [{"id": cat_id, "name": name, "supercategory": "object"} for name, cat_id in category_id_map.items()]

    annotations: list[dict[str, Any]] = []
    annotation_id = 1
    for detection in detections:
        class_name = str(detection.get("class") or detection.get("class_name") or "").strip().lower()
        if not class_name:
            continue
        category_id = category_id_map.get(class_name)
        if category_id is None:
            continue

        bbox_raw = detection.get("bbox")
        if not isinstance(bbox_raw, list) or len(bbox_raw) < 4:
            continue

        try:
            x1 = float(bbox_raw[0])
            y1 = float(bbox_raw[1])
            x2 = float(bbox_raw[2])
            y2 = float(bbox_raw[3])
        except (TypeError, ValueError):
            continue

        left = min(x1, x2)
        top = min(y1, y2)
        box_width = max(0.0, abs(x2 - x1))
        box_height = max(0.0, abs(y2 - y1))
        if box_width <= 0.0 or box_height <= 0.0:
            continue

        annotation = {
            "id": annotation_id,
            "image_id": 1,
            "category_id": int(category_id),
            "bbox": [round(left, 2), round(top, 2), round(box_width, 2), round(box_height, 2)],
            "area": round(box_width * box_height, 2),
            "iscrowd": 0,
            "segmentation": [],
        }
        confidence = detection.get("confidence")
        try:
            annotation["score"] = round(float(confidence), 4)
        except (TypeError, ValueError):
            pass

        annotations.append(annotation)
        annotation_id += 1

    return {
        "info": {
            "description": "YOLO-World detection result (single-image COCO)",
            "version": "1.0",
            "year": detected_at.year,
            "date_created": detected_at.isoformat(),
        },
        "licenses": [],
        "images": [
            {
                "id": 1,
                "file_name": image_key,
                "width": width,
                "height": height,
            }
        ],
        "annotations": annotations,
        "categories": categories,
        "meta": {
            "source_image_id": image_id,
            "source_clip_id": source_clip_id,
            "model": model_name,
            "confidence_threshold": confidence_threshold,
            "iou_threshold": iou_threshold,
            "requested_classes": requested_classes,
            "requested_classes_count": len(requested_classes),
            "class_source": class_source,
            "resolved_config_id": resolved_config_id,
            "detected_at": detected_at.isoformat(),
        },
    }


def convert_detection_payload_to_coco(
    payload: Mapping[str, Any],
    *,
    fallback_image_id: str,
    fallback_source_clip_id: str | None,
    fallback_image_key: str,
    fallback_image_width: object,
    fallback_image_height: object,
    default_class_source: str = "manual_import",
    default_confidence_threshold: float = 0.25,
    default_iou_threshold: float = 0.45,
    default_model: str = "yolov8l-worldv2",
) -> dict[str, Any]:
    """Convert legacy YOLO detection JSON payload into single-image COCO JSON.

    Legacy payload shape:
    {
      "image_id": "...",
      "source_clip_id": "...",
      "image_key": "...",
      "model": "yolov8l-worldv2",
      "confidence_threshold": 0.25,
      "iou_threshold": 0.45,
      "detections": [...],
      "requested_classes": [...],
      "class_source": "...",
      "resolved_config_id": "...",
      "detected_at": "..."
    }
    """
    if is_coco_detection_payload(payload):
        return deepcopy(dict(payload))

    image_id = str(payload.get("image_id") or fallback_image_id).strip() or fallback_image_id

    source_clip_raw = payload.get("source_clip_id")
    if source_clip_raw is None:
        source_clip_id = fallback_source_clip_id
    else:
        source_clip_id = str(source_clip_raw).strip() or None

    image_key = str(payload.get("image_key") or fallback_image_key).strip() or fallback_image_key

    image_width = payload.get("image_width") or payload.get("width") or fallback_image_width
    image_height = payload.get("image_height") or payload.get("height") or fallback_image_height

    detections_raw = payload.get("detections")
    detections = list(detections_raw) if isinstance(detections_raw, list) else []

    requested_classes = _normalize_requested_classes(payload.get("requested_classes"))

    class_source = str(payload.get("class_source") or "").strip() or default_class_source
    resolved_config_raw = str(payload.get("resolved_config_id") or "").strip()
    resolved_config_id = resolved_config_raw or None

    confidence_threshold = _coerce_float(payload.get("confidence_threshold"), default_confidence_threshold)
    iou_threshold = _coerce_float(payload.get("iou_threshold"), default_iou_threshold)
    detected_at = _parse_detected_at(payload.get("detected_at"))
    model_name = str(payload.get("model") or default_model).strip() or default_model

    return build_coco_detection_payload(
        image_id=image_id,
        source_clip_id=source_clip_id,
        image_key=image_key,
        image_width=image_width,
        image_height=image_height,
        detections=detections,
        requested_classes=requested_classes,
        class_source=class_source,
        resolved_config_id=resolved_config_id,
        confidence_threshold=confidence_threshold,
        iou_threshold=iou_threshold,
        detected_at=detected_at,
        model_name=model_name,
    )
