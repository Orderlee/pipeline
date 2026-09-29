from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("dagster")

from vlm_pipeline.defs.yolo.assets import _build_coco_detection_payload  # noqa: E402
from vlm_pipeline.lib.detection_coco import convert_detection_payload_to_coco  # noqa: E402


def test_build_coco_detection_payload_converts_xyxy_to_coco_bbox() -> None:
    payload = _build_coco_detection_payload(
        image_id="img-1",
        source_clip_id="clip-1",
        image_key="folder/image/frame_0001.jpg",
        image_width=1920,
        image_height=1080,
        detections=[
            {
                "class": "smoke",
                "confidence": 0.95,
                "bbox": [100.0, 120.0, 220.0, 320.0],
            }
        ],
        requested_classes=["smoke", "fire"],
        class_source="dispatch_tags",
        resolved_config_id="cfg-1",
        confidence_threshold=0.25,
        iou_threshold=0.45,
        detected_at=datetime(2026, 3, 30, 13, 0, 0),
        effective_request_confidence_threshold=0.25,
        class_confidence_thresholds={"smoke": 0.25, "fire": 0.25},
    )

    assert set(payload.keys()) == {"info", "licenses", "images", "annotations", "categories", "meta"}
    assert payload["images"] == [
        {
            "id": 1,
            "file_name": "folder/image/frame_0001.jpg",
            "width": 1920,
            "height": 1080,
        }
    ]
    assert payload["categories"] == [
        {"id": 1, "name": "smoke", "supercategory": "object"},
        {"id": 2, "name": "fire", "supercategory": "object"},
    ]

    annotation = payload["annotations"][0]
    assert annotation["image_id"] == 1
    assert annotation["category_id"] == 1
    assert annotation["bbox"] == [100.0, 120.0, 120.0, 200.0]
    assert annotation["area"] == 24000.0
    assert annotation["iscrowd"] == 0
    assert annotation["segmentation"] == []
    assert annotation["score"] == 0.95
    assert payload["meta"]["effective_request_confidence_threshold"] == 0.25
    assert payload["meta"]["class_confidence_thresholds"] == {"fire": 0.25, "smoke": 0.25}


def test_build_coco_detection_payload_adds_detected_class_not_in_requested_classes() -> None:
    payload = _build_coco_detection_payload(
        image_id="img-2",
        source_clip_id=None,
        image_key="frame.jpg",
        image_width=640,
        image_height=480,
        detections=[
            {"class": "knife", "bbox": [10, 20, 30, 60]},
        ],
        requested_classes=[],
        class_source="server_default",
        resolved_config_id=None,
        confidence_threshold=0.2,
        iou_threshold=0.5,
        detected_at=datetime(2026, 3, 30, 13, 5, 0),
        effective_request_confidence_threshold=0.2,
        class_confidence_thresholds={},
    )

    assert payload["categories"] == [
        {"id": 1, "name": "knife", "supercategory": "object"},
    ]
    assert payload["annotations"][0]["category_id"] == 1


def test_convert_detection_payload_to_coco_converts_legacy_payload() -> None:
    payload = convert_detection_payload_to_coco(
        {
            "image_id": "legacy-image",
            "source_clip_id": "legacy-clip",
            "image_key": "unit/image/frame_0001.jpg",
            "model": "yolov8l-worldv2",
            "confidence_threshold": 0.33,
            "iou_threshold": 0.52,
            "requested_classes": ["Smoke", "Person"],
            "class_source": "manual_json",
            "resolved_config_id": "cfg-legacy",
            "detected_at": "2026-03-30T15:10:00",
            "detections": [
                {
                    "class_name": "smoke",
                    "confidence": 0.88,
                    "bbox": [10, 20, 50, 80],
                }
            ],
        },
        fallback_image_id="fallback-image",
        fallback_source_clip_id=None,
        fallback_image_key="fallback/frame.jpg",
        fallback_image_width=1280,
        fallback_image_height=720,
    )

    assert payload["images"] == [
        {
            "id": 1,
            "file_name": "unit/image/frame_0001.jpg",
            "width": 1280,
            "height": 720,
        }
    ]
    assert payload["categories"] == [
        {"id": 1, "name": "smoke", "supercategory": "object"},
        {"id": 2, "name": "person", "supercategory": "object"},
    ]
    assert payload["annotations"] == [
        {
            "id": 1,
            "image_id": 1,
            "category_id": 1,
            "bbox": [10.0, 20.0, 40.0, 60.0],
            "area": 2400.0,
            "iscrowd": 0,
            "segmentation": [],
            "score": 0.88,
        }
    ]
    assert payload["meta"]["class_source"] == "manual_json"
    assert payload["meta"]["resolved_config_id"] == "cfg-legacy"
    assert payload["meta"]["requested_classes"] == ["smoke", "person"]
    assert payload["meta"]["effective_request_confidence_threshold"] == 0.25
    # smoke 는 lib/yolo_thresholds.py:60 에서 0.56 으로 튜닝됨 — person 은 전역 기본값
    assert payload["meta"]["class_confidence_thresholds"] == {"person": 0.25, "smoke": 0.56}


def test_convert_detection_payload_to_coco_keeps_existing_coco_shape() -> None:
    original = {
        "info": {"description": "already coco"},
        "licenses": [],
        "images": [{"id": 1, "file_name": "frame.jpg", "width": 640, "height": 480}],
        "annotations": [],
        "categories": [],
        "meta": {"source_image_id": "img-1"},
    }

    payload = convert_detection_payload_to_coco(
        original,
        fallback_image_id="fallback-image",
        fallback_source_clip_id=None,
        fallback_image_key="fallback/frame.jpg",
        fallback_image_width=640,
        fallback_image_height=480,
    )

    assert payload == original
    assert payload is not original
