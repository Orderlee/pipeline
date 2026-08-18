"""SAM3 detection → COCO 변환 테스트.

convert_sam3_detections_for_coco 헬퍼와,
build_coco_detection_payload와 결합 시 올바른 COCO 구조가 나오는지 검증.
"""

from __future__ import annotations

from datetime import datetime


from vlm_pipeline.lib.detection_coco import (
    build_coco_detection_payload,
    convert_sam3_detections_for_coco,
    is_coco_detection_payload,
)


# ---------------------------------------------------------------------------
# convert_sam3_detections_for_coco 단위 테스트
# ---------------------------------------------------------------------------


class TestSam3DetectionsToCoco:
    def test_basic_conversion(self):
        sam_dets = [
            {
                "prompt_class": "fire",
                "mask_bbox": [10.0, 20.0, 40.0, 60.0],
                "model_box": [10.0, 20.0, 40.0, 60.0],
                "score": 0.91,
                "mask_rle": {"size": [10, 10], "counts": [100]},
            },
        ]
        result = convert_sam3_detections_for_coco(sam_dets)
        assert len(result) == 1
        assert result[0]["class"] == "fire"
        assert result[0]["bbox"] == [10.0, 20.0, 40.0, 60.0]
        assert result[0]["confidence"] == 0.91

    def test_multiple_detections(self):
        sam_dets = [
            {"prompt_class": "person", "mask_bbox": [0, 0, 100, 200], "score": 0.8},
            {"prompt_class": "car", "mask_bbox": [50, 50, 150, 250], "score": 0.6},
        ]
        result = convert_sam3_detections_for_coco(sam_dets)
        assert len(result) == 2
        assert result[0]["class"] == "person"
        assert result[1]["class"] == "car"

    def test_missing_prompt_class_skipped(self):
        sam_dets = [
            {"prompt_class": "", "mask_bbox": [0, 0, 10, 10], "score": 0.5},
            {"mask_bbox": [0, 0, 10, 10], "score": 0.5},
        ]
        result = convert_sam3_detections_for_coco(sam_dets)
        assert len(result) == 0

    def test_missing_mask_bbox_skipped(self):
        sam_dets = [
            {"prompt_class": "fire", "score": 0.9},
            {"prompt_class": "fire", "mask_bbox": [1, 2], "score": 0.9},
        ]
        result = convert_sam3_detections_for_coco(sam_dets)
        assert len(result) == 0

    def test_no_score_omits_confidence(self):
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 30, 40]},
        ]
        result = convert_sam3_detections_for_coco(sam_dets)
        assert len(result) == 1
        assert "confidence" not in result[0]

    def test_class_name_normalized_lowercase(self):
        sam_dets = [
            {"prompt_class": "  Fire  ", "mask_bbox": [0, 0, 10, 10], "score": 0.5},
        ]
        result = convert_sam3_detections_for_coco(sam_dets)
        assert result[0]["class"] == "fire"

    def test_empty_detections(self):
        result = convert_sam3_detections_for_coco([])
        assert result == []


# ---------------------------------------------------------------------------
# build_coco_detection_payload 결합 테스트
# ---------------------------------------------------------------------------


class TestSam3CocoPipeline:
    """SAM3 detection → COCO 변환 → build_coco_detection_payload 전체 파이프라인."""

    def _build_payload(self, sam_dets):
        coco_dets = convert_sam3_detections_for_coco(sam_dets)
        return build_coco_detection_payload(
            image_id="img-001",
            source_clip_id="clip-001",
            image_key="unit/image/frame_0001.jpg",
            image_width=640,
            image_height=480,
            detections=coco_dets,
            requested_classes=["fire", "smoke"],
            class_source="dispatch_tags",
            resolved_config_id=None,
            confidence_threshold=0.0,
            iou_threshold=0.0,
            detected_at=datetime(2026, 4, 8, 12, 0, 0),
            model_name="sam3.1",
        )

    def test_valid_coco_structure(self):
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
        ]
        payload = self._build_payload(sam_dets)
        assert is_coco_detection_payload(payload)
        assert isinstance(payload["images"], list)
        assert isinstance(payload["annotations"], list)
        assert isinstance(payload["categories"], list)
        assert isinstance(payload["meta"], dict)

    def test_model_name_is_sam3(self):
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
        ]
        payload = self._build_payload(sam_dets)
        assert payload["meta"]["model"] == "sam3.1"

    def test_annotations_bbox_xywh(self):
        """COCO bbox는 [x, y, width, height] 형식이어야 함."""
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
        ]
        payload = self._build_payload(sam_dets)
        annotation = payload["annotations"][0]
        bbox = annotation["bbox"]
        assert len(bbox) == 4
        assert bbox[0] == 10.0  # x
        assert bbox[1] == 20.0  # y
        assert bbox[2] == 30.0  # width = 40 - 10
        assert bbox[3] == 40.0  # height = 60 - 20

    def test_annotations_have_score(self):
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.91},
        ]
        payload = self._build_payload(sam_dets)
        assert payload["annotations"][0]["score"] == 0.91

    def test_categories_match_classes(self):
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
            {"prompt_class": "smoke", "mask_bbox": [50, 60, 90, 100], "score": 0.8},
        ]
        payload = self._build_payload(sam_dets)
        cat_names = {c["name"] for c in payload["categories"]}
        assert "fire" in cat_names
        assert "smoke" in cat_names

    def test_annotation_count_matches(self):
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
            {"prompt_class": "fire", "mask_bbox": [100, 200, 140, 260], "score": 0.7},
            {"prompt_class": "smoke", "mask_bbox": [50, 60, 90, 100], "score": 0.8},
        ]
        payload = self._build_payload(sam_dets)
        assert len(payload["annotations"]) == 3

    def test_sam3_meta_extensible(self):
        """SAM3 고유 메타 필드가 COCO meta에 추가 가능한지 검증."""
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
        ]
        payload = self._build_payload(sam_dets)
        payload["meta"]["sam3_total_latency_ms"] = 25.0
        payload["meta"]["sam3_device"] = "cuda:0"
        assert payload["meta"]["sam3_total_latency_ms"] == 25.0
        assert payload["meta"]["sam3_device"] == "cuda:0"

    def test_zero_detections_still_valid_coco(self):
        payload = self._build_payload([])
        assert is_coco_detection_payload(payload)
        assert len(payload["annotations"]) == 0
        assert len(payload["categories"]) >= 2  # requested_classes=["fire", "smoke"]

    def test_info_description(self):
        """info.description이 SAM3 모델명을 반영하지 않아도 valid COCO 구조면 OK."""
        sam_dets = [
            {"prompt_class": "fire", "mask_bbox": [10, 20, 40, 60], "score": 0.9},
        ]
        payload = self._build_payload(sam_dets)
        assert "info" in payload
        assert "description" in payload["info"]
