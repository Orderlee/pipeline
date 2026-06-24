"""detection_coco.parse_coco_annotation_boxes() 단위 검증.

image_label_annotations projection 의 입력 파서 — COCO payload → per-box dict.
순수 함수(외부 의존 없음). box_index 정렬, category 매핑/fallback, malformed skip,
score CHECK([0,1]) 범위 클램프(범위 밖→None, 박스 보존)를 회귀 방지한다.
"""

from __future__ import annotations

import sys
from pathlib import Path

# 워크트리 src 가 PYTHONPATH 에 없으면 삽입 (editable install 이 다른 clone 을 가리키는 경우 대비).
_SRC = Path(__file__).resolve().parents[2] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from vlm_pipeline.lib.detection_coco import parse_coco_annotation_boxes  # noqa: E402


def _coco(annotations: list[dict], categories: list[dict] | None = None) -> dict:
    return {
        "images": [{"id": 1, "file_name": "f.jpg", "width": 1920, "height": 1080}],
        "annotations": annotations,
        "categories": categories
        if categories is not None
        else [{"id": 1, "name": "fire"}, {"id": 2, "name": "person on the ground"}],
    }


def test_parses_boxes_with_category_and_box_index() -> None:
    payload = _coco(
        [
            {"id": 10, "category_id": 1, "bbox": [100, 200, 50, 60], "score": 0.9},
            {"id": 11, "category_id": 2, "bbox": [10, 20, 5, 6]},
        ]
    )
    boxes = parse_coco_annotation_boxes(payload)
    assert len(boxes) == 2
    assert boxes[0] == {
        "box_index": 0,
        "category": "fire",
        "bbox_x": 100.0,
        "bbox_y": 200.0,
        "bbox_w": 50.0,
        "bbox_h": 60.0,
        "score": 0.9,
    }
    assert boxes[1]["box_index"] == 1
    assert boxes[1]["category"] == "person on the ground"
    assert boxes[1]["score"] is None  # score 없음


def test_unknown_category_id_falls_back_to_str_id() -> None:
    payload = _coco([{"category_id": 99, "bbox": [0, 0, 4, 4]}], categories=[{"id": 1, "name": "fire"}])
    boxes = parse_coco_annotation_boxes(payload)
    assert boxes[0]["category"] == "99"


def test_skips_malformed_boxes() -> None:
    payload = _coco(
        [
            {"category_id": 1, "bbox": [0, 0, 10, 10]},  # ok
            {"category_id": 1, "bbox": [0, 0, 0, 10]},  # w<=0 → skip
            {"category_id": 1, "bbox": [-1, 0, 10, 10]},  # x<0 → skip
            {"category_id": 1, "bbox": [0, 0, 10]},  # len<4 → skip
            {"category_id": 1},  # bbox 없음 → skip
            "not-a-dict",  # → skip
        ]
    )
    boxes = parse_coco_annotation_boxes(payload)
    assert len(boxes) == 1
    # box_index 는 원본 배열 위치를 유지 (skip 돼도 enumerate index)
    assert boxes[0]["box_index"] == 0


def test_score_out_of_range_dropped_box_kept() -> None:
    payload = _coco([{"category_id": 1, "bbox": [0, 0, 10, 10], "score": 4.2}])
    boxes = parse_coco_annotation_boxes(payload)
    assert len(boxes) == 1
    assert boxes[0]["score"] is None  # 0..1 밖 → None (CHECK 제약 회피), 박스는 보존


def test_non_coco_payload_returns_empty() -> None:
    assert parse_coco_annotation_boxes(None) == []
    assert parse_coco_annotation_boxes([]) == []
    assert parse_coco_annotation_boxes({"foo": "bar"}) == []
