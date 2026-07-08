"""label_qa_fiftyone 순수 헬퍼 — bbox 정규화 + GT 그룹핑. fiftyone/psycopg2 불필요(lazy import)."""

from __future__ import annotations

import importlib.util
import pathlib

_MOD_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "label_qa_fiftyone.py"


def _load():
    spec = importlib.util.spec_from_file_location("label_qa_fiftyone", _MOD_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # top-level 에 fiftyone/psycopg2 import 없어야 성공
    return mod


def test_to_relative_bbox():
    m = _load()
    assert m.to_relative_bbox(50, 100, 20, 40, 200, 400) == [0.25, 0.25, 0.1, 0.1]


def test_to_relative_bbox_bad_size_is_none():
    m = _load()
    assert m.to_relative_bbox(0, 0, 10, 10, 0, 10) is None
    assert m.to_relative_bbox(0, 0, 10, 10, 100, 0) is None


def test_gt_detections_by_image_groups_and_keeps_labels():
    m = _load()
    rows = [
        {"image_id": "i1", "category": "fire", "bbox_x": 0, "bbox_y": 0, "bbox_w": 5, "bbox_h": 5},
        {"image_id": "i1", "category": "smoke", "bbox_x": 1, "bbox_y": 1, "bbox_w": 2, "bbox_h": 2},
        {"image_id": "i2", "category": "fire", "bbox_x": 3, "bbox_y": 3, "bbox_w": 4, "bbox_h": 4},
    ]
    g = m.gt_detections_by_image(rows)
    assert set(g) == {"i1", "i2"}
    assert len(g["i1"]) == 2 and g["i2"][0]["bbox_abs"] == [3, 3, 4, 4]
    assert {d["label"] for d in g["i1"]} == {"fire", "smoke"}


def test_sam3_detection_key_mirrors_key_builder():
    m = _load()
    # 원본 vlm_pipeline.lib.key_builders.build_sam3_detection_key 와 동일 결과여야 함
    from vlm_pipeline.lib.key_builders import build_sam3_detection_key

    for k in ("proj/a/frames/f1.jpg", "proj/a/image/f1.jpg", "f1.jpg", "a/b/c/image/x.png"):
        assert m.sam3_detection_key(k) == build_sam3_detection_key(k)


def test_pseudo_bbox_key_mirrors_key_builder():
    m = _load()
    # 원본 vlm_pipeline.lib.key_builders.build_pseudo_bbox_key 와 동일 결과여야 함 (C-1 스냅샷 키)
    from vlm_pipeline.lib.key_builders import build_pseudo_bbox_key

    for k in ("proj/a/frames/f1.jpg", "proj/a/image/f1.jpg", "f1.jpg", "a/b/c/image/x.png"):
        assert m.pseudo_bbox_key(k) == build_pseudo_bbox_key(k)


def test_ls_sync_converters_sam3_key_mirrors_key_builder():
    """L-5 drift guard: src/gemini/ls_sync_converters.py 의 인라인 _sam3_key_from_image_key 가
    build_sam3_detection_key() 와 동일 규약을 유지하는지(원본 키빌더 변경 시 조용한 drift 방지)."""
    from gemini.ls_sync_converters import _sam3_key_from_image_key
    from vlm_pipeline.lib.key_builders import build_sam3_detection_key

    for k in ("proj/a/frames/f1.jpg", "proj/a/image/f1.jpg", "f1.jpg", "a/b/c/image/x.png"):
        assert _sam3_key_from_image_key(k) == build_sam3_detection_key(k)


def test_coco_boxes_mirrors_parser():
    m = _load()
    from vlm_pipeline.lib.detection_coco import parse_coco_annotation_boxes

    payload = {
        "images": [{"id": 1, "file_name": "f.jpg"}],
        "categories": [{"id": 1, "name": "fire"}, {"id": 2, "name": "smoke"}],
        "annotations": [
            {"category_id": 1, "bbox": [0, 0, 10, 10], "score": 0.9},
            {"category_id": 2, "bbox": [1, 1, -3, 5]},  # w<=0 → skip
            {"category_id": 2, "bbox": [2, 2, 4, 4], "score": 5.0},  # score 범위밖 → None
        ],
    }
    mine = m.coco_boxes(payload)
    ref = parse_coco_annotation_boxes(payload)
    # 카테고리/개수/좌표/스코어가 원본 파서와 일치 (필드명만: bbox_abs vs bbox_x/y/w/h)
    assert len(mine) == len(ref) == 2
    assert [b["category"] for b in mine] == [r["category"] for r in ref]
    assert mine[0]["bbox_abs"] == [ref[0]["bbox_x"], ref[0]["bbox_y"], ref[0]["bbox_w"], ref[0]["bbox_h"]]
    assert mine[0]["score"] == ref[0]["score"] and mine[1]["score"] is None
