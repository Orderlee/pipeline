"""pseudo_label_*_qa 러너 — mock db+minio 로 GT-존재 시나리오 end-to-end.

bbox pseudo = 생성 시점 write-once 스냅샷(sam3_segmentations/<stem>.pseudo.json), GT = image_label_annotations.
timestamp pseudo = 보존 스냅샷(*.pseudo.json), GT = finalized labels 이벤트.

C-1: bbox 는 라이브 sam3_segmentations/<stem>.json 을 pseudo 로 읽지 않는다 — LS 리뷰가
그 키를 사람수정본으로 in-place 덮어써 pseudo==GT 로 오염되기 때문. 반드시 `.pseudo.json`.
"""

from __future__ import annotations

from vlm_pipeline.defs.train.label_qa import (
    _run_bbox_label_qa,
    _run_timestamp_label_qa,
    format_qa_report_md,
)
from vlm_pipeline.lib.key_builders import build_pseudo_bbox_key, build_pseudo_events_key


class _DB:
    def __init__(self, *, bbox_rows=None, ts_rows=None):
        self._bbox = bbox_rows or []
        self._ts = ts_rows or []

    def find_sam3_finalized_bbox_candidates(self, folder=None):
        return self._bbox

    def find_finalized_timestamp_events(self, folder=None):
        return self._ts


class _MinIO:
    def __init__(self, objs):
        self._objs = objs  # {key: parsed json}

    def exists(self, bucket, key):
        return key in self._objs

    def download_json(self, bucket, key):
        return self._objs[key]


def _coco(cls, bbox):
    return {
        "images": [{"id": 1, "file_name": "f.jpg"}],
        "annotations": [{"id": 1, "image_id": 1, "category_id": 1, "bbox": bbox}],
        "categories": [{"id": 1, "name": cls}],
    }


def _gt_box(cls, x, y, w, h, image_key="src/a/frames/f1.jpg", image_id="img1"):
    return {
        "image_id": image_id,
        "image_key": image_key,
        "category": cls,
        "bbox_x": x,
        "bbox_y": y,
        "bbox_w": w,
        "bbox_h": h,
    }


def test_bbox_qa_perfect_match():
    key = build_pseudo_bbox_key("src/a/frames/f1.jpg")
    db = _DB(bbox_rows=[_gt_box("fire", 0, 0, 10, 10)])
    minio = _MinIO({key: _coco("fire", [0, 0, 10, 10])})  # pseudo snapshot == GT box
    r = _run_bbox_label_qa(db, minio)
    assert r["gt_items"] == 1 and r["missing_pseudo"] == 0
    assert r["per_class"]["fire"]["f1"] == 1.0
    assert r["modality"] == "bbox"


def test_bbox_qa_ignores_live_key_reviewed_by_ls():
    """C-1 regression: 라이브 sam3_segmentations/<stem>.json(LS 사람수정본)만 있고
    .pseudo.json 스냅샷이 없으면 pseudo==GT 로 오염되지 않고 missing_pseudo 로 집계돼야 함."""
    live_key = "src/a/frames/sam3_segmentations/f1.json"
    db = _DB(bbox_rows=[_gt_box("fire", 0, 0, 10, 10)])
    minio = _MinIO({live_key: _coco("fire", [0, 0, 10, 10])})  # 스냅샷 없음, 라이브 키만 존재
    r = _run_bbox_label_qa(db, minio)
    assert r["missing_pseudo"] == 1
    assert r["per_class"]["fire"]["recall"] == 0.0
    assert r["per_class"]["fire"]["fn"] == 1


def test_bbox_qa_missing_pseudo_is_recall_miss():
    db = _DB(bbox_rows=[_gt_box("fire", 0, 0, 10, 10)])
    minio = _MinIO({})  # 원본 COCO 없음 → 미검출로 취급
    r = _run_bbox_label_qa(db, minio)
    assert r["missing_pseudo"] == 1
    assert r["per_class"]["fire"]["recall"] == 0.0
    assert r["per_class"]["fire"]["fn"] == 1


def test_bbox_qa_empty_gt_is_empty_report():
    r = _run_bbox_label_qa(_DB(bbox_rows=[]), _MinIO({}))
    assert r["gt_items"] == 0 and r["per_class"] == {} and r["macro_f1"] == 0.0


def test_timestamp_qa_temporal_match():
    lk = "src/a/events/v1.json"
    pk = build_pseudo_events_key(lk)  # src/a/events/v1.pseudo.json
    db = _DB(
        ts_rows=[{"labels_key": lk, "asset_id": "a1", "timestamp_start_sec": 1.0, "timestamp_end_sec": 9.0}]
    )  # GT [1,9]
    minio = _MinIO({pk: [{"category": "fire", "timestamp": [0.0, 10.0]}]})  # pseudo [0,10], tIoU 0.8
    r = _run_timestamp_label_qa(db, minio)
    assert r["gt_items"] == 1 and r["missing_pseudo"] == 0
    assert r["per_class"]["event"]["f1"] == 1.0
    assert r["modality"] == "timestamp"


def test_timestamp_qa_missing_snapshot_is_recall_miss():
    db = _DB(
        ts_rows=[
            {"labels_key": "x/events/v.json", "asset_id": "a", "timestamp_start_sec": 0.0, "timestamp_end_sec": 5.0}
        ]
    )
    minio = _MinIO({})  # 보존 스냅샷 없음(스냅샷 배포 전 finalize 된 옛 영상)
    r = _run_timestamp_label_qa(db, minio)
    assert r["missing_pseudo"] == 1
    assert r["per_class"]["event"]["recall"] == 0.0


def test_format_qa_report_md_renders_table():
    key = build_pseudo_bbox_key("src/a/frames/f1.jpg")
    db = _DB(bbox_rows=[_gt_box("fire", 0, 0, 10, 10)])
    md = format_qa_report_md(_run_bbox_label_qa(db, _MinIO({key: _coco("fire", [0, 0, 10, 10])})))
    assert "| class | precision | recall | f1 |" in md
    assert "| fire |" in md and "**micro**" in md and "bbox QA" in md
