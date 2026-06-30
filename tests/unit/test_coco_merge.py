"""lib.coco_merge — N개 프로젝트 COCO 병합 (union / allowlist / remap / id 충돌). 순수."""

from __future__ import annotations

from vlm_pipeline.lib.coco_merge import merge_coco

# 두 프로젝트 — image id 가 둘 다 1 부터(충돌), 클래스 일부 겹침/다름.
_A = {
    "images": [{"id": 1, "file_name": "a/x.jpg"}, {"id": 2, "file_name": "a/y.jpg"}],
    "annotations": [
        {"id": 1, "image_id": 1, "category_id": 10, "bbox": [0, 0, 1, 1]},
        {"id": 2, "image_id": 2, "category_id": 11, "bbox": [1, 1, 2, 2]},
    ],
    "categories": [{"id": 10, "name": "fire"}, {"id": 11, "name": "smoke"}],
}
_B = {
    "images": [{"id": 1, "file_name": "b/z.jpg"}],
    "annotations": [
        {"id": 1, "image_id": 1, "category_id": 5, "bbox": [2, 2, 3, 3]},
        {"id": 2, "image_id": 1, "category_id": 6, "bbox": [3, 3, 4, 4]},
    ],
    "categories": [{"id": 5, "name": "flame"}, {"id": 6, "name": "person"}],
}


def test_union_remaps_ids_no_collision():
    merged, prov = merge_coco([("projA", _A), ("projB", _B)])
    # 3 images total, ids unique
    assert len(merged["images"]) == 3
    assert len({im["id"] for im in merged["images"]}) == 3
    # each image tagged with its source
    assert {im["source"] for im in merged["images"]} == {"projA", "projB"}
    # annotations reference valid (remapped) image ids
    img_ids = {im["id"] for im in merged["images"]}
    assert all(a["image_id"] in img_ids for a in merged["annotations"])
    assert len(merged["annotations"]) == 4
    assert prov["projA"] == {"images": 2, "annotations": 2, "orphan_annotations": 0}
    assert prov["projB"] == {"images": 1, "annotations": 2, "orphan_annotations": 0}


def test_orphan_annotation_skipped_not_crash():
    # malformed: annotation references image_id 99 not in images → skip+count, no crash (Codex review).
    bad = {
        "images": [{"id": 1, "file_name": "x.jpg"}],
        "annotations": [
            {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 1, 1]},
            {"id": 2, "image_id": 99, "category_id": 1, "bbox": [0, 0, 1, 1]},  # orphan
        ],
        "categories": [{"id": 1, "name": "fire"}],
    }
    merged, prov = merge_coco([("p", bad)])
    assert len(merged["annotations"]) == 1            # orphan dropped, not crashed
    assert prov["p"] == {"images": 1, "annotations": 1, "orphan_annotations": 1}


def test_allowlist_keeps_only_selected_classes():
    merged, _ = merge_coco([("projA", _A), ("projB", _B)], class_allowlist=["fire", "smoke"])
    names = {c["name"] for c in merged["categories"]}
    assert names == {"fire", "smoke"}            # flame/person dropped
    # only fire+smoke annotations survive (2 from A, 0 from B)
    assert len(merged["annotations"]) == 2
    # images still all present (negatives allowed)
    assert len(merged["images"]) == 3


def test_remap_unifies_synonyms_before_allowlist():
    # projB 의 'flame' → 'fire' 통합, allowlist=fire 면 B 의 flame 도 살아남아 fire 로
    merged, _ = merge_coco(
        [("projA", _A), ("projB", _B)],
        class_remap={"flame": "fire"},
        class_allowlist=["fire"],
    )
    names = {c["name"] for c in merged["categories"]}
    assert names == {"fire"}
    # A.fire(1) + B.flame→fire(1) = 2 fire annotations
    assert len(merged["annotations"]) == 2
    # 하나의 통합 fire category id 로
    assert len({a["category_id"] for a in merged["annotations"]}) == 1


def test_empty_sources():
    merged, prov = merge_coco([])
    assert merged == {"images": [], "annotations": [], "categories": []}
    assert prov == {}
