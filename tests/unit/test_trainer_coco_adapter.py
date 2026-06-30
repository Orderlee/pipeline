"""trainer_lib.build_sam3_coco_annfile() — COCO → per-split sam3 annFile adapter.

Pure function (no torch/sam3 import). Filters COCO to a split's image_ids, keeps
ALL categories (sam3 COCO_FROM_JSON makes one text-query per category), preserves
absolute xywh boxes (sam3 loader normalizes by w/h), and orders images by id.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
_spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
trainer_lib = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trainer_lib)


def _coco() -> dict:
    return {
        "images": [
            {"id": 2, "file_name": "b.jpg", "width": 640, "height": 480},
            {"id": 1, "file_name": "a.jpg", "width": 1920, "height": 1080},
            {"id": 3, "file_name": "c.jpg", "width": 800, "height": 600},
        ],
        "annotations": [
            {"id": 10, "image_id": 1, "category_id": 1, "bbox": [100, 200, 50, 60]},
            {"id": 11, "image_id": 2, "category_id": 2, "bbox": [10, 20, 5, 6]},
            {"id": 12, "image_id": 3, "category_id": 1, "bbox": [1, 2, 3, 4]},
        ],
        "categories": [{"id": 1, "name": "fire"}, {"id": 2, "name": "smoke"}],
    }


def test_filters_to_split_image_ids_and_orders_by_id() -> None:
    out = trainer_lib.build_sam3_coco_annfile(_coco(), split_image_ids=[3, 1])
    assert [img["id"] for img in out["images"]] == [1, 3]
    assert {a["image_id"] for a in out["annotations"]} == {1, 3}
    # boxes untouched (absolute xywh)
    assert out["annotations"][0]["bbox"] == [100, 200, 50, 60]


def test_keeps_all_categories_even_if_absent_in_split() -> None:
    out = trainer_lib.build_sam3_coco_annfile(_coco(), split_image_ids=[1])
    # only category 1 used by image 1, but both categories must survive (query coverage)
    assert sorted(c["id"] for c in out["categories"]) == [1, 2]


def test_empty_split_yields_no_images_no_annotations() -> None:
    out = trainer_lib.build_sam3_coco_annfile(_coco(), split_image_ids=[])
    assert out["images"] == []
    assert out["annotations"] == []
    assert sorted(c["id"] for c in out["categories"]) == [1, 2]
