"""Pure box-mAP scorer (SAM3 GT-anchored AP). stdlib+numpy only — no pycocotools, no GPU."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load(mod_name: str, rel: str):
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / rel
    spec = importlib.util.spec_from_file_location(mod_name, src)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_bm = _load("vlm_pipeline_lib_box_map_fresh", "box_map.py")
box_average_precision = _bm.box_average_precision
mean_average_precision = _bm.mean_average_precision


def test_ap_perfect_single_box() -> None:
    gt = {"fire": [[0.0, 0.0, 10.0, 10.0]]}
    pred = {"fire": [([0.0, 0.0, 10.0, 10.0], 0.9)]}
    ap = box_average_precision(gt, pred, iou_threshold=0.5)
    assert ap["fire"] == 1.0


def test_ap_no_pred_is_zero() -> None:
    gt = {"fire": [[0.0, 0.0, 10.0, 10.0]]}
    pred: dict[str, list] = {"fire": []}
    ap = box_average_precision(gt, pred, iou_threshold=0.5)
    assert ap["fire"] == 0.0


def test_map_two_classes_perfect() -> None:
    gt = [{"fire": [[0, 0, 10, 10]], "smoke": [[20, 20, 30, 30]]}]
    pred = [{"fire": [([0, 0, 10, 10], 0.9)], "smoke": [([20, 20, 30, 30], 0.8)]}]
    out = mean_average_precision(gt, pred, iou_thresholds=(0.5,))
    assert out["map"] == 1.0
    assert out["per_class_ap"] == {"fire": 1.0, "smoke": 1.0}
    assert out["image_count"] == 1


def test_map_pools_across_images() -> None:
    gt = [{"fire": [[0, 0, 10, 10]]}, {"fire": [[0, 0, 10, 10]]}]
    # one image perfect, one image misses (low IoU box) -> recall 0.5 -> AP 0.5
    pred = [{"fire": [([0, 0, 10, 10], 0.9)]}, {"fire": [([100, 100, 110, 110], 0.9)]}]
    out = mean_average_precision(gt, pred, iou_thresholds=(0.5,))
    assert out["per_class_ap"]["fire"] == 0.5
    assert out["map"] == 0.5


def test_map_length_mismatch_raises() -> None:
    import pytest
    with pytest.raises(ValueError):
        mean_average_precision([{"a": []}], [], iou_thresholds=(0.5,))
