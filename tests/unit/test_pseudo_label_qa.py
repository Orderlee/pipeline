"""pseudo_label_qa — pure P/R/F1 scorer (bbox spatial + timestamp temporal). No infra."""

from __future__ import annotations

from vlm_pipeline.lib.pseudo_label_qa import score_bbox_quality, score_timestamp_quality


def test_bbox_perfect_match():
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10]]}],
        [{"fire": [([0, 0, 10, 10], 0.9)]}],
    )
    assert r["per_class"]["fire"] == {"tp": 1, "fp": 0, "fn": 0, "precision": 1.0, "recall": 1.0, "f1": 1.0}
    assert r["micro"]["f1"] == 1.0 and r["macro_f1"] == 1.0


def test_bbox_missing_box_drops_recall():
    # GT 2 boxes, pseudo only found 1 → recall 0.5, precision 1.0
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10], [20, 20, 30, 30]]}],
        [{"fire": [([0, 0, 10, 10], 0.9)]}],
    )
    c = r["per_class"]["fire"]
    assert (c["tp"], c["fp"], c["fn"]) == (1, 0, 1)
    assert c["recall"] == 0.5 and c["precision"] == 1.0


def test_bbox_extra_box_drops_precision():
    # GT 1 box, pseudo produced 2 (1 spurious) → precision 0.5, recall 1.0
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10]]}],
        [{"fire": [([0, 0, 10, 10], 0.9), ([50, 50, 60, 60], 0.8)]}],
    )
    c = r["per_class"]["fire"]
    assert (c["tp"], c["fp"], c["fn"]) == (1, 1, 0)
    assert c["precision"] == 0.5 and c["recall"] == 1.0


def test_bbox_wrong_class_is_fp_and_fn():
    # pseudo said 'smoke' where GT is 'fire', same box → fire: fn=1, smoke: fp=1
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10]]}],
        [{"smoke": [([0, 0, 10, 10], 0.9)]}],
    )
    assert r["per_class"]["fire"] == {"tp": 0, "fp": 0, "fn": 1, "precision": 0.0, "recall": 0.0, "f1": 0.0}
    assert r["per_class"]["smoke"]["fp"] == 1
    assert r["micro"]["tp"] == 0


def test_bbox_iou_threshold_rejects_loose_overlap():
    # pred box overlaps GT but IoU < 0.5 → no match (fp + fn)
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10]]}],
        [{"fire": [([5, 5, 15, 15], 0.9)]}],  # IoU = 25/175 ≈ 0.14
        iou_threshold=0.5,
    )
    c = r["per_class"]["fire"]
    assert (c["tp"], c["fp"], c["fn"]) == (0, 1, 1)


def test_bbox_score_threshold_filters_low_conf():
    # low-conf pseudo box filtered out → becomes a miss (fn), not a match
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10]]}],
        [{"fire": [([0, 0, 10, 10], 0.2)]}],
        score_threshold=0.5,
    )
    c = r["per_class"]["fire"]
    assert (c["tp"], c["fp"], c["fn"]) == (0, 0, 1)


def test_bbox_macro_vs_micro_across_classes():
    # fire perfect (1/1), smoke all-miss (0/1) → macro_f1 = mean(1.0, 0.0) = 0.5
    r = score_bbox_quality(
        [{"fire": [[0, 0, 10, 10]], "smoke": [[20, 20, 30, 30]]}],
        [{"fire": [([0, 0, 10, 10], 0.9)]}],
    )
    assert r["per_class"]["fire"]["f1"] == 1.0
    assert r["per_class"]["smoke"]["f1"] == 0.0
    assert r["macro_f1"] == 0.5
    # micro pools: tp=1, fp=0, fn=1 → recall 0.5
    assert r["micro"]["recall"] == 0.5


def test_timestamp_temporal_iou_match():
    # GT event [0,10], pseudo [1,9] → tIoU = 8/10 = 0.8 ≥ 0.5 → match
    r = score_timestamp_quality(
        [{"event": [[0.0, 10.0]]}],
        [{"event": [([1.0, 9.0], 0.8)]}],
    )
    assert r["per_class"]["event"]["tp"] == 1 and r["per_class"]["event"]["f1"] == 1.0


def test_timestamp_off_interval_is_miss():
    # GT [0,10], pseudo [20,30] → no overlap → fp + fn
    r = score_timestamp_quality(
        [{"event": [[0.0, 10.0]]}],
        [{"event": [([20.0, 30.0], 0.8)]}],
    )
    c = r["per_class"]["event"]
    assert (c["tp"], c["fp"], c["fn"]) == (0, 1, 1)


def test_length_mismatch_raises():
    import pytest

    with pytest.raises(ValueError):
        score_bbox_quality([{"fire": [[0, 0, 1, 1]]}], [])
