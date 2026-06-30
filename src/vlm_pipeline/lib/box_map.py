"""GT-anchored box AP / mAP (Pascal-VOC 11-free, COCO-style all-point interpolation).

Pure Python + numpy. Used by defs/train/eval.py for SAM3 detection eval when the
official sam3.eval.coco_eval (pycocotools) is NOT importable (CI / dagster image).
Boxes are xyxy floats. Predictions are (box, score) with score in [0, 1].

Layer: L1-2 pure lib. No dagster / defs / resources imports.
"""

from __future__ import annotations

from typing import Any

import numpy as np


def _iou(box_a: list[float], box_b: list[float]) -> float:
    ax1, ay1, ax2, ay2 = (float(v) for v in box_a[:4])
    bx1, by1, bx2, by2 = (float(v) for v in box_b[:4])
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0.0:
        return 0.0
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - inter
    return inter / union if union > 0.0 else 0.0


def _ap_from_pr(recall: np.ndarray, precision: np.ndarray) -> float:
    """COCO-style all-point AP: monotonize precision then integrate over recall."""
    mrec = np.concatenate(([0.0], recall, [1.0]))
    mpre = np.concatenate(([0.0], precision, [0.0]))
    for i in range(mpre.size - 1, 0, -1):
        mpre[i - 1] = max(mpre[i - 1], mpre[i])
    idx = np.where(mrec[1:] != mrec[:-1])[0]
    return float(np.sum((mrec[idx + 1] - mrec[idx]) * mpre[idx + 1]))


def _ap_single_class(
    gts: list[list[float]],
    preds: list[tuple[list[float], float]],
    iou_threshold: float,
) -> float:
    n_gt = len(gts)
    if n_gt == 0:
        # No GT for this class: AP is undefined -> treat as 1.0 if also no preds,
        # else 0.0 (false positives only). Callers exclude classes absent from class_map.
        return 1.0 if not preds else 0.0
    if not preds:
        return 0.0
    ordered = sorted(preds, key=lambda p: float(p[1]), reverse=True)
    matched = [False] * n_gt
    tp = np.zeros(len(ordered), dtype=np.float64)
    fp = np.zeros(len(ordered), dtype=np.float64)
    for k, (pbox, _score) in enumerate(ordered):
        best_iou, best_j = 0.0, -1
        for j, gbox in enumerate(gts):
            if matched[j]:
                continue
            iou = _iou(pbox, gbox)
            if iou > best_iou:
                best_iou, best_j = iou, j
        if best_j >= 0 and best_iou >= iou_threshold:
            matched[best_j] = True
            tp[k] = 1.0
        else:
            fp[k] = 1.0
    cum_tp = np.cumsum(tp)
    cum_fp = np.cumsum(fp)
    recall = cum_tp / float(n_gt)
    precision = cum_tp / np.maximum(cum_tp + cum_fp, np.finfo(np.float64).eps)
    return round(_ap_from_pr(recall, precision), 6)


def box_average_precision(
    gt_by_class: dict[str, list[list[float]]],
    pred_by_class: dict[str, list[tuple[list[float], float]]],
    iou_threshold: float = 0.5,
) -> dict[str, float]:
    """Per-class AP at one IoU threshold for a single image (or pooled set)."""
    classes = set(gt_by_class) | set(pred_by_class)
    return {
        cls: _ap_single_class(
            gt_by_class.get(cls, []),
            pred_by_class.get(cls, []),
            iou_threshold,
        )
        for cls in classes
    }


def mean_average_precision(
    per_image_gt: list[dict[str, list[list[float]]]],
    per_image_pred: list[dict[str, list[tuple[list[float], float]]]],
    iou_thresholds: tuple[float, ...] = (0.5,),
) -> dict[str, Any]:
    """Pool detections across images per class, average AP over IoU thresholds.

    per_image_gt[i] / per_image_pred[i] correspond to the same image i.
    Returns {"map", "per_class_ap", "iou_thresholds", "image_count"}.
    """
    if len(per_image_gt) != len(per_image_pred):
        raise ValueError("per_image_gt and per_image_pred length mismatch")
    pooled_gt: dict[str, list[list[float]]] = {}
    pooled_pred: dict[str, list[tuple[list[float], float]]] = {}
    for gt_img, pred_img in zip(per_image_gt, per_image_pred):
        for cls, boxes in gt_img.items():
            pooled_gt.setdefault(cls, []).extend(boxes)
        for cls, dets in pred_img.items():
            pooled_pred.setdefault(cls, []).extend(dets)
    classes = sorted(set(pooled_gt) | set(pooled_pred))
    per_class_ap: dict[str, float] = {}
    for cls in classes:
        thr_aps = [
            _ap_single_class(pooled_gt.get(cls, []), pooled_pred.get(cls, []), thr)
            for thr in iou_thresholds
        ]
        per_class_ap[cls] = round(float(np.mean(thr_aps)), 6)
    map_value = round(float(np.mean(list(per_class_ap.values()))), 6) if per_class_ap else 0.0
    return {
        "map": map_value,
        "per_class_ap": per_class_ap,
        "iou_thresholds": list(iou_thresholds),
        "image_count": len(per_image_gt),
    }
