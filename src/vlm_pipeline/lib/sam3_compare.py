"""Helpers for SAM3 vs YOLO bbox agreement benchmarking."""

from __future__ import annotations

from collections.abc import Mapping
from statistics import median
from typing import Any


IOU_THRESHOLDS = (0.3, 0.5, 0.7)


def xywh_to_xyxy(box: list[float]) -> list[float]:
    x, y, w, h = [float(value) for value in box[:4]]
    return [x, y, x + max(0.0, w), y + max(0.0, h)]


def box_iou(box_a: list[float], box_b: list[float]) -> float:
    ax1, ay1, ax2, ay2 = [float(value) for value in box_a[:4]]
    bx1, by1, bx2, by2 = [float(value) for value in box_b[:4]]
    inter_x1 = max(ax1, bx1)
    inter_y1 = max(ay1, by1)
    inter_x2 = min(ax2, bx2)
    inter_y2 = min(ay2, by2)
    inter_w = max(0.0, inter_x2 - inter_x1)
    inter_h = max(0.0, inter_y2 - inter_y1)
    inter_area = inter_w * inter_h
    if inter_area <= 0.0:
        return 0.0
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union_area = area_a + area_b - inter_area
    if union_area <= 0.0:
        return 0.0
    return inter_area / union_area


def parse_yolo_coco_payload(payload: Mapping[str, Any]) -> dict[str, list[list[float]]]:
    categories = {
        int(category.get("id")): str(category.get("name") or "").strip().lower()
        for category in (payload.get("categories") or [])
        if str(category.get("name") or "").strip()
    }
    boxes_by_class: dict[str, list[list[float]]] = {}
    for annotation in payload.get("annotations") or []:
        category_id = int(annotation.get("category_id") or 0)
        class_name = categories.get(category_id)
        bbox = annotation.get("bbox")
        if not class_name or not isinstance(bbox, list) or len(bbox) < 4:
            continue
        boxes_by_class.setdefault(class_name, []).append(xywh_to_xyxy([float(value) for value in bbox[:4]]))
    return boxes_by_class


def group_sam_detections_by_prompt(detections: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for detection in detections:
        prompt_class = str(detection.get("prompt_class") or "").strip().lower()
        bbox = detection.get("mask_bbox")
        if not prompt_class or not isinstance(bbox, list) or len(bbox) < 4:
            continue
        grouped.setdefault(prompt_class, []).append(detection)
    return grouped


def greedy_match_boxes(
    yolo_boxes: list[list[float]],
    sam_boxes: list[list[float]],
) -> list[tuple[int, int, float]]:
    candidates: list[tuple[float, int, int]] = []
    for yolo_idx, yolo_box in enumerate(yolo_boxes):
        for sam_idx, sam_box in enumerate(sam_boxes):
            candidates.append((box_iou(yolo_box, sam_box), yolo_idx, sam_idx))
    matches: list[tuple[int, int, float]] = []
    used_yolo: set[int] = set()
    used_sam: set[int] = set()
    for score, yolo_idx, sam_idx in sorted(candidates, key=lambda item: item[0], reverse=True):
        if score <= 0.0 or yolo_idx in used_yolo or sam_idx in used_sam:
            continue
        used_yolo.add(yolo_idx)
        used_sam.add(sam_idx)
        matches.append((yolo_idx, sam_idx, score))
    return matches


def mean_abs_box_delta(box_a: list[float] | None, box_b: list[float] | None) -> float | None:
    if not isinstance(box_a, list) or not isinstance(box_b, list) or len(box_a) < 4 or len(box_b) < 4:
        return None
    deltas = [abs(float(box_a[idx]) - float(box_b[idx])) for idx in range(4)]
    return sum(deltas) / 4.0


def compare_prompt_boxes(
    *,
    image_id: str,
    prompt_class: str,
    yolo_boxes: list[list[float]],
    sam_detections: list[dict[str, Any]],
    benchmark_id: str,
    yolo_labels_key: str,
    sam3_labels_key: str,
) -> dict[str, Any]:
    sam_boxes = [list(map(float, detection["mask_bbox"][:4])) for detection in sam_detections]
    matches = greedy_match_boxes(yolo_boxes, sam_boxes)
    matched_ious = [score for *_indices, score in matches]
    total_yolo = len(yolo_boxes)
    total_sam = len(sam_boxes)
    unmatched_yolo = max(0, total_yolo - len(matches))
    unmatched_sam = max(0, total_sam - len(matches))
    model_vs_mask = [
        delta
        for delta in (
            mean_abs_box_delta(detection.get("model_box"), detection.get("mask_bbox"))
            for detection in sam_detections
        )
        if delta is not None
    ]
    return {
        "benchmark_id": benchmark_id,
        "image_id": image_id,
        "prompt_class": prompt_class,
        "yolo_box_count": total_yolo,
        "sam_box_count": total_sam,
        "bbox_count_delta": total_sam - total_yolo,
        "matched_pair_count": len(matches),
        "matched_iou_mean": round(sum(matched_ious) / len(matched_ious), 4) if matched_ious else 0.0,
        "matched_iou_median": round(float(median(matched_ious)), 4) if matched_ious else 0.0,
        "yolo_to_sam_coverage": round(len(matches) / total_yolo, 4) if total_yolo else 0.0,
        "sam_to_yolo_coverage": round(len(matches) / total_sam, 4) if total_sam else 0.0,
        "unmatched_yolo_rate": round(unmatched_yolo / total_yolo, 4) if total_yolo else 0.0,
        "unmatched_sam_rate": round(unmatched_sam / total_sam, 4) if total_sam else 0.0,
        "prompt_hit_rate": 1.0 if total_yolo > 0 and total_sam > 0 else 0.0,
        "sam_model_box_vs_mask_bbox_delta": round(sum(model_vs_mask) / len(model_vs_mask), 4) if model_vs_mask else 0.0,
        "match_count_iou_0_3": sum(1 for score in matched_ious if score >= 0.3),
        "match_count_iou_0_5": sum(1 for score in matched_ious if score >= 0.5),
        "match_count_iou_0_7": sum(1 for score in matched_ious if score >= 0.7),
        "yolo_labels_key": yolo_labels_key,
        "sam3_labels_key": sam3_labels_key,
    }


def summarize_benchmark_rows(
    rows: list[dict[str, Any]],
    *,
    benchmark_id: str,
    total_images: int,
    sam3_total_latency_ms: list[float],
    yolo_latency_ms: list[float],
    gpu_memory_peak_gb: float | None,
) -> dict[str, Any]:
    def _avg(values: list[float]) -> float:
        return round(sum(values) / len(values), 2) if values else 0.0

    def _p95(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * 0.95)))
        return round(float(ordered[index]), 2)

    matched_means = [float(row["matched_iou_mean"]) for row in rows if float(row["matched_iou_mean"]) > 0.0]
    prompt_hits = [float(row["prompt_hit_rate"]) for row in rows]
    yolo_coverages = [float(row["yolo_to_sam_coverage"]) for row in rows]
    sam_coverages = [float(row["sam_to_yolo_coverage"]) for row in rows]
    unmatched_yolo = [float(row["unmatched_yolo_rate"]) for row in rows]
    unmatched_sam = [float(row["unmatched_sam_rate"]) for row in rows]
    model_vs_mask = [float(row["sam_model_box_vs_mask_bbox_delta"]) for row in rows]

    yolo_total_ms = sum(yolo_latency_ms)
    sam_total_ms = sum(sam3_total_latency_ms)
    total_seconds = sam_total_ms / 1000.0 if sam_total_ms > 0 else 0.0

    return {
        "benchmark_id": benchmark_id,
        "total_images": total_images,
        "total_prompt_rows": len(rows),
        "matched_iou_mean": _avg(matched_means),
        "matched_iou_median": round(float(median(matched_means)), 4) if matched_means else 0.0,
        "yolo_to_sam_coverage": _avg(yolo_coverages),
        "sam_to_yolo_coverage": _avg(sam_coverages),
        "bbox_count_delta": round(sum(float(row["bbox_count_delta"]) for row in rows) / len(rows), 2) if rows else 0.0,
        "unmatched_yolo_rate": _avg(unmatched_yolo),
        "unmatched_sam_rate": _avg(unmatched_sam),
        "prompt_hit_rate": _avg(prompt_hits),
        "sam_model_box_vs_mask_bbox_delta": _avg(model_vs_mask),
        "match_rate_iou_0_3": round(sum(int(row["match_count_iou_0_3"]) for row in rows) / max(1, sum(int(row["yolo_box_count"]) for row in rows)), 4) if rows else 0.0,
        "match_rate_iou_0_5": round(sum(int(row["match_count_iou_0_5"]) for row in rows) / max(1, sum(int(row["yolo_box_count"]) for row in rows)), 4) if rows else 0.0,
        "match_rate_iou_0_7": round(sum(int(row["match_count_iou_0_7"]) for row in rows) / max(1, sum(int(row["yolo_box_count"]) for row in rows)), 4) if rows else 0.0,
        "sam3_total_latency_ms": round(sam_total_ms, 2),
        "yolo_total_latency_ms": round(yolo_total_ms, 2),
        "sam3_avg_latency_ms": _avg(sam3_total_latency_ms),
        "sam3_p95_latency_ms": _p95(sam3_total_latency_ms),
        "yolo_avg_latency_ms": _avg(yolo_latency_ms),
        "yolo_p95_latency_ms": _p95(yolo_latency_ms),
        "images_per_min": round((total_images / total_seconds) * 60.0, 2) if total_seconds > 0 else 0.0,
        "gpu_memory_peak_gb": round(float(gpu_memory_peak_gb or 0.0), 2),
        "agreement_mode": "yolo_proxy_bbox_agreement",
        "iou_thresholds": list(IOU_THRESHOLDS),
    }
