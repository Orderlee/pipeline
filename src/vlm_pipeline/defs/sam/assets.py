"""SAM3 shadow compare assets for YOLO bbox/segmentation benchmarking."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Mapping
from datetime import datetime
from hashlib import sha1
from pathlib import PurePosixPath
from typing import Any

from dagster import Field, asset

from vlm_pipeline.lib.sam3 import get_sam3_client
from vlm_pipeline.lib.sam3_compare import (
    compare_prompt_boxes,
    group_sam_detections_by_prompt,
    parse_yolo_coco_payload,
    summarize_benchmark_rows,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="sam3_shadow_compare",
    description="SAM3 shadow compare: YOLO bbox 결과와 병렬 segmentation/bbox agreement benchmark",
    group_name="benchmark",
    config_schema={
        "processed_clip_frame_limit": Field(int, default_value=200),
        "raw_video_frame_limit": Field(int, default_value=100),
        "max_per_source_unit": Field(int, default_value=50),
        "score_threshold": Field(float, default_value=0.0),
        "max_masks_per_prompt": Field(int, default_value=50),
        "benchmark_id": Field(str, is_required=False),
    },
)
def sam3_shadow_compare(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict[str, Any]:
    return _run_sam3_shadow_compare(context, db, minio)


def _run_sam3_shadow_compare(context, db: DuckDBResource, minio: MinIOResource) -> dict[str, Any]:
    processed_clip_frame_limit = int(context.op_config.get("processed_clip_frame_limit", 200))
    raw_video_frame_limit = int(context.op_config.get("raw_video_frame_limit", 100))
    max_per_source_unit = int(context.op_config.get("max_per_source_unit", 50))
    score_threshold = float(context.op_config.get("score_threshold", 0.0))
    max_masks_per_prompt = int(context.op_config.get("max_masks_per_prompt", 50))
    benchmark_id = str(context.op_config.get("benchmark_id") or "").strip() or datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    db.ensure_runtime_schema()

    candidates: list[dict[str, Any]] = []
    if processed_clip_frame_limit > 0:
        candidates.extend(
            db.find_sam3_shadow_candidates(
                image_role="processed_clip_frame",
                limit=max(1, processed_clip_frame_limit),
                max_per_source_unit=max(1, max_per_source_unit),
            )
        )
    if raw_video_frame_limit > 0:
        candidates.extend(
            db.find_sam3_shadow_candidates(
                image_role="raw_video_frame",
                limit=max(1, raw_video_frame_limit),
                max_per_source_unit=max(1, max_per_source_unit),
            )
        )
    if not candidates:
        summary = {
            "benchmark_id": benchmark_id,
            "processed_images": 0,
            "failed_images": 0,
            "pair_rows": 0,
            "skipped": True,
        }
        context.add_output_metadata(summary)
        context.log.info("sam3_shadow_compare: 대상 이미지 없음")
        return summary

    client = get_sam3_client()
    if not client.wait_until_ready(max_wait_sec=120):
        raise RuntimeError("sam3_server_not_ready")

    minio.ensure_bucket("vlm-labels")

    pair_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    sam3_total_latency_ms: list[float] = []
    yolo_latency_ms: list[float] = []
    gpu_memory_peak_gb = 0.0
    processed = 0
    failed = 0

    total_candidates = len(candidates)
    for idx, candidate in enumerate(candidates, start=1):
        image_id = str(candidate.get("image_id") or "")
        try:
            image_bucket = str(candidate.get("image_bucket") or "vlm-processed")
            image_key = str(candidate.get("image_key") or "")
            yolo_bucket = str(candidate.get("yolo_labels_bucket") or "vlm-labels")
            yolo_key = str(candidate.get("yolo_labels_key") or "")

            image_bytes = minio.download(image_bucket, image_key)
            yolo_payload = json.loads(minio.download(yolo_bucket, yolo_key).decode("utf-8"))
            yolo_boxes_by_class = parse_yolo_coco_payload(yolo_payload)
            prompts = _resolve_prompt_classes(yolo_payload, yolo_boxes_by_class)
            if not prompts:
                context.log.warning(
                    "sam3_shadow_compare 진행: [%d/%d] image=%s prompts 없음, 건너뜀",
                    idx,
                    total_candidates,
                    image_id,
                )
                continue

            sam_result = client.segment(
                image_bytes,
                prompts=prompts,
                filename=PurePosixPath(image_key).name or "image.jpg",
                score_threshold=score_threshold,
                max_masks_per_prompt=max(1, max_masks_per_prompt),
            )
            sam_detections = list(sam_result.get("detections") or [])
            grouped_sam = group_sam_detections_by_prompt(sam_detections)

            detected_at = datetime.now()
            sam3_labels_key = _build_sam3_segmentation_key(image_key)
            artifact_payload = {
                "benchmark_id": benchmark_id,
                "image_id": image_id,
                "source_asset_id": candidate.get("source_asset_id"),
                "source_clip_id": candidate.get("source_clip_id"),
                "source_unit_name": candidate.get("source_unit_name"),
                "raw_key": candidate.get("raw_key"),
                "image_bucket": image_bucket,
                "image_key": image_key,
                "image_role": candidate.get("image_role"),
                "requested_classes": prompts,
                "class_source": _extract_yolo_class_source(yolo_payload),
                "yolo_labels_bucket": yolo_bucket,
                "yolo_labels_key": yolo_key,
                "yolo_latency_ms": _extract_yolo_latency_ms(yolo_payload),
                "sam3_total_latency_ms": float(sam_result.get("elapsed_ms") or 0.0),
                "sam3_per_prompt_latency_ms": sam_result.get("per_prompt_latency_ms") or {},
                "sam3_device": sam_result.get("device"),
                "gpu_memory_peak_gb": sam_result.get("gpu_memory_peak_gb"),
                "detections": sam_detections,
                "generated_at": detected_at.isoformat(),
            }
            minio.upload(
                "vlm-labels",
                sam3_labels_key,
                json.dumps(artifact_payload, ensure_ascii=False).encode("utf-8"),
                "application/json",
            )

            label_rows.append(
                {
                    "image_label_id": _stable_sam3_image_label_id(image_id, sam3_labels_key),
                    "image_id": image_id,
                    "source_clip_id": candidate.get("source_clip_id"),
                    "labels_bucket": "vlm-labels",
                    "labels_key": sam3_labels_key,
                    "label_format": "sam3_segmentation_json",
                    "label_tool": "sam3",
                    "label_source": "auto",
                    "review_status": "auto_generated",
                    "label_status": "completed",
                    "object_count": len(sam_detections),
                    "created_at": detected_at,
                }
            )

            for prompt_class in _merge_prompt_classes(prompts, yolo_boxes_by_class, grouped_sam):
                pair_rows.append(
                    compare_prompt_boxes(
                        image_id=image_id,
                        prompt_class=prompt_class,
                        yolo_boxes=list(yolo_boxes_by_class.get(prompt_class, [])),
                        sam_detections=list(grouped_sam.get(prompt_class, [])),
                        benchmark_id=benchmark_id,
                        yolo_labels_key=yolo_key,
                        sam3_labels_key=sam3_labels_key,
                    )
                )

            sam_latency = float(sam_result.get("elapsed_ms") or 0.0)
            yolo_latency = _extract_yolo_latency_ms(yolo_payload)
            sam3_total_latency_ms.append(sam_latency)
            yolo_latency_ms.append(yolo_latency)
            gpu_memory_peak_gb = max(gpu_memory_peak_gb, float(sam_result.get("gpu_memory_peak_gb") or 0.0))
            processed += 1
            context.log.info(
                "sam3_shadow_compare 진행: [%d/%d] image=%s prompts=%d sam_boxes=%d ✅",
                idx,
                total_candidates,
                image_id,
                len(prompts),
                len(sam_detections),
            )
        except Exception as exc:
            failed += 1
            context.log.error(
                "sam3_shadow_compare 진행: [%d/%d] image=%s ❌ %s",
                idx,
                total_candidates,
                image_id,
                exc,
            )

    if label_rows:
        db.batch_insert_image_labels(label_rows)

    summary = summarize_benchmark_rows(
        pair_rows,
        benchmark_id=benchmark_id,
        total_images=processed,
        sam3_total_latency_ms=sam3_total_latency_ms,
        yolo_latency_ms=yolo_latency_ms,
        gpu_memory_peak_gb=gpu_memory_peak_gb,
    )
    summary.update(
        {
            "processed_images": processed,
            "failed_images": failed,
            "pair_rows": len(pair_rows),
        }
    )

    benchmark_root = _build_sam3_benchmark_root(benchmark_id)
    minio.upload(
        "vlm-labels",
        f"{benchmark_root}/summary.json",
        json.dumps(summary, ensure_ascii=False).encode("utf-8"),
        "application/json",
    )
    minio.upload(
        "vlm-labels",
        f"{benchmark_root}/pairs.csv",
        _serialize_pairs_csv(pair_rows),
        "text/csv",
    )

    context.add_output_metadata(
        {
            "benchmark_id": benchmark_id,
            "processed_images": processed,
            "failed_images": failed,
            "pair_rows": len(pair_rows),
            "matched_iou_mean": summary.get("matched_iou_mean", 0.0),
            "yolo_to_sam_coverage": summary.get("yolo_to_sam_coverage", 0.0),
            "sam_to_yolo_coverage": summary.get("sam_to_yolo_coverage", 0.0),
            "sam3_avg_latency_ms": summary.get("sam3_avg_latency_ms", 0.0),
            "yolo_avg_latency_ms": summary.get("yolo_avg_latency_ms", 0.0),
            "gpu_memory_peak_gb": summary.get("gpu_memory_peak_gb", 0.0),
        }
    )
    return summary


def _build_sam3_segmentation_key(image_key: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    raw_parent = parent.parent if parent.name == "image" else parent
    if str(raw_parent) and str(raw_parent) != ".":
        return str(raw_parent / "sam3_segmentations" / f"{stem}.json")
    return str(PurePosixPath("sam3_segmentations") / f"{stem}.json")


def _build_sam3_benchmark_root(benchmark_id: str) -> str:
    normalized = str(benchmark_id or "").strip() or "unknown"
    return str(PurePosixPath("benchmarks") / "sam3_vs_yolo" / normalized)


def _stable_sam3_image_label_id(image_id: str, labels_key: str) -> str:
    token = "|".join([str(image_id or "").strip(), str(labels_key or "").strip()])
    return sha1(token.encode("utf-8")).hexdigest()


def _extract_yolo_class_source(payload: Mapping[str, Any]) -> str | None:
    meta = payload.get("meta")
    if not isinstance(meta, Mapping):
        return None
    rendered = str(meta.get("class_source") or "").strip()
    return rendered or None


def _extract_yolo_latency_ms(payload: Mapping[str, Any]) -> float:
    meta = payload.get("meta")
    if not isinstance(meta, Mapping):
        return 0.0
    try:
        return float(meta.get("elapsed_ms") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _resolve_prompt_classes(payload: Mapping[str, Any], yolo_boxes_by_class: Mapping[str, list[list[float]]]) -> list[str]:
    meta = payload.get("meta")
    if isinstance(meta, Mapping) and isinstance(meta.get("requested_classes"), list):
        resolved: list[str] = []
        seen: set[str] = set()
        for value in meta.get("requested_classes") or []:
            rendered = str(value or "").strip().lower()
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            resolved.append(rendered)
        if resolved:
            return resolved
    return sorted(str(key).strip().lower() for key in yolo_boxes_by_class.keys() if str(key).strip())


def _merge_prompt_classes(
    prompts: list[str],
    yolo_boxes_by_class: Mapping[str, list[list[float]]],
    sam_grouped: Mapping[str, list[dict[str, Any]]],
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*prompts, *yolo_boxes_by_class.keys(), *sam_grouped.keys()]:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        merged.append(rendered)
    return merged


def _serialize_pairs_csv(rows: list[dict[str, Any]]) -> bytes:
    fieldnames = [
        "benchmark_id",
        "image_id",
        "prompt_class",
        "yolo_box_count",
        "sam_box_count",
        "bbox_count_delta",
        "matched_pair_count",
        "matched_iou_mean",
        "matched_iou_median",
        "yolo_to_sam_coverage",
        "sam_to_yolo_coverage",
        "unmatched_yolo_rate",
        "unmatched_sam_rate",
        "prompt_hit_rate",
        "sam_model_box_vs_mask_bbox_delta",
        "match_count_iou_0_3",
        "match_count_iou_0_5",
        "match_count_iou_0_7",
        "yolo_labels_key",
        "sam3_labels_key",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key) for key in fieldnames})
    return buffer.getvalue().encode("utf-8")
