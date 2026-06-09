"""Dispatch DB record + RunRequest builders — extracted from service.py (Pattern B, #12)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from dagster import RunRequest

from vlm_pipeline.defs.dispatch._types import DispatchAppliedParams, PreparedDispatchRequest
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.lib.yolo_thresholds import (
    resolve_active_class_confidence_thresholds,
    resolve_effective_request_confidence_threshold,
)

_OUTPUT_TO_STEPS = {
    "bbox": [
        ("archive_move", 1),
        ("frame_extract", 2),
        ("yolo_detect", 3),
    ],
    "timestamp_video": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
    ],
    "captioning_video": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
        ("gemini_caption", 3),
        ("frame_extract", 4),
    ],
    "captioning_image": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
        ("gemini_caption", 3),
        ("frame_extract", 4),
        ("gemini_image_caption", 5),
    ],
    "classification_video": [
        ("archive_move", 1),
        ("gemini_video_classification", 2),
    ],
    "classification_image": [
        ("archive_move", 1),
        ("frame_extract", 2),
        ("yolo_detect", 3),
        ("image_classification", 4),
    ],
}


def build_dispatch_request_record(
    prepared: PreparedDispatchRequest,
    *,
    archive_dest: Path,
    applied_params: DispatchAppliedParams,
    processed_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "request_id": prepared.request_id,
        "folder_name": prepared.folder_name,
        "run_mode": prepared.run_mode,
        "outputs": prepared.storage_outputs,
        "labeling_method": prepared.storage_labeling_method,
        "categories": prepared.storage_categories,
        "classes": prepared.storage_classes,
        "image_profile": prepared.image_profile,
        "status": "running",
        "archive_pending_path": None,
        "archive_path": str(archive_dest),
        "max_frames_per_video": None,
        "jpeg_quality": applied_params.jpeg_quality,
        "confidence_threshold": applied_params.confidence_threshold,
        "iou_threshold": applied_params.iou_threshold,
        "requested_by": prepared.requested_by,
        "requested_at": prepared.requested_at,
        "processed_at": processed_at or datetime.now(),
    }


def build_dispatch_pipeline_rows(
    prepared: PreparedDispatchRequest,
    *,
    model_defaults: Mapping[str, Mapping[str, Any]],
    applied_params: DispatchAppliedParams,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_steps: set[str] = set()
    applied_params_payload = {
        "jpeg_quality": applied_params.jpeg_quality,
        "confidence": applied_params.confidence_threshold,
        "iou": applied_params.iou_threshold,
        "categories": prepared.categories,
        "classes": prepared.classes,
        "labeling_method": prepared.labeling_method,
    }
    if "bbox" in prepared.labeling_method and applied_params.confidence_threshold is not None:
        class_confidence_thresholds = resolve_active_class_confidence_thresholds(
            prepared.classes,
            applied_params.confidence_threshold,
        )
        applied_params_payload["class_confidence_thresholds"] = class_confidence_thresholds
        applied_params_payload["effective_request_confidence_threshold"] = (
            resolve_effective_request_confidence_threshold(
                applied_params.confidence_threshold,
                class_confidence_thresholds,
            )
        )
    applied_params_json = json.dumps(applied_params_payload, default=str)
    for output_key in prepared.labeling_method:
        steps = _OUTPUT_TO_STEPS.get(output_key, [])
        defaults = model_defaults.get(output_key, {})
        for step_name, step_order in steps:
            if step_name in seen_steps:
                continue
            seen_steps.add(step_name)
            rows.append(
                {
                    "run_id": str(uuid4()),
                    "request_id": prepared.request_id,
                    "folder_name": prepared.folder_name,
                    "step_name": step_name,
                    "step_order": step_order,
                    "step_status": "pending",
                    "model_name": defaults.get("model_name"),
                    "model_version": defaults.get("model_version"),
                    "applied_params": (applied_params_json if step_name in {"frame_extract", "yolo_detect"} else None),
                }
            )
    return rows


def build_dispatch_run_request(
    prepared: PreparedDispatchRequest,
    *,
    manifest_path: Path,
    applied_params: DispatchAppliedParams,
) -> RunRequest:
    sanitized_folder = sanitize_path_component(prepared.folder_name)
    common_tags = {
        "dispatch_request_id": prepared.request_id,
        "folder_name": sanitized_folder,
        "folder_name_original": prepared.folder_name,
        "image_profile": prepared.image_profile,
        "manifest_path": str(manifest_path),
    }
    if prepared.categories:
        common_tags["categories"] = json.dumps(prepared.categories, ensure_ascii=False)
    if prepared.classes:
        common_tags["classes"] = json.dumps(prepared.classes, ensure_ascii=False)
    if prepared.gemini_descriptions:
        common_tags["gemini_descriptions"] = json.dumps(prepared.gemini_descriptions, ensure_ascii=False)

    if prepared.archive_only:
        return RunRequest(
            run_key=prepared.request_id,
            job_name="ingest_job",
            tags={**common_tags, "dispatch_archive_only": "true"},
        )

    run_tags = {
        **common_tags,
        "outputs": prepared.outputs_str,
        "requested_outputs": prepared.outputs_str,
        "run_mode": prepared.run_mode,
        "labeling_method": prepared.outputs_str,
    }
    if applied_params.jpeg_quality is not None:
        run_tags["jpeg_quality"] = str(applied_params.jpeg_quality)
    if applied_params.confidence_threshold is not None:
        run_tags["confidence_threshold"] = str(applied_params.confidence_threshold)
    if applied_params.iou_threshold is not None:
        run_tags["iou_threshold"] = str(applied_params.iou_threshold)
    return RunRequest(
        run_key=prepared.request_id,
        job_name="dispatch_stage_job",
        tags=run_tags,
    )
