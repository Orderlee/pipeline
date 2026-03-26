"""Dispatch sensor/service helpers."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from dagster import RunRequest, SensorEvaluationContext
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.defs.ingest.archive import resolve_unique_directory
from vlm_pipeline.lib.sanitizer import sanitize_path_component
from vlm_pipeline.lib.staging_dispatch import format_dispatch_storage_list, parse_dispatch_request_payload
from vlm_pipeline.resources.config import PipelineConfig

_OUTPUT_TO_STEPS = {
    "bbox": [
        ("archive_move", 1),
        ("frame_extract", 2),
        ("yolo_detect", 3),
    ],
    "timestamp": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
    ],
    "captioning": [
        ("archive_move", 1),
        ("gemini_timestamp", 2),
        ("gemini_caption", 3),
        ("frame_extract", 4),
    ],
}

_DISPATCH_RUN_JOBS = {"dispatch_stage_job", "ingest_job"}


@dataclass(frozen=True)
class DispatchAppliedParams:
    max_frames_per_video: int | None
    jpeg_quality: int | None
    confidence_threshold: float | None
    iou_threshold: float | None


@dataclass(frozen=True)
class PreparedDispatchRequest:
    request_id: str
    folder_name: str
    incoming_folder_path: Path
    run_mode: str
    outputs_str: str
    labeling_method: list[str]
    categories: list[str]
    classes: list[str]
    image_profile: str
    requested_by: str | None
    requested_at: str | None
    archive_only: bool
    storage_outputs: str
    storage_labeling_method: str
    storage_categories: str
    storage_classes: str


def resolve_dispatch_request_id_from_tags(tags: Mapping[str, Any]) -> str | None:
    request_id = str(tags.get("dispatch_request_id") or tags.get("dagster/run_key") or "").strip()
    return request_id or None


def resolve_dispatch_folder_name_from_tags(tags: Mapping[str, Any]) -> str | None:
    folder_name = str(tags.get("folder_name_original") or tags.get("folder_name") or "").strip()
    return folder_name or None


def has_active_dispatch_run(context: SensorEvaluationContext, folder_name: str) -> bool:
    normalized_folder = str(folder_name or "").strip()
    if not normalized_folder:
        return False
    try:
        active_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
            limit=100,
        )
    except Exception:
        return False

    for run in active_runs:
        if run.job_name not in _DISPATCH_RUN_JOBS:
            continue
        tags = getattr(run, "tags", {}) or {}
        if resolve_dispatch_folder_name_from_tags(tags) == normalized_folder:
            return True
    return False


def move_dispatch_file(file_path: Path, target_dir: Path, *, context=None) -> bool:
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / file_path.name
    suffix = 2
    while destination.exists():
        destination = target_dir / f"{file_path.stem}__{suffix}{file_path.suffix}"
        suffix += 1
    try:
        file_path.replace(destination)
        if context is not None:
            context.log.info(f"dispatch JSON 이동 완료: {file_path} -> {destination}")
        return True
    except Exception as primary_exc:
        try:
            shutil.copy2(str(file_path), str(destination))
            file_path.unlink(missing_ok=True)
            if context is not None:
                context.log.info(f"dispatch JSON 복사+삭제 완료: {file_path} -> {destination}")
            return True
        except Exception as fallback_exc:
            if context is not None:
                context.log.warning(
                    "dispatch JSON 이동 실패: "
                    f"{file_path} -> {destination} "
                    f"(rename_err={primary_exc}, fallback_err={fallback_exc})"
                )
            return False


def prepare_dispatch_request(req_data: Mapping[str, Any], *, incoming_dir: Path) -> PreparedDispatchRequest:
    request_id = str(req_data.get("request_id") or "").strip()
    folder_name = str(req_data.get("folder_name") or "").strip()
    if not request_id or not folder_name:
        raise ValueError("missing_request_id_or_folder")

    payload = parse_dispatch_request_payload(req_data)
    labeling_method = payload["labeling_method"]
    categories = payload["categories"]
    classes = payload["classes"]
    return PreparedDispatchRequest(
        request_id=request_id,
        folder_name=folder_name,
        incoming_folder_path=incoming_dir / folder_name,
        run_mode=str(payload["run_mode"] or ""),
        outputs_str=str(payload["outputs_str"] or ""),
        labeling_method=labeling_method,
        categories=categories,
        classes=classes,
        image_profile=str(req_data.get("image_profile") or "current"),
        requested_by=str(req_data.get("requested_by") or "").strip() or None,
        requested_at=str(req_data.get("requested_at") or "").strip() or None,
        archive_only=bool(payload.get("archive_only")),
        storage_outputs=format_dispatch_storage_list(labeling_method),
        storage_labeling_method=format_dispatch_storage_list(labeling_method),
        storage_categories=format_dispatch_storage_list(categories),
        storage_classes=format_dispatch_storage_list(classes),
    )


def resolve_dispatch_applied_params(
    req_data: Mapping[str, Any],
    model_defaults: Mapping[str, Mapping[str, Any]],
) -> DispatchAppliedParams:
    bbox_defaults = model_defaults.get("bbox", {})
    jpeg_quality = req_data.get("jpeg_quality") or bbox_defaults.get("default_jpeg_quality")
    confidence = req_data.get("confidence_threshold") or bbox_defaults.get("default_confidence")
    iou = req_data.get("iou_threshold") or bbox_defaults.get("default_iou")
    return DispatchAppliedParams(
        max_frames_per_video=None,
        jpeg_quality=int(jpeg_quality) if jpeg_quality is not None else None,
        confidence_threshold=float(confidence) if confidence is not None else None,
        iou_threshold=float(iou) if iou is not None else None,
    )


def write_dispatch_manifest(
    config: PipelineConfig,
    prepared: PreparedDispatchRequest,
) -> tuple[Path, Path]:
    archive_dir = Path(config.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_dest = resolve_unique_directory(archive_dir / prepared.folder_name)

    now = datetime.now()
    manifest_id = f"dispatch_{prepared.request_id}_{now:%Y%m%d_%H%M%S}"
    manifest = {
        "manifest_id": manifest_id,
        "generated_at": now.isoformat(),
        "source_dir": str(Path(config.incoming_dir)),
        "source_unit_type": "directory",
        "source_unit_path": str(prepared.incoming_folder_path),
        "source_unit_name": prepared.folder_name,
        "source_unit_total_file_count": 0,
        "file_count": 0,
        "transfer_tool": "dispatch_sensor",
        "archive_requested": True,
        "categories": prepared.categories,
        "classes": prepared.classes,
        "labeling_method": prepared.labeling_method,
        "files": [],
    }

    manifest_dir = Path(config.manifest_dir) / "dispatch"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{manifest_id}.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return manifest_path, archive_dest


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
    applied_params_json = json.dumps(
        {
            "jpeg_quality": applied_params.jpeg_quality,
            "confidence": applied_params.confidence_threshold,
            "iou": applied_params.iou_threshold,
            "categories": prepared.categories,
            "classes": prepared.classes,
            "labeling_method": prepared.labeling_method,
        },
        default=str,
    )
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
                    "applied_params": (
                        applied_params_json if step_name in {"frame_extract", "yolo_detect"} else None
                    ),
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


def record_failed_dispatch_request(db, request_id: str, data: Mapping[str, Any], error_message: str) -> None:
    try:
        try:
            prepared = prepare_dispatch_request(data, incoming_dir=Path("/"))
            outputs = prepared.storage_outputs
            labeling_method = prepared.storage_labeling_method
            categories = prepared.storage_categories
            classes = prepared.storage_classes
            run_mode = prepared.run_mode
        except ValueError:
            outputs_list = data.get("outputs") if isinstance(data.get("outputs"), list) else []
            labeling_list = data.get("labeling_method") if isinstance(data.get("labeling_method"), list) else []
            category_list = data.get("categories") if isinstance(data.get("categories"), list) else []
            class_list = data.get("classes") if isinstance(data.get("classes"), list) else []
            outputs = format_dispatch_storage_list(labeling_list or outputs_list)
            labeling_method = format_dispatch_storage_list(labeling_list)
            categories = format_dispatch_storage_list(category_list)
            classes = format_dispatch_storage_list(class_list)
            run_mode = str(data.get("run_mode") or "")

        db.upsert_failed_dispatch_request(
            {
                "request_id": request_id,
                "folder_name": data.get("folder_name"),
                "run_mode": run_mode,
                "outputs": outputs,
                "labeling_method": labeling_method,
                "categories": categories,
                "classes": classes,
                "image_profile": data.get("image_profile"),
                "max_frames_per_video": data.get("max_frames_per_video"),
                "jpeg_quality": data.get("jpeg_quality"),
                "confidence_threshold": data.get("confidence_threshold"),
                "iou_threshold": data.get("iou_threshold"),
                "error_message": error_message,
                "processed_at": datetime.now(),
            }
        )
    except Exception:
        pass
