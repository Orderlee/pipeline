"""Bbox / detection artifact import helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.key_builders import build_yolo_label_key as _build_yolo_label_key
from vlm_pipeline.lib.yolo_coco import convert_detection_payload_to_coco

from vlm_pipeline.defs.label.artifact_import_support import (
    _ArtifactImportSummary,
    _coerce_source_unit_dirs,
    _ensure_processed_media_rows,
    _find_existing_image_row,
    _now,
    _stable_id,
)


def _import_bbox_json_files(
    context,
    db,
    minio,
    *,
    source_unit_name: str,
    source_unit_dir: Path | None = None,
    source_unit_dirs: list[Path] | tuple[Path, ...] | None = None,
    json_paths: list[Path],
    failures: list[dict[str, Any]],
) -> _ArtifactImportSummary:
    summary = _ArtifactImportSummary()
    completed_assets: set[str] = set()
    normalized_source_unit_dirs = _coerce_source_unit_dirs(
        source_unit_dir=source_unit_dir,
        source_unit_dirs=source_unit_dirs,
    )
    for json_path in json_paths:
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            failures.append({"stage": "bbox", "path": str(json_path), "error": f"json_parse_failed:{exc}"})
            summary.skipped += 1
            continue

        if not isinstance(payload, dict):
            failures.append({"stage": "bbox", "path": str(json_path), "error": "invalid_payload_type"})
            summary.skipped += 1
            continue

        image_row = _find_existing_image_row(
            db,
            source_unit_name=source_unit_name,
            payload=payload,
            json_path=json_path,
        )
        if image_row is None:
            image_row = _ensure_processed_media_rows(
                context,
                db,
                minio,
                source_unit_name=source_unit_name,
                source_unit_dirs=normalized_source_unit_dirs,
                payload=payload,
                json_path=json_path,
            )
        if image_row is None:
            failures.append({"stage": "bbox", "path": str(json_path), "error": "image_not_matched"})
            summary.not_matched += 1
            continue

        coco_payload = convert_detection_payload_to_coco(
            payload,
            fallback_image_id=str(image_row["image_id"]),
            fallback_source_clip_id=image_row.get("source_clip_id"),
            fallback_image_key=str(image_row["image_key"]),
            fallback_image_width=image_row.get("width"),
            fallback_image_height=image_row.get("height"),
            default_class_source="manual_import",
        )
        annotation_count = len(coco_payload.get("annotations") or [])
        labels_key = _build_yolo_label_key(str(image_row["image_key"]))
        minio.upload(
            "vlm-labels",
            labels_key,
            json.dumps(coco_payload, ensure_ascii=False).encode("utf-8"),
            "application/json",
        )
        db.insert_image_label(
            {
                "image_label_id": _stable_id(str(image_row["image_id"]), labels_key),
                "image_id": image_row["image_id"],
                "source_clip_id": image_row.get("source_clip_id"),
                "labels_bucket": "vlm-labels",
                "labels_key": labels_key,
                "label_format": "coco",
                "label_tool": "yolo-world",
                "label_source": "manual",
                "review_status": "reviewed",
                "label_status": "completed",
                "object_count": annotation_count,
                "created_at": _now(),
            }
        )
        completed_assets.add(str(image_row["source_asset_id"]))
        summary.processed += 1
        summary.inserted += 1

    for asset_id in completed_assets:
        db.update_bbox_status(asset_id, "completed", completed_at=_now())
    return summary
