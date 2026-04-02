"""Image classification artifact import helpers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.key_builders import build_image_classification_key as _build_image_classification_key

from vlm_pipeline.defs.label.artifact_import_support import (
    _ArtifactImportSummary,
    _coerce_source_unit_dirs,
    _ensure_processed_media_rows,
    _find_existing_image_row,
    _now,
    _stable_id,
)


def _import_image_classification_json_files(
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
    normalized_source_unit_dirs = _coerce_source_unit_dirs(
        source_unit_dir=source_unit_dir,
        source_unit_dirs=source_unit_dirs,
    )
    for json_path in json_paths:
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {"stage": "image_classification", "path": str(json_path), "error": f"json_parse_failed:{exc}"}
            )
            summary.skipped += 1
            continue

        if not isinstance(payload, dict):
            failures.append({"stage": "image_classification", "path": str(json_path), "error": "invalid_payload_type"})
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
            failures.append({"stage": "image_classification", "path": str(json_path), "error": "image_not_matched"})
            summary.not_matched += 1
            continue

        predicted_classes = payload.get("predicted_classes")
        if isinstance(predicted_classes, list):
            normalized_predicted_classes = [
                str(value or "").strip().lower()
                for value in predicted_classes
                if str(value or "").strip()
            ]
        else:
            normalized_predicted_classes = []

        raw_class_counts = payload.get("class_counts")
        normalized_class_counts: dict[str, int] = {}
        if isinstance(raw_class_counts, dict):
            for key, value in raw_class_counts.items():
                rendered_key = str(key or "").strip().lower()
                if not rendered_key:
                    continue
                try:
                    normalized_class_counts[rendered_key] = int(value)
                except (TypeError, ValueError):
                    continue

        if not normalized_class_counts and normalized_predicted_classes:
            normalized_class_counts = {class_name: 1 for class_name in normalized_predicted_classes}
        if not normalized_predicted_classes and normalized_class_counts:
            normalized_predicted_classes = [
                class_name
                for class_name, _count in sorted(normalized_class_counts.items(), key=lambda item: (-item[1], item[0]))
            ]
        if not normalized_predicted_classes and not normalized_class_counts:
            failures.append(
                {
                    "stage": "image_classification",
                    "path": str(json_path),
                    "error": "predicted_classes_missing",
                }
            )
            summary.skipped += 1
            continue

        classification_key = _build_image_classification_key(str(image_row["image_key"]))
        generated_at_raw = str(payload.get("generated_at") or "").strip()
        try:
            generated_at = datetime.fromisoformat(generated_at_raw) if generated_at_raw else _now()
        except ValueError:
            generated_at = _now()

        normalized_payload = {
            **payload,
            "image_id": str(image_row["image_id"]),
            "source_clip_id": image_row.get("source_clip_id"),
            "image_key": str(image_row["image_key"]),
            "predicted_classes": normalized_predicted_classes,
            "class_counts": normalized_class_counts,
            "bbox_labels_key": str(payload.get("bbox_labels_key") or "").strip() or None,
            "requested_classes": payload.get("requested_classes") if isinstance(payload.get("requested_classes"), list) else [],
            "class_source": str(payload.get("class_source") or "manual_import").strip() or "manual_import",
            "generated_at": generated_at.isoformat(),
        }
        minio.upload(
            "vlm-labels",
            classification_key,
            json.dumps(normalized_payload, ensure_ascii=False).encode("utf-8"),
            "application/json",
        )
        db.insert_image_label(
            {
                "image_label_id": _stable_id(str(image_row["image_id"]), classification_key),
                "image_id": image_row["image_id"],
                "source_clip_id": image_row.get("source_clip_id"),
                "labels_bucket": "vlm-labels",
                "labels_key": classification_key,
                "label_format": "image_classification_json",
                "label_tool": "yolo-world-classification",
                "label_source": "manual",
                "review_status": "reviewed",
                "label_status": "completed",
                "object_count": len(normalized_predicted_classes),
                "created_at": generated_at,
            }
        )
        summary.processed += 1
        summary.inserted += 1

    return summary
