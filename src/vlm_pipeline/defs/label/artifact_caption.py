"""Image caption artifact import helpers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.key_builders import build_image_caption_key as _build_image_caption_key

from vlm_pipeline.defs.label.artifact_import_support import (
    _ArtifactImportSummary,
    _coerce_source_unit_dirs,
    _ensure_processed_media_rows,
    _find_existing_image_row,
    _now,
)


def _import_image_caption_json_files(
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
            failures.append({"stage": "image_caption", "path": str(json_path), "error": f"json_parse_failed:{exc}"})
            summary.skipped += 1
            continue

        if not isinstance(payload, dict):
            failures.append({"stage": "image_caption", "path": str(json_path), "error": "invalid_payload_type"})
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
            failures.append({"stage": "image_caption", "path": str(json_path), "error": "image_not_matched"})
            summary.not_matched += 1
            continue

        caption_text = str(payload.get("caption_text") or "").strip()
        if not caption_text:
            failures.append({"stage": "image_caption", "path": str(json_path), "error": "caption_text_missing"})
            summary.skipped += 1
            continue

        caption_key = _build_image_caption_key(str(image_row["image_key"]))
        minio.upload(
            "vlm-labels",
            caption_key,
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            "application/json",
        )
        generated_at_raw = str(payload.get("generated_at") or "").strip()
        try:
            generated_at = datetime.fromisoformat(generated_at_raw) if generated_at_raw else _now()
        except ValueError:
            generated_at = _now()

        score_value = payload.get("relevance_score")
        try:
            score = float(score_value) if score_value is not None else None
        except (TypeError, ValueError):
            score = None

        db.update_image_caption_metadata(
            str(image_row["image_id"]),
            image_caption_text=caption_text,
            caption_score=score,
            caption_bucket="vlm-labels",
            caption_key=caption_key,
            generated_at=generated_at,
        )
        completed_assets.add(str(image_row["source_asset_id"]))
        summary.processed += 1
        summary.inserted += 1

    for asset_id in completed_assets:
        db.update_caption_status(asset_id, "completed", completed_at=_now())
    return summary
