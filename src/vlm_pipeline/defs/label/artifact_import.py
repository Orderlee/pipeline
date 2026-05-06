"""Local label artifact import orchestrator.

Owns ``_ArtifactImportSummary``, ``_write_failure_log``, and the public entry
point ``import_local_label_artifacts``. Sibling import implementations
(``artifact_bbox``, ``artifact_caption``, ``artifact_classification``) are
loaded via deferred bottom-of-file imports to break the historical circular
dependency: each sibling needs ``_ArtifactImportSummary`` from this module,
and this module's orchestrator calls into each sibling's ``_import_*_json_files``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from vlm_pipeline.defs.label.artifact_import_utils import now as _now
from vlm_pipeline.defs.label.artifact_scan import (
    _BBOX_DIR_NAMES,
    _CAPTION_DIR_NAMES,
    _IMAGE_CLASSIFICATION_DIR_NAMES,
    _classify_loose_artifact_payload,
    _coerce_source_unit_dirs,
    _scan_artifact_json_paths,
    _scan_loose_artifact_json_paths,
)
from vlm_pipeline.defs.label.import_support import (
    EVENT_LABEL_DIR_NAMES,
    VIDEO_CLASSIFICATION_DIR_NAMES,
    import_event_label_files,
)


@dataclass
class _ArtifactImportSummary:
    processed: int = 0
    inserted: int = 0
    skipped: int = 0
    not_matched: int = 0


def _write_failure_log(config, source_unit_name: str, rows: list[dict[str, Any]], *, prefix: str) -> Path | None:
    if not rows:
        return None
    failed_dir = Path(config.manifest_dir) / "failed"
    failed_dir.mkdir(parents=True, exist_ok=True)
    path = failed_dir / f"{prefix}_{source_unit_name}_{_now():%Y%m%d_%H%M%S}.jsonl"
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


# ---------------------------------------------------------------------------
# Deferred sibling imports — siblings import _ArtifactImportSummary from this
# module, so loading them eagerly at the top would cycle. Production callers
# enter via the facade (artifact_import_support), which loads this module
# fully before triggering sibling loads — the same pattern as the original
# artifact_import_support.py used.
# ---------------------------------------------------------------------------
from vlm_pipeline.defs.label.artifact_bbox import (  # noqa: E402
    _import_bbox_json_files,
)
from vlm_pipeline.defs.label.artifact_caption import (  # noqa: E402
    _import_image_caption_json_files,
)
from vlm_pipeline.defs.label.artifact_classification import (  # noqa: E402
    _import_image_classification_json_files,
)


def import_local_label_artifacts(
    context,
    db,
    minio,
    *,
    config,
    source_unit_name: str,
    source_unit_dir: Path | None = None,
    source_unit_dirs: list[Path] | tuple[Path, ...] | None = None,
    scan_global_dirs: bool,
    scan_local_dirs: bool,
    failure_log_prefix: str,
    update_timestamp_status: bool = True,
) -> dict[str, Any]:
    normalized_source_unit_dirs = _coerce_source_unit_dirs(
        source_unit_dir=source_unit_dir,
        source_unit_dirs=source_unit_dirs,
    )
    if not normalized_source_unit_dirs and not scan_global_dirs:
        return {
            "event_labels_loaded": 0,
            "event_labels_inserted": 0,
            "event_labels_skipped": 0,
            "event_labels_not_matched": 0,
            "video_classifications_loaded": 0,
            "video_classifications_inserted": 0,
            "video_classifications_skipped": 0,
            "video_classifications_not_matched": 0,
            "bbox_processed": 0,
            "bbox_inserted": 0,
            "bbox_skipped": 0,
            "bbox_not_matched": 0,
            "image_captions_processed": 0,
            "image_captions_inserted": 0,
            "image_captions_skipped": 0,
            "image_captions_not_matched": 0,
            "image_classifications_processed": 0,
            "image_classifications_inserted": 0,
            "image_classifications_skipped": 0,
            "image_classifications_not_matched": 0,
            "failure_count": 0,
        }

    incoming_dir = Path(config.incoming_dir)
    event_json_paths = _scan_artifact_json_paths(
        incoming_dir=incoming_dir,
        source_unit_dirs=normalized_source_unit_dirs,
        dir_names=EVENT_LABEL_DIR_NAMES,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
    )
    video_classification_json_paths = _scan_artifact_json_paths(
        incoming_dir=incoming_dir,
        source_unit_dirs=normalized_source_unit_dirs,
        dir_names=VIDEO_CLASSIFICATION_DIR_NAMES,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
    )
    bbox_json_paths = _scan_artifact_json_paths(
        incoming_dir=incoming_dir,
        source_unit_dirs=normalized_source_unit_dirs,
        dir_names=_BBOX_DIR_NAMES,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
    )
    caption_json_paths = _scan_artifact_json_paths(
        incoming_dir=incoming_dir,
        source_unit_dirs=normalized_source_unit_dirs,
        dir_names=_CAPTION_DIR_NAMES,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
    )
    image_classification_json_paths = _scan_artifact_json_paths(
        incoming_dir=incoming_dir,
        source_unit_dirs=normalized_source_unit_dirs,
        dir_names=_IMAGE_CLASSIFICATION_DIR_NAMES,
        scan_global_dirs=scan_global_dirs,
        scan_local_dirs=scan_local_dirs,
    )
    known_paths = [
        *event_json_paths,
        *video_classification_json_paths,
        *bbox_json_paths,
        *caption_json_paths,
        *image_classification_json_paths,
    ]
    loose_json_paths = _scan_loose_artifact_json_paths(
        source_unit_dirs=normalized_source_unit_dirs,
        known_paths=known_paths,
    )
    for loose_json_path in loose_json_paths:
        try:
            payload = json.loads(loose_json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        classified = _classify_loose_artifact_payload(payload)
        if classified == "event":
            event_json_paths.append(loose_json_path)
        elif classified == "video_classification":
            video_classification_json_paths.append(loose_json_path)
        elif classified == "bbox":
            bbox_json_paths.append(loose_json_path)
        elif classified == "image_caption":
            caption_json_paths.append(loose_json_path)
        elif classified == "image_classification":
            image_classification_json_paths.append(loose_json_path)

    failures: list[dict[str, Any]] = []
    event_result = import_event_label_files(
        context,
        db,
        minio,
        event_json_paths,
        source_unit_name=source_unit_name,
        update_timestamp_status=update_timestamp_status,
    )
    video_classification_result = import_event_label_files(
        context,
        db,
        minio,
        video_classification_json_paths,
        source_unit_name=source_unit_name,
        update_timestamp_status=False,
    )
    bbox_result = _import_bbox_json_files(
        context,
        db,
        minio,
        source_unit_name=source_unit_name,
        source_unit_dirs=normalized_source_unit_dirs,
        json_paths=bbox_json_paths,
        failures=failures,
    )
    caption_result = _import_image_caption_json_files(
        context,
        db,
        minio,
        source_unit_name=source_unit_name,
        source_unit_dirs=normalized_source_unit_dirs,
        json_paths=caption_json_paths,
        failures=failures,
    )
    image_classification_result = _import_image_classification_json_files(
        context,
        db,
        minio,
        source_unit_name=source_unit_name,
        source_unit_dirs=normalized_source_unit_dirs,
        json_paths=image_classification_json_paths,
        failures=failures,
    )
    failure_log_path = _write_failure_log(
        config,
        source_unit_name,
        failures,
        prefix=failure_log_prefix,
    )

    summary = {
        "event_labels_loaded": event_result.loaded,
        "event_labels_inserted": event_result.inserted,
        "event_labels_skipped": event_result.skipped,
        "event_labels_not_matched": event_result.not_matched,
        "video_classifications_loaded": video_classification_result.loaded,
        "video_classifications_inserted": video_classification_result.inserted,
        "video_classifications_skipped": video_classification_result.skipped,
        "video_classifications_not_matched": video_classification_result.not_matched,
        "bbox_processed": bbox_result.processed,
        "bbox_inserted": bbox_result.inserted,
        "bbox_skipped": bbox_result.skipped,
        "bbox_not_matched": bbox_result.not_matched,
        "image_captions_processed": caption_result.processed,
        "image_captions_inserted": caption_result.inserted,
        "image_captions_skipped": caption_result.skipped,
        "image_captions_not_matched": caption_result.not_matched,
        "image_classifications_processed": image_classification_result.processed,
        "image_classifications_inserted": image_classification_result.inserted,
        "image_classifications_skipped": image_classification_result.skipped,
        "image_classifications_not_matched": image_classification_result.not_matched,
        "failure_count": len(failures),
    }
    if failure_log_path is not None:
        summary["failure_log_path"] = str(failure_log_path)
    return summary
