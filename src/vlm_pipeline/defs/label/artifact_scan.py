"""Filesystem scanning for prebuilt local label artifacts (no DB / MinIO)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from vlm_pipeline.defs.label.artifact_import_utils import (
    artifact_identity as _artifact_identity,
    is_bbox_payload as _is_bbox_payload,
    is_image_caption_payload as _is_image_caption_payload,
)
from vlm_pipeline.defs.label.import_support import (
    detect_label_format,
    iter_label_files,
)

_BBOX_DIR_NAMES = ("detections",)
_CAPTION_DIR_NAMES = ("image_captions",)
_IMAGE_CLASSIFICATION_DIR_NAMES = ("image_classifications",)


def _coerce_source_unit_dirs(
    *,
    source_unit_dir: Path | None = None,
    source_unit_dirs: list[Path] | tuple[Path, ...] | None = None,
) -> list[Path]:
    ordered: list[Path] = []
    seen: set[str] = set()
    candidates = list(source_unit_dirs or [])
    if source_unit_dir is not None:
        candidates.insert(0, source_unit_dir)

    for candidate in candidates:
        path = Path(candidate)
        if not path.exists() or not path.is_dir():
            continue
        try:
            identity = str(path.resolve())
        except OSError:
            identity = str(path)
        if identity in seen:
            continue
        seen.add(identity)
        ordered.append(path)
    return ordered


def resolve_local_artifact_source_dirs(
    *,
    config,
    source_unit_name: str,
    archive_unit_dir_hint: Path | None = None,
) -> list[Path]:
    candidates: list[Path] = []
    incoming_source_unit_dir = Path(config.incoming_dir) / source_unit_name
    if incoming_source_unit_dir.is_dir():
        candidates.append(incoming_source_unit_dir)
    if archive_unit_dir_hint is not None:
        archive_path = Path(archive_unit_dir_hint)
        if archive_path.is_dir():
            candidates.append(archive_path)
    return _coerce_source_unit_dirs(source_unit_dirs=candidates)


def _scan_local_artifact_dirs(source_unit_dir: Path, dir_names: tuple[str, ...]) -> list[Path]:
    found: list[Path] = []
    wanted = {name.lower() for name in dir_names}
    for path in source_unit_dir.rglob("*"):
        if path.is_dir() and path.name.lower() in wanted:
            found.append(path)
    return sorted(found)


def _scan_artifact_json_paths(
    *,
    incoming_dir: Path,
    source_unit_dir: Path | None = None,
    source_unit_dirs: list[Path] | tuple[Path, ...] | None = None,
    dir_names: tuple[str, ...],
    scan_global_dirs: bool,
    scan_local_dirs: bool,
) -> list[Path]:
    ordered_roots: list[Path] = []
    for unit_dir in _coerce_source_unit_dirs(source_unit_dir=source_unit_dir, source_unit_dirs=source_unit_dirs):
        if scan_local_dirs:
            ordered_roots.extend(_scan_local_artifact_dirs(unit_dir, dir_names))
    if scan_global_dirs:
        for dir_name in dir_names:
            root = incoming_dir / dir_name
            if root.exists():
                ordered_roots.append(root)

    selected: dict[str, Path] = {}
    for root in ordered_roots:
        for path in iter_label_files(root):
            selected.setdefault(_artifact_identity(root, path), path)
    return list(selected.values())


def _scan_loose_artifact_json_paths(
    *,
    source_unit_dirs: list[Path],
    known_paths: list[Path],
) -> list[Path]:
    known = {str(path.resolve()) for path in known_paths}
    discovered: list[Path] = []
    seen: set[str] = set()
    for source_unit_dir in source_unit_dirs:
        for path in sorted(source_unit_dir.rglob("*.json")):
            if not path.is_file():
                continue
            resolved = str(path.resolve())
            if resolved in known or resolved in seen:
                continue
            seen.add(resolved)
            discovered.append(path)
    return discovered


def _classify_loose_artifact_payload(payload: Any) -> str | None:
    label_format = detect_label_format(payload)
    if label_format == "image_classification_json":
        return "image_classification"
    if label_format == "video_classification_json":
        return "video_classification"
    if label_format in {"auto_event_json", "labelme", "yolo"}:
        return "event"
    if _is_bbox_payload(payload):
        return "bbox"
    if _is_image_caption_payload(payload):
        return "image_caption"
    return None
