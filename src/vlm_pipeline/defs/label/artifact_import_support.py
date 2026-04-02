"""Shared helpers for importing prebuilt local label artifacts."""

from __future__ import annotations

import json
import mimetypes
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from pathlib import Path, PurePosixPath
from typing import Any

from vlm_pipeline.defs.label.import_support import (
    EVENT_LABEL_DIR_NAMES,
    VIDEO_CLASSIFICATION_DIR_NAMES,
    detect_label_format,
    import_event_label_files,
    iter_label_files,
)
from vlm_pipeline.lib.checksum import sha256sum
from vlm_pipeline.lib.video_frames import describe_frame_bytes
from vlm_pipeline.lib.video_loader import load_video_once

_BBOX_DIR_NAMES = ("detections",)
_CAPTION_DIR_NAMES = ("image_captions",)
_IMAGE_CLASSIFICATION_DIR_NAMES = ("image_classifications",)
_IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".webp")
_VIDEO_SUFFIXES = (".mp4", ".mov", ".avi", ".mkv")


@dataclass
class _ArtifactImportSummary:
    processed: int = 0
    inserted: int = 0
    skipped: int = 0
    not_matched: int = 0


def _mime_type_for_path(path: Path, default: str) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or default


def _now() -> datetime:
    return datetime.now()


def _stable_id(*parts: object) -> str:
    rendered = "|".join(str(part or "") for part in parts)
    return sha1(rendered.encode("utf-8")).hexdigest()


def _artifact_identity(root: Path, path: Path) -> str:
    return f"{root.name.lower()}/{path.relative_to(root).as_posix()}"


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


def _is_bbox_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if "images" in payload and "annotations" in payload:
        return True
    return any(key in payload for key in ("detections", "boxes", "annotations", "objects"))


def _is_image_caption_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if not any(str(payload.get(key) or "").strip() for key in ("image_key", "image_id")):
        return False
    return bool(str(payload.get("caption_text") or "").strip())


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


def _normalize_unit_scoped_key(source_unit_name: str, explicit_key: str | None, file_path: Path, source_unit_dir: Path) -> str:
    raw_key = str(explicit_key or "").strip().replace("\\", "/")
    if raw_key:
        key_path = PurePosixPath(raw_key)
        if key_path.parts and key_path.parts[0] == source_unit_name:
            return str(key_path)
        if len(key_path.parts) >= 2:
            return str(PurePosixPath(source_unit_name, *key_path.parts[1:]))
        return str(PurePosixPath(source_unit_name, raw_key))
    return str(PurePosixPath(source_unit_name, file_path.relative_to(source_unit_dir).as_posix()))


def _derive_clip_stem_from_image_stem(image_stem: str) -> str:
    match = re.match(r"^(?P<clip>.+)_\d{8}$", image_stem)
    if match:
        return str(match.group("clip"))
    return image_stem


def _derive_raw_stem_from_clip_stem(clip_stem: str) -> str:
    for pattern in (
        r"^(?P<raw>.+)_e\d{3}_\d{8}_\d{8}$",
        r"^(?P<raw>.+)_e\d{3}$",
        r"^(?P<raw>.+)_\d{8}_\d{8}$",
    ):
        match = re.match(pattern, clip_stem)
        if match:
            return str(match.group("raw"))
    return clip_stem


def _parse_frame_index_from_stem(image_stem: str) -> int | None:
    match = re.match(r"^.+_(\d{8})$", image_stem)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _find_candidate_files(root: Path, *, stem: str, suffixes: tuple[str, ...], preferred_dir: str | None = None) -> list[Path]:
    candidates: list[Path] = []
    wanted_suffixes = {suffix.lower() for suffix in suffixes}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in wanted_suffixes:
            continue
        if path.stem != stem:
            continue
        if preferred_dir and preferred_dir not in {part.lower() for part in path.parts}:
            continue
        candidates.append(path)
    return sorted(candidates)


def _resolve_local_file(
    source_unit_dirs: list[Path],
    *,
    explicit_key: str | None,
    stem: str,
    suffixes: tuple[str, ...],
    preferred_dir: str | None = None,
) -> tuple[Path, Path] | None:
    raw_key = str(explicit_key or "").strip().replace("\\", "/")
    if raw_key:
        key_path = PurePosixPath(raw_key)
        parts = list(key_path.parts)
        for source_unit_dir in source_unit_dirs:
            if not parts or not parts[0]:
                continue
            if parts[0] == source_unit_dir.name:
                relative_parts = parts[1:]
            else:
                relative_parts = parts[1:] if len(parts) > 1 else parts
            if not relative_parts:
                continue
            candidate = source_unit_dir.joinpath(*relative_parts)
            if candidate.exists() and candidate.is_file():
                return source_unit_dir, candidate

    for source_unit_dir in source_unit_dirs:
        matches = _find_candidate_files(
            source_unit_dir,
            stem=stem,
            suffixes=suffixes,
            preferred_dir=preferred_dir,
        )
        if matches:
            return source_unit_dir, matches[0]
    return None


def _find_raw_asset_for_media(db, source_unit_name: str, *, image_stem: str) -> dict[str, Any] | None:
    clip_stem = _derive_clip_stem_from_image_stem(image_stem)
    raw_stem = _derive_raw_stem_from_clip_stem(clip_stem)
    matched = db.find_by_raw_key_stem(raw_stem, source_unit_name=source_unit_name)
    if matched:
        return matched
    raw_rows = db.list_raw_files_by_source_unit_name(source_unit_name)
    if len(raw_rows) == 1:
        return raw_rows[0]
    return None


def _find_existing_image_row(
    db,
    *,
    source_unit_name: str,
    payload: dict[str, Any],
    json_path: Path,
) -> dict[str, Any] | None:
    raw_image_key = str(payload.get("image_key") or "").strip()
    if raw_image_key:
        image_key = _normalize_unit_scoped_key(
            source_unit_name,
            raw_image_key,
            json_path,
            json_path.parent,
        )
        matched = db.find_image_metadata_by_image_key(image_key)
        if matched:
            return matched

    image_id = str(payload.get("image_id") or "").strip()
    if image_id:
        matched = db.find_image_metadata_by_image_id(image_id)
        if matched:
            return matched

    stem = Path(str(payload.get("image_key") or json_path.stem)).stem or json_path.stem
    return db.find_image_metadata_by_stem(stem, source_unit_name=source_unit_name)


def _derive_clip_key_from_image_key(image_key: str, clip_suffix: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = _derive_clip_stem_from_image_stem(key_path.stem or "clip")
    parent = key_path.parent
    if parent.name == "image":
        clip_parent = parent.parent / "clips"
    elif str(parent) and str(parent) != ".":
        clip_parent = parent / "clips"
    else:
        clip_parent = PurePosixPath("clips")
    return str(clip_parent / f"{stem}{clip_suffix}")


def _ensure_processed_media_rows(
    context,
    db,
    minio,
    *,
    source_unit_name: str,
    source_unit_dirs: list[Path],
    payload: dict[str, Any],
    json_path: Path,
) -> dict[str, Any] | None:
    existing = _find_existing_image_row(
        db,
        source_unit_name=source_unit_name,
        payload=payload,
        json_path=json_path,
    )
    if existing:
        return existing

    image_key_hint = str(payload.get("image_key") or "").strip() or None
    image_stem = Path(image_key_hint or json_path.stem).stem or json_path.stem
    image_match = _resolve_local_file(
        source_unit_dirs,
        explicit_key=image_key_hint,
        stem=image_stem,
        suffixes=_IMAGE_SUFFIXES,
        preferred_dir="image",
    )
    if image_match is None:
        return None
    image_source_unit_dir, image_path = image_match

    source_asset = _find_raw_asset_for_media(
        db,
        source_unit_name,
        image_stem=image_path.stem,
    )
    if not source_asset:
        return None

    image_key = _normalize_unit_scoped_key(source_unit_name, image_key_hint, image_path, image_source_unit_dir)
    clip_stem = _derive_clip_stem_from_image_stem(image_path.stem)
    clip_match = _resolve_local_file(
        source_unit_dirs,
        explicit_key=_derive_clip_key_from_image_key(image_key, ".mp4"),
        stem=clip_stem,
        suffixes=_VIDEO_SUFFIXES,
        preferred_dir="clips",
    )

    source_clip_id: str | None = None
    if clip_match is not None:
        clip_source_unit_dir, clip_path = clip_match
        clip_key = _normalize_unit_scoped_key(source_unit_name, None, clip_path, clip_source_unit_dir)
        clip_meta = load_video_once(clip_path, include_env_metadata=False)
        source_clip_id = _stable_id(str(source_asset["asset_id"]), clip_key)
        minio.upload_file(
            "vlm-processed",
            clip_key,
            clip_path,
            content_type=_mime_type_for_path(clip_path, "video/mp4"),
        )
        db.insert_processed_clip(
            {
                "clip_id": source_clip_id,
                "source_asset_id": source_asset["asset_id"],
                "source_label_id": None,
                "event_index": 0,
                "clip_start_sec": None,
                "clip_end_sec": None,
                "checksum": clip_meta.get("checksum"),
                "file_size": clip_meta.get("file_size"),
                "processed_bucket": "vlm-processed",
                "clip_key": clip_key,
                "label_key": None,
                "data_source": "prelabeled_import",
                "caption_text": None,
                "width": clip_meta["video_metadata"].get("width"),
                "height": clip_meta["video_metadata"].get("height"),
                "codec": clip_meta["video_metadata"].get("codec"),
                "duration_sec": clip_meta["video_metadata"].get("duration_sec"),
                "fps": clip_meta["video_metadata"].get("fps"),
                "frame_count": clip_meta["video_metadata"].get("frame_count"),
                "image_extract_status": "completed",
                "image_extract_count": 1,
                "image_extract_error": None,
                "image_extracted_at": _now(),
                "process_status": "completed",
                "created_at": _now(),
            }
        )
        db.update_clip_image_extract_status(
            source_clip_id,
            "completed",
            count=1,
            extracted_at=_now(),
        )

    image_bytes = image_path.read_bytes()
    image_meta = describe_frame_bytes(image_bytes)
    minio.upload_file(
        "vlm-processed",
        image_key,
        image_path,
        content_type=_mime_type_for_path(image_path, "image/jpeg"),
    )
    image_id = _stable_id(str(source_asset["asset_id"]), source_clip_id or "", image_key)
    db.upsert_image_metadata_rows(
        [
            {
                "image_id": image_id,
                "source_asset_id": source_asset["asset_id"],
                "source_clip_id": source_clip_id,
                "image_bucket": "vlm-processed",
                "image_key": image_key,
                "image_role": "processed_clip_frame",
                "frame_index": payload.get("frame_index") or _parse_frame_index_from_stem(image_path.stem),
                "frame_sec": payload.get("frame_sec"),
                "checksum": sha256sum(image_path),
                "file_size": image_meta.get("file_size"),
                "width": image_meta.get("width"),
                "height": image_meta.get("height"),
                "color_mode": image_meta.get("color_mode"),
                "bit_depth": image_meta.get("bit_depth"),
                "has_alpha": image_meta.get("has_alpha"),
                "orientation": image_meta.get("orientation"),
                "caption_text": None,
                "image_caption_text": None,
                "image_caption_score": None,
                "image_caption_bucket": None,
                "image_caption_key": None,
                "image_caption_generated_at": None,
                "extracted_at": _now(),
            }
        ]
    )
    db.update_frame_status(str(source_asset["asset_id"]), "completed", completed_at=_now())
    return db.find_image_metadata_by_image_id(image_id)


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
# Sub-module re-exports (backward compatibility)
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
