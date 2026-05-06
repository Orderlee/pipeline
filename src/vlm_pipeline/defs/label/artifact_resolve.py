"""DB + filesystem resolution for prebuilt local label artifacts (depends on artifact_scan)."""

from __future__ import annotations

import mimetypes
from pathlib import Path, PurePosixPath
from typing import Any

from vlm_pipeline.defs.label.artifact_import_utils import (
    derive_clip_key_from_image_key as _derive_clip_key_from_image_key,
    derive_clip_stem_from_image_stem as _derive_clip_stem_from_image_stem,
    derive_raw_stem_from_clip_stem as _derive_raw_stem_from_clip_stem,
    normalize_unit_scoped_key as _normalize_unit_scoped_key,
    now as _now,
    parse_frame_index_from_stem as _parse_frame_index_from_stem,
    stable_id as _stable_id,
)
from vlm_pipeline.lib.checksum import sha256sum
from vlm_pipeline.lib.video_frames import describe_frame_bytes
from vlm_pipeline.lib.video_loader import load_video_once

_IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".webp")
_VIDEO_SUFFIXES = (".mp4", ".mov", ".avi", ".mkv")


def _mime_type_for_path(path: Path, default: str) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or default


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
