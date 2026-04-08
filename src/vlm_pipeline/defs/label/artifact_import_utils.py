"""Pure utility helpers for artifact import — no DB/MinIO dependencies."""

from __future__ import annotations

import re
from datetime import datetime
from hashlib import sha1
from pathlib import Path, PurePosixPath
from typing import Any


def now() -> datetime:
    return datetime.now()


def stable_id(*parts: object) -> str:
    rendered = "|".join(str(part or "") for part in parts)
    return sha1(rendered.encode("utf-8")).hexdigest()


def artifact_identity(root: Path, path: Path) -> str:
    return f"{root.name.lower()}/{path.relative_to(root).as_posix()}"


def normalize_unit_scoped_key(source_unit_name: str, explicit_key: str | None, file_path: Path, source_unit_dir: Path) -> str:
    raw_key = str(explicit_key or "").strip().replace("\\", "/")
    if raw_key:
        key_path = PurePosixPath(raw_key)
        if key_path.parts and key_path.parts[0] == source_unit_name:
            return str(key_path)
        if len(key_path.parts) >= 2:
            return str(PurePosixPath(source_unit_name, *key_path.parts[1:]))
        return str(PurePosixPath(source_unit_name, raw_key))
    return str(PurePosixPath(source_unit_name, file_path.relative_to(source_unit_dir).as_posix()))


def derive_clip_stem_from_image_stem(image_stem: str) -> str:
    match = re.match(r"^(?P<clip>.+)_\d{8}$", image_stem)
    if match:
        return str(match.group("clip"))
    return image_stem


def derive_raw_stem_from_clip_stem(clip_stem: str) -> str:
    for pattern in (
        r"^(?P<raw>.+)_e\d{3}_\d{8}_\d{8}$",
        r"^(?P<raw>.+)_e\d{3}$",
        r"^(?P<raw>.+)_\d{8}_\d{8}$",
    ):
        match = re.match(pattern, clip_stem)
        if match:
            return str(match.group("raw"))
    return clip_stem


def parse_frame_index_from_stem(image_stem: str) -> int | None:
    match = re.match(r"^.+_(\d{8})$", image_stem)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def is_bbox_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if "images" in payload and "annotations" in payload:
        return True
    return any(key in payload for key in ("detections", "boxes", "annotations", "objects"))


def is_image_caption_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if not any(str(payload.get(key) or "").strip() for key in ("image_key", "image_id")):
        return False
    return bool(str(payload.get("caption_text") or "").strip())


def derive_clip_key_from_image_key(image_key: str, clip_suffix: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    parent = key_path.parent
    image_stem = key_path.stem
    clip_stem = derive_clip_stem_from_image_stem(image_stem)
    clip_parent = parent.parent if parent.name == "image" else parent
    clip_filename = f"{clip_stem}{clip_suffix}"
    return str(clip_parent / clip_filename) if str(clip_parent) not in (".", "") else clip_filename
