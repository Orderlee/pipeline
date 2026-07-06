"""MinIO object key builders — shared across defs/ modules.

Extracted from defs/process/assets, defs/yolo/assets, defs/label/assets
so that cross-domain modules (e.g. artifact_import_support) can use them
without importing private symbols across layer boundaries.
"""

from __future__ import annotations

from pathlib import PurePosixPath


def build_processed_clip_key(
    raw_key: str,
    *,
    event_index: int,
    clip_start_sec: float | None,
    clip_end_sec: float | None,
    media_type: str,
) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "asset"
    suffix = ".mp4" if str(media_type or "").strip().lower() == "video" else (key_path.suffix or ".jpg")
    parent = key_path.parent
    if clip_start_sec is not None and clip_end_sec is not None:
        start_ms = int(round(float(clip_start_sec) * 1000))
        end_ms = int(round(float(clip_end_sec) * 1000))
        filename = f"{stem}_{start_ms:08d}_{end_ms:08d}{suffix}"
    else:
        filename = f"{stem}_e{int(event_index):03d}{suffix}"
    if str(parent) and str(parent) != ".":
        return str(parent / "clips" / filename)
    return str(PurePosixPath("clips") / filename)


def build_processed_clip_image_key(clip_key: str, frame_index: int) -> str:
    clip_path = PurePosixPath(str(clip_key or "").strip())
    clip_stem = clip_path.stem or "clip"
    parent = clip_path.parent
    if parent.name == "clips":
        image_parent = parent.parent / "image"
    elif str(parent) and str(parent) != ".":
        image_parent = parent / "image"
    else:
        image_parent = PurePosixPath("image")
    return str(image_parent / f"{clip_stem}_{int(frame_index):08d}.jpg")


def build_image_caption_key(image_key: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    if parent.name == "image":
        caption_parent = parent.parent / "image_captions"
    elif str(parent) and str(parent) != ".":
        caption_parent = parent / "image_captions"
    else:
        caption_parent = PurePosixPath("image_captions")
    return str(caption_parent / f"{stem}.json")


def build_yolo_label_key(image_key: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    if parent.name == "image":
        raw_parent = parent.parent
    else:
        raw_parent = parent
    if str(raw_parent) and str(raw_parent) != ".":
        return str(raw_parent / "detections" / f"{stem}.json")
    return str(PurePosixPath("detections") / f"{stem}.json")


def build_image_classification_key(image_key: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    if parent.name == "image":
        raw_parent = parent.parent
    else:
        raw_parent = parent
    if str(raw_parent) and str(raw_parent) != ".":
        return str(raw_parent / "image_classifications" / f"{stem}.json")
    return str(PurePosixPath("image_classifications") / f"{stem}.json")


def build_video_classification_key(raw_key: str) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    if str(parent) and str(parent) != ".":
        return str(parent / "video_classifications" / f"{stem}.json")
    return str(PurePosixPath("video_classifications") / f"{stem}.json")


def build_gemini_label_key(raw_key: str) -> str:
    """`<raw_parent>/events/<video_stem>.json` convention."""
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    if str(parent) and str(parent) != ".":
        return str(parent / "events" / f"{stem}.json")
    return str(PurePosixPath("events") / f"{stem}.json")


def build_raw_video_image_key(raw_key: str, frame_index: int) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    if parent.name == "image":
        image_parent = parent
    elif str(parent) and str(parent) != ".":
        image_parent = parent / "image"
    else:
        image_parent = PurePosixPath("image")
    return str(image_parent / f"{stem}_{int(frame_index):08d}.jpg")


def build_sam3_detection_key(image_key: str) -> str:
    """Build MinIO key for SAM3 detection (COCO) JSON.

    Path segment remains ``sam3_segmentations/`` for backward compatibility
    with existing stored objects.
    """
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    raw_parent = parent.parent if parent.name == "image" else parent
    if str(raw_parent) and str(raw_parent) != ".":
        return str(raw_parent / "sam3_segmentations" / f"{stem}.json")
    return str(PurePosixPath("sam3_segmentations") / f"{stem}.json")


build_sam3_segmentation_key = build_sam3_detection_key
