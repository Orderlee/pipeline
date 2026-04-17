"""PROCESS 공유 유틸 — path/key 빌더, 숫자 coercion, stable ID.

helpers.py에서 분리. 순수 유틸 / 외부 딜리게이션만 담는다.
"""

from __future__ import annotations

from hashlib import sha1
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from datetime import datetime

from vlm_pipeline.resources.minio import MinIOResource


def _materialize_object_path(
    minio: MinIOResource, bucket: str, key: str, *, fallback_name: str,
) -> tuple[Path, Path]:
    suffix = Path(str(key or fallback_name)).suffix or Path(fallback_name).suffix or ".mp4"
    tmp_file = NamedTemporaryFile(delete=False, suffix=suffix)
    try:
        minio.download_fileobj(str(bucket or "").strip(), str(key or "").strip(), tmp_file)
    finally:
        tmp_file.close()
    return Path(tmp_file.name), Path(tmp_file.name)


def _materialize_video_path(minio: MinIOResource, candidate: dict) -> tuple[Path, Path | None]:
    from vlm_pipeline.lib.media_utils import materialize_video_path
    return materialize_video_path(minio, candidate)


def _delete_minio_keys(minio: MinIOResource, bucket: str, keys: list[str]) -> None:
    for key in keys:
        if not key:
            continue
        try:
            minio.delete(bucket, key)
        except Exception:
            continue


def _build_processed_clip_key(
    raw_key: str, *, event_index: int,
    clip_start_sec: float | None, clip_end_sec: float | None, media_type: str,
) -> str:
    from vlm_pipeline.lib.key_builders import build_processed_clip_key
    return build_processed_clip_key(
        raw_key, event_index=event_index,
        clip_start_sec=clip_start_sec, clip_end_sec=clip_end_sec, media_type=media_type,
    )


def _build_processed_clip_image_key(clip_key: str, frame_index: int) -> str:
    from vlm_pipeline.lib.key_builders import build_processed_clip_image_key
    return build_processed_clip_image_key(clip_key, frame_index)


def _build_image_caption_key(image_key: str) -> str:
    from vlm_pipeline.lib.key_builders import build_image_caption_key
    return build_image_caption_key(image_key)


def _build_image_caption_payload(
    *,
    image_id: str,
    source_asset_id: str,
    source_clip_id: str | None,
    image_bucket: str,
    image_key: str,
    frame_index: int | None,
    frame_sec: float | None,
    caption_text: str,
    relevance_score: float | None,
    model: str | None,
    generated_at: datetime,
    event_category: str | None,
    event_caption_text: str | None,
    parent_label_key: str | None,
) -> dict[str, Any]:
    return {
        "image_id": image_id,
        "source_asset_id": source_asset_id,
        "source_clip_id": source_clip_id,
        "image_bucket": image_bucket,
        "image_key": image_key,
        "frame_index": frame_index,
        "frame_sec": frame_sec,
        "caption_text": caption_text,
        "relevance_score": relevance_score,
        "model": model,
        "generated_at": generated_at.isoformat(),
        "event_category": event_category,
        "event_caption_text": event_caption_text,
        "parent_label_key": parent_label_key,
        "selection_method": "top1_relevance",
    }


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _stable_clip_id(
    label_id: str, event_index: int,
    clip_start_sec: float | None, clip_end_sec: float | None, clip_key: str,
) -> str:
    token = "|".join([
        str(label_id), str(event_index),
        str(clip_start_sec), str(clip_end_sec), str(clip_key),
    ])
    return sha1(token.encode("utf-8")).hexdigest()
