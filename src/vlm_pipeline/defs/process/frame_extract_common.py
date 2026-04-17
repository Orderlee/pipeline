"""clip_to_frame 공통 구조 — candidate 파싱, 초기 row 빌더, 실패 cleanup."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .helpers_key_utils import _coerce_float, _delete_minio_keys


@dataclass
class _CandidateFields:
    """단일 후보에서 파싱한 공통 필드."""
    asset_id: str
    raw_bucket: str
    raw_key: str
    media_type: str
    archive_path: str | None
    label_id: str
    labels_key: str
    label_source: str
    event_index: int
    event_start_sec: float | None
    event_end_sec: float | None
    caption_text: str | None
    source_video_duration_sec: float | None
    clip_created_at: datetime = field(default_factory=datetime.now)


def _parse_candidate(cand: dict[str, Any]) -> _CandidateFields:
    return _CandidateFields(
        asset_id=cand["asset_id"],
        raw_bucket=cand["raw_bucket"],
        raw_key=cand["raw_key"],
        media_type=cand["media_type"],
        archive_path=cand.get("archive_path"),
        label_id=cand["label_id"],
        labels_key=cand["labels_key"],
        label_source=str(cand.get("label_source") or "manual"),
        event_index=int(cand.get("event_index") or 0),
        event_start_sec=_coerce_float(cand.get("timestamp_start_sec")),
        event_end_sec=_coerce_float(cand.get("timestamp_end_sec")),
        caption_text=str(cand.get("caption_text") or "").strip() or None,
        source_video_duration_sec=_coerce_float(cand.get("video_duration_sec")),
    )


def _build_initial_clip_row(cf: _CandidateFields, clip_id: str, clip_key: str) -> dict[str, Any]:
    return {
        "clip_id": clip_id,
        "source_asset_id": cf.asset_id,
        "source_label_id": cf.label_id,
        "event_index": cf.event_index,
        "clip_start_sec": cf.event_start_sec,
        "clip_end_sec": cf.event_end_sec,
        "processed_bucket": "vlm-processed",
        "clip_key": clip_key,
        "label_key": cf.labels_key,
        "data_source": cf.label_source,
        "caption_text": cf.caption_text,
        "image_extract_status": "pending" if cf.media_type == "video" else "completed",
        "image_extract_count": 0,
        "process_status": "processing",
        "created_at": cf.clip_created_at,
    }


def _cleanup_failed_clip(
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    clip_id: str | None,
    asset_id: str,
    error_message: str,
    uploaded_clip_key: str | None,
    uploaded_frame_keys: list[str],
    uploaded_caption_keys: list[str],
) -> None:
    if clip_id:
        db.update_processed_clip_status(clip_id, "failed")
        db.update_clip_image_extract_status(
            clip_id, "failed", count=0, error=error_message, extracted_at=datetime.now(),
        )
        db.replace_processed_clip_frame_metadata(asset_id, clip_id, [])
    cleanup_keys = list(uploaded_frame_keys)
    if uploaded_clip_key:
        cleanup_keys.append(uploaded_clip_key)
    if cleanup_keys:
        _delete_minio_keys(minio, "vlm-processed", cleanup_keys)
    if uploaded_caption_keys:
        _delete_minio_keys(minio, "vlm-labels", uploaded_caption_keys)
