"""PROCESS 공유 헬퍼 — 프레임 추출, 이미지 caption relevance scoring, empty_output 억제."""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.file_loader import build_nonexistent_temp_path, cleanup_temp_path
from vlm_pipeline.lib.gemini import (
    GeminiAnalyzer,
    extract_clean_json_text,
    is_vertex_rate_limit_error,
)
from vlm_pipeline.lib.vertex_chunking import (
    build_event_frame_image_prompt,
    build_event_frame_relevance_prompt,
    parse_event_frame_image_caption_response,
    parse_event_frame_relevance_response,
    select_top_relevance_index,
)
from vlm_pipeline.lib.video_frames import (
    describe_frame_bytes,
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
)
from vlm_pipeline.resources.minio import MinIOResource

from .helpers_key_utils import (
    _build_image_caption_key,
    _build_image_caption_payload,
    _build_processed_clip_image_key,
    _delete_minio_keys,
)

_MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET = 3
_VERTEX_RELEVANCE_RETRY_DELAY_SEC = 2.0

_is_vertex_rate_limit_error = is_vertex_rate_limit_error


def _should_log_clip_frame_progress(frame_index: int, total_frames: int) -> bool:
    if total_frames <= 0:
        return frame_index == 1
    return frame_index == 1 or frame_index == total_frames or frame_index % 5 == 0


def _is_empty_output_frame_error(message: str) -> bool:
    normalized = str(message or "")
    return "ffmpeg_frame_extract_failed:empty_output" in normalized


def _track_empty_output_failure(
    asset_id: str,
    error_message: str,
    *,
    consecutive_failures: dict[str, int],
    suppressed_asset_ids: set[str],
) -> bool:
    if not _is_empty_output_frame_error(error_message):
        consecutive_failures[asset_id] = 0
        return False

    next_count = consecutive_failures.get(asset_id, 0) + 1
    consecutive_failures[asset_id] = next_count
    if next_count >= _MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET:
        suppressed_asset_ids.add(asset_id)
        return True
    return False


def _reset_empty_output_failure(asset_id: str, *, consecutive_failures: dict[str, int]) -> None:
    consecutive_failures[asset_id] = 0


def _score_event_frame_image_relevance(
    analyzer: GeminiAnalyzer,
    frame_bytes: bytes,
    *,
    event_category: str | None,
    event_caption_text: str | None,
) -> float:
    if not str(event_category or "").strip() and not str(event_caption_text or "").strip():
        return 0.0

    prompt = build_event_frame_relevance_prompt(
        event_category=event_category,
        event_caption_text=event_caption_text,
    )
    temp_path = build_nonexistent_temp_path(".jpg")
    temp_path.write_bytes(frame_bytes)
    try:
        response_text = analyzer.analyze_image(str(temp_path), prompt=prompt)
        cleaned = extract_clean_json_text(response_text)
        return parse_event_frame_relevance_response(cleaned)
    finally:
        cleanup_temp_path(temp_path)


def _score_event_frame_image_relevance_with_retry(
    analyzer: GeminiAnalyzer,
    frame_bytes: bytes,
    *,
    clip_id: str,
    frame_index: int,
    event_category: str | None,
    event_caption_text: str | None,
    image_caption_log=None,
) -> float | None:
    attempts = 2
    for attempt in range(1, attempts + 1):
        try:
            return _score_event_frame_image_relevance(
                analyzer,
                frame_bytes,
                event_category=event_category,
                event_caption_text=event_caption_text,
            )
        except Exception as exc:
            if not _is_vertex_rate_limit_error(exc):
                raise
            if image_caption_log is not None:
                image_caption_log.warning(
                    "frame image relevance 429: clip_id=%s frame_index=%s attempt=%d/%d err=%s",
                    clip_id,
                    frame_index,
                    attempt,
                    attempts,
                    exc,
                )
            if attempt >= attempts:
                if image_caption_log is not None:
                    image_caption_log.warning(
                        "frame image relevance skip: clip_id=%s frame_index=%s reason=vertex_429_exhausted",
                        clip_id,
                        frame_index,
                    )
                return None
            time.sleep(_VERTEX_RELEVANCE_RETRY_DELAY_SEC)
    return None


def _generate_event_frame_image_caption(
    analyzer: GeminiAnalyzer,
    frame_bytes: bytes,
    *,
    event_category: str | None,
    event_caption_text: str | None,
) -> str | None:
    if not str(event_category or "").strip() and not str(event_caption_text or "").strip():
        return None

    prompt = build_event_frame_image_prompt(
        event_category=event_category,
        event_caption_text=event_caption_text,
    )
    temp_path = build_nonexistent_temp_path(".jpg")
    temp_path.write_bytes(frame_bytes)
    try:
        response_text = analyzer.analyze_image(str(temp_path), prompt=prompt)
        cleaned = extract_clean_json_text(response_text)
        is_relevant, caption_text = parse_event_frame_image_caption_response(cleaned)
        if not is_relevant:
            return None
        return caption_text
    finally:
        cleanup_temp_path(temp_path)


def _extract_clip_frames(
    minio: MinIOResource,
    *,
    clip_id: str,
    source_asset_id: str,
    clip_path: Path,
    clip_key: str,
    duration_sec: float,
    fps: float | None,
    frame_count: int | None,
    max_frames: int,
    jpeg_quality: int,
    image_profile: str = "current",
    frame_interval_sec: float | None = None,
    image_caption_analyzer: GeminiAnalyzer | None = None,
    image_caption_event_category: str | None = None,
    image_caption_event_caption_text: str | None = None,
    image_caption_parent_label_key: str | None = None,
    store_image_caption_json: bool = False,
    image_caption_log=None,
    progress_log=None,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """clip 비디오에서 프레임 추출 후, 최고 관련도 1개 프레임만 이미지 캡션 생성."""
    now = datetime.now()
    timestamps = plan_frame_timestamps(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=max_frames,
        image_profile=image_profile,
        frame_interval_sec=frame_interval_sec,
    )

    frame_rows: list[dict[str, Any]] = []
    uploaded_keys: list[str] = []
    uploaded_caption_keys: list[str] = []
    caption_candidates: list[dict[str, Any]] = []
    total_timestamps = len(timestamps)

    try:
        for frame_index, frame_sec in enumerate(timestamps, start=1):
            if progress_log is not None and _should_log_clip_frame_progress(frame_index, total_timestamps):
                progress_log.info(
                    "clip frame progress: clip_id=%s frame=%d/%d sec=%.3f",
                    clip_id,
                    frame_index,
                    total_timestamps,
                    float(frame_sec),
                )
            frame_bytes = extract_frame_jpeg_bytes(
                clip_path, frame_sec, jpeg_quality=jpeg_quality,
            )
            image_key = _build_processed_clip_image_key(clip_key, frame_index)
            minio.upload("vlm-processed", image_key, frame_bytes, "image/jpeg")
            uploaded_keys.append(image_key)
            if progress_log is not None and _should_log_clip_frame_progress(frame_index, total_timestamps):
                progress_log.info(
                    "clip frame upload progress: clip_id=%s uploaded=%d/%d image_key=%s",
                    clip_id,
                    frame_index,
                    total_timestamps,
                    image_key,
                )

            frame_meta = describe_frame_bytes(frame_bytes)
            row_index = len(frame_rows)
            relevance_score = None
            if image_caption_analyzer is not None and (
                str(image_caption_event_category or "").strip()
                or str(image_caption_event_caption_text or "").strip()
            ):
                try:
                    relevance_score = _score_event_frame_image_relevance_with_retry(
                        image_caption_analyzer,
                        frame_bytes,
                        clip_id=clip_id,
                        frame_index=frame_index,
                        event_category=image_caption_event_category,
                        event_caption_text=image_caption_event_caption_text,
                        image_caption_log=image_caption_log,
                    )
                except Exception as exc:
                    if image_caption_log is not None:
                        image_caption_log.warning(
                            "frame image relevance 실패: clip_id=%s frame_index=%s err=%s",
                            clip_id,
                            frame_index,
                            exc,
                        )
                else:
                    caption_candidates.append(
                        {
                            "row_index": row_index,
                            "frame_index": frame_index,
                            "frame_bytes": frame_bytes,
                            "relevance_score": relevance_score,
                        }
                    )
            frame_rows.append({
                "image_id": str(uuid4()),
                "source_clip_id": clip_id,
                "image_bucket": "vlm-processed",
                "image_key": image_key,
                "image_role": "processed_clip_frame",
                "frame_index": frame_index,
                "frame_sec": float(frame_sec),
                "checksum": sha256_bytes(frame_bytes),
                "file_size": len(frame_bytes),
                "width": frame_meta["width"],
                "height": frame_meta["height"],
                "color_mode": frame_meta["color_mode"],
                "bit_depth": frame_meta["bit_depth"],
                "has_alpha": frame_meta["has_alpha"],
                "orientation": frame_meta["orientation"],
                "image_caption_text": None,
                "image_caption_score": relevance_score,
                "image_caption_bucket": None,
                "image_caption_key": None,
                "image_caption_generated_at": None,
                "extracted_at": now,
            })

        best_candidate_index = select_top_relevance_index(
            [candidate.get("relevance_score") for candidate in caption_candidates]
        )
        if best_candidate_index is not None and image_caption_analyzer is not None:
            best_candidate = caption_candidates[best_candidate_index]
            if image_caption_log is not None:
                image_caption_log.info(
                    "frame image caption top-1 선택: clip_id=%s frame_index=%s score=%.3f",
                    clip_id,
                    best_candidate["frame_index"],
                    float(best_candidate["relevance_score"]),
                )
            try:
                image_caption_text = _generate_event_frame_image_caption(
                    image_caption_analyzer,
                    best_candidate["frame_bytes"],
                    event_category=image_caption_event_category,
                    event_caption_text=image_caption_event_caption_text,
                )
            except Exception as exc:
                if image_caption_log is not None:
                    image_caption_log.warning(
                        "selected frame image caption 실패: clip_id=%s frame_index=%s err=%s",
                        clip_id,
                        best_candidate["frame_index"],
                        exc,
                    )
            else:
                if image_caption_text is not None:
                    caption_generated_at = datetime.now()
                    selected_row = frame_rows[best_candidate["row_index"]]
                    selected_row["image_caption_text"] = image_caption_text
                    if store_image_caption_json:
                        caption_key = _build_image_caption_key(selected_row["image_key"])
                        caption_payload = _build_image_caption_payload(
                            image_id=selected_row["image_id"],
                            source_asset_id=source_asset_id,
                            source_clip_id=clip_id,
                            image_bucket=selected_row["image_bucket"],
                            image_key=selected_row["image_key"],
                            frame_index=selected_row["frame_index"],
                            frame_sec=selected_row["frame_sec"],
                            caption_text=image_caption_text,
                            relevance_score=selected_row["image_caption_score"],
                            model=getattr(image_caption_analyzer, "model_name", None),
                            generated_at=caption_generated_at,
                            event_category=image_caption_event_category,
                            event_caption_text=image_caption_event_caption_text,
                            parent_label_key=image_caption_parent_label_key,
                        )
                        minio.upload(
                            "vlm-labels",
                            caption_key,
                            json.dumps(caption_payload, ensure_ascii=False, indent=2).encode("utf-8"),
                            "application/json",
                        )
                        uploaded_caption_keys.append(caption_key)
                        selected_row["image_caption_bucket"] = "vlm-labels"
                        selected_row["image_caption_key"] = caption_key
                        selected_row["image_caption_generated_at"] = caption_generated_at
    except Exception:
        if uploaded_keys:
            _delete_minio_keys(minio, "vlm-processed", uploaded_keys)
        if uploaded_caption_keys:
            _delete_minio_keys(minio, "vlm-labels", uploaded_caption_keys)
        raise

    return frame_rows, uploaded_keys, uploaded_caption_keys
