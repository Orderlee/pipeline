"""PROCESS 공유 헬퍼 — clip_captioning / clip_to_frame / raw_video_to_frame 에서 사용하는 유틸리티."""

from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime
from hashlib import sha1
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile, gettempdir
from typing import Any
from uuid import uuid4

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.file_loader import build_nonexistent_temp_path
from vlm_pipeline.lib.gemini import (
    GeminiAnalyzer,
    extract_clean_json_text,
    is_vertex_rate_limit_error,
    load_clean_json,
)
from vlm_pipeline.lib.staging_vertex import (
    build_event_frame_relevance_prompt,
    build_event_frame_image_prompt,
    normalize_gemini_events,
    parse_event_frame_relevance_response,
    parse_event_frame_image_caption_response,
    select_top_relevance_index,
)
from vlm_pipeline.lib.video_frames import (
    describe_frame_bytes,
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .clip_windows import (
    candidate_clip_window_plan_key,
    plan_asset_event_clip_extraction_windows,
    resolve_event_clip_extraction_window,
    resolve_event_only_clip_extraction_window,
    sort_process_candidates,
    window_values_match,
)

_MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET = 3
_VERTEX_RELEVANCE_RETRY_DELAY_SEC = 2.0

_candidate_clip_window_plan_key = candidate_clip_window_plan_key
_sort_process_candidates = sort_process_candidates
_resolve_event_only_clip_extraction_window = resolve_event_only_clip_extraction_window
_resolve_event_clip_extraction_window = resolve_event_clip_extraction_window
_plan_asset_event_clip_extraction_windows = plan_asset_event_clip_extraction_windows
_window_values_match = window_values_match


def _log_clip_extraction_window(
    context,
    *,
    asset_id: str,
    event_start_sec: float,
    event_end_sec: float,
    extract_start_sec: float,
    extract_end_sec: float,
    window_strategy: str,
) -> None:
    buffered_start_sec = float(event_start_sec) - _DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC
    buffered_end_sec = float(event_end_sec) + _DEFAULT_EVENT_CLIP_POST_BUFFER_SEC
    context.log.info(
        "clip_to_frame extraction window: asset=%s event=[%.3f, %.3f] extract=[%.3f, %.3f] "
        "start_clamped=%s end_clamped=%s strategy=%s",
        asset_id,
        float(event_start_sec),
        float(event_end_sec),
        extract_start_sec,
        extract_end_sec,
        extract_start_sec != buffered_start_sec,
        extract_end_sec != buffered_end_sec,
        window_strategy,
    )


def _extract_video_clip_media(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    asset_id: str,
    clip_id: str,
    clip_key: str,
    video_path: Path,
    event_start_sec: float,
    event_end_sec: float,
    source_duration_sec: float | None,
    extract_start_sec: float,
    extract_end_sec: float,
    window_strategy: str,
    video_width: object,
    video_height: object,
    video_codec: object,
) -> dict[str, Any]:
    def _extract_once(
        *,
        start_sec: float,
        end_sec: float,
        strategy: str,
    ) -> dict[str, Any]:
        _log_clip_extraction_window(
            context,
            asset_id=asset_id,
            event_start_sec=event_start_sec,
            event_end_sec=event_end_sec,
            extract_start_sec=start_sec,
            extract_end_sec=end_sec,
            window_strategy=strategy,
        )
        temp_clip_path = _extract_video_clip_path(
            video_path,
            clip_start_sec=start_sec,
            clip_end_sec=end_sec,
        )
        file_bytes = temp_clip_path.read_bytes()
        minio.upload("vlm-processed", clip_key, file_bytes, "video/mp4")
        clip_meta = _ffprobe_clip_meta(temp_clip_path)
        return {
            "temp_clip_path": temp_clip_path,
            "uploaded_clip_key": clip_key,
            "file_bytes": file_bytes,
            "clip_duration": clip_meta.get("duration_sec"),
            "clip_fps": clip_meta.get("fps"),
            "clip_frame_count": clip_meta.get("frame_count"),
            "width": clip_meta.get("width") or video_width,
            "height": clip_meta.get("height") or video_height,
            "codec": clip_meta.get("codec") or video_codec or "mp4",
            "checksum": sha256_bytes(file_bytes),
            "file_size": len(file_bytes),
            "extract_start_sec": start_sec,
            "extract_end_sec": end_sec,
            "window_strategy": strategy,
        }

    extracted = _extract_once(
        start_sec=extract_start_sec,
        end_sec=extract_end_sec,
        strategy=window_strategy,
    )
    existing_clip = db.find_processed_clip_by_checksum(
        str(extracted["checksum"]),
        source_asset_id=asset_id,
        exclude_clip_id=clip_id,
    )
    if existing_clip is None:
        return extracted

    retry_start_sec, retry_end_sec = _resolve_event_only_clip_extraction_window(
        event_start_sec=event_start_sec,
        event_end_sec=event_end_sec,
        source_duration_sec=source_duration_sec,
    )
    if _window_values_match(
        float(extracted["extract_start_sec"]),
        float(extracted["extract_end_sec"]),
        retry_start_sec,
        retry_end_sec,
    ):
        raise RuntimeError(
            "duplicate_clip_media_after_window_split:"
            f"existing_clip_id={existing_clip['clip_id']}:checksum={extracted['checksum']}"
        )

    context.log.warning(
        "clip_to_frame checksum collision: asset=%s clip_id=%s existing_clip_id=%s checksum=%s "
        "strategy=%s -> retry event_only",
                asset_id,
        clip_id,
        existing_clip["clip_id"],
        extracted["checksum"],
        extracted["window_strategy"],
    )
    _delete_minio_keys(minio, "vlm-processed", [clip_key])
    _cleanup_temp_path(extracted["temp_clip_path"])
    extracted = _extract_once(
        start_sec=retry_start_sec,
        end_sec=retry_end_sec,
        strategy="event_only_retry",
    )
    existing_clip = db.find_processed_clip_by_checksum(
        str(extracted["checksum"]),
        source_asset_id=asset_id,
        exclude_clip_id=clip_id,
    )
    if existing_clip is not None:
        raise RuntimeError(
            "duplicate_clip_media_after_window_split:"
            f"existing_clip_id={existing_clip['clip_id']}:checksum={extracted['checksum']}"
        )
    return extracted


# ───────────────────────────────────────────────────────────────
# helpers
# ───────────────────────────────────────────────────────────────

def _filter_valid_events(events: list) -> list[dict]:
    """Gemini 응답에서 유효한 이벤트만 필터링."""
    valid = []
    for event in events:
        if not isinstance(event, dict):
            continue
        ts = event.get("timestamp")
        if not isinstance(ts, list) or len(ts) < 2:
            continue
        try:
            start = float(ts[0])
            end = float(ts[1])
        except (TypeError, ValueError):
            continue
        if start < 0 or end <= start:
            continue
        valid.append(event)
    valid.sort(key=lambda e: float(e["timestamp"][0]))
    return valid


def _build_gemini_label_rows(
    asset_id: str,
    labels_key: str,
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    valid_events = normalize_gemini_events(events)
    event_count = len(valid_events)
    rows: list[dict[str, Any]] = []
    for event_index, event in enumerate(valid_events):
        start_sec = float(event["timestamp"][0])
        end_sec = float(event["timestamp"][1])
        if end_sec <= start_sec:
            continue

        ko_caption = str(event.get("ko_caption") or "").strip()
        en_caption = str(event.get("en_caption") or "").strip()
        caption_text = ko_caption or en_caption or None
        rows.append(
            {
                "label_id": _stable_gemini_label_id(asset_id, event_index, start_sec, end_sec),
                "asset_id": asset_id,
                "labels_bucket": "vlm-labels",
                "labels_key": labels_key,
                "label_format": "gemini_event_json",
                "label_tool": "gemini",
                "label_source": "auto",
                "review_status": "auto_generated",
                "event_index": event_index,
                "event_count": event_count,
                "timestamp_start_sec": start_sec,
                "timestamp_end_sec": end_sec,
                "caption_text": caption_text,
                "object_count": 0,
                "label_status": "completed",
            }
        )
    return rows


def _stable_gemini_label_id(
    asset_id: str, event_index: int, start_sec: float | None, end_sec: float | None,
) -> str:
    token = f"{asset_id}|gemini|auto|{event_index}|{start_sec}|{end_sec}"
    return sha1(token.encode("utf-8")).hexdigest()


def _ffprobe_clip_meta(clip_path: Path) -> dict[str, Any]:
    """ffprobe로 clip의 duration, fps, frame_count, width, height, codec 추출."""
    from vlm_pipeline.lib.video_loader import _run_ffprobe_with_retry

    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", str(clip_path),
    ]
    try:
        result = _run_ffprobe_with_retry(cmd, timeout_sec=30)
        stdout = result.stdout if isinstance(result.stdout, str) else (result.stdout or b"").decode("utf-8", errors="replace")
        if result.returncode != 0:
            return {}
        data = json.loads(stdout)
    except Exception:
        return {}

    video_stream = None
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    if not video_stream:
        return {}

    duration = None
    fmt_duration = data.get("format", {}).get("duration")
    stream_duration = video_stream.get("duration")
    for d in (stream_duration, fmt_duration):
        if d:
            try:
                duration = float(d)
                break
            except (TypeError, ValueError):
                pass

    fps = None
    r_frame_rate = video_stream.get("r_frame_rate", "")
    if "/" in str(r_frame_rate):
        parts = r_frame_rate.split("/")
        try:
            fps = float(parts[0]) / float(parts[1])
        except (ValueError, ZeroDivisionError):
            pass

    frame_count = None
    nb_frames = video_stream.get("nb_frames")
    if nb_frames:
        try:
            frame_count = int(nb_frames)
        except (TypeError, ValueError):
            pass

    return {
        "duration_sec": duration,
        "fps": fps,
        "frame_count": frame_count,
        "width": video_stream.get("width"),
        "height": video_stream.get("height"),
        "codec": video_stream.get("codec_name"),
    }


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


_is_vertex_rate_limit_error = is_vertex_rate_limit_error


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
    temp_path = _build_nonexistent_temp_path(".jpg")
    temp_path.write_bytes(frame_bytes)
    try:
        response_text = analyzer.analyze_image(str(temp_path), prompt=prompt)
        cleaned = extract_clean_json_text(response_text)
        return parse_event_frame_relevance_response(cleaned)
    finally:
        _cleanup_temp_path(temp_path)


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


def _cleanup_temp_path(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _build_nonexistent_temp_path(suffix: str) -> Path:
    return build_nonexistent_temp_path(suffix)


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
    temp_path = _build_nonexistent_temp_path(".jpg")
    temp_path.write_bytes(frame_bytes)
    try:
        response_text = analyzer.analyze_image(str(temp_path), prompt=prompt)
        cleaned = extract_clean_json_text(response_text)
        is_relevant, caption_text = parse_event_frame_image_caption_response(cleaned)
        if not is_relevant:
            return None
        return caption_text
    finally:
        _cleanup_temp_path(temp_path)


def _load_gemini_label_event(
    minio: MinIOResource,
    labels_key: str,
    event_index: int,
    *,
    cache: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    normalized_key = str(labels_key or "").strip()
    if not normalized_key:
        return {}

    cached = cache.get(normalized_key)
    if cached is None:
        raw_bytes = minio.download("vlm-labels", normalized_key)
        cached = normalize_gemini_events(
            load_clean_json(raw_bytes.decode("utf-8", errors="replace"))
        )
        cache[normalized_key] = cached

    if 0 <= int(event_index) < len(cached):
        return dict(cached[int(event_index)])
    return {}


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


def _extract_video_clip_path(
    video_path: Path, *, clip_start_sec: float, clip_end_sec: float,
) -> Path:
    duration_sec = max(0.05, float(clip_end_sec) - float(clip_start_sec))
    output_path = _build_nonexistent_temp_path(".mp4")

    copy_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", f"{float(clip_start_sec):.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(video_path), "-map", "0:v:0", "-map", "0:a?",
        "-c", "copy", "-movflags", "+faststart", str(output_path),
    ]
    copy_proc = subprocess.run(copy_cmd, capture_output=True, check=False)
    if copy_proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return output_path
    _cleanup_temp_path(output_path)
    output_path = _build_nonexistent_temp_path(".mp4")

    reencode_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", f"{float(clip_start_sec):.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(video_path), "-map", "0:v:0", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac",
        "-movflags", "+faststart", str(output_path),
    ]
    reencode_proc = subprocess.run(reencode_cmd, capture_output=True, check=False)
    if reencode_proc.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        stderr = (reencode_proc.stderr or copy_proc.stderr or b"").decode("utf-8", errors="ignore").strip()
        _cleanup_temp_path(output_path)
        raise RuntimeError(f"ffmpeg_clip_extract_failed:{stderr or 'empty_output'}")
    return output_path


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
