"""PROCESS 공유 헬퍼 — ffprobe clip meta, event 필터/정규화, Gemini label row 빌더."""

from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.gemini import load_clean_json
from vlm_pipeline.lib.vertex_chunking import normalize_gemini_events
from vlm_pipeline.resources.minio import MinIOResource


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


def _stable_gemini_label_id(
    asset_id: str, event_index: int, start_sec: float | None, end_sec: float | None,
) -> str:
    token = f"{asset_id}|gemini|auto|{event_index}|{start_sec}|{end_sec}"
    return sha1(token.encode("utf-8")).hexdigest()


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
