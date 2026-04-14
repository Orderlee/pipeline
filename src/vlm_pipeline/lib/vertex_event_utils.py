"""Vertex event processing helpers for the active prod/test runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .gemini import load_clean_json


@dataclass(frozen=True)
class VideoChunkWindow:
    """Absolute video chunk window used for Gemini video analysis requests."""

    chunk_index: int
    start_sec: float
    end_sec: float

    @property
    def duration_sec(self) -> float:
        return max(0.0, float(self.end_sec) - float(self.start_sec))


def plan_overlapping_video_chunks(
    duration_sec: float | int | None,
    *,
    window_sec: float = 660.0,
    stride_sec: float = 600.0,
) -> list[VideoChunkWindow]:
    """Plan [10 min stride + 1 min overlap] chunk windows for a video."""
    try:
        total_duration = float(duration_sec or 0.0)
    except (TypeError, ValueError):
        total_duration = 0.0

    if total_duration <= 0:
        return []

    try:
        window_value = float(window_sec)
        stride_value = float(stride_sec)
    except (TypeError, ValueError):
        return []

    if window_value <= 0 or stride_value <= 0:
        return []

    chunks: list[VideoChunkWindow] = []
    start_sec = 0.0
    chunk_index = 1
    epsilon = 1e-6

    while start_sec < total_duration - epsilon:
        end_sec = min(start_sec + window_value, total_duration)
        chunks.append(
            VideoChunkWindow(
                chunk_index=chunk_index,
                start_sec=round(start_sec, 3),
                end_sec=round(end_sec, 3),
            )
        )
        if end_sec >= total_duration - epsilon:
            break
        start_sec += stride_value
        chunk_index += 1

    return chunks


def normalize_gemini_events(payload: Any) -> list[dict[str, Any]]:
    """Filter Gemini event payload into sorted, stable event dicts."""
    if isinstance(payload, dict):
        events = [payload]
    elif isinstance(payload, list):
        events = payload
    else:
        events = []

    normalized: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue

        timestamp = event.get("timestamp")
        if not isinstance(timestamp, list) or len(timestamp) < 2:
            continue

        try:
            start_sec = float(timestamp[0])
            end_sec = float(timestamp[1])
        except (TypeError, ValueError):
            continue

        if start_sec < 0 or end_sec <= start_sec:
            continue

        category = str(event.get("category") or "").strip()
        ko_caption = str(event.get("ko_caption") or "").strip()
        en_caption = str(event.get("en_caption") or "").strip()
        normalized.append(
            {
                "category": category or None,
                "duration": round(end_sec - start_sec, 3),
                "timestamp": [round(start_sec, 3), round(end_sec, 3)],
                "ko_caption": ko_caption or None,
                "en_caption": en_caption or None,
            }
        )

    normalized.sort(key=lambda item: (float(item["timestamp"][0]), float(item["timestamp"][1])))
    return normalized


def offset_gemini_events(
    events: list[dict[str, Any]],
    *,
    offset_sec: float,
    chunk_end_sec: float | None = None,
) -> list[dict[str, Any]]:
    """Convert chunk-relative timestamps into absolute video timestamps."""
    try:
        offset_value = float(offset_sec)
    except (TypeError, ValueError):
        offset_value = 0.0

    normalized_chunk_end = None
    if chunk_end_sec is not None:
        try:
            normalized_chunk_end = float(chunk_end_sec)
        except (TypeError, ValueError):
            normalized_chunk_end = None

    adjusted: list[dict[str, Any]] = []
    for event in normalize_gemini_events(events):
        start_sec = float(event["timestamp"][0]) + offset_value
        end_sec = float(event["timestamp"][1]) + offset_value
        if normalized_chunk_end is not None:
            start_sec = max(offset_value, min(start_sec, normalized_chunk_end))
            end_sec = max(offset_value, min(end_sec, normalized_chunk_end))
        if end_sec <= start_sec:
            continue
        adjusted.append(
            {
                "category": event.get("category"),
                "duration": round(end_sec - start_sec, 3),
                "timestamp": [round(start_sec, 3), round(end_sec, 3)],
                "ko_caption": event.get("ko_caption"),
                "en_caption": event.get("en_caption"),
            }
        )

    adjusted.sort(key=lambda item: (float(item["timestamp"][0]), float(item["timestamp"][1])))
    return adjusted


def merge_overlapping_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge same-category events that overlap after chunk offset correction."""
    merged: list[dict[str, Any]] = []
    for event in normalize_gemini_events(events):
        if not merged:
            merged.append(dict(event))
            continue

        prev = merged[-1]
        prev_category = str(prev.get("category") or "").strip().lower()
        curr_category = str(event.get("category") or "").strip().lower()
        prev_start = float(prev["timestamp"][0])
        prev_end = float(prev["timestamp"][1])
        curr_start = float(event["timestamp"][0])
        curr_end = float(event["timestamp"][1])
        overlaps = curr_start <= prev_end and curr_end >= prev_start

        if prev_category and curr_category and prev_category == curr_category and overlaps:
            start_sec = min(prev_start, curr_start)
            end_sec = max(prev_end, curr_end)
            prev["timestamp"] = [round(start_sec, 3), round(end_sec, 3)]
            prev["duration"] = round(end_sec - start_sec, 3)
            prev["ko_caption"] = _prefer_longer_text(prev.get("ko_caption"), event.get("ko_caption"))
            prev["en_caption"] = _prefer_longer_text(prev.get("en_caption"), event.get("en_caption"))
            continue

        merged.append(dict(event))

    return merged


def build_event_frame_relevance_prompt(
    *,
    event_category: str | None,
    event_caption_text: str | None,
) -> str:
    """Build a strict JSON prompt for frame-to-event relevance scoring."""
    category_text = str(event_category or "unknown").strip() or "unknown"
    caption_text = str(event_caption_text or "").strip() or "N/A"
    return (
        "You are ranking how visually similar a CCTV frame is to a parent abnormal event.\n\n"
        f'Parent event category: "{category_text}"\n'
        f'Parent event description: "{caption_text}"\n\n'
        "Task:\n"
        "Return one relevance score between 0.0 and 1.0 based only on visible evidence in the frame.\n\n"
        "Return JSON only in this shape:\n"
        '{"relevance_score": 0.0}\n\n'
        "Rules:\n"
        "- 1.0 means the frame strongly matches the parent event.\n"
        "- 0.0 means the frame is unrelated to the parent event.\n"
        "- Use only the visible frame, not guesses.\n"
        "- Do not include markdown fences or extra explanation."
    )


def build_event_frame_image_prompt(
    *,
    event_category: str | None,
    event_caption_text: str | None,
) -> str:
    """Build a strict JSON prompt for event-frame relevance + captioning."""
    category_text = str(event_category or "unknown").strip() or "unknown"
    caption_text = str(event_caption_text or "").strip() or "N/A"
    return (
        "You are validating whether a CCTV frame is relevant to a parent abnormal event.\n\n"
        f'Parent event category: "{category_text}"\n'
        f'Parent event description: "{caption_text}"\n\n'
        "Tasks:\n"
        "1. Decide whether this frame is visually relevant to the parent event.\n"
        "2. If relevant, write one concise Korean caption grounded only in the visible frame.\n"
        "3. If not relevant, caption must be null.\n\n"
        "Return JSON only in this shape:\n"
        '{"is_relevant": true, "caption": "..." }\n\n'
        "Rules:\n"
        "- is_relevant must be boolean.\n"
        "- caption must be a string when is_relevant is true.\n"
        "- caption must be null when is_relevant is false.\n"
        "- Do not include markdown fences or extra explanation."
    )


def parse_event_frame_relevance_response(payload_text: str) -> float:
    """Parse strict JSON response for frame-to-event relevance score."""
    payload = load_clean_json(payload_text)
    if not isinstance(payload, dict):
        raise ValueError("image_relevance_response_not_object")

    score_value = payload.get("relevance_score")
    if isinstance(score_value, bool) or score_value is None:
        raise ValueError("image_relevance_response_missing_numeric_score")

    try:
        score = float(score_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("image_relevance_response_invalid_score") from exc

    if score < 0.0:
        return 0.0
    if score > 1.0:
        return 1.0
    return score


def parse_event_frame_image_caption_response(payload_text: str) -> tuple[bool, str | None]:
    """Parse strict JSON response for event-frame relevance + caption."""
    payload = load_clean_json(payload_text)
    if not isinstance(payload, dict):
        raise ValueError("image_caption_response_not_object")

    is_relevant = payload.get("is_relevant")
    if not isinstance(is_relevant, bool):
        raise ValueError("image_caption_response_missing_boolean_relevance")

    caption_value = payload.get("caption")
    caption_text = str(caption_value or "").strip() or None
    if not is_relevant:
        return False, None
    if caption_text is None:
        raise ValueError("image_caption_response_missing_caption")
    return True, caption_text


def select_top_relevance_index(scores: list[float | None]) -> int | None:
    """Return the first index with the highest valid relevance score."""
    best_index: int | None = None
    best_score: float | None = None
    for index, score in enumerate(scores):
        if score is None:
            continue
        normalized_score = float(score)
        if best_index is None or normalized_score > float(best_score):
            best_index = index
            best_score = normalized_score
    return best_index


def _prefer_longer_text(left: Any, right: Any) -> str | None:
    left_text = str(left or "").strip()
    right_text = str(right or "").strip()
    if len(right_text) > len(left_text):
        return right_text or None
    return left_text or None


__all__ = [
    "VideoChunkWindow",
    "build_event_frame_image_prompt",
    "build_event_frame_relevance_prompt",
    "merge_overlapping_events",
    "normalize_gemini_events",
    "offset_gemini_events",
    "parse_event_frame_image_caption_response",
    "parse_event_frame_relevance_response",
    "plan_overlapping_video_chunks",
    "select_top_relevance_index",
]
