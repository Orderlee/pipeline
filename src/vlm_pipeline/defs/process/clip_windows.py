"""PROCESS 클립 윈도우 계획 — 이벤트 기반 클립 추출 구간 결정 로직.

helpers.py에서 분리된 클립 윈도우 계획 관련 함수.
"""

from __future__ import annotations

from typing import Any


_DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC = 5.0
_DEFAULT_EVENT_CLIP_POST_BUFFER_SEC = 5.0


def _coerce_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def candidate_clip_window_plan_key(candidate: dict[str, Any]) -> tuple[str, int]:
    return (
        str(candidate.get("label_id") or "").strip(),
        int(candidate.get("event_index") or 0),
    )


def sort_process_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        candidates,
        key=lambda row: (
            str(row.get("asset_id") or ""),
            float(_coerce_float_or_none(row.get("timestamp_start_sec")) or 0.0),
            float(_coerce_float_or_none(row.get("timestamp_end_sec")) or 0.0),
            int(row.get("event_index") or 0),
            str(row.get("label_id") or ""),
        ),
    )


def window_values_match(
    left_start_sec: float,
    left_end_sec: float,
    right_start_sec: float,
    right_end_sec: float,
    *,
    tolerance: float = 1e-6,
) -> bool:
    return (
        abs(float(left_start_sec) - float(right_start_sec)) <= tolerance
        and abs(float(left_end_sec) - float(right_end_sec)) <= tolerance
    )


def is_full_duration_window(
    start_sec: float,
    end_sec: float,
    source_duration_sec: float | None,
    *,
    tolerance: float = 1e-6,
) -> bool:
    normalized = _coerce_float_or_none(source_duration_sec)
    if normalized is None or normalized <= 0:
        return False
    return abs(float(start_sec)) <= tolerance and abs(float(end_sec) - normalized) <= tolerance


def build_adjacent_event_cut_point(left_event: dict[str, Any], right_event: dict[str, Any]) -> float:
    left_event_end_sec = float(left_event["event_end_sec"])
    right_event_start_sec = float(right_event["event_start_sec"])
    if left_event_end_sec <= right_event_start_sec:
        return (left_event_end_sec + right_event_start_sec) / 2.0

    left_center_sec = (float(left_event["event_start_sec"]) + left_event_end_sec) / 2.0
    right_center_sec = (right_event_start_sec + float(right_event["event_end_sec"])) / 2.0
    return (left_center_sec + right_center_sec) / 2.0


def resolve_event_only_clip_extraction_window(
    *,
    event_start_sec: float | None,
    event_end_sec: float | None,
    source_duration_sec: float | None,
) -> tuple[float, float]:
    normalized_event_start = _coerce_float_or_none(event_start_sec)
    normalized_event_end = _coerce_float_or_none(event_end_sec)
    if normalized_event_start is None or normalized_event_end is None:
        raise RuntimeError("video_clip_range_missing")
    if normalized_event_end <= normalized_event_start:
        raise RuntimeError("video_clip_range_missing")

    extract_start_sec = max(0.0, normalized_event_start)
    extract_end_sec = normalized_event_end
    normalized_source_duration = _coerce_float_or_none(source_duration_sec)
    if normalized_source_duration is not None and normalized_source_duration > 0:
        extract_end_sec = min(normalized_source_duration, extract_end_sec)

    if extract_end_sec <= extract_start_sec:
        raise RuntimeError(
            "video_clip_range_invalid:"
            f"event_start={normalized_event_start:.3f}:"
            f"event_end={normalized_event_end:.3f}:"
            f"extract_start={extract_start_sec:.3f}:"
            f"extract_end={extract_end_sec:.3f}"
        )

    return extract_start_sec, extract_end_sec


def resolve_event_clip_extraction_window(
    *,
    event_start_sec: float | None,
    event_end_sec: float | None,
    source_duration_sec: float | None,
    event_category: str | None = None,
) -> tuple[float, float]:
    """Resolve the actual clip extraction window with boundary-safe buffering."""
    normalized_event_start = _coerce_float_or_none(event_start_sec)
    normalized_event_end = _coerce_float_or_none(event_end_sec)
    if normalized_event_start is None or normalized_event_end is None:
        raise RuntimeError("video_clip_range_missing")
    if normalized_event_end <= normalized_event_start:
        raise RuntimeError("video_clip_range_missing")

    pre_buffer_sec = _DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC
    post_buffer_sec = _DEFAULT_EVENT_CLIP_POST_BUFFER_SEC

    buffered_start_sec = normalized_event_start - float(pre_buffer_sec)
    buffered_end_sec = normalized_event_end + float(post_buffer_sec)
    extract_start_sec = max(0.0, buffered_start_sec)
    extract_end_sec = buffered_end_sec

    normalized_source_duration = _coerce_float_or_none(source_duration_sec)
    if normalized_source_duration is not None and normalized_source_duration > 0:
        extract_end_sec = min(normalized_source_duration, buffered_end_sec)
        if extract_end_sec <= extract_start_sec:
            extract_end_sec = min(normalized_source_duration, normalized_event_end)

    if extract_end_sec <= extract_start_sec:
        raise RuntimeError(
            "video_clip_range_invalid:"
            f"event_start={normalized_event_start:.3f}:"
            f"event_end={normalized_event_end:.3f}:"
            f"extract_start={extract_start_sec:.3f}:"
            f"extract_end={extract_end_sec:.3f}"
        )

    return extract_start_sec, extract_end_sec


def plan_asset_event_clip_extraction_windows(
    candidates: list[dict[str, Any]],
) -> dict[tuple[str, int], dict[str, float | str]]:
    plans: dict[tuple[str, int], dict[str, float | str]] = {}
    grouped_candidates: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        asset_id = str(candidate.get("asset_id") or "").strip()
        if not asset_id:
            continue
        grouped_candidates.setdefault(asset_id, []).append(candidate)

    for asset_candidates in grouped_candidates.values():
        planned_events: list[dict[str, Any]] = []
        for candidate in sort_process_candidates(asset_candidates):
            if str(candidate.get("media_type") or "").strip().lower() != "video":
                continue
            event_start_sec = _coerce_float_or_none(candidate.get("timestamp_start_sec"))
            event_end_sec = _coerce_float_or_none(candidate.get("timestamp_end_sec"))
            if event_start_sec is None or event_end_sec is None or event_end_sec <= event_start_sec:
                continue
            source_duration_sec = _coerce_float_or_none(candidate.get("video_duration_sec"))
            base_start_sec, base_end_sec = resolve_event_clip_extraction_window(
                event_start_sec=event_start_sec,
                event_end_sec=event_end_sec,
                source_duration_sec=source_duration_sec,
                event_category=None,
            )
            planned_events.append(
                {
                    "key": candidate_clip_window_plan_key(candidate),
                    "event_start_sec": event_start_sec,
                    "event_end_sec": event_end_sec,
                    "source_duration_sec": source_duration_sec,
                    "base_extract_start_sec": base_start_sec,
                    "base_extract_end_sec": base_end_sec,
                }
            )

        pair_cut_points: dict[int, float] = {}
        for idx in range(len(planned_events) - 1):
            left_event = planned_events[idx]
            right_event = planned_events[idx + 1]
            same_base_window = window_values_match(
                float(left_event["base_extract_start_sec"]),
                float(left_event["base_extract_end_sec"]),
                float(right_event["base_extract_start_sec"]),
                float(right_event["base_extract_end_sec"]),
            )
            both_full_duration = is_full_duration_window(
                float(left_event["base_extract_start_sec"]),
                float(left_event["base_extract_end_sec"]),
                _coerce_float_or_none(left_event.get("source_duration_sec")),
            ) and is_full_duration_window(
                float(right_event["base_extract_start_sec"]),
                float(right_event["base_extract_end_sec"]),
                _coerce_float_or_none(right_event.get("source_duration_sec")),
            )
            if not same_base_window and not both_full_duration:
                continue
            pair_cut_points[idx] = build_adjacent_event_cut_point(left_event, right_event)

        for idx, planned_event in enumerate(planned_events):
            extract_start_sec = float(planned_event["base_extract_start_sec"])
            extract_end_sec = float(planned_event["base_extract_end_sec"])
            window_strategy = "buffered"
            prev_cut_point = pair_cut_points.get(idx - 1)
            next_cut_point = pair_cut_points.get(idx)

            if prev_cut_point is not None or next_cut_point is not None:
                adjusted_start_sec = extract_start_sec if prev_cut_point is None else max(extract_start_sec, prev_cut_point)
                adjusted_end_sec = extract_end_sec if next_cut_point is None else min(extract_end_sec, next_cut_point)
                adjusted_start_sec = min(adjusted_start_sec, float(planned_event["event_start_sec"]))
                adjusted_end_sec = max(adjusted_end_sec, float(planned_event["event_end_sec"]))
                adjusted_start_sec = max(0.0, adjusted_start_sec)
                source_duration_sec = _coerce_float_or_none(planned_event.get("source_duration_sec"))
                if source_duration_sec is not None and source_duration_sec > 0:
                    adjusted_end_sec = min(source_duration_sec, adjusted_end_sec)
                if adjusted_end_sec <= adjusted_start_sec:
                    adjusted_start_sec, adjusted_end_sec = resolve_event_only_clip_extraction_window(
                        event_start_sec=planned_event.get("event_start_sec"),
                        event_end_sec=planned_event.get("event_end_sec"),
                        source_duration_sec=planned_event.get("source_duration_sec"),
                    )
                if not window_values_match(
                    extract_start_sec,
                    extract_end_sec,
                    adjusted_start_sec,
                    adjusted_end_sec,
                ):
                    extract_start_sec = adjusted_start_sec
                    extract_end_sec = adjusted_end_sec
                    window_strategy = "split_by_adjacent_events"

            plans[planned_event["key"]] = {
                "extract_start_sec": extract_start_sec,
                "extract_end_sec": extract_end_sec,
                "base_extract_start_sec": float(planned_event["base_extract_start_sec"]),
                "base_extract_end_sec": float(planned_event["base_extract_end_sec"]),
                "window_strategy": window_strategy,
            }

    return plans
