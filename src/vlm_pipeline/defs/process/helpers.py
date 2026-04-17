"""PROCESS 공유 헬퍼 — 도메인별 4개 submodule의 facade.

기존 ``from .helpers import _X`` 경로를 유지하기 위해 public/legacy 심볼을 그대로 re-export한다.
실제 구현은:
- ``helpers_key_utils``  — path/key builders, coercion, stable ID
- ``helpers_metadata``   — ffprobe, event 필터, Gemini label row
- ``helpers_frame_caption`` — 프레임 추출, 이미지 caption relevance/생성
- ``helpers_clip_media`` — event 기반 video clip 추출 + dedup
"""

from __future__ import annotations

from .clip_windows import (
    _DEFAULT_EVENT_CLIP_POST_BUFFER_SEC,
    _DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC,
    candidate_clip_window_plan_key,
    plan_asset_event_clip_extraction_windows,
    resolve_event_clip_extraction_window,
    resolve_event_only_clip_extraction_window,
    sort_process_candidates,
    window_values_match,
)
from .helpers_clip_media import (
    _extract_video_clip_media,
    _extract_video_clip_path,
    _log_clip_extraction_window,
)
from .helpers_frame_caption import (
    _MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET,
    _VERTEX_RELEVANCE_RETRY_DELAY_SEC,
    _extract_clip_frames,
    _generate_event_frame_image_caption,
    _is_empty_output_frame_error,
    _is_vertex_rate_limit_error,
    _reset_empty_output_failure,
    _score_event_frame_image_relevance,
    _score_event_frame_image_relevance_with_retry,
    _should_log_clip_frame_progress,
    _track_empty_output_failure,
)
from .helpers_key_utils import (
    _build_image_caption_key,
    _build_image_caption_payload,
    _build_processed_clip_image_key,
    _build_processed_clip_key,
    _coerce_float,
    _coerce_int,
    _delete_minio_keys,
    _materialize_object_path,
    _materialize_video_path,
    _stable_clip_id,
)
from .helpers_metadata import (
    _build_gemini_label_rows,
    _ffprobe_clip_meta,
    _filter_valid_events,
    _load_gemini_label_event,
    _stable_gemini_label_id,
)

# Legacy underscore aliases for clip_windows re-exports — frame_extract.py 등에서 사용.
_candidate_clip_window_plan_key = candidate_clip_window_plan_key
_sort_process_candidates = sort_process_candidates
_resolve_event_only_clip_extraction_window = resolve_event_only_clip_extraction_window
_resolve_event_clip_extraction_window = resolve_event_clip_extraction_window
_plan_asset_event_clip_extraction_windows = plan_asset_event_clip_extraction_windows
_window_values_match = window_values_match

__all__ = [
    # clip_windows re-exports (public + legacy underscore)
    "_DEFAULT_EVENT_CLIP_POST_BUFFER_SEC",
    "_DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC",
    "candidate_clip_window_plan_key",
    "plan_asset_event_clip_extraction_windows",
    "resolve_event_clip_extraction_window",
    "resolve_event_only_clip_extraction_window",
    "sort_process_candidates",
    "window_values_match",
    "_candidate_clip_window_plan_key",
    "_sort_process_candidates",
    "_resolve_event_only_clip_extraction_window",
    "_resolve_event_clip_extraction_window",
    "_plan_asset_event_clip_extraction_windows",
    "_window_values_match",
    # clip_media
    "_extract_video_clip_media",
    "_extract_video_clip_path",
    "_log_clip_extraction_window",
    # frame_caption
    "_MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET",
    "_VERTEX_RELEVANCE_RETRY_DELAY_SEC",
    "_extract_clip_frames",
    "_generate_event_frame_image_caption",
    "_is_empty_output_frame_error",
    "_is_vertex_rate_limit_error",
    "_reset_empty_output_failure",
    "_score_event_frame_image_relevance",
    "_score_event_frame_image_relevance_with_retry",
    "_should_log_clip_frame_progress",
    "_track_empty_output_failure",
    # key_utils
    "_build_image_caption_key",
    "_build_image_caption_payload",
    "_build_processed_clip_image_key",
    "_build_processed_clip_key",
    "_coerce_float",
    "_coerce_int",
    "_delete_minio_keys",
    "_materialize_object_path",
    "_materialize_video_path",
    "_stable_clip_id",
    # metadata
    "_build_gemini_label_rows",
    "_ffprobe_clip_meta",
    "_filter_valid_events",
    "_load_gemini_label_event",
    "_stable_gemini_label_id",
]
