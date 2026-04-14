"""Compatibility wrapper for legacy test-era Vertex event helpers."""

from __future__ import annotations

from vlm_pipeline.lib.vertex_event_utils import (
    VideoChunkWindow,
    build_event_frame_image_prompt,
    build_event_frame_relevance_prompt,
    merge_overlapping_events,
    normalize_gemini_events,
    offset_gemini_events,
    parse_event_frame_image_caption_response,
    parse_event_frame_relevance_response,
    plan_overlapping_video_chunks,
    select_top_relevance_index,
)

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
