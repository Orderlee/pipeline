"""Compatibility wrapper for legacy Gemini imports."""

from __future__ import annotations

from vlm_pipeline.lib.gemini import (
    GeminiAnalyzer,
    extract_clean_json_text,
    json_save_path_same_dir_as_video,
    list_video_files,
    process_video_folder,
    resolve_gemini_credentials_path,
    save_response_as_json,
)
from vlm_pipeline.lib.gemini_prompts import IMAGE_PROMPT, VIDEO_PROMPT

__all__ = [
    "GeminiAnalyzer",
    "IMAGE_PROMPT",
    "VIDEO_PROMPT",
    "extract_clean_json_text",
    "json_save_path_same_dir_as_video",
    "list_video_files",
    "process_video_folder",
    "resolve_gemini_credentials_path",
    "save_response_as_json",
]
