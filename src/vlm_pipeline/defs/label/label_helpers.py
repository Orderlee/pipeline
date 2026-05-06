"""LABEL helpers — thin facade re-exporting from domain submodules.

도메인별 분할:
- ``helpers_common``         — ``_build_nonexistent_temp_path`` + ``int_env`` 재내보내기
- ``helpers_keys``           — Gemini / classification label key 빌더
- ``helpers_video``          — 비디오 다운로드(materialize) + ffmpeg 세그먼트 추출
- ``helpers_gemini``         — Gemini analyzer / 요청 준비 / chunked 분석 / 응답 (de)serialize / preview 렌더
- ``helpers_classification`` — video classification prompt / 응답 파싱 / dispatch 후보

기존 caller(``timestamp.py``, ``assets.py``, ``tests/unit/test_gemini_json_parsing.py``)는
``from vlm_pipeline.defs.label.label_helpers import <symbol>`` 경로를 그대로 사용.
"""

from __future__ import annotations

from .helpers_classification import (
    build_video_classification_prompt,
    find_dispatch_video_classification_candidates,
    parse_video_classification_response,
    resolve_dispatch_video_class_candidates,
    stable_video_classification_label_id,
)
from .helpers_common import int_env
from .helpers_gemini import (
    analyze_routed_video_events,
    clone_gemini_analyzer,
    init_gemini_analyzer,
    parse_gemini_events_response,
    prepare_gemini_video_for_request,
    serialize_gemini_events,
)
from .helpers_keys import build_gemini_label_key, build_video_classification_key
from .helpers_video import materialize_video

__all__ = [
    "analyze_routed_video_events",
    "build_gemini_label_key",
    "build_video_classification_key",
    "build_video_classification_prompt",
    "clone_gemini_analyzer",
    "find_dispatch_video_classification_candidates",
    "init_gemini_analyzer",
    "int_env",
    "materialize_video",
    "parse_gemini_events_response",
    "parse_video_classification_response",
    "prepare_gemini_video_for_request",
    "resolve_dispatch_video_class_candidates",
    "serialize_gemini_events",
    "stable_video_classification_label_id",
]
