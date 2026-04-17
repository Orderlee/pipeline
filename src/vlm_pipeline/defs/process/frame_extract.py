"""clip_to_frame — MVP 및 spec/dispatch 라우팅 구현 (facade).

실제 구현은 도메인별 submodule로 분리됨:
- ``frame_extract_common``  — _CandidateFields, 초기 row 빌더, 실패 cleanup
- ``frame_extract_mvp``     — MVP 흐름 (clip_to_frame_mvp)
- ``frame_extract_routed``  — spec/dispatch 흐름 (clip_to_frame_routed_impl)

기존 ``from .frame_extract import clip_to_frame_mvp, clip_to_frame_routed_impl``
경로를 유지하기 위해 두 심볼을 re-export한다.
"""

from __future__ import annotations

from .frame_extract_mvp import clip_to_frame_mvp
from .frame_extract_routed import clip_to_frame_routed_impl

__all__ = ["clip_to_frame_mvp", "clip_to_frame_routed_impl"]
