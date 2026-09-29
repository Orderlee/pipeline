from __future__ import annotations

from pathlib import Path

import vlm_pipeline.lib.gemini_prompts as gemini_prompts


def test_gemini_prompts_exports_expected_symbols() -> None:
    source = Path(gemini_prompts.__file__).read_text(encoding="utf-8")

    assert "from __future__ import annotations" not in source
    assert hasattr(gemini_prompts, "IMAGE_PROMPT")
    assert hasattr(gemini_prompts, "VIDEO_PROMPT")
    assert hasattr(gemini_prompts, "VIDEO_EVENT_PROMPT")
    assert hasattr(gemini_prompts, "VIDEO_EVENT_SCHEMA")
    # build_video_event_prompt() 는 category-aware prompt builder 로 defs/label/timestamp.py
    # 가 실제로 사용 중 (하이브리드 프리셋 카테고리 주입, 56b222d) — legacy 시절과 달리 현재는
    # gemini_prompts 모듈의 정식 export.
    assert hasattr(gemini_prompts, "build_video_event_prompt")
    # 이 두 헬퍼는 lib/vertex_chunking.py 소속이라 gemini_prompts 에는 여전히 없어야 한다.
    assert not hasattr(gemini_prompts, "DEFAULT_EVENT_DETECTION_PROMPTS")
    assert not hasattr(gemini_prompts, "build_event_frame_relevance_prompt")
    assert not hasattr(gemini_prompts, "build_event_frame_image_prompt")


def test_video_event_prompt_matches_legacy_rules_and_example() -> None:
    prompt = gemini_prompts.VIDEO_EVENT_PROMPT

    assert '"category": "smoke"' in prompt
    assert '"timestamp": [12.0, 15.5]' in prompt
    assert "Smoke emerges from the left side of the building and gradually spreads" in prompt
    # 카테고리 예시 문구는 여러 차례 바뀌었으므로(9aaebe1, d04360f) 정확한 문자열을 얼리지
    # 않고 핵심 카테고리들이 example 목록에 포함되는지만 확인한다.
    for category in ("fire", "smoke", "fall", "intrusion", "fight", "vehicle_accident"):
        assert f'"{category}"' in prompt
    # normal_activity 는 이벤트로 강제 보고하지 않는다는 규칙이 (표현은 바뀌어도) 반드시 있어야 함.
    assert "normal_activity" in prompt
    assert "notable" in prompt.lower()
