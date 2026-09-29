from __future__ import annotations

import pytest

from vlm_pipeline.lib import gemini as gemini_lib


class _DummyModel:
    def __init__(self, side_effects):
        self._side_effects = list(side_effects)
        self.calls = 0

    def generate_content(self, parts):
        self.calls += 1
        effect = self._side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


def _build_analyzer(side_effects):
    analyzer = gemini_lib.GeminiAnalyzer.__new__(gemini_lib.GeminiAnalyzer)
    analyzer.model = _DummyModel(side_effects)
    analyzer._rate_limit_max_retries = 2
    analyzer._rate_limit_base_delay_sec = 0.1
    analyzer._rate_limit_backoff = 2.0
    analyzer._rate_limit_max_delay_sec = 1.0
    # Phase 4-A (#10) — 503/504 계열은 별도 예산으로 분리되어 있음 (gemini.py 참고).
    # 이 테스트들은 429 경로만 검증하므로 재시도는 0으로 비활성화.
    analyzer._server_error_max_retries = 0
    analyzer._server_error_base_delay_sec = 10.0
    analyzer._server_error_backoff = 2.0
    analyzer._server_error_max_delay_sec = 60.0
    analyzer._call_count = 0
    analyzer._total_prompt_tokens = 0
    analyzer._total_response_tokens = 0
    analyzer._total_tokens = 0
    return analyzer


def test_is_vertex_rate_limit_error_detects_429_message():
    assert gemini_lib._is_vertex_rate_limit_error(RuntimeError("429 Resource exhausted"))
    assert gemini_lib._is_vertex_rate_limit_error(RuntimeError("resource exhausted"))
    assert not gemini_lib._is_vertex_rate_limit_error(RuntimeError("permission denied"))


def test_generate_content_with_retry_retries_and_succeeds(monkeypatch):
    analyzer = _build_analyzer(
        [
            RuntimeError("429 Resource exhausted"),
            {"text": "ok"},
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(gemini_lib.time, "sleep", lambda value: sleeps.append(value))

    response = analyzer._generate_content_with_retry(
        ["part"],
        content_type="video",
        source_name="sample.mp4",
    )

    assert response == {"text": "ok"}
    assert analyzer.model.calls == 2
    assert sleeps == [0.1]


def test_generate_content_with_retry_raises_non_429_without_retry(monkeypatch):
    analyzer = _build_analyzer([RuntimeError("permission denied")])
    sleeps: list[float] = []
    monkeypatch.setattr(gemini_lib.time, "sleep", lambda value: sleeps.append(value))

    with pytest.raises(RuntimeError, match="permission denied"):
        analyzer._generate_content_with_retry(
            ["part"],
            content_type="video",
            source_name="sample.mp4",
        )

    assert analyzer.model.calls == 1
    assert sleeps == []


def test_generate_content_with_retry_exhausts_429_and_raises(monkeypatch):
    analyzer = _build_analyzer(
        [
            RuntimeError("429 Resource exhausted"),
            RuntimeError("429 Resource exhausted"),
            RuntimeError("429 Resource exhausted"),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(gemini_lib.time, "sleep", lambda value: sleeps.append(value))

    with pytest.raises(RuntimeError, match="429 Resource exhausted"):
        analyzer._generate_content_with_retry(
            ["part"],
            content_type="video",
            source_name="sample.mp4",
        )

    assert analyzer.model.calls == 3
    assert sleeps == [0.1, 0.2]
