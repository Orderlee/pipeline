"""Phase 4-A (#10) — Vertex 503/deadline retry + usage counter unit tests.

기존 ``tests/unit/test_gemini_retry.py`` 는 429 케이스를 cover 하지만 git 미추적
(allowlist 정책상). 본 파일은 503/deadline 새 경로 + usage_metadata 누적을 검증하기
위한 *추적되는* 신규 테스트.

CI host pydantic skew 회피: ``importlib.util.spec_from_file_location()`` 으로
workspace source 를 직접 load (Phase 3-C/D 와 동일 패턴 — runner _work 의 stale
editable install 우회).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_gemini_module():
    """CI runner 의 stale editable install 회피 + relative import 보존.

    Phase 3-C 패턴(spec_from_file_location) 은 gemini.py 의 ``from .env_utils import ...``
    relative import 를 깬다. 대신 sys.modules 에서 캐시된 모듈을 명시 제거 후
    ``importlib.import_module`` 으로 재로드 — workspace 의 src/ 가 PYTHONPATH 에 있으면
    매 호출마다 최신 source 가 잡힌다.
    """
    # workspace src 우선순위 확보 (anaconda site-packages 보다 앞).
    workspace_src = Path(__file__).resolve().parents[2] / "src"
    src_str = str(workspace_src)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

    # 캐시 제거 — 부모 패키지부터 자식까지 모두.
    for mod_name in list(sys.modules):
        if mod_name == "vlm_pipeline" or mod_name.startswith("vlm_pipeline."):
            del sys.modules[mod_name]

    module = importlib.import_module("vlm_pipeline.lib.gemini")
    # 명시적 sanity check — 새 method 가 안 잡히면 CI 환경 진단이 쉽도록 즉시 fail.
    if not hasattr(module, "is_vertex_server_error"):
        raise RuntimeError(
            f"Phase 4-A method missing from imported module ({module.__file__}). "
            "CI runner 의 stale editable install 가능성."
        )
    return module


_gemini_lib = _load_gemini_module()


# ─── helpers ────────────────────────────────────────────────────────────────


class _DummyModel:
    """generate_content 호출에 미리 정의된 side_effect 순서로 응답."""

    def __init__(self, side_effects: list[Any]):
        self._side_effects = list(side_effects)
        self.calls = 0

    def generate_content(self, parts, generation_config=None):  # noqa: ARG002
        self.calls += 1
        effect = self._side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


def _build_analyzer(side_effects: list[Any]) -> Any:
    """vertexai SDK 부팅 없이 GeminiAnalyzer 인스턴스만 조립."""
    analyzer = _gemini_lib.GeminiAnalyzer.__new__(_gemini_lib.GeminiAnalyzer)
    analyzer.model = _DummyModel(side_effects)
    analyzer._rate_limit_max_retries = 2
    analyzer._rate_limit_base_delay_sec = 0.1
    analyzer._rate_limit_backoff = 2.0
    analyzer._rate_limit_max_delay_sec = 1.0
    analyzer._server_error_max_retries = 3
    analyzer._server_error_base_delay_sec = 0.2
    analyzer._server_error_backoff = 2.0
    analyzer._server_error_max_delay_sec = 2.0
    analyzer._call_count = 0
    analyzer._total_prompt_tokens = 0
    analyzer._total_response_tokens = 0
    analyzer._total_tokens = 0
    return analyzer


def _patch_sleep(monkeypatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(_gemini_lib.time, "sleep", lambda value: sleeps.append(value))
    return sleeps


def _usage_response(text: str, prompt: int, candidates: int, total: int):
    return SimpleNamespace(
        text=text,
        usage_metadata=SimpleNamespace(
            prompt_token_count=prompt,
            candidates_token_count=candidates,
            total_token_count=total,
        ),
    )


# ─── is_vertex_server_error predicate ─────────────────────────────────────


def test_predicate_matches_503_message():
    assert _gemini_lib.is_vertex_server_error(RuntimeError("503 Service Unavailable"))


def test_predicate_matches_504_message():
    assert _gemini_lib.is_vertex_server_error(RuntimeError("504 Deadline Exceeded"))


def test_predicate_matches_lowercase_service_unavailable():
    assert _gemini_lib.is_vertex_server_error(RuntimeError("Service Unavailable: retry"))


def test_predicate_matches_deadline_exceeded():
    assert _gemini_lib.is_vertex_server_error(RuntimeError("Deadline exceeded for request"))


def test_predicate_matches_internal_error():
    assert _gemini_lib.is_vertex_server_error(RuntimeError("500 Internal error encountered"))


def test_predicate_matches_underscore_deadline_exceeded():
    """gRPC StatusCode 가 ``DEADLINE_EXCEEDED`` (underscore) 로 표기되어도 매칭.

    codex review 에서 지적된 false negative 시나리오. 공백 변환 누락 방지.
    """
    assert _gemini_lib.is_vertex_server_error(RuntimeError("DEADLINE_EXCEEDED: server is slow"))


def test_predicate_matches_unavailable_status_repr():
    """gRPC ``StatusCode.UNAVAILABLE`` 표기 ("503" 숫자 없이) 도 매칭."""
    assert _gemini_lib.is_vertex_server_error(RuntimeError("StatusCode.UNAVAILABLE: backend disconnect"))


def test_predicate_matches_gateway_timeout():
    """일부 proxy/load-balancer 경유 시 'gateway timeout' 메시지로 도달."""
    assert _gemini_lib.is_vertex_server_error(RuntimeError("502 Bad Gateway / 504 Gateway Timeout"))


def test_predicate_rejects_429():
    """429 는 rate-limit 경로 — server-error predicate 는 False."""
    assert not _gemini_lib.is_vertex_server_error(RuntimeError("429 Resource exhausted"))


def test_predicate_rejects_permission_denied():
    assert not _gemini_lib.is_vertex_server_error(RuntimeError("permission denied"))


# ─── 503 retry success / exhaust ──────────────────────────────────────────


def test_503_retry_then_success(monkeypatch):
    """503 한 번 raise 후 정상 응답 — retry 1회 + 성공."""
    analyzer = _build_analyzer(
        [
            RuntimeError("503 Service Unavailable"),
            _usage_response("ok", prompt=10, candidates=5, total=15),
        ]
    )
    sleeps = _patch_sleep(monkeypatch)

    response = analyzer._generate_content_with_retry(
        ["part"],
        content_type="video",
        source_name="sample.mp4",
    )

    assert response.text == "ok"
    assert analyzer.model.calls == 2
    assert sleeps == [0.2]  # server_error_base_delay_sec * (2^0)
    assert analyzer._call_count == 1
    assert analyzer._total_prompt_tokens == 10
    assert analyzer._total_response_tokens == 5
    assert analyzer._total_tokens == 15


def test_504_deadline_retry_then_success(monkeypatch):
    """504 deadline 도 server-error 분류 — 같은 backoff 적용."""
    analyzer = _build_analyzer(
        [
            RuntimeError("504 Deadline Exceeded"),
            _usage_response("done", prompt=7, candidates=3, total=10),
        ]
    )
    _patch_sleep(monkeypatch)
    response = analyzer._generate_content_with_retry(["part"], content_type="video", source_name="x.mp4")
    assert response.text == "done"
    assert analyzer.model.calls == 2


def test_503_exhausts_after_max_retries(monkeypatch):
    """503 가 budget(3회 retry)+1 초기 호출 = 4회 모두 실패 → raise."""
    analyzer = _build_analyzer(
        [
            RuntimeError("503 Service Unavailable"),
            RuntimeError("503 Service Unavailable"),
            RuntimeError("503 Service Unavailable"),
            RuntimeError("503 Service Unavailable"),
        ]
    )
    sleeps = _patch_sleep(monkeypatch)

    with pytest.raises(RuntimeError, match="503 Service Unavailable"):
        analyzer._generate_content_with_retry(["part"], content_type="video", source_name="x.mp4")

    assert analyzer.model.calls == 4  # initial + 3 retries
    # backoff: 0.2, 0.4, 0.8 (cap 2.0 가져서 trim 안 됨).
    assert sleeps == [0.2, 0.4, 0.8]
    assert analyzer._call_count == 0  # 성공한 호출 없음


def test_server_error_backoff_caps_at_max_delay(monkeypatch):
    """server_error_max_delay_sec=2.0 일 때 backoff 가 cap 적용되는지 확인."""
    analyzer = _build_analyzer(
        [
            RuntimeError("503"),
            RuntimeError("503"),
            RuntimeError("503"),
            _usage_response("ok", prompt=0, candidates=0, total=0),
        ]
    )
    analyzer._server_error_base_delay_sec = 1.5
    analyzer._server_error_backoff = 4.0
    analyzer._server_error_max_delay_sec = 2.0
    sleeps = _patch_sleep(monkeypatch)

    analyzer._generate_content_with_retry(["part"], content_type="image", source_name="img.jpg")

    # 첫 retry: min(2.0, 1.5*4^0) = 1.5
    # 두번째: min(2.0, 1.5*4^1=6.0) = 2.0
    # 세번째: min(2.0, 1.5*4^2=24.0) = 2.0
    assert sleeps == [1.5, 2.0, 2.0]


# ─── 429 + 503 mixed budgets ──────────────────────────────────────────────


def test_429_and_503_use_separate_budgets(monkeypatch):
    """rate-limit 예산과 server-error 예산이 *서로 다른 카운터*. 둘 다 다 쓰지 않으면 성공."""
    analyzer = _build_analyzer(
        [
            RuntimeError("429 Resource exhausted"),
            RuntimeError("503 Service Unavailable"),
            RuntimeError("429 Resource exhausted"),
            _usage_response("finally", prompt=20, candidates=10, total=30),
        ]
    )
    sleeps = _patch_sleep(monkeypatch)

    response = analyzer._generate_content_with_retry(["part"], content_type="video", source_name="x.mp4")

    assert response.text == "finally"
    assert analyzer.model.calls == 4
    # 429 두 번(0.1, 0.2) + 503 한 번(0.2) 순서. 시점은 호출 순서대로.
    assert sleeps == [0.1, 0.2, 0.2]
    assert analyzer._call_count == 1
    assert analyzer._total_prompt_tokens == 20


def test_non_transient_error_raised_immediately(monkeypatch):
    """403/permission denied 등 분류 외 예외는 retry 없이 즉시 전파."""
    analyzer = _build_analyzer([RuntimeError("permission denied")])
    sleeps = _patch_sleep(monkeypatch)

    with pytest.raises(RuntimeError, match="permission denied"):
        analyzer._generate_content_with_retry(["part"], content_type="video", source_name="x.mp4")

    assert analyzer.model.calls == 1
    assert sleeps == []
    assert analyzer._call_count == 0


# ─── usage counter ────────────────────────────────────────────────────────


def test_usage_counter_accumulates_across_calls(monkeypatch):
    analyzer = _build_analyzer(
        [
            _usage_response("a", prompt=10, candidates=5, total=15),
            _usage_response("b", prompt=20, candidates=8, total=28),
        ]
    )
    _patch_sleep(monkeypatch)
    analyzer._generate_content_with_retry(["p1"], content_type="image", source_name="1.jpg")
    analyzer._generate_content_with_retry(["p2"], content_type="image", source_name="2.jpg")

    summary = analyzer.usage_summary()
    assert summary == {
        "call_count": 2,
        "total_prompt_tokens": 30,
        "total_response_tokens": 13,
        "total_tokens": 43,
    }


def test_usage_counter_safe_when_metadata_missing(monkeypatch):
    """usage_metadata 가 없는 response 도 hard-fail 안 함 — call_count 만 증가."""
    response_without_usage = SimpleNamespace(text="legacy")
    analyzer = _build_analyzer([response_without_usage])
    _patch_sleep(monkeypatch)

    out = analyzer._generate_content_with_retry(["part"], content_type="image", source_name="x.jpg")

    assert out is response_without_usage
    summary = analyzer.usage_summary()
    assert summary["call_count"] == 1
    assert summary["total_prompt_tokens"] == 0
    assert summary["total_response_tokens"] == 0
    assert summary["total_tokens"] == 0


def test_usage_counter_safe_when_field_none(monkeypatch):
    """일부 field 가 None 이어도 0 으로 누적."""
    response = SimpleNamespace(
        text="partial",
        usage_metadata=SimpleNamespace(
            prompt_token_count=12,
            candidates_token_count=None,  # SDK 가 가끔 빈 candidate 시 None
            total_token_count=12,
        ),
    )
    analyzer = _build_analyzer([response])
    _patch_sleep(monkeypatch)
    analyzer._generate_content_with_retry(["part"], content_type="image", source_name="x.jpg")
    summary = analyzer.usage_summary()
    assert summary["call_count"] == 1
    assert summary["total_prompt_tokens"] == 12
    assert summary["total_response_tokens"] == 0
    assert summary["total_tokens"] == 12
