"""video_scene.classify_video_scene 파싱 검증 — Vertex/ffmpeg 는 mock.

design: docs/design-docs/camera-angle-grouping-2026-07-29.md §1, §3, §7.
camera_angle 은 이 모듈이 담당하지 않는다(lib/video_angle_dav2.py 로 분리,
tests/unit/test_video_angle_dav2.py 참고) — 이 파일의 회귀 테스트는 그 사실을 명시적으로
검증한다(test_classify_video_scene_result_never_contains_camera_angle_keys).
"""

from __future__ import annotations

import time

import pytest

from vlm_pipeline.lib import video_scene


def test_prompt_never_asks_for_degree_estimate() -> None:
    """§1 핵심 교정사항 — "각도 몇 도"류 절대각도 추정을 프롬프트가 요구하면 안 된다."""
    assert "deg" not in video_scene.PROMPT.lower()


def test_prompt_asks_for_all_five_axes_in_five_lines() -> None:
    """통합 프롬프트가 5축을 모두 요구하고, 정확히 5줄 답변 형식을 강제하는지."""
    for axis in (
        "subject_scale",
        "occlusion_state",
        "environment_type",
        "daynight_type",
        "weather",
    ):
        assert axis in video_scene.PROMPT
    assert "camera_angle" not in video_scene.PROMPT
    assert "5줄" in video_scene.PROMPT


def test_match_whitelist_picks_earliest_hit() -> None:
    text = "cloudy, definitely not clear"
    assert video_scene._match_whitelist(text, video_scene.WEATHER_VALUES) == "cloudy"


def test_match_whitelist_normalizes_spaces_and_dashes() -> None:
    assert video_scene._match_whitelist("Not Applicable", video_scene.WEATHER_VALUES) == "not_applicable"
    assert video_scene._match_whitelist("not-applicable", video_scene.WEATHER_VALUES) == "not_applicable"


def test_match_whitelist_no_hit_returns_none() -> None:
    assert video_scene._match_whitelist("no idea what this is", video_scene.WEATHER_VALUES) is None


def test_match_whitelist_tolerates_trailing_parenthetical_annotation() -> None:
    """실측 함정(2026-07-29) — Gemini 가 ``not_applicable(실내)`` 처럼 프롬프트의 괄호
    설명을 값에 그대로 붙여 답한 사례가 있다. 부분 문자열 매칭이므로 괄호가 붙어도
    ``not_applicable`` 을 정상적으로 잡아야 한다."""
    assert video_scene._match_whitelist("not_applicable(실내)", video_scene.WEATHER_VALUES) == "not_applicable"


# ─── 줄 단위 파싱 (day/night 라벨-값 충돌 회귀 테스트) ────────────────────────────


def test_split_key_value_lines_isolates_each_axis_value() -> None:
    raw = (
        "subject_scale: subject_legible\n"
        "occlusion_state: unoccluded\n"
        "environment_type: outdoor\n"
        "daynight_type: night\n"
        "weather: clear\n"
    )
    lines = video_scene._split_key_value_lines(raw)
    assert lines["daynight_type"].strip() == "night"


def test_parse_axes_daynight_night_not_confused_with_label_substring() -> None:
    """회귀 테스트 — `daynight_type` 라벨 문자열 자체가 `day`/`night` 를 부분 문자열로
    포함한다("day"+"night"+"_type"). 줄 단위로 격리하지 않고 전체 텍스트를 화이트리스트
    매칭하면, 실제 값이 'night' 여도 라벨의 'day' 가 항상 더 먼저 걸려 오분류된다."""
    raw = (
        "subject_scale: subject_legible\n"
        "occlusion_state: unoccluded\n"
        "environment_type: outdoor\n"
        "daynight_type: night\n"
        "weather: clear\n"
    )
    parsed = video_scene._parse_axes(raw)
    assert parsed["daynight_type"] == "night"


def test_parse_axes_daynight_day_still_parses_correctly() -> None:
    raw = "daynight_type: day\n"
    parsed = video_scene._parse_axes(raw)
    assert parsed["daynight_type"] == "day"


def test_parse_axes_missing_line_defaults_to_none() -> None:
    """모델이 5줄 중 일부를 누락해도 나머지 축은 정상 파싱되고, 누락된 축만 None."""
    raw = "subject_scale: subject_marginal\n"
    parsed = video_scene._parse_axes(raw)
    assert parsed["subject_scale"] == "subject_marginal"
    assert parsed["occlusion_state"] is None
    assert parsed["environment_type"] is None
    assert parsed["daynight_type"] is None
    assert parsed["weather"] is None


def test_parse_axes_out_of_whitelist_value_defaults_to_none() -> None:
    raw = "environment_type: somewhere_weird\nweather: tornado\n"
    parsed = video_scene._parse_axes(raw)
    assert parsed["environment_type"] is None
    assert parsed["weather"] is None


def test_parse_axes_weather_not_applicable_with_trailing_parenthetical() -> None:
    """실측 함정(2026-07-29) — 프롬프트의 ``not_applicable(실내)`` 표기를 모델이 값에
    그대로 echo 하는 경우가 있다. 축 전체 파싱 경로에서도 정상적으로 not_applicable 로
    떨어져야 한다."""
    raw = "weather: not_applicable(실내)\n"
    parsed = video_scene._parse_axes(raw)
    assert parsed["weather"] == "not_applicable"


def test_parse_axes_never_contains_camera_angle_key() -> None:
    """회귀 테스트 — camera_angle 은 DAv2 전용으로 분리됐으므로 이 축이 응답에 섞여
    들어와도(구 프롬프트를 재사용한 모델 등) _AXIS_WHITELISTS/파싱 결과에 나타나면 안 된다."""
    raw = "camera_angle: oblique_view\nsubject_scale: subject_legible\n"
    parsed = video_scene._parse_axes(raw)
    assert "camera_angle" not in parsed
    assert parsed["subject_scale"] == "subject_legible"


# ─── classify_video_scene 통합 ────────────────────────────────────────────────


def test_classify_video_scene_normal_five_axis_response(monkeypatch) -> None:
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])
    monkeypatch.setattr(
        video_scene,
        "_call_gemini",
        lambda *_a, **_k: (
            "subject_scale: subject_legible\n"
            "occlusion_state: unoccluded\n"
            "environment_type: outdoor\n"
            "daynight_type: day\n"
            "weather: clear\n"
        ),
    )
    result = video_scene.classify_video_scene("/tmp/sample.mp4", model="gemini-2.5-flash")
    assert result == {
        "subject_scale": "subject_legible",
        "occlusion_state": "unoccluded",
        "environment_type": "outdoor",
        "daynight_type": "day",
        "weather": "clear",
        "env_method": "gemini-2.5-flash",
    }


def test_classify_video_scene_result_never_contains_camera_angle_keys(monkeypatch) -> None:
    """camera_angle 은 lib/video_angle_dav2.py 로 이전됐다 — Gemini 경로는 더 이상
    camera_angle/angle_method 를 반환하면 안 된다(회귀 방지)."""
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])
    monkeypatch.setattr(
        video_scene,
        "_call_gemini",
        lambda *_a, **_k: (
            "subject_scale: subject_legible\n"
            "occlusion_state: unoccluded\n"
            "environment_type: outdoor\n"
            "daynight_type: day\n"
            "weather: clear\n"
        ),
    )
    result = video_scene.classify_video_scene("/tmp/sample.mp4", model="gemini-2.5-flash")
    assert "camera_angle" not in result
    assert "angle_method" not in result
    assert set(result.keys()) == {
        "subject_scale",
        "occlusion_state",
        "environment_type",
        "daynight_type",
        "weather",
        "env_method",
    }


def test_classify_video_scene_out_of_whitelist_falls_back(monkeypatch) -> None:
    """taxonomy 에 없는 값은 전부 None 으로 남는다. env_method 는 호출 자체는 성공했으므로
    resolved 모델명이 들어간다."""
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])
    monkeypatch.setattr(video_scene, "_call_gemini", lambda *_a, **_k: "I cannot tell what this frame shows.")
    result = video_scene.classify_video_scene("/tmp/sample.mp4", model="gemini-2.5-flash")
    assert result["subject_scale"] is None
    assert result["occlusion_state"] is None
    assert result["environment_type"] is None
    assert result["daynight_type"] is None
    assert result["weather"] is None
    assert result["env_method"] == "gemini-2.5-flash"


def test_classify_video_scene_no_frames_returns_all_none(monkeypatch) -> None:
    """프레임 추출 실패 경로 — 6개 키 전부 None (호출자가 터미널 마커로 해석)."""
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [])
    result = video_scene.classify_video_scene("/tmp/sample.mp4")
    assert result == {
        "subject_scale": None,
        "occlusion_state": None,
        "environment_type": None,
        "daynight_type": None,
        "weather": None,
        "env_method": None,
    }


def test_classify_video_scene_propagates_vertex_errors(monkeypatch) -> None:
    """Vertex 호출 실패는 예외로 전파 — 호출자가 'deferred' 유지 재시도로 처리해야 한다."""
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])

    def _boom(*_a, **_k):
        raise TimeoutError("vertex unreachable")

    monkeypatch.setattr(video_scene, "_call_gemini", _boom)
    with pytest.raises(TimeoutError):
        video_scene.classify_video_scene("/tmp/sample.mp4")


def test_classify_video_scene_multi_frame_majority_vote(monkeypatch) -> None:
    """scene_backfill_helpers 가 ``SCENE_FRAMES`` env 로 frames>1 을 넘길 수 있으므로 다수결
    plumbing 자체는 유지한다(백엔드만 Gemini 로 교체) — process_one_video 의 기존 호출
    계약(``qwen_classify_fn(archive_path, frames=frames)``)과의 하위호환을 위해 보존."""
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"f1", b"f2", b"f3"])
    responses = iter(
        [
            "subject_scale: subject_marginal\nocclusion_state: unoccluded\n"
            "environment_type: outdoor\ndaynight_type: night\nweather: rain\n",
            "subject_scale: subject_marginal\nocclusion_state: partially_occluded\n"
            "environment_type: outdoor\ndaynight_type: night\nweather: cloudy\n",
            "subject_scale: subject_legible\nocclusion_state: partially_occluded\n"
            "environment_type: indoor\ndaynight_type: day\nweather: cloudy\n",
        ]
    )
    monkeypatch.setattr(video_scene, "_call_gemini", lambda *_a, **_k: next(responses))
    result = video_scene.classify_video_scene("/tmp/sample.mp4", frames=3, model="gemini-2.5-flash")
    assert result["subject_scale"] == "subject_marginal"  # 2/3
    assert result["occlusion_state"] == "partially_occluded"  # 2/3
    assert result["environment_type"] == "outdoor"  # 2/3
    assert result["daynight_type"] == "night"  # 2/3
    assert result["weather"] == "cloudy"  # 2/3
    assert result["env_method"] == "gemini-2.5-flash"


def test_classify_video_scene_resolves_model_from_env(monkeypatch) -> None:
    monkeypatch.setenv("SCENE_GEMINI_MODEL", "gemini-2.5-flash-002")
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])
    captured: dict = {}

    def _capture(_frame_bytes, *, model, timeout):  # noqa: ARG001
        captured["model"] = model
        return "weather: clear"

    monkeypatch.setattr(video_scene, "_call_gemini", _capture)
    result = video_scene.classify_video_scene("/tmp/sample.mp4")
    assert captured["model"] == "gemini-2.5-flash-002"
    assert result["env_method"] == "gemini-2.5-flash-002"


def test_classify_video_scene_defaults_to_gemini_2_5_flash_without_env(monkeypatch) -> None:
    monkeypatch.delenv("SCENE_GEMINI_MODEL", raising=False)
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])
    monkeypatch.setattr(video_scene, "_call_gemini", lambda *_a, **_k: "weather: clear")
    result = video_scene.classify_video_scene("/tmp/sample.mp4")
    assert result["env_method"] == "gemini-2.5-flash"


def test_classify_video_scene_explicit_model_kwarg_takes_priority_over_env(monkeypatch) -> None:
    monkeypatch.setenv("SCENE_GEMINI_MODEL", "should-not-be-used")
    monkeypatch.setattr(video_scene, "_extract_frames", lambda *_a, **_k: [b"fake-jpeg"])
    captured: dict = {}

    def _capture(_frame_bytes, *, model, timeout):  # noqa: ARG001
        captured["model"] = model
        return "weather: clear"

    monkeypatch.setattr(video_scene, "_call_gemini", _capture)
    video_scene.classify_video_scene("/tmp/sample.mp4", model="explicit-model")
    assert captured["model"] == "explicit-model"


# ─── _call_gemini: generation_config 수치 회귀 (thinking 토큰 함정) ──────────────────


def test_call_gemini_uses_temperature_zero_and_max_output_tokens_2048(monkeypatch) -> None:
    """2.5 계열은 thinking 토큰을 먹으므로 max_output_tokens 를 작게 주면 빈 응답이 온다 —
    2048 로 회귀하지 않는지 지킨다."""
    captured: dict = {}
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "placeholder-before-test")

    class _FakePart:
        @staticmethod
        def from_data(*, data, mime_type):
            captured["frame_bytes"] = data
            captured["mime_type"] = mime_type
            return "fake-part"

    class _FakeModel:
        def __init__(self, model_name):
            captured["model_name"] = model_name

        def generate_content(self, parts, generation_config=None):
            captured["parts"] = parts
            captured["generation_config"] = generation_config
            return "fake-response"

    class _FakeVertexAI:
        @staticmethod
        def init(*, project, location):
            captured["project"] = project
            captured["location"] = location

    def _fake_generation_config_cls(*, temperature, max_output_tokens):
        captured["temperature"] = temperature
        captured["max_output_tokens"] = max_output_tokens
        return {"temperature": temperature, "max_output_tokens": max_output_tokens}

    monkeypatch.setattr(video_scene, "_load_vertex_ai", lambda: (_FakeVertexAI, _FakeModel, _FakePart))
    monkeypatch.setattr(video_scene, "_load_generation_config_cls", lambda: _fake_generation_config_cls)
    monkeypatch.setattr(video_scene, "resolve_gemini_credentials_path", lambda: "/tmp/fake-creds.json")
    monkeypatch.setattr(video_scene, "_extract_response_text", lambda _resp: "weather: clear")

    result = video_scene._call_gemini(b"fake-jpeg", model="gemini-2.5-flash", timeout=5.0)

    assert result == "weather: clear"
    assert captured["temperature"] == 0
    assert captured["max_output_tokens"] == 2048
    assert captured["model_name"] == "gemini-2.5-flash"
    assert captured["frame_bytes"] == b"fake-jpeg"
    assert captured["mime_type"] == "image/jpeg"
    assert captured["parts"] == ["fake-part", video_scene.PROMPT]


def test_call_gemini_wall_clock_timeout_returns_promptly_without_waiting_for_slow_call(monkeypatch) -> None:
    """``with ThreadPoolExecutor() as ex:`` 패턴은 __exit__ 이 shutdown(wait=True) 를 호출해
    timeout 이 지나도 원 작업이 끝날 때까지 반환하지 않는 함정이 있다(모듈 docstring 참고)
    — shutdown(wait=False) 로 실제 timeout 만큼만 대기하는지 회귀 검증한다."""
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "placeholder-before-test")

    class _FakePart:
        @staticmethod
        def from_data(*, data, mime_type):  # noqa: ARG004
            return "fake-part"

    class _SlowModel:
        def __init__(self, model_name):  # noqa: ARG002
            pass

        def generate_content(self, parts, generation_config=None):  # noqa: ARG002
            time.sleep(0.6)
            return "too-slow"

    class _FakeVertexAI:
        @staticmethod
        def init(*, project, location):  # noqa: ARG004
            return None

    monkeypatch.setattr(video_scene, "_load_vertex_ai", lambda: (_FakeVertexAI, _SlowModel, _FakePart))
    monkeypatch.setattr(video_scene, "_load_generation_config_cls", lambda: (lambda **_k: object()))
    monkeypatch.setattr(video_scene, "resolve_gemini_credentials_path", lambda: "/tmp/fake-creds.json")

    start = time.monotonic()
    with pytest.raises(Exception):  # concurrent.futures.TimeoutError
        video_scene._call_gemini(b"fake-jpeg", model="gemini-2.5-flash", timeout=0.15)
    elapsed = time.monotonic() - start
    assert elapsed < 0.5, "shutdown(wait=False) 없이 blocking with-executor 패턴을 썼다면 0.6s 가까이 걸렸을 것"
