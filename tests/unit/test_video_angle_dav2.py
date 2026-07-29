"""video_angle_dav2.classify_camera_angle HTTP 클라이언트 검증 — ffmpeg/urllib 는 mock.

design 배경: docs/design-docs/camera-angle-grouping-2026-07-29.md.
camera_angle 은 DAv2(Depth Anything V2-S + 바닥평면 피팅) HTTP 서비스가 전담한다(2026-07-29
실측 GT 98편 plan-vs-rest AUC 0.947, 같은 조건 Gemini 2.5 Flash 는 오검출 26/94) — 채택
근거는 video_angle_dav2.py 모듈 docstring 참고.
"""

from __future__ import annotations

import json

import pytest

from vlm_pipeline.lib import video_angle_dav2

# ─── classify_camera_angle: 정상 / indeterminate / 오류 전파 / 프레임없음 ─────────────


def test_classify_camera_angle_success_parses_200_response(monkeypatch) -> None:
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: b"fake-jpeg")
    monkeypatch.setattr(
        video_angle_dav2,
        "_call_dav2",
        lambda *_a, **_k: {"camera_angle": "plan_view", "tilt_deg": 42.1, "angle_method": "dav2-s+plane"},
    )
    result = video_angle_dav2.classify_camera_angle("/tmp/sample.mp4")
    # 2키 dict 만 반환한다 — tilt_deg 는 저장 컬럼이 없어 클라이언트 반환값에서 제외한다.
    assert result == {"camera_angle": "plan_view", "angle_method": "dav2-s+plane"}


def test_classify_camera_angle_indeterminate_200_is_not_an_exception(monkeypatch) -> None:
    """서비스가 판정 근거 부족으로 200 + indeterminate 를 주면 정상 응답으로 그대로 반환한다
    (예외로 취급하지 않는다) — docker/angle/app.py 의 ValueError 캐치 경로에 대응."""
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: b"fake-jpeg")
    monkeypatch.setattr(
        video_angle_dav2,
        "_call_dav2",
        lambda *_a, **_k: {
            "camera_angle": "indeterminate",
            "tilt_deg": None,
            "angle_method": "dav2-s+plane:평면 피팅용 포인트 부족",
        },
    )
    result = video_angle_dav2.classify_camera_angle("/tmp/sample.mp4")
    assert result == {"camera_angle": "indeterminate", "angle_method": "dav2-s+plane:평면 피팅용 포인트 부족"}


def test_classify_camera_angle_propagates_http_errors(monkeypatch) -> None:
    """HTTP 실패/타임아웃은 예외로 전파 — 호출자가 'deferred' 유지 재시도로 처리해야 한다."""
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: b"fake-jpeg")

    def _boom(*_a, **_k):
        raise TimeoutError("dav2 unreachable")

    monkeypatch.setattr(video_angle_dav2, "_call_dav2", _boom)
    with pytest.raises(TimeoutError):
        video_angle_dav2.classify_camera_angle("/tmp/sample.mp4")


def test_classify_camera_angle_no_frames_returns_both_none(monkeypatch) -> None:
    """프레임 추출 실패 경로 — 2키 전부 None, DAv2 HTTP 호출 자체가 없어야 한다."""
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: None)

    def _must_not_be_called(*_a, **_k):
        raise AssertionError("프레임이 없으면 DAv2 HTTP 호출 자체가 없어야 함")

    monkeypatch.setattr(video_angle_dav2, "_call_dav2", _must_not_be_called)
    result = video_angle_dav2.classify_camera_angle("/tmp/sample.mp4")
    assert result == {"camera_angle": None, "angle_method": None}


# ─── api_url / timeout 해석 ────────────────────────────────────────────────────


def test_classify_camera_angle_resolves_api_url_from_env(monkeypatch) -> None:
    monkeypatch.setenv("ANGLE_API_URL", "http://angle-dav2-1:9999")
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: b"fake-jpeg")
    captured: dict = {}

    def _capture(api_url, _frame_bytes, *, timeout):
        captured["api_url"] = api_url
        captured["timeout"] = timeout
        return {"camera_angle": "non_plan", "angle_method": "dav2-s+plane"}

    monkeypatch.setattr(video_angle_dav2, "_call_dav2", _capture)
    video_angle_dav2.classify_camera_angle("/tmp/sample.mp4")
    assert captured["api_url"] == "http://angle-dav2-1:9999"
    assert captured["timeout"] == video_angle_dav2.DEFAULT_ANGLE_TIMEOUT_SEC


def test_classify_camera_angle_defaults_api_url_without_env(monkeypatch) -> None:
    monkeypatch.delenv("ANGLE_API_URL", raising=False)
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: b"fake-jpeg")
    captured: dict = {}

    def _capture(api_url, _frame_bytes, *, timeout):  # noqa: ARG001
        captured["api_url"] = api_url
        return {"camera_angle": "plan_view", "angle_method": "dav2-s+plane"}

    monkeypatch.setattr(video_angle_dav2, "_call_dav2", _capture)
    video_angle_dav2.classify_camera_angle("/tmp/sample.mp4")
    assert captured["api_url"] == "http://angle-dav2-1:8000"


def test_classify_camera_angle_explicit_kwargs_take_priority_over_env(monkeypatch) -> None:
    monkeypatch.setenv("ANGLE_API_URL", "http://should-not-be-used:1")
    monkeypatch.setattr(video_angle_dav2, "_extract_single_frame", lambda *_a, **_k: b"fake-jpeg")
    captured: dict = {}

    def _capture(api_url, _frame_bytes, *, timeout):
        captured["api_url"] = api_url
        captured["timeout"] = timeout
        return {"camera_angle": "plan_view", "angle_method": "dav2-s+plane"}

    monkeypatch.setattr(video_angle_dav2, "_call_dav2", _capture)
    video_angle_dav2.classify_camera_angle("/tmp/sample.mp4", api_url="http://explicit:1234", timeout=12.5)
    assert captured["api_url"] == "http://explicit:1234"
    assert captured["timeout"] == 12.5


# ─── _call_dav2: 실제 API 계약(POST /angle, multipart file 필드) 검증 ────────────────


def test_call_dav2_posts_multipart_body_with_file_field(monkeypatch) -> None:
    """docker/angle/app.py 의 실제 계약: ``POST /angle``, multipart ``file=<jpeg bytes>``."""
    captured: dict = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self):
            return json.dumps({"camera_angle": "plan_view", "tilt_deg": 31.4, "angle_method": "dav2-s+plane"}).encode()

    def _fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["method"] = req.get_method()
        captured["headers"] = {k.lower(): v for k, v in req.header_items()}
        captured["body"] = req.data
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(video_angle_dav2.urllib.request, "urlopen", _fake_urlopen)
    result = video_angle_dav2._call_dav2("http://angle-dav2-1:8000", b"fake-jpeg-bytes", timeout=5.0)

    assert result == {"camera_angle": "plan_view", "tilt_deg": 31.4, "angle_method": "dav2-s+plane"}
    assert captured["url"] == "http://angle-dav2-1:8000/angle"
    assert captured["method"] == "POST"
    assert captured["headers"]["content-type"].startswith("multipart/form-data")
    assert b'name="file"' in captured["body"]
    assert b"fake-jpeg-bytes" in captured["body"]
    assert captured["timeout"] == 5.0


def test_call_dav2_strips_trailing_slash_from_api_url(monkeypatch) -> None:
    captured: dict = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self):
            return b'{"camera_angle": "non_plan", "tilt_deg": 5.0, "angle_method": "dav2-s+plane"}'

    def _fake_urlopen(req, timeout):  # noqa: ARG001
        captured["url"] = req.full_url
        return _FakeResponse()

    monkeypatch.setattr(video_angle_dav2.urllib.request, "urlopen", _fake_urlopen)
    video_angle_dav2._call_dav2("http://angle-dav2-1:8000/", b"x", timeout=5.0)
    assert captured["url"] == "http://angle-dav2-1:8000/angle"


def test_call_dav2_propagates_urlopen_errors(monkeypatch) -> None:
    def _boom(_req, timeout):  # noqa: ARG001
        raise TimeoutError("connection timed out")

    monkeypatch.setattr(video_angle_dav2.urllib.request, "urlopen", _boom)
    with pytest.raises(TimeoutError):
        video_angle_dav2._call_dav2("http://angle-dav2-1:8000", b"x", timeout=5.0)
