"""load_video_once 의 씬 분류 인라인 게이트 검증 — ffprobe/ffmpeg/Vertex/HTTP 는 mock,
classify_video_scene(Gemini)/classify_camera_angle(DAv2) 호출만 격리해 테스트한다.

design: docs/design-docs/camera-angle-grouping-2026-07-29.md §3.2, §5(ingest 인라인 경로).
camera_angle 은 Gemini 가 아니라 DAv2 서비스 전용(video_angle_dav2.py)이 담당하므로
video_loader.load_video_once 는 두 라벨러를 각각 독립적으로 호출한다 — 한쪽이 예외를 던져도
다른 쪽 결과는 그대로 반영돼야 한다(실패 독립성).
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from vlm_pipeline.lib import video_loader

_FAKE_FFPROBE_JSON = json.dumps(
    {
        "streams": [
            {
                "codec_type": "video",
                "width": 1920,
                "height": 1080,
                "codec_name": "h264",
                "profile": "High",
                "has_b_frames": 1,
                "level": 40,
                "avg_frame_rate": "25/1",
                "nb_frames": "125",
            }
        ],
        "format": {"duration": "5.0", "bit_rate": "1000000"},
    }
)

_QWEN_SUCCESS_RESULT = {
    "subject_scale": "subject_legible",
    "occlusion_state": "unoccluded",
    "environment_type": "outdoor",
    "daynight_type": "day",
    "weather": "clear",
    "env_method": "qwen2.5-vl",
}

_ANGLE_SUCCESS_RESULT = {"camera_angle": "oblique_view", "angle_method": "gemini-2.5-flash"}


def _fake_ffprobe(_cmd, timeout_sec):  # noqa: ARG001 - _run_ffprobe_with_retry 시그니처 유지
    return SimpleNamespace(returncode=0, stdout=_FAKE_FFPROBE_JSON, stderr="")


def _make_sample_video(tmp_path, monkeypatch):
    monkeypatch.setattr(video_loader, "_run_ffprobe_with_retry", _fake_ffprobe)
    video_path = tmp_path / "sample.mp4"
    video_path.write_bytes(b"not-a-real-video-just-bytes-for-checksum")
    return video_path


def _must_not_be_called(*_a, **_k):
    raise AssertionError("include_scene_metadata=False(기본값)면 호출되면 안 됨")


def test_scene_gate_disabled_by_default_does_not_call_either_classifier(tmp_path, monkeypatch) -> None:
    """include_scene_metadata 생략(기본 False) 이면 Qwen/Gemini 둘 다 호출조차 안 된다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)

    monkeypatch.setattr(video_loader, "classify_video_scene", _must_not_be_called)
    monkeypatch.setattr(video_loader, "classify_camera_angle", _must_not_be_called)
    result = video_loader.load_video_once(sample_video, include_env_metadata=False)
    vm = result["video_metadata"]
    assert vm["angle_method"] == "deferred"
    assert vm["camera_angle"] is None
    assert vm["subject_scale"] is None
    assert vm["occlusion_state"] is None
    assert vm["weather"] is None


def test_scene_gate_both_exceptions_do_not_break_ingest(tmp_path, monkeypatch) -> None:
    """Qwen/Gemini 둘 다 예외를 던져도 load_video_once 는 절대 예외를 전파하지 않고
    두 축 모두 'deferred' 로 떨어진다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)

    def _qwen_boom(*_a, **_k):
        raise TimeoutError("qwen down")

    def _gemini_boom(*_a, **_k):
        raise TimeoutError("vertex unreachable")

    monkeypatch.setattr(video_loader, "classify_video_scene", _qwen_boom)
    monkeypatch.setattr(video_loader, "classify_camera_angle", _gemini_boom)
    result = video_loader.load_video_once(sample_video, include_env_metadata=False, include_scene_metadata=True)
    vm = result["video_metadata"]
    assert vm["angle_method"] == "deferred"
    assert vm["camera_angle"] is None
    assert vm["subject_scale"] is None
    assert vm["occlusion_state"] is None
    assert vm["weather"] is None


def test_scene_gate_gemini_fails_qwen_succeeds_preserves_qwen_result(tmp_path, monkeypatch) -> None:
    """실패 독립성 — Gemini 가 예외를 던져도 Qwen 의 5축 결과는 그대로 반영되고
    angle_method 만 'deferred' 로 남는다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)

    def _gemini_boom(*_a, **_k):
        raise TimeoutError("vertex unreachable")

    monkeypatch.setattr(video_loader, "classify_video_scene", lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT))
    monkeypatch.setattr(video_loader, "classify_camera_angle", _gemini_boom)
    result = video_loader.load_video_once(sample_video, include_env_metadata=True, include_scene_metadata=True)
    vm = result["video_metadata"]
    assert vm["subject_scale"] == "subject_legible"
    assert vm["occlusion_state"] == "unoccluded"
    assert vm["weather"] == "clear"
    assert vm["camera_angle"] is None
    assert vm["angle_method"] == "deferred"


def test_scene_gate_qwen_fails_gemini_succeeds_preserves_gemini_result(tmp_path, monkeypatch) -> None:
    """대칭 케이스 — Qwen 이 예외를 던져도 Gemini 의 camera_angle 결과는 그대로 반영된다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)

    def _qwen_boom(*_a, **_k):
        raise TimeoutError("qwen down")

    monkeypatch.setattr(video_loader, "classify_video_scene", _qwen_boom)
    monkeypatch.setattr(video_loader, "classify_camera_angle", lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT))
    result = video_loader.load_video_once(sample_video, include_env_metadata=True, include_scene_metadata=True)
    vm = result["video_metadata"]
    assert vm["camera_angle"] == "oblique_view"
    assert vm["angle_method"] == "gemini-2.5-flash"
    assert vm["subject_scale"] is None
    assert vm["occlusion_state"] is None
    assert vm["weather"] is None


def test_scene_gate_gemini_no_frames_normalizes_to_deferred_not_none(tmp_path, monkeypatch) -> None:
    """classify_camera_angle 이 (예외 없이) 프레임 추출 실패로 2키 None 을 반환해도,
    video_loader 는 angle_method 를 'deferred' 문자열로 정규화해야 한다 — None 이면
    find_deferred_scene_videos 의 WHERE angle_method='deferred' 에 걸리지 않아 영원히
    백필 대상에서 빠진다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)

    monkeypatch.setattr(video_loader, "classify_video_scene", lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT))
    monkeypatch.setattr(
        video_loader, "classify_camera_angle", lambda *_a, **_k: {"camera_angle": None, "angle_method": None}
    )
    result = video_loader.load_video_once(sample_video, include_env_metadata=True, include_scene_metadata=True)
    vm = result["video_metadata"]
    assert vm["angle_method"] == "deferred"
    assert vm["camera_angle"] is None
    assert vm["subject_scale"] == "subject_legible"


def test_scene_gate_success_takes_over_env_when_places365_paused(tmp_path, monkeypatch) -> None:
    """Places365 가 일시정지(include_env_metadata=False)면 environment_type/daynight_type/
    env_method 소유권이 Qwen 씬 호출로 넘어간다 — "Places365 가 했던 역할을 Qwen 이
    대신한다". outdoor_score/avg_brightness 는 Qwen 이 캘리브레이션된 연속값을 못 주므로
    None 유지. camera_angle/angle_method 는 Gemini 가 별도로 채운다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)

    monkeypatch.setattr(video_loader, "classify_video_scene", lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT))
    monkeypatch.setattr(video_loader, "classify_camera_angle", lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT))
    result = video_loader.load_video_once(sample_video, include_env_metadata=False, include_scene_metadata=True)
    vm = result["video_metadata"]
    assert vm["subject_scale"] == "subject_legible"
    assert vm["occlusion_state"] == "unoccluded"
    assert vm["weather"] == "clear"
    assert vm["camera_angle"] == "oblique_view"
    assert vm["angle_method"] == "gemini-2.5-flash"
    # Places365 일시정지 상태이므로 Qwen 호출의 값이 env 3필드를 인수한다.
    assert vm["environment_type"] == "outdoor"
    assert vm["daynight_type"] == "day"
    assert vm["env_method"] == "qwen2.5-vl"
    # VLM 은 캘리브레이션된 연속값을 못 준다 — 이 둘은 계속 None.
    assert vm["outdoor_score"] is None
    assert vm["avg_brightness"] is None


def test_scene_gate_does_not_override_places365_when_both_enabled(tmp_path, monkeypatch) -> None:
    """두 게이트가 동시에 켜지면 Places365 값을 유지한다 — 이중 기록 방지."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)
    monkeypatch.setattr(video_loader, "classify_video_scene", lambda *_a, **_k: dict(_QWEN_SUCCESS_RESULT))
    monkeypatch.setattr(video_loader, "classify_camera_angle", lambda *_a, **_k: dict(_ANGLE_SUCCESS_RESULT))
    monkeypatch.setattr(
        video_loader,
        "classify_video_environment",
        lambda *_a, **_k: {
            "environment_type": "indoor",
            "daynight_type": "night",
            "outdoor_score": 0.1,
            "avg_brightness": 42.0,
            "env_method": "places365_cuda",
        },
    )
    vm = video_loader.load_video_once(sample_video, include_env_metadata=True, include_scene_metadata=True)[
        "video_metadata"
    ]
    assert vm["env_method"] == "places365_cuda"
    assert vm["environment_type"] == "indoor"
    assert vm["daynight_type"] == "night"
    # 씬 전용 축은 그대로 반영된다.
    assert vm["camera_angle"] == "oblique_view"
    assert vm["weather"] == "clear"


def test_scene_gate_not_gated_defaults_angle_method_deferred_not_none(tmp_path, monkeypatch) -> None:
    """정책상 off(게이트 False)면 angle_method 는 'deferred' 문자열이어야 한다 — None 이면
    find_deferred_scene_videos 의 WHERE angle_method='deferred' 에 걸리지 않아 영원히
    백필 대상에서 빠진다."""
    sample_video = _make_sample_video(tmp_path, monkeypatch)
    result = video_loader.load_video_once(sample_video, include_env_metadata=False, include_scene_metadata=False)
    assert result["video_metadata"]["angle_method"] == "deferred"
