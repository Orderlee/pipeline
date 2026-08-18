from __future__ import annotations

from pathlib import Path

from vlm_pipeline.lib import video_reencode


def test_needs_reencode_for_non_h264() -> None:
    required, reason = video_reencode.needs_reencode(
        {
            "original_codec": "hevc",
            "original_profile": "main",
            "original_has_b_frames": False,
            "original_level_int": 42,
        },
        Path("/tmp/sample.mp4"),
    )
    assert required is True
    assert reason == "codec=hevc"


def test_needs_reencode_for_low_keyframe_ratio(monkeypatch) -> None:
    monkeypatch.setattr(video_reencode, "probe_keyframe_info", lambda _path: 0.01)
    required, reason = video_reencode.needs_reencode(
        {
            "original_codec": "h264",
            "original_profile": "baseline",
            "original_has_b_frames": False,
            "original_level_int": 42,
        },
        Path("/tmp/sample.mp4"),
    )
    assert required is True
    assert reason == "keyframe_ratio=0.0100"


def test_needs_reencode_skips_when_standard_and_probe_ok(monkeypatch) -> None:
    monkeypatch.setattr(video_reencode, "probe_keyframe_info", lambda _path: 0.08)
    required, reason = video_reencode.needs_reencode(
        {
            "original_codec": "h264",
            "original_profile": "baseline",
            "original_has_b_frames": False,
            "original_level_int": 42,
        },
        Path("/tmp/sample.mp4"),
    )
    assert required is False
    assert reason is None


# ── 동적 타임아웃 계산 테스트 ──────────────────────────────────


def test_compute_reencode_timeout_short_video(monkeypatch) -> None:
    """짧은 영상(60초)은 기본 타임아웃(600초)이 적용된다."""
    monkeypatch.setattr(video_reencode, "_probe_duration_sec", lambda _p: 60.0)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_MAX_SEC", raising=False)
    timeout = video_reencode._compute_reencode_timeout(Path("/tmp/short.mp4"))
    assert timeout == 600


def test_compute_reencode_timeout_long_video(monkeypatch) -> None:
    """10분(600초) 영상 → 600 × 3 = 1800초 타임아웃."""
    monkeypatch.setattr(video_reencode, "_probe_duration_sec", lambda _p: 600.0)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_MAX_SEC", raising=False)
    timeout = video_reencode._compute_reencode_timeout(Path("/tmp/long.mp4"))
    assert timeout == 1800


def test_compute_reencode_timeout_very_long_video_capped(monkeypatch) -> None:
    """1시간(3600초) 영상 → 3600 × 3 = 10800이지만 max 7200초로 cap."""
    monkeypatch.setattr(video_reencode, "_probe_duration_sec", lambda _p: 3600.0)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_MAX_SEC", raising=False)
    timeout = video_reencode._compute_reencode_timeout(Path("/tmp/verylongvideo.mp4"))
    assert timeout == 7200


def test_compute_reencode_timeout_probe_failure(monkeypatch) -> None:
    """duration 조회 실패 시 기본 타임아웃 사용."""
    monkeypatch.setattr(video_reencode, "_probe_duration_sec", lambda _p: None)
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_SEC", raising=False)
    timeout = video_reencode._compute_reencode_timeout(Path("/tmp/broken.mp4"))
    assert timeout == 600


def test_compute_reencode_timeout_env_override(monkeypatch) -> None:
    """환경변수로 기본 타임아웃을 높이면 짧은 영상에도 적용."""
    monkeypatch.setattr(video_reencode, "_probe_duration_sec", lambda _p: 60.0)
    monkeypatch.setenv("VIDEO_REENCODE_TIMEOUT_SEC", "1200")
    monkeypatch.delenv("VIDEO_REENCODE_TIMEOUT_MAX_SEC", raising=False)
    timeout = video_reencode._compute_reencode_timeout(Path("/tmp/short.mp4"))
    assert timeout == 1200
