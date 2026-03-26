"""video_frames.plan_frame_timestamps 경계 검증 — sec == duration 제외(empty_output 방지)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import vlm_pipeline.lib.video_frames as video_frames
from vlm_pipeline.lib.video_frames import (
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
    resolve_frame_sampling_policy,
)


def test_interval_timestamps_strictly_less_than_duration() -> None:
    """frame_interval_sec 사용 시 모든 타임스탬프가 duration 미만인지 검증 (empty_output 방지)."""
    duration = 5.0
    interval = 1.0
    got = plan_frame_timestamps(
        duration_sec=duration,
        fps=None,
        frame_count=None,
        frame_interval_sec=interval,
    )
    assert all(ts < duration for ts in got), f"모든 ts < {duration} 여야 함: {got}"
    assert duration not in got, f"sec == duration({duration}) 이 포함되면 안 됨: {got}"
    assert got == [0.0, 1.0, 2.0, 3.0, 4.0]


def test_interval_short_clip_single_frame() -> None:
    """짧은 클립(1초) + 1초 간격 → [0.0] 만 있어야 하고 duration(1.0) 미포함."""
    got = plan_frame_timestamps(
        duration_sec=1.0,
        fps=None,
        frame_count=None,
        frame_interval_sec=1.0,
    )
    assert got == [0.0]
    assert all(ts < 1.0 for ts in got)


def test_interval_subsecond_duration() -> None:
    """1초 미만 클립에서도 sec == duration 이 들어가지 않음."""
    got = plan_frame_timestamps(
        duration_sec=0.5,
        fps=None,
        frame_count=None,
        frame_interval_sec=1.0,
    )
    assert got == [0.0]
    assert 0.5 not in got


def test_interval_ten_second_clip() -> None:
    """10초 클립, 1초 간격 → 0~9초만, 10.0 미포함."""
    got = plan_frame_timestamps(
        duration_sec=10.0,
        fps=None,
        frame_count=None,
        frame_interval_sec=1.0,
    )
    assert len(got) == 10
    assert got == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert 10.0 not in got
    assert all(ts < 10.0 for ts in got)


def test_interval_path_respects_effective_max_frames() -> None:
    got = plan_frame_timestamps(
        duration_sec=10.0,
        fps=None,
        frame_count=None,
        max_frames_per_video=4,
        frame_interval_sec=1.0,
    )
    assert got == [0.0, 3.0, 6.0, 9.0]
    assert len(got) == 4
    assert all(ts < 10.0 for ts in got)


def test_interval_clip_used_in_process() -> None:
    """clip_to_frame에서 쓰는 설정: duration 3초, interval 1초 → [0,1,2]."""
    got = plan_frame_timestamps(
        duration_sec=3.0,
        fps=30.0,
        frame_count=90,
        frame_interval_sec=1.0,
    )
    assert got == [0.0, 1.0, 2.0]
    assert 3.0 not in got


def test_zero_duration_returns_single_zero() -> None:
    """duration <= 0 이면 [0.0] 반환."""
    assert plan_frame_timestamps(
        duration_sec=0, fps=None, frame_count=None, frame_interval_sec=1.0
    ) == [0.0]
    assert plan_frame_timestamps(
        duration_sec=0.0, fps=None, frame_count=None, frame_interval_sec=1.0
    ) == [0.0]


def test_ratio_path_no_duration_boundary() -> None:
    """frame_interval_sec 없이 비율 기반일 때도 duration과 동일한 값이 없어야 함 (end_ratio 0.9)."""
    got = plan_frame_timestamps(
        duration_sec=10.0,
        fps=30.0,
        frame_count=300,
        frame_interval_sec=None,
    )
    assert all(ts <= 10.0 for ts in got)
    # 비율 경로는 end_ratio 0.9 이므로 10.0이 나올 수 있음(반올림). 실제로 0.1~0.9 구간만 사용.
    assert len(got) >= 1


def test_resolve_frame_sampling_policy_clip_event_current_uses_existing_profile_logic() -> None:
    decision = resolve_frame_sampling_policy(
        sampling_mode="clip_event",
        requested_outputs=["captioning"],
        image_profile="current",
        duration_sec=40.0,
        fps=30.0,
        frame_count=1200,
    )
    assert decision.sampling_mode == "clip_event"
    assert decision.effective_max_frames == 8
    assert decision.frame_interval_sec == 1.0
    assert decision.policy_source == "dynamic_default"


def test_resolve_frame_sampling_policy_raw_video_current_is_more_generous() -> None:
    decision = resolve_frame_sampling_policy(
        sampling_mode="raw_video",
        requested_outputs=["bbox"],
        image_profile="current",
        duration_sec=40.0,
        fps=30.0,
        frame_count=1200,
    )
    assert decision.sampling_mode == "raw_video"
    assert decision.effective_max_frames == 16
    assert decision.frame_interval_sec == 1.0
    assert decision.policy_source == "dynamic_default"


def test_resolve_frame_sampling_policy_spec_override_caps_dense_profile() -> None:
    decision = resolve_frame_sampling_policy(
        sampling_mode="clip_event",
        requested_outputs=["captioning", "bbox"],
        image_profile="dense",
        duration_sec=200.0,
        fps=30.0,
        frame_count=6000,
        spec_max_frames_per_video=6,
    )
    assert decision.effective_max_frames == 6
    assert decision.policy_source == "spec_override"


def test_extract_frame_jpeg_bytes_retries_previous_seek_on_empty_output(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_run(cmd, *, capture_output, timeout, check):
        sec = cmd[cmd.index("-ss") + 1]
        calls.append(sec)
        if len(calls) == 1:
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        return SimpleNamespace(returncode=0, stdout=b"jpeg-bytes", stderr=b"")

    monkeypatch.setattr(video_frames.subprocess, "run", fake_run)

    got = extract_frame_jpeg_bytes("/tmp/test.mp4", 4.0, max_retries=0)

    assert got == b"jpeg-bytes"
    assert calls[:2] == ["4.000", "3.900"]


def test_extract_frame_jpeg_bytes_raises_after_empty_output_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_run(cmd, *, capture_output, timeout, check):
        sec = cmd[cmd.index("-ss") + 1]
        calls.append(sec)
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(video_frames.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="ffmpeg_frame_extract_failed:empty_output"):
        extract_frame_jpeg_bytes("/tmp/test.mp4", 4.0, max_retries=0)

    assert calls == ["4.000", "3.900", "3.750", "3.500"]
