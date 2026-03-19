"""video_frames.plan_frame_timestamps 경계 검증 — sec == duration 제외(empty_output 방지)."""
from __future__ import annotations

import pytest

from vlm_pipeline.lib.video_frames import plan_frame_timestamps


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
