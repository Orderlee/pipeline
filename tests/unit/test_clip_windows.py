from __future__ import annotations

import pytest

from vlm_pipeline.defs.process.clip_windows import (
    plan_asset_event_clip_extraction_windows,
    resolve_event_clip_extraction_window,
)


def test_resolve_event_clip_extraction_window_applies_buffer_when_within_duration() -> None:
    start_sec, end_sec = resolve_event_clip_extraction_window(
        event_start_sec=60.0,
        event_end_sec=70.0,
        source_duration_sec=300.0,
    )
    assert start_sec == pytest.approx(55.0)
    assert end_sec == pytest.approx(75.0)


def test_resolve_event_clip_extraction_window_clamps_post_buffer_to_duration() -> None:
    # 이벤트 끝(290s)에 +5s 버퍼가 영상 끝(300s)을 넘으면 duration으로 클램프
    start_sec, end_sec = resolve_event_clip_extraction_window(
        event_start_sec=290.0,
        event_end_sec=298.0,
        source_duration_sec=300.0,
    )
    assert start_sec == pytest.approx(285.0)
    assert end_sec == pytest.approx(300.0)


def test_resolve_event_clip_extraction_window_raises_when_event_entirely_beyond_duration() -> None:
    # 이벤트 전체가 영상 길이를 넘어서면 복구 불가 → RuntimeError
    with pytest.raises(RuntimeError) as excinfo:
        resolve_event_clip_extraction_window(
            event_start_sec=444.0,
            event_end_sec=450.0,
            source_duration_sec=348.029,
        )
    assert "video_clip_range_invalid" in str(excinfo.value)


def test_plan_asset_event_clip_extraction_windows_skips_event_beyond_duration_without_raising() -> None:
    candidates = [
        {
            "asset_id": "asset-1",
            "label_id": "label-good",
            "event_index": 0,
            "media_type": "video",
            "timestamp_start_sec": 60.0,
            "timestamp_end_sec": 70.0,
            "video_duration_sec": 300.0,
        },
        {
            "asset_id": "asset-1",
            "label_id": "label-bad",
            "event_index": 1,
            "media_type": "video",
            "timestamp_start_sec": 444.0,
            "timestamp_end_sec": 450.0,
            "video_duration_sec": 348.029,
        },
    ]

    # 어떤 candidate든 raise 하지 않아야 한다 (plan 함수 내 방어).
    plans = plan_asset_event_clip_extraction_windows(candidates)

    # 정상 이벤트만 plans에 포함, 잘못된 이벤트는 조용히 skip.
    assert ("label-good", 0) in plans
    assert ("label-bad", 1) not in plans

    good_plan = plans[("label-good", 0)]
    assert good_plan["extract_start_sec"] == pytest.approx(55.0)
    assert good_plan["extract_end_sec"] == pytest.approx(75.0)


def test_plan_asset_event_clip_extraction_windows_skips_when_event_start_equals_duration() -> None:
    candidates = [
        {
            "asset_id": "asset-1",
            "label_id": "label-boundary",
            "event_index": 0,
            "media_type": "video",
            "timestamp_start_sec": 300.0,
            "timestamp_end_sec": 305.0,
            "video_duration_sec": 300.0,
        },
    ]
    plans = plan_asset_event_clip_extraction_windows(candidates)
    assert plans == {}


def test_plan_asset_event_clip_extraction_windows_allows_partial_overflow() -> None:
    # 이벤트 시작은 영상 내부, 끝만 영상 길이 초과 → 플랜에 포함되며 duration으로 클램프
    candidates = [
        {
            "asset_id": "asset-1",
            "label_id": "label-partial",
            "event_index": 0,
            "media_type": "video",
            "timestamp_start_sec": 295.0,
            "timestamp_end_sec": 310.0,
            "video_duration_sec": 300.0,
        },
    ]
    plans = plan_asset_event_clip_extraction_windows(candidates)
    assert ("label-partial", 0) in plans
    plan = plans[("label-partial", 0)]
    assert plan["extract_start_sec"] == pytest.approx(290.0)
    assert plan["extract_end_sec"] == pytest.approx(300.0)
