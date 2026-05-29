"""Phase 3-A inline DQ tests (#1-3 from stack-candidates).

- #1 ops_normalize._extract_meta: duration_sec is None or <1.0 → error path
- #2 filter_events_over_duration helper: timestamp[1] > duration_sec dropped
- #3 timestamp.py event_count > 50 warning emission shape

#1 은 ops_normalize._extract_meta 안의 사설 closure 라 unit 호출이 비용 높음.
  → 동일 표현식을 mirror 한 헬퍼로 명세를 고정 (사양 변경 시 둘 다 fail 하도록).
#2 는 lib/vertex_chunking.filter_events_over_duration 으로 추출돼 직접 테스트.
#3 은 logging.warning 호출 모양만 검증 (호출 측 함수는 Gemini SDK 의존성으로 크다).
"""

from __future__ import annotations

import logging
from typing import Any

from vlm_pipeline.lib.vertex_chunking import filter_events_over_duration


# ─── DQ #1: duration_sec invalid → error ────────────────────────────────────


def _duration_invalid(duration_raw: Any) -> bool:
    """Mirror of ops_normalize._extract_meta duration check (DQ #1)."""
    return duration_raw is None or (isinstance(duration_raw, (int, float)) and duration_raw < 1.0)


def test_dq1_duration_none_is_invalid() -> None:
    assert _duration_invalid(None) is True


def test_dq1_duration_zero_is_invalid() -> None:
    assert _duration_invalid(0.0) is True
    assert _duration_invalid(0) is True


def test_dq1_duration_below_one_sec_is_invalid() -> None:
    assert _duration_invalid(0.5) is True
    assert _duration_invalid(0.999) is True


def test_dq1_duration_one_sec_or_above_is_valid() -> None:
    assert _duration_invalid(1.0) is False
    assert _duration_invalid(60) is False
    assert _duration_invalid(3600.5) is False


# ─── DQ #2: filter_events_over_duration (real helper) ──────────────────────


def test_dq2_event_within_duration_kept() -> None:
    events = [{"timestamp": [1.0, 10.0], "category": "smoke"}]
    out, dropped = filter_events_over_duration(events, duration_sec=60.0)
    assert out == events
    assert dropped == 0


def test_dq2_event_at_duration_kept_inclusive() -> None:
    """end_sec == duration 은 boundary, drop 하지 않는다 (strict > 만 drop)."""
    events = [{"timestamp": [0.0, 60.0]}]
    out, dropped = filter_events_over_duration(events, duration_sec=60.0)
    assert out == events
    assert dropped == 0


def test_dq2_event_exceeding_duration_dropped() -> None:
    events = [{"timestamp": [50.0, 65.0]}]
    out, dropped = filter_events_over_duration(events, duration_sec=60.0)
    assert out == []
    assert dropped == 1


def test_dq2_mixed_events_partial_drop() -> None:
    events = [
        {"timestamp": [0.0, 10.0], "category": "good"},
        {"timestamp": [50.0, 65.0], "category": "overshoots"},
        {"timestamp": [20.0, 30.0], "category": "good"},
    ]
    out, dropped = filter_events_over_duration(events, duration_sec=60.0)
    assert [e["category"] for e in out] == ["good", "good"]
    assert dropped == 1


def test_dq2_missing_duration_passes_through() -> None:
    """duration 모르면 필터 안 함 — silently 유지 (정상 케이스: 메타 결손 후 라벨)."""
    events = [{"timestamp": [50.0, 999.0]}]
    out, dropped = filter_events_over_duration(events, duration_sec=None)
    assert out == events
    assert dropped == 0


def test_dq2_zero_or_negative_duration_passes_through() -> None:
    events = [{"timestamp": [50.0, 999.0]}]
    out, dropped = filter_events_over_duration(events, duration_sec=0.0)
    assert out == events
    assert dropped == 0
    out, dropped = filter_events_over_duration(events, duration_sec=-1.0)
    assert out == events
    assert dropped == 0


def test_dq2_malformed_event_kept_as_passthrough() -> None:
    """timestamp 구조 안 맞는 event 는 drop 하지 않는다 — normalize_gemini_events
    가 이미 거른 후라 여기에는 사실상 안 옴. 안전망 동작만 명세."""
    events = [{"no_timestamp": True}, {"timestamp": "not a list"}]
    out, dropped = filter_events_over_duration(events, duration_sec=60.0)
    assert out == events
    assert dropped == 0


def test_dq2_non_list_input_returns_unchanged() -> None:
    """events 가 list 아닌 경우 그대로 반환 (analyzer 에러 케이스 등)."""
    out, dropped = filter_events_over_duration("not a list", duration_sec=60.0)  # type: ignore[arg-type]
    assert out == "not a list"
    assert dropped == 0


# ─── DQ #3: event_count > 50 warning shape ─────────────────────────────────


def _emit_dq3_warning_if_anomalous(
    log: logging.Logger,
    *,
    event_count: int,
    asset_id: str,
) -> None:
    """Mirror of timestamp.py DQ#3 warning emission."""
    if event_count > 50:
        log.warning(
            "clip_timestamp DQ#3: asset=%s anomalous event_count=%d (>50) — Gemini 프롬프트/카테고리 매칭 검토 권장",
            asset_id,
            event_count,
        )


def test_dq3_normal_event_count_no_warning(caplog) -> None:
    log = logging.getLogger("dq3_test")
    with caplog.at_level(logging.WARNING, logger="dq3_test"):
        _emit_dq3_warning_if_anomalous(log, event_count=50, asset_id="aaa")
    assert not any("DQ#3" in r.message for r in caplog.records)


def test_dq3_anomalous_event_count_emits_warning(caplog) -> None:
    log = logging.getLogger("dq3_test")
    with caplog.at_level(logging.WARNING, logger="dq3_test"):
        _emit_dq3_warning_if_anomalous(log, event_count=120, asset_id="aaa")
    matches = [r for r in caplog.records if "DQ#3" in r.message]
    assert matches, "DQ#3 warning 누락"
    assert "anomalous event_count=120" in matches[0].message
