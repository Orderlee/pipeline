"""run_failure_alert_sensor 의 메시지 조립 + 전역(job 무필터) 등록 검증."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")

from vlm_pipeline.defs.shared.sensor_run_alert import (  # noqa: E402
    _ERROR_MAX_CHARS,
    build_failure_message,
    run_failure_alert_sensor,
)

BASE = "http://10.0.0.10:3030"


def test_message_has_job_and_run_link():
    msg = build_failure_message(job_name="sam3_detect_job", run_id="abc123", error=None, base_url=BASE)
    assert "sam3_detect_job" in msg
    assert f"{BASE}/runs/abc123" in msg


def test_trailing_slash_in_base_url_does_not_double():
    msg = build_failure_message(job_name="j", run_id="r", error=None, base_url=BASE + "/")
    assert f"{BASE}/runs/r" in msg
    assert "//runs/" not in msg


def test_long_error_is_truncated():
    msg = build_failure_message(job_name="j", run_id="r", error="X" * 5000, base_url=BASE)
    assert "생략" in msg
    # 잘린 뒤에도 Slack 한 메시지에 들어갈 크기여야 한다.
    assert len(msg) < _ERROR_MAX_CHARS + 200


def test_blank_error_omits_code_block():
    assert "```" not in build_failure_message(job_name="j", run_id="r", error="   ", base_url=BASE)


def test_sensor_is_registered_in_production_definitions():
    """센서 파일만 만들고 definitions 등록을 빠뜨리면 알림이 조용히 사라진다."""
    from vlm_pipeline.definitions_production import build_production_sensors

    names = [getattr(s, "name", "") for s in build_production_sensors(dispatch_target_jobs=[])]
    assert run_failure_alert_sensor.name in names
