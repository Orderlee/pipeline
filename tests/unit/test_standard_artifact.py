"""docker/analysis/analysis_standard.py — 아티팩트 발행 계약.

이 계약이 없으면 소비자(FiftyOne 패널)가 **부분 JSON** 이나 **스테이지가 죽은 report** 를
정상으로 읽는다. 둘 다 예외 없이 틀린 표를 만든다.
"""

from __future__ import annotations

import importlib.util
import json
import math
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "analysis_standard.py"
_SPEC = importlib.util.spec_from_file_location("analysis_standard", str(_PATH))
std = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(std)


def test_publish_atomic_leaves_no_partial_file_on_serialization_failure(tmp_path):
    """NaN 은 JSON 표준이 아니다 — allow_nan=False 로 터뜨리고 파일을 남기지 않는다."""
    target = tmp_path / "standard_report.json"
    with pytest.raises(ValueError):
        std.publish_atomic({"macro_f1": math.nan}, str(target))
    assert not target.exists()
    assert list(tmp_path.glob("*.tmp*")) == []


def test_publish_atomic_preserves_previous_version_on_failure(tmp_path):
    """실패한 발행이 이미 있던 정상본을 날리면 안 된다."""
    target = tmp_path / "standard_report.json"
    std.publish_atomic({"status": "ok", "n": 1}, str(target))
    with pytest.raises(ValueError):
        std.publish_atomic({"bad": math.inf}, str(target))
    assert json.loads(target.read_text(encoding="utf-8"))["n"] == 1


def test_publish_atomic_replaces_previous_version_wholesale(tmp_path):
    target = tmp_path / "standard_report.json"
    std.publish_atomic({"status": "ok", "n": 1}, str(target))
    std.publish_atomic({"status": "refused", "n": 2}, str(target))
    assert json.loads(target.read_text(encoding="utf-8"))["status"] == "refused"


def test_stage_status_marks_failed_stage_as_error_not_ok():
    R = {"S0": {"n": 1}, "S3": {"error": "KeyError: 'gt'"}}
    st = std.stage_status(R)
    assert st["S0"] == "ok"
    assert st["S3"] == "error"


def test_stage_status_ignores_non_stage_keys():
    R = {"S0": {"n": 1}, "guardrails": [], "started": "x", "verdict": {"error": "not a stage"}}
    assert std.stage_status(R) == {"S0": "ok"}


def test_report_with_any_failed_stage_is_not_displayable():
    """스테이지 하나가 죽었는데 표를 그리면 그게 조용한 오답이다."""
    R = {"S0": {"n": 1}, "S6": {"error": "boom"}}
    out = std.finalize_status(R)
    assert out["display_allowed"] is False
    assert out["failed_stages"] == ["S6"]


def test_report_with_all_stages_ok_is_displayable():
    R = {"S0": {"n": 1}, "S6": {"top_set": ["v1"]}}
    out = std.finalize_status(R)
    assert out["display_allowed"] is True
    assert out["status"] == "ok"
    assert out["complete"] is True
