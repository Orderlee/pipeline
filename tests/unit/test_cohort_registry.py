"""docker/analysis/cohort.py — 코호트 레지스트리와 거부 계약.

핵심 회귀 가드: **군집키를 자동 유도하지 않는다.** sitej_subway 는 camera 가 58대라
자동 유도하면 그걸 고르는데, 연출 동시녹화라 카메라 홀드아웃에 누수가 있어 올바른 키는
session 이다. 잘못 잡으면 CI 가 좁아져 '유의하다'는 거짓 결론이 나온다.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "cohort.py"
_SPEC = importlib.util.spec_from_file_location("cohort", str(_PATH))
co = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(co)


def test_sourcei_group_is_camera():
    cfg = co.resolve("sourcei")
    assert cfg["group"] == "camera"
    assert cfg["prompts"] == "sourcei-prompts"


def test_sitej_group_is_session_not_camera():
    # camera 58대가 있어도 session 이어야 한다 — 연출 동시녹화 누수.
    assert co.resolve("sitej_subway")["group"] == "session"


def test_every_cohort_declares_group_and_classes_explicitly():
    # 자동 유도 금지. 레지스트리에 없으면 그건 설정 누락이지 기본값 사용 대상이 아니다.
    for name, cfg in co.COHORTS.items():
        assert cfg.get("group"), f"{name}: group 미지정"
        assert cfg.get("negative_class"), f"{name}: negative_class 미지정"
        assert cfg.get("target_classes"), f"{name}: target_classes 미지정"
        assert cfg.get("prompts"), f"{name}: prompts 미지정"


def test_resolve_returns_a_copy_so_callers_cannot_mutate_registry():
    co.resolve("sourcei")["group"] = "TAMPERED"
    assert co.COHORTS["sourcei"]["group"] == "camera"


def test_unregistered_cohort_is_refused_not_defaulted():
    with pytest.raises(co.CohortRefused) as e:
        co.resolve("some_new_site")
    assert e.value.reason_code == "G0_COHORT_NOT_REGISTERED"


def test_frames_is_explicitly_refused_with_reason():
    """frames 는 GT 가 203,869 중 40장뿐이고 군집 필드가 없다 — hard skip 이 계약이다."""
    with pytest.raises(co.CohortRefused) as e:
        co.resolve("frames")
    assert e.value.reason_code == "G0_INSUFFICIENT_GT"


def test_refusal_artifact_forbids_display_and_is_terminal():
    art = co.refusal_artifact("frames", "G0_INSUFFICIENT_GT", "GT 40/203869")
    assert art["status"] == "refused"
    assert art["display_allowed"] is False
    assert art["complete"] is True  # 부분 쓰기와 구별돼야 한다
    assert art["reason_code"] == "G0_INSUFFICIENT_GT"
    assert art["cohort"] == "frames"


def test_refusal_carries_actionable_detail():
    """사유 코드만 있고 '무엇을 하라'가 없으면 운영자가 같은 실수를 반복한다."""
    with pytest.raises(co.CohortRefused) as e:
        co.resolve("some_new_site")
    assert "cohort.py" in e.value.detail
