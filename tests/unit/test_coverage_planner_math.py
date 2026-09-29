"""`lib/coverage_planner.py` — 설계서 §5.2 계산식과 Phase F.3 mode gate 의 단위 계약.

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md §5.2 / Phase F.1·F.3.

여기서 고정하는 것은 "함수가 안 죽는다" 가 아니라 **숫자와 사유**다:

  * desired/deficit/share-cap 이 설계서 식과 자릿수까지 같은가
  * planned=0 일 때 그 0 이 (a) 사실 없음 (b) context 미검증 (c) 진짜 0 중 무엇인지
    행 안에서 구분되는가  ← 이 레포가 반복해 당한 "부재에 기댄 안전" 의 방어
  * plan_only campaign 이 dispatch 로 갈 수 없는가
"""

from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

import pytest

from vlm_pipeline.lib.coverage_planner import (
    BLOCK_CAMPAIGN_CAP,
    BLOCK_CONTEXT_COVERAGE,
    BLOCK_DAILY_BUDGET,
    BLOCK_NO_FINALIZED_FACTS,
    BLOCK_NO_REFERENCE,
    BLOCK_NO_TARGETS,
    BLOCK_SHARE_CAP,
    MODE_APPROVAL_REQUIRED,
    MODE_AUTO_DISPATCH,
    MODE_DISABLED,
    MODE_PLAN_ONLY,
    CellObservation,
    PolicyPlanInput,
    campaign_decision,
    deficit_count,
    desired_count,
    is_dispatch_allowed,
    plan_policy,
    share_headroom,
    validate_target_set,
)

# ─── helpers ─────────────────────────────────────────────────────────────────


def _cell(**over) -> CellObservation:
    """ "모든 게이트가 열려 있는" 셀. 각 테스트는 막고 싶은 항 하나만 덮어쓴다."""
    payload = {
        "target_id": "t1",
        "dimensions": {"class": "falldown", "environment_type": "outdoor", "daynight_type": "day"},
        "target_share": "1.0",
        "min_finalized_count": 0,
        "real_finalized_count": 100,
        "synthetic_accepted_count": 0,
        "pending_reserved_count": 0,
        "class_finalized_total": 100,
        "class_context_verified_total": 100,
        "class_context_unverified_total": 0,
        "class_context_missing_total": 0,
        "reference_available_count": 50,
        "per_target_cap": None,
        "priority": 100,
    }
    payload.update(over)
    return CellObservation(**payload)


def _policy(cells, **over) -> PolicyPlanInput:
    payload = {
        "policy_id": "p1",
        "mode": MODE_APPROVAL_REQUIRED,
        "balance_dimensions": ("class", "environment_type", "daynight_type"),
        "horizon_finalized_total": 200,
        "max_synthetic_share": "0.5",
        "max_per_campaign": 50,
        "remaining_daily_budget": 50,
        "cells": tuple(cells),
    }
    payload.update(over)
    return PolicyPlanInput(**payload)


# ─── 1. desired / deficit ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("horizon", "share", "min_count", "expected"),
    [
        (100, "0.35", 0, 35),
        (100, "0.25", 0, 25),
        (100, "0.15", 0, 15),
        # 올림: 0.15 × 101 = 15.15 → 16
        (101, "0.15", 0, 16),
        # min_finalized_count 가 바닥이다 (horizon 이 0 이어도 살아 있다).
        (0, "0.35", 12, 12),
        (100, "0.05", 12, 12),
        (100, "0.20", 12, 20),
    ],
)
def test_desired_count_matches_the_design_formula(horizon, share, min_count, expected):
    assert desired_count(horizon, share, min_count) == expected


def test_desired_count_is_exact_not_floating_point():
    """0.35 × 20 = 7 이어야 한다. float 로 하면 7.000000000000001 → ceil 8 로 튄다.

    같은 입력이 처리계에 따라 다른 계획을 내면 Phase F 완료 기준("결정론적으로 같은
    campaign 초안")이 깨진다.
    """
    assert desired_count(20, 0.35) == 7
    assert desired_count(20, "0.35") == 7
    assert desired_count(20, Decimal("0.35")) == 7
    assert desired_count(20, Fraction(7, 20)) == 7
    # 0.1 + 0.2 계열의 고전적 사고를 막는지
    assert desired_count(70, 0.1) == 7
    assert desired_count(3, 0.1) == 1


def test_deficit_never_goes_negative():
    assert deficit_count(35, 10) == 25
    assert deficit_count(35, 35) == 0
    assert deficit_count(35, 99) == 0


# ─── 2. share cap — (S + P) / (R + S + P) <= a ───────────────────────────────


@pytest.mark.parametrize(
    ("real", "synth", "reserved", "a", "expected"),
    [
        # a=0.5, R=10 → P_total_max = 10
        (10, 0, 0, "0.5", 10),
        (10, 5, 0, "0.5", 5),
        (10, 5, 3, "0.5", 2),
        # floor: a=0.2, R=9 → 0.2*9/0.8 = 2.25 → 2
        (9, 0, 0, "0.2", 2),
        (8, 0, 0, "0.2", 2),
        # a=0 이면 합성 자체가 금지
        (1000, 0, 0, "0", 0),
        # 이미 초과 배정된 상태는 음수가 아니라 0 으로 절단
        (10, 20, 0, "0.5", 0),
        (10, 0, 99, "0.5", 0),
    ],
)
def test_share_headroom_solves_the_cap(real, synth, reserved, a, expected):
    assert share_headroom(real, synth, reserved, a) == expected


@pytest.mark.parametrize("a", ["0.01", "0.5", "0.9", "0.99"])
def test_share_headroom_is_zero_when_there_is_no_real_data(a):
    """**오늘 prod 의 상태.** R=0 이면 a 가 얼마든 생성 여유가 0 이다.

    별도 킬스위치가 아니라 계산식 자체가 fail-closed 다 — real finalized 가 없는 셀에
    합성만 채워 넣으면 (S+P)/(S+P) = 1 이라 어떤 cap 도 만족할 수 없기 때문이다.
    """
    assert share_headroom(0, 0, 0, a) == 0


def test_share_cap_result_actually_satisfies_the_inequality():
    """푼 값이 **정확히 최대 허용치**인가 — 대수 검산.

    두 방향을 모두 본다:
      * p > 0 이면 p 를 더해도 부등식이 성립한다 (안전)
      * 언제나 p+1 은 위반이다 (타이트 — 쓸 수 있는 여유를 남기지 않는다)

    p == 0 일 때 현재 상태 자체가 이미 위반일 수 있다(R=0, S>0 처럼 과거에 초과 배정된
    상태). 이 함수는 그것을 **고치지 못하고 악화만 막는다** — 그래서 p>0 일 때만 성립을
    요구한다.
    """
    a = Fraction(2, 5)  # 0.4
    for real in range(0, 40):
        for synth in range(0, 10):
            p = share_headroom(real, synth, 0, a)
            if p > 0:
                assert Fraction(synth + p, real + synth + p) <= a, (real, synth, p)
            # 한 장 더는 반드시 위반이어야 상한이 타이트하다
            assert Fraction(synth + p + 1, real + synth + p + 1) > a, (real, synth, p)


def test_share_cap_of_one_does_not_bind():
    """a=1 은 '합성 100% 허용' 이라 이 항이 binding 이 아니다 — 호출자가 준 대체 상한을 쓴다."""
    assert share_headroom(0, 0, 0, "1", unbounded_fallback=7) == 7
    assert share_headroom(5, 5, 0, "1", unbounded_fallback=0) == 0


# ─── 3. block reason — "0" 과 "관측 불가" 의 구분 ────────────────────────────


def test_no_facts_at_all_is_not_a_ratio_of_zero():
    """(a) 사실 자체가 없다. **오늘 prod 가 정확히 이 상태다** (투영 job 이 없어 0행)."""
    plan = plan_policy(
        _policy([_cell(real_finalized_count=0, class_finalized_total=0, class_context_verified_total=0)])
    )
    cell = plan.cells[0]
    assert cell.planned_count == 0
    assert cell.block_reason == BLOCK_NO_FINALIZED_FACTS
    assert plan.blocked_reason == BLOCK_NO_FINALIZED_FACTS


def test_facts_without_verified_context_is_measurement_failure_not_zero():
    """(b) **실측 재현**: finalized bbox 248장, 그중 부모 영상이 환경을 가진 것 0장.

    이때 셀의 0 은 "그 환경이 드물다" 가 아니라 **잴 수 없었다** 는 뜻이다. 둘을 같은
    모양으로 저장하면 planner 가 관측 실패를 부족분으로 읽고 생성을 지시한다.
    """
    plan = plan_policy(
        _policy(
            [
                _cell(
                    real_finalized_count=0,
                    class_finalized_total=248,
                    class_context_verified_total=0,
                    class_context_unverified_total=0,
                    class_context_missing_total=248,
                )
            ]
        )
    )
    cell = plan.cells[0]
    assert cell.planned_count == 0
    assert cell.block_reason == BLOCK_CONTEXT_COVERAGE
    # 사실이 있었다는 것 자체는 행 안에 남는다 — (a) 와 구분되는 지점.
    assert cell.class_finalized_total == 248
    assert cell.class_context_missing_total == 248


def test_weather_axis_collapses_to_context_coverage_block():
    """`weather` 실값이 prod 에 0행이므로 balance dimension 으로 선언하면 여기 걸린다.

    설계서 §5.1: "현재처럼 context 값이 deferred/unknown 이면 해당 policy 는
    blocked_context_coverage 가 되어야 하며, unknown 을 균등 분배로 추정해서는 안 된다."
    """
    plan = plan_policy(
        _policy(
            [
                _cell(
                    dimensions={"class": "falldown", "weather": "rain"},
                    # env 는 검증됐지만 weather 축은 한 행도 verified 가 아니다 → 0
                    class_finalized_total=18046,
                    class_context_verified_total=0,
                    class_context_unverified_total=18046,
                    real_finalized_count=0,
                )
            ],
            balance_dimensions=("class", "weather"),
        )
    )
    assert plan.cells[0].block_reason == BLOCK_CONTEXT_COVERAGE
    assert plan.planned_total == 0


def test_a_genuine_zero_is_distinguished_from_a_measurement_failure():
    """(c) 진짜 0. 같은 클래스가 관측됐으므로 이 셀의 0 은 측정 실패가 아니다.

    사유는 `blocked_share_cap` 이다 — (a)/(b) 와 **다른 값**이라는 것이 요점이다.
    운영자가 사유만 보고 "데이터를 더 모아라"(a/b) 와 "이 셀엔 실데이터가 먼저 필요하다"
    (share cap) 를 구분할 수 있다.
    """
    plan = plan_policy(_policy([_cell(real_finalized_count=0, class_context_verified_total=100)]))
    cell = plan.cells[0]
    assert cell.deficit_count == 200
    assert cell.class_context_verified_total == 100  # 측정은 됐다
    assert cell.block_reason == BLOCK_SHARE_CAP
    assert cell.block_reason not in (BLOCK_NO_FINALIZED_FACTS, BLOCK_CONTEXT_COVERAGE)
    assert cell.planned_count == 0


@pytest.mark.parametrize("a", ["0.1", "0.5", "0.9", "0.99"])
def test_a_cell_with_no_real_data_can_never_be_bootstrapped_by_synthetic(a):
    """**설계의 중요한 귀결**: R=0 인 셀은 어떤 share cap 으로도 합성으로 채울 수 없다.

    (S+P)/(R+S+P) 에서 R=0 이면 비율이 1 이라 a<1 인 어떤 cap 도 만족할 수 없다. 즉
    합성은 실데이터 분포를 **보강**할 뿐 없는 셀을 **발명**하지 못한다 — §5.3 이 말한
    "주변환경을 새로 text-to-image 로 발명하지 않는다" 가 계산식에서도 성립한다.

    운영 함의: 야간 outdoor 처럼 실데이터가 103건뿐인 셀은 합성으로 늘릴 수 있지만,
    실데이터가 0건인 셀은 사람이 먼저 실제 데이터를 넣어야 한다.
    """
    plan = plan_policy(
        _policy(
            [_cell(real_finalized_count=0, class_context_verified_total=100, reference_available_count=999)],
            max_synthetic_share=a,
        )
    )
    assert plan.cells[0].share_headroom_count == 0
    assert plan.cells[0].planned_count == 0
    assert plan.cells[0].block_reason == BLOCK_SHARE_CAP


def test_satisfied_cell_is_not_blocked():
    plan = plan_policy(_policy([_cell(real_finalized_count=500)]))
    cell = plan.cells[0]
    assert cell.deficit_count == 0
    assert cell.block_reason is None
    assert plan.blocked_reason is None


@pytest.mark.parametrize(
    ("override", "policy_over", "expected"),
    [
        ({"reference_available_count": 0}, {}, BLOCK_NO_REFERENCE),
        ({}, {"max_synthetic_share": "0"}, BLOCK_SHARE_CAP),
        ({}, {"remaining_daily_budget": 0}, BLOCK_DAILY_BUDGET),
        ({}, {"max_per_campaign": 0}, BLOCK_CAMPAIGN_CAP),
    ],
)
def test_each_zero_limit_leaves_its_own_reason(override, policy_over, expected):
    """§5.2: "…중 하나가 0 이면 campaign 은 부분 생성이 아니라 명시적 block/defer 사유를 남긴다." """
    plan = plan_policy(_policy([_cell(real_finalized_count=20, **override)], **policy_over))
    assert plan.cells[0].block_reason == expected
    assert plan.cells[0].planned_count == 0


def test_no_targets_is_its_own_reason():
    """target 이 0개면 cell 행이 없어 사유를 적을 곳이 snapshot 행뿐이다."""
    plan = plan_policy(_policy([]))
    assert plan.cells == ()
    assert plan.blocked_reason == BLOCK_NO_TARGETS


# ─── 4. planned — §5.2 의 min() ──────────────────────────────────────────────


def test_planned_is_the_min_of_every_term():
    plan = plan_policy(
        _policy(
            [_cell(real_finalized_count=40, reference_available_count=3)],
            max_synthetic_share="0.5",
            max_per_campaign=50,
            remaining_daily_budget=50,
        )
    )
    cell = plan.cells[0]
    assert cell.desired_count == 200
    assert cell.deficit_count == 160
    assert cell.share_headroom_count == 40  # 0.5*40/0.5 - 0
    assert cell.reference_available_count == 3
    assert cell.planned_count == 3  # reference 가 binding


def test_planned_never_exceeds_any_cap():
    """033 의 `*_planned_within_caps_check` 와 같은 불변식을 코드 쪽에서도 고정한다."""
    for references in (0, 1, 7, 100):
        for budget in (0, 2, 9, 100):
            for cap in (0, 4, 60):
                plan = plan_policy(
                    _policy(
                        [_cell(real_finalized_count=40, reference_available_count=references)],
                        remaining_daily_budget=budget,
                        max_per_campaign=cap,
                    )
                )
                cell = plan.cells[0]
                assert cell.planned_count <= cell.deficit_count
                assert cell.planned_count <= cell.reference_available_count
                assert cell.planned_count <= cell.share_headroom_count
                assert cell.planned_count <= cell.budget_headroom_count
                assert cell.planned_count <= cell.campaign_cap_count
                if cell.planned_count > 0:
                    assert cell.block_reason is None


def test_daily_budget_is_shared_across_cells_not_granted_to_each():
    """설계서 식은 셀 단위 min() 이라 그대로 쓰면 N 개 셀이 각각 전체 예산을 쓴다.

    예산은 **공유 자원**이므로 우선순위 순으로 소진한다. 설계서보다 좁은 계획만 나오므로
    어긋남이 아니라 조임이다(보고서에 명시한 조정 지점).
    """
    cells = [
        _cell(target_id=f"t{i}", real_finalized_count=40, reference_available_count=100, priority=i) for i in range(4)
    ]
    plan = plan_policy(_policy(cells, remaining_daily_budget=10, max_per_campaign=100))
    assert plan.planned_total == 10, [c.planned_count for c in plan.cells]
    # 앞선 우선순위가 먼저 가져간다
    assert plan.cells[0].planned_count == 10
    assert [c.planned_count for c in plan.cells[1:]] == [0, 0, 0]
    # 예산을 다 쓴 뒤의 셀은 사유를 남긴다 — 조용한 0 이 아니다.
    assert plan.cells[1].block_reason == BLOCK_DAILY_BUDGET


def test_campaign_cap_is_also_shared():
    cells = [
        _cell(target_id=f"t{i}", real_finalized_count=40, reference_available_count=100, priority=i) for i in range(3)
    ]
    plan = plan_policy(_policy(cells, remaining_daily_budget=1000, max_per_campaign=5))
    assert plan.planned_total == 5


def test_per_target_cap_overrides_the_policy_cap():
    plan = plan_policy(
        _policy(
            [_cell(real_finalized_count=40, reference_available_count=100, per_target_cap=2)],
            max_per_campaign=50,
        )
    )
    assert plan.cells[0].planned_count == 2


def test_pending_reservations_consume_the_share_headroom():
    """§5.2: "예약 상태도 P 에 포함해 …같은 여유를 초과 배정하지 못하게 한다." """
    base = plan_policy(_policy([_cell(real_finalized_count=10, reference_available_count=100)]))
    with_reservations = plan_policy(
        _policy([_cell(real_finalized_count=10, reference_available_count=100, pending_reserved_count=7)])
    )
    assert base.cells[0].share_headroom_count == 10
    assert with_reservations.cells[0].share_headroom_count == 3


def test_plan_is_deterministic():
    """같은 입력 → 같은 계획 (Phase F 완료 기준)."""
    cells = [
        _cell(target_id="b", real_finalized_count=40, reference_available_count=9, priority=5),
        _cell(target_id="a", real_finalized_count=40, reference_available_count=9, priority=5),
        _cell(target_id="c", real_finalized_count=40, reference_available_count=9, priority=1),
    ]
    first = plan_policy(_policy(list(cells), remaining_daily_budget=12))
    second = plan_policy(_policy(list(reversed(cells)), remaining_daily_budget=12))
    assert [(c.target_id, c.planned_count) for c in first.cells] == [
        (c.target_id, c.planned_count) for c in second.cells
    ]
    assert first.planned_total == second.planned_total == 12


def test_policy_level_totals_are_passed_through_not_summed_from_cells():
    """class 단위 총계는 같은 클래스의 셀마다 반복되므로 더하면 중복 계산된다."""
    cells = [_cell(target_id=f"t{i}", class_finalized_total=248) for i in range(4)]
    plan = plan_policy(
        _policy(
            cells,
            eligible_units_total=248,
            context_verified_units_total=0,
            context_unverified_units_total=0,
            context_missing_units_total=248,
            reference_pool_total=11,
        )
    )
    assert plan.eligible_units_total == 248  # 248*4 가 아니다
    assert plan.context_missing_units_total == 248
    assert plan.reference_pool_total == 11


# ─── 5. Phase F.3 — mode gate ────────────────────────────────────────────────


def test_disabled_mode_creates_no_campaign():
    decision = campaign_decision(MODE_DISABLED, planned_total=10, blocked_reason=None)
    assert decision.create_campaign is False
    assert decision.dispatchable is False


@pytest.mark.parametrize("mode", [MODE_PLAN_ONLY, MODE_APPROVAL_REQUIRED])
def test_plan_only_and_approval_required_stop_at_planned(mode):
    decision = campaign_decision(mode, planned_total=10, blocked_reason=None)
    assert decision.create_campaign is True
    assert decision.status == "planned"
    assert decision.auto_approve is False
    assert decision.dispatchable is False


def test_auto_dispatch_self_approves():
    decision = campaign_decision(MODE_AUTO_DISPATCH, planned_total=10, blocked_reason=None)
    assert decision.status == "approved"
    assert decision.auto_approve is True
    assert decision.dispatchable is True


@pytest.mark.parametrize("mode", [MODE_PLAN_ONLY, MODE_APPROVAL_REQUIRED, MODE_AUTO_DISPATCH])
def test_empty_or_blocked_plan_is_blocked_regardless_of_mode(mode):
    """빈 campaign 을 approved 로 남기면 '승인됐는데 아무것도 안 나온' 상태가 사유 없이 쌓인다."""
    assert campaign_decision(mode, planned_total=0, blocked_reason=None).status == "blocked"
    assert campaign_decision(mode, planned_total=5, blocked_reason=BLOCK_SHARE_CAP).status == "blocked"
    assert campaign_decision(mode, planned_total=5, blocked_reason=BLOCK_SHARE_CAP).dispatchable is False


@pytest.mark.parametrize("status", ["planned", "approved", "dispatching", "awaiting_review", "closed", "blocked"])
@pytest.mark.parametrize("mode", [MODE_DISABLED, MODE_PLAN_ONLY])
def test_plan_only_campaign_is_never_dispatchable(mode, status):
    """설계서 완료 기준: "승인하지 않은 campaign 은 ComfyUI 요청을 0건 생성한다." """
    assert is_dispatch_allowed(mode, status) is False


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("planned", False),
        ("approved", True),
        ("dispatching", True),
        ("awaiting_review", False),
        ("closed", False),
        ("cancelled", False),
        ("blocked", False),
    ],
)
def test_dispatch_requires_an_approved_campaign(status, expected):
    assert is_dispatch_allowed(MODE_APPROVAL_REQUIRED, status) is expected


# ─── 6. 활성화 게이트 (§6.2 "activate 시 검증") ──────────────────────────────


def _target(tid, dims, share):
    return {"target_id": tid, "dimensions": dims, "target_share": share}


_PILOT = [
    _target("t1", {"class": "falldown", "environment_type": "indoor", "daynight_type": "day"}, "0.35"),
    _target("t2", {"class": "falldown", "environment_type": "indoor", "daynight_type": "night"}, "0.25"),
    _target("t3", {"class": "falldown", "environment_type": "outdoor", "daynight_type": "day"}, "0.25"),
    _target("t4", {"class": "falldown", "environment_type": "outdoor", "daynight_type": "night"}, "0.15"),
]
_DIMS = ("class", "environment_type", "daynight_type")


def test_the_design_pilot_target_set_passes():
    """설계서 §5.1 의 첫 pilot 표 그대로 — 합이 정확히 1 이다."""
    assert validate_target_set(_DIMS, _PILOT) == []


def test_share_sum_must_be_exactly_one():
    broken = [
        *_PILOT[:3],
        _target("t4", {"class": "falldown", "environment_type": "outdoor", "daynight_type": "night"}, "0.20"),
    ]
    problems = validate_target_set(_DIMS, broken)
    assert any("target_share 합" in p for p in problems), problems


def test_every_target_must_carry_every_declared_dimension():
    broken = [_target("t1", {"class": "falldown", "environment_type": "indoor"}, "1.0")]
    problems = validate_target_set(_DIMS, broken)
    assert any("dimension 불일치" in p and "daynight_type" in p for p in problems), problems


def test_duplicate_cells_are_rejected():
    """같은 셀이 둘이면 같은 이미지가 두 deficit 을 메운다 (§5.1)."""
    dup = [
        _target("t1", {"class": "falldown", "environment_type": "indoor", "daynight_type": "day"}, "0.5"),
        _target("t2", {"class": "falldown", "environment_type": "indoor", "daynight_type": "day"}, "0.5"),
    ]
    problems = validate_target_set(_DIMS, dup)
    assert any("같은 셀" in p for p in problems), problems


@pytest.mark.parametrize("sentinel", ["deferred", "unknown", "indeterminate"])
def test_sentinels_cannot_be_target_values(sentinel):
    broken = [_target("t1", {"class": "falldown", "environment_type": sentinel, "daynight_type": "day"}, "1.0")]
    problems = validate_target_set(_DIMS, broken)
    assert any("미분류 마커" in p for p in problems), problems


def test_empty_target_set_is_a_violation():
    problems = validate_target_set(_DIMS, [])
    assert any("target 이 하나도 없다" in p for p in problems), problems


def test_unknown_balance_dimension_is_rejected():
    problems = validate_target_set(("class", "source_unit_name"), _PILOT)
    assert any("알 수 없는 balance dimension" in p for p in problems), problems


# ─── 7. 오늘 prod 를 그대로 태운 회귀 ────────────────────────────────────────


def test_todays_production_reality_yields_blocked_not_a_ratio():
    """2026-09-21 실측을 그대로 입력으로 넣었을 때의 결과를 고정한다.

    coverage_unit_facts 0행(투영 job 부재) + context 18,046 asset + weather 0행 →
    네 셀 전부 `blocked_no_finalized_facts`, planned 0. 나중에 투영 job 이 생겨 248 units 가
    들어오면 사유가 `blocked_context_coverage` 로 **바뀌어야** 한다 — 둘이 같은 값이 되면
    이 테스트가 깨진다.
    """
    dims = [
        {"class": "falldown", "environment_type": "indoor", "daynight_type": "day"},
        {"class": "falldown", "environment_type": "indoor", "daynight_type": "night"},
        {"class": "falldown", "environment_type": "outdoor", "daynight_type": "day"},
        {"class": "falldown", "environment_type": "outdoor", "daynight_type": "night"},
    ]
    shares = ["0.35", "0.25", "0.25", "0.15"]

    empty = plan_policy(
        _policy(
            [
                _cell(
                    target_id=f"t{i}",
                    dimensions=d,
                    target_share=s,
                    real_finalized_count=0,
                    class_finalized_total=0,
                    class_context_verified_total=0,
                    class_context_missing_total=0,
                    reference_available_count=0,
                    priority=i,
                )
                for i, (d, s) in enumerate(zip(dims, shares))
            ]
        )
    )
    assert empty.planned_total == 0
    assert {c.block_reason for c in empty.cells} == {BLOCK_NO_FINALIZED_FACTS}
    assert empty.blocked_reason == BLOCK_NO_FINALIZED_FACTS

    projected = plan_policy(
        _policy(
            [
                _cell(
                    target_id=f"t{i}",
                    dimensions=d,
                    target_share=s,
                    real_finalized_count=0,
                    class_finalized_total=248,
                    class_context_verified_total=0,
                    class_context_missing_total=248,
                    reference_available_count=0,
                    priority=i,
                )
                for i, (d, s) in enumerate(zip(dims, shares))
            ]
        )
    )
    assert projected.planned_total == 0
    assert {c.block_reason for c in projected.cells} == {BLOCK_CONTEXT_COVERAGE}
    assert projected.blocked_reason == BLOCK_CONTEXT_COVERAGE
    # 두 사유는 절대 같은 값이면 안 된다 — 그것이 이 파일의 존재 이유다.
    assert empty.blocked_reason != projected.blocked_reason
