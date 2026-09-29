"""Synthetic coverage planner — 순수 계산 (L1-2, no dagster/defs/resources/PG import).

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md §5.2 + Phase F.1/F.3.
스키마 정본: `sql/migrations/postgres/033_coverage_control_plane.sql`.

구현하는 식은 설계서 §5.2 그대로다::

    desired_i = max(min_finalized_count_i, ceil(horizon_finalized_total × target_share_i))
    deficit_i = max(0, desired_i − eligible_finalized_i)
    planned_i = min(deficit_i, max_per_campaign, remaining_daily_budget,
                    eligible_reference_pool_i, synthetic_share_headroom_i)
    share cap : (S + P) / (R + S + P) ≤ a

이 모듈이 **하지 않는 것**
--------------------------
DB 를 읽지 않고, HTTP 를 호출하지 않고, ComfyUI 를 모른다. 입력은 전부 호출자가 읽어다
준 값이며(`resources/postgres_coverage.py`), 출력은 그대로 `coverage_snapshot_cells` 한
행이 된다. 그래서 이 파일만 보고 "오늘 왜 0장인가" 를 재현할 수 있다.

0 과 관측 불가를 구분하는 규칙
------------------------------
planned=0 은 **세 가지 서로 다른 사실**일 수 있다. 이 모듈은 셋을 절대 같은 값으로 내지
않는다 (033 헤더의 (a)/(b)/(c) 와 같은 구분):

  (a) `class_finalized_total == 0`            → ``blocked_no_finalized_facts``
      사실 자체가 없다. 비율을 말할 분모가 없다.
  (b) `class_context_verified_total == 0`     → ``blocked_context_coverage``
      사실은 있는데 policy 가 선언한 축이 하나도 검증되지 않았다. 셀의 0 은 비율이 아니라
      **측정 실패**다. `weather` 처럼 실값이 0행인 축을 dimension 으로 선언하면 여기 걸린다.
  (c) 위 둘이 아닌 0                           → block_reason 없음. 진짜 0 이다.

(a)/(b) 를 "deficit 이 크다" 로 읽으면 planner 는 **관측하지 못한 것을 부족분으로 착각해
생성을 지시한다.** 이 레포가 반복해 당한 "부재에 기댄 안전"([[project_safety_by_absence]])
의 정확한 재현이라, 아래 `_cell_block_reason()` 이 그 둘을 deficit 보다 **먼저** 본다.

결정론
------
설계서 Phase F 완료 기준: "planner 가 target ratio 와 final count 에서 결정론적으로 같은
campaign 초안을 만든다." 그래서 (1) 셀 순회 순서를 `(priority, target_id)` 로 전순서화하고
(2) 비율 연산을 전부 `fractions.Fraction` 으로 한다. float 로 하면 `0.35 × 20` 이 처리계에
따라 7.000000000000001 이 되어 ceil 이 8 로 튄다 — 같은 입력이 다른 계획을 낸다.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from fractions import Fraction
from typing import Any

# ─── block reason 어휘 (033 의 CHECK 와 1:1) ──────────────────────────────────

BLOCK_NO_TARGETS = "blocked_no_targets"
BLOCK_NO_FINALIZED_FACTS = "blocked_no_finalized_facts"
BLOCK_CONTEXT_COVERAGE = "blocked_context_coverage"
BLOCK_NO_REFERENCE = "blocked_no_reference"
BLOCK_SHARE_CAP = "blocked_share_cap"
BLOCK_DAILY_BUDGET = "blocked_daily_budget"
BLOCK_CAMPAIGN_CAP = "blocked_campaign_cap"
BLOCK_RETRY_CEILING = "blocked_retry_ceiling"
BLOCK_POLICY_MODE = "blocked_policy_mode"

#: policy 단위 rollup 이 여러 사유 중 하나를 고를 때의 **고정** 우선순위.
#: 앞쪽일수록 "더 근본적인 막힘" 이다 — 측정 자체가 안 되는 것이 예산 부족보다 먼저다.
#: 고정 순서여야 같은 입력이 같은 사유를 낸다(결정론).
BLOCK_PRECEDENCE: tuple[str, ...] = (
    BLOCK_NO_TARGETS,
    BLOCK_NO_FINALIZED_FACTS,
    BLOCK_CONTEXT_COVERAGE,
    BLOCK_NO_REFERENCE,
    BLOCK_SHARE_CAP,
    BLOCK_DAILY_BUDGET,
    BLOCK_CAMPAIGN_CAP,
    BLOCK_RETRY_CEILING,
    BLOCK_POLICY_MODE,
)

# ─── policy mode (033 의 CHECK 와 1:1) ────────────────────────────────────────

MODE_DISABLED = "disabled"
MODE_PLAN_ONLY = "plan_only"
MODE_APPROVAL_REQUIRED = "approval_required"
MODE_AUTO_DISPATCH = "auto_dispatch"

POLICY_MODES: tuple[str, ...] = (MODE_DISABLED, MODE_PLAN_ONLY, MODE_APPROVAL_REQUIRED, MODE_AUTO_DISPATCH)

#: 032 의 6 context 축 + 'class'. 033 의 `balance_dimensions` CHECK 와 같은 어휘.
ALLOWED_DIMENSIONS: tuple[str, ...] = (
    "class",
    "environment_type",
    "daynight_type",
    "weather",
    "camera_angle",
    "subject_scale",
    "occlusion_state",
)

#: 축 값 자리에 오면 안 되는 미분류 마커 (032 `*_axis_sentinel_check` 와 같은 어휘).
AXIS_SENTINELS: frozenset[str] = frozenset({"deferred", "unknown", "indeterminate"})


def _as_fraction(value: Any) -> Fraction:
    """비율을 **정확한** 유리수로. float 는 str 경유로 십진 표기를 보존한다.

    ``Fraction(0.35)`` 는 3152519739159347/9007199254740992 이지만
    ``Fraction("0.35")`` 는 7/20 이다. psycopg2 가 NUMERIC 을 Decimal 로 주므로 실전에서는
    Decimal 경로가 쓰이고, float 경로는 테스트/수기 입력 방어용이다.
    """
    if isinstance(value, Fraction):
        return value
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, Decimal):
        return Fraction(value)
    if isinstance(value, float):
        return Fraction(str(value))
    return Fraction(str(value))


def _ceil_fraction(value: Fraction) -> int:
    """유리수 올림. ``math.ceil`` 은 Fraction 을 지원하지만 의도를 드러내려 감싼다."""
    return math.ceil(value)


# ─── 입력 ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CellObservation:
    """한 target cell 에 대해 **DB 에서 읽어온 사실**. 계산 결과는 하나도 들어 있지 않다."""

    target_id: str
    dimensions: dict[str, str]
    target_share: Any = 0
    min_finalized_count: int = 0
    #: §5.2 의 R — 이 셀의 real finalized (LS finalized + 선언 축 전부 verified)
    real_finalized_count: int = 0
    #: §5.2 의 S — 이 셀의 accepted synthetic. 정본은 032 `coverage_unit_facts`(LS finalized)다.
    synthetic_accepted_count: int = 0
    #: §5.2 의 P(기존) — 아직 살아 있는 예약. `v_synthetic_coverage_reservations` 에서 온다.
    pending_reserved_count: int = 0
    #: class 단위 3분할. 셋의 합은 class_finalized_total 이어야 한다(033 의 CHECK).
    class_finalized_total: int = 0
    class_context_verified_total: int = 0
    class_context_unverified_total: int = 0
    class_context_missing_total: int = 0
    #: `v_generation_reference_candidates` 에서 이 셀의 context 와 맞는 승인 reference 수
    reference_available_count: int = 0
    #: target 별 per-campaign cap. None 이면 policy.max_per_campaign 을 쓴다.
    per_target_cap: int | None = None
    priority: int = 100


@dataclass(frozen=True)
class PolicyPlanInput:
    """policy 1개분 계획 입력. 호출자가 DB 에서 조립해 준다."""

    policy_id: str
    mode: str
    balance_dimensions: tuple[str, ...]
    horizon_finalized_total: int
    #: §5.2 의 a
    max_synthetic_share: Any = 0
    max_per_campaign: int = 0
    remaining_daily_budget: int = 0
    cells: tuple[CellObservation, ...] = ()
    # ↓ policy 단위 rollup. **셀에서 합산해 구하지 않는다** — class 단위 총계는 같은
    #   클래스의 셀마다 반복되므로 더하면 중복 계산된다. 호출자가 DB 에서 한 번에 센 값을
    #   그대로 넘긴다. target 이 0개여도 이 넷은 값을 가질 수 있고, 그래야 "잴 대상이
    #   없었다" 와 "사실이 하나도 없었다" 가 구분된다.
    eligible_units_total: int = 0
    context_verified_units_total: int = 0
    context_unverified_units_total: int = 0
    context_missing_units_total: int = 0
    reference_pool_total: int = 0


# ─── 출력 ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CellPlan:
    """`coverage_snapshot_cells` 한 행과 1:1. 계산의 **모든 중간항**을 들고 있다.

    중간항을 남기는 이유: 나중에 "왜 3장만 계획했나" 를 물었을 때 join 없이 한 행으로
    답해야 하기 때문이다. min() 의 어느 항이 binding 이었는지가 행 안에 있다.
    """

    target_id: str
    dimensions: dict[str, str]
    target_share: Fraction
    min_finalized_count: int
    desired_count: int
    eligible_finalized_count: int
    deficit_count: int
    planned_count: int
    real_finalized_count: int
    synthetic_accepted_count: int
    pending_reserved_count: int
    class_finalized_total: int
    class_context_verified_total: int
    class_context_unverified_total: int
    class_context_missing_total: int
    reference_available_count: int
    share_headroom_count: int
    budget_headroom_count: int
    campaign_cap_count: int
    block_reason: str | None


@dataclass(frozen=True)
class PolicyPlan:
    policy_id: str
    mode: str
    cells: tuple[CellPlan, ...] = ()
    planned_total: int = 0
    blocked_reason: str | None = None
    #: policy 단위 rollup. cell 이 0개여도 "사실이 하나도 없었다" 를 말할 수 있어야 한다.
    eligible_units_total: int = 0
    context_verified_units_total: int = 0
    context_unverified_units_total: int = 0
    context_missing_units_total: int = 0
    reference_pool_total: int = 0
    block_reason_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class CampaignDecision:
    """F.3 mode gate 의 결과. 033 의 `*_mode_gate_check` 와 같은 규칙을 코드에서도 쓴다."""

    #: campaign 을 만들 것인가. disabled 면 False (행 자체를 만들지 않는다).
    create_campaign: bool
    #: `synthetic_generation_campaigns.status` 초기값
    status: str
    #: planner 가 스스로 승인해도 되는가 (auto_dispatch 한정)
    auto_approve: bool
    #: dispatch sensor 가 집어갈 수 있는가
    dispatchable: bool
    reason: str


# ─── §5.2 계산식 ──────────────────────────────────────────────────────────────


def desired_count(horizon_finalized_total: int, target_share: Any, min_finalized_count: int = 0) -> int:
    """``max(min_finalized_count, ceil(horizon × share))`` — §5.2 그대로.

    horizon 이 0 이어도 min_finalized_count 가 살아 있다(설계서: "명시적인
    horizon_finalized_total **또는** cell별 min_finalized_count 를 가져야 한다").
    """
    horizon = max(0, int(horizon_finalized_total))
    share = _as_fraction(target_share)
    if share < 0:
        share = Fraction(0)
    return max(max(0, int(min_finalized_count)), _ceil_fraction(share * horizon))


def deficit_count(desired: int, eligible_finalized: int) -> int:
    """``max(0, desired − eligible_finalized)`` — §5.2 그대로."""
    return max(0, int(desired) - int(eligible_finalized))


def share_headroom(
    real_finalized: int,
    synthetic_accepted: int,
    pending_reserved: int,
    max_synthetic_share: Any,
    *,
    unbounded_fallback: int = 0,
) -> int:
    """``(S + P) / (R + S + P) ≤ a`` 를 **새로 예약 가능한 수**로 푼 값.

    대수::

        S + P ≤ a(R + S + P)
        P(1 − a) ≤ aR − S(1 − a)
        P_total_max = floor( a·R / (1 − a) ) − S          (a < 1)

    반환값은 `P_total_max − pending_reserved` 의 음수 절단이다. 기존 예약을 빼는 이유는
    §5.2 가 명시한 대로 "예약 상태도 P 에 포함해 동시에 여러 planner tick 이 같은 여유를
    초과 배정하지 못하게" 하기 위함이다.

    ⚠️ **R = 0 이면 a 가 얼마든 결과는 0 이다** (``floor(0) − S ≤ 0``). real finalized 가
       하나도 없는 셀은 share cap 자체가 생성량 0 을 강제한다 — 별도 킬스위치가 아니라
       계산식이 fail-closed 다. 오늘 prod 의 모든 셀이 정확히 이 상태다.

    ``a >= 1`` 은 "합성 100% 허용" 이라 이 항이 binding 이 아니다. 호출자가 다른 상한
    (보통 deficit)을 `unbounded_fallback` 으로 넘겨 "여기서는 share 가 안 막는다" 를
    표현한다 — 무한대를 정수 컬럼에 억지로 넣지 않기 위한 선택.
    """
    real = max(0, int(real_finalized))
    synth = max(0, int(synthetic_accepted))
    reserved = max(0, int(pending_reserved))
    a = _as_fraction(max_synthetic_share)

    if a <= 0:
        return 0
    if a >= 1:
        return max(0, int(unbounded_fallback))

    total_max = math.floor((a * real) / (1 - a)) - synth
    return max(0, total_max - reserved)


def _cell_block_reason(
    *,
    class_finalized_total: int,
    class_context_verified_total: int,
    deficit: int,
    reference_available: int,
    share_headroom_count: int,
    budget_headroom: int,
    campaign_cap: int,
) -> str | None:
    """block 사유 판정. **측정 실패를 부족분보다 먼저 본다.**

    순서가 계약이다:
      1. 사실 자체가 없으면(`class_finalized_total == 0`) 비율을 말할 분모가 없다.
      2. 사실은 있는데 선언 축이 하나도 검증 안 됐으면 셀의 0 은 측정 실패다.
      3. 그제서야 deficit==0(=만족)을 본다.
      4. 나머지는 §5.2 min() 항 중 0 인 것 — 0 이면 "부분 생성" 이 아니라 명시적 사유다.
    """
    if class_finalized_total <= 0:
        return BLOCK_NO_FINALIZED_FACTS
    if class_context_verified_total <= 0:
        return BLOCK_CONTEXT_COVERAGE
    if deficit <= 0:
        return None
    if reference_available <= 0:
        return BLOCK_NO_REFERENCE
    if share_headroom_count <= 0:
        return BLOCK_SHARE_CAP
    if budget_headroom <= 0:
        return BLOCK_DAILY_BUDGET
    if campaign_cap <= 0:
        return BLOCK_CAMPAIGN_CAP
    return None


def _cell_sort_key(cell: CellObservation) -> tuple[int, str]:
    """전순서 — 같은 입력이 같은 배분을 내게 한다(결정론)."""
    return (int(cell.priority), str(cell.target_id))


def plan_policy(policy: PolicyPlanInput) -> PolicyPlan:
    """policy 1개분 계획. 셀별 `CellPlan` + policy 단위 rollup.

    예산·campaign cap 은 **셀 사이에 공유되는 자원**이라 우선순위 순으로 소진한다.
    설계서 §5.2 의 min() 은 셀 단위 식이라 그대로 쓰면 N 개 셀이 각각 전체 예산을 쓸 수
    있다고 계산해 합계가 예산을 초과한다 — 그래서 여기서는 **남은 양**을 항으로 쓴다.
    (설계서보다 좁은 계획만 나오므로 어긋남이 아니라 조임이다. 보고서에 명시.)
    """
    cells_in = tuple(sorted(policy.cells, key=_cell_sort_key))

    remaining_budget = max(0, int(policy.remaining_daily_budget))
    remaining_campaign = max(0, int(policy.max_per_campaign))

    plans: list[CellPlan] = []
    planned_total = 0
    reason_counts: dict[str, int] = {}

    for cell in cells_in:
        share = _as_fraction(cell.target_share)
        desired = desired_count(policy.horizon_finalized_total, share, cell.min_finalized_count)
        real = max(0, int(cell.real_finalized_count))
        synth = max(0, int(cell.synthetic_accepted_count))
        reserved = max(0, int(cell.pending_reserved_count))
        eligible = real + synth
        deficit = deficit_count(desired, eligible)
        reference = max(0, int(cell.reference_available_count))

        headroom = share_headroom(
            real,
            synth,
            reserved,
            policy.max_synthetic_share,
            unbounded_fallback=deficit,
        )

        # target 별 cap 이 있으면 그것이, 없으면 policy cap 이 셀 상한이다(§5.2 그대로).
        # 그 위에 "campaign 전체 남은 양" 을 한 번 더 씌운다(위 docstring).
        per_cell_cap = policy.max_per_campaign if cell.per_target_cap is None else max(0, int(cell.per_target_cap))
        campaign_cap = min(max(0, int(per_cell_cap)), remaining_campaign)

        block_reason = _cell_block_reason(
            class_finalized_total=cell.class_finalized_total,
            class_context_verified_total=cell.class_context_verified_total,
            deficit=deficit,
            reference_available=reference,
            share_headroom_count=headroom,
            budget_headroom=remaining_budget,
            campaign_cap=campaign_cap,
        )

        planned = 0
        if block_reason is None and deficit > 0:
            planned = min(deficit, reference, headroom, remaining_budget, campaign_cap)
            planned = max(0, planned)

        plans.append(
            CellPlan(
                target_id=cell.target_id,
                dimensions=dict(cell.dimensions),
                target_share=share,
                min_finalized_count=max(0, int(cell.min_finalized_count)),
                desired_count=desired,
                eligible_finalized_count=eligible,
                deficit_count=deficit,
                planned_count=planned,
                real_finalized_count=real,
                synthetic_accepted_count=synth,
                pending_reserved_count=reserved,
                class_finalized_total=max(0, int(cell.class_finalized_total)),
                class_context_verified_total=max(0, int(cell.class_context_verified_total)),
                class_context_unverified_total=max(0, int(cell.class_context_unverified_total)),
                class_context_missing_total=max(0, int(cell.class_context_missing_total)),
                reference_available_count=reference,
                share_headroom_count=headroom,
                # 이 셀이 배분받을 때의 **남은 양**. 033 의 planned<=budget/cap CHECK 와 맞는다.
                budget_headroom_count=remaining_budget,
                campaign_cap_count=campaign_cap,
                block_reason=block_reason,
            )
        )

        if block_reason is not None:
            reason_counts[block_reason] = reason_counts.get(block_reason, 0) + 1

        planned_total += planned
        remaining_budget -= planned
        remaining_campaign -= planned

    blocked_reason = _rollup_block_reason(plans, planned_total, reason_counts)

    return PolicyPlan(
        policy_id=policy.policy_id,
        mode=policy.mode,
        cells=tuple(plans),
        planned_total=planned_total,
        blocked_reason=blocked_reason,
        # 입력 그대로 통과시킨다 — 셀 합산은 class 총계를 중복 계산한다(PolicyPlanInput 주석).
        eligible_units_total=max(0, int(policy.eligible_units_total)),
        context_verified_units_total=max(0, int(policy.context_verified_units_total)),
        context_unverified_units_total=max(0, int(policy.context_unverified_units_total)),
        context_missing_units_total=max(0, int(policy.context_missing_units_total)),
        reference_pool_total=max(0, int(policy.reference_pool_total)),
        block_reason_counts=reason_counts,
    )


def _rollup_block_reason(
    plans: list[CellPlan],
    planned_total: int,
    reason_counts: dict[str, int],
) -> str | None:
    """policy 단위 사유. **cell 이 0개인 경우를 가장 먼저 처리한다.**

    target 이 없으면 cell 행도 없어서 사유를 적을 곳이 snapshot 행뿐이다. 그때 사유가
    NULL 이면 "아무 일도 없었다" 와 "잴 대상이 없었다" 가 같은 모양이 된다.
    """
    if not plans:
        return BLOCK_NO_TARGETS
    if planned_total > 0:
        return None
    if not reason_counts:
        # 전 셀이 deficit 0 = 이미 목표 충족. blocked 가 아니다.
        return None
    for reason in BLOCK_PRECEDENCE:
        if reason in reason_counts:
            return reason
    return None


# ─── Phase F.3 — policy mode gate ─────────────────────────────────────────────


def campaign_decision(mode: str, *, planned_total: int, blocked_reason: str | None) -> CampaignDecision:
    """mode 별로 campaign 을 어디까지 진행시킬지. 033 의 `*_mode_gate_check` 와 같은 규칙.

    * ``disabled``          — campaign 행 자체를 만들지 않는다.
    * ``plan_only``         — status='planned' 에서 끝. 승인해도 dispatch 로 못 간다
                              (033 이 CHECK 로 못 박는다 — 코드만의 약속이 아니다).
    * ``approval_required`` — status='planned'. 사람이 승인해야 dispatch 가능. POC 기본값.
    * ``auto_dispatch``     — planner 가 status='approved' 로 만든다. policy 의
                              ``coverage_ready`` 가 TRUE 여야 이 mode 자체가 존재할 수 있다.

    계획이 0장이거나 blocked 면 mode 와 무관하게 `status='blocked'` 다 — 빈 campaign 을
    approved 로 남기면 "승인됐는데 아무것도 안 나온" 상태가 사유 없이 쌓인다.
    """
    if mode == MODE_DISABLED:
        return CampaignDecision(
            create_campaign=False,
            status="blocked",
            auto_approve=False,
            dispatchable=False,
            reason=BLOCK_POLICY_MODE,
        )

    if blocked_reason is not None or planned_total <= 0:
        return CampaignDecision(
            create_campaign=True,
            status="blocked",
            auto_approve=False,
            dispatchable=False,
            reason=blocked_reason or BLOCK_NO_FINALIZED_FACTS,
        )

    if mode == MODE_AUTO_DISPATCH:
        return CampaignDecision(
            create_campaign=True,
            status="approved",
            auto_approve=True,
            dispatchable=True,
            reason="auto_dispatch",
        )

    # plan_only / approval_required 는 둘 다 planned 에서 멈춘다. 차이는 그 다음 —
    # approval_required 만 사람 승인으로 dispatch 로 갈 수 있고, plan_only 는 스키마가 막는다.
    return CampaignDecision(
        create_campaign=True,
        status="planned",
        auto_approve=False,
        dispatchable=False,
        reason=mode,
    )


def is_dispatch_allowed(policy_mode_at_plan: str, campaign_status: str) -> bool:
    """dispatch sensor 가 이 campaign 의 task 를 집어도 되는가.

    033 의 `synthetic_generation_campaigns_mode_gate_check` 와 **같은 판정**을 코드에서도
    한다. 스키마는 상태 전이를 막고 이 함수는 읽기 시점에 한 번 더 거른다 — 둘 중 하나가
    빠져도 나머지가 막게 하는 의도적 중복이다.
    """
    if policy_mode_at_plan in (MODE_DISABLED, MODE_PLAN_ONLY):
        return False
    return campaign_status in ("approved", "dispatching")


# ─── 정합성 검사 (활성화 게이트에서 쓴다) ─────────────────────────────────────


def validate_target_set(
    balance_dimensions: tuple[str, ...] | list[str],
    targets: list[dict[str, Any]],
) -> list[str]:
    """policy activate 전 target 집합 검증. 위반 사유 리스트(비면 통과).

    설계서 §6.2 의 "policy activate 시 share 합=1, 선언된 dimension 전부 존재, target 간
    중복 없음 검증" 을 그대로 구현한다. 셋 중 **중복 없음만 스키마가 막을 수 있고**
    (`(policy_id, dimensions_hash)` UNIQUE) 나머지 둘은 행 간/테이블 간 조건이라 CHECK 로
    못 건다 — 그래서 여기 있다. 033 헤더 "조정 4" 참조.

    각 target dict 는 최소 ``{"target_id": ..., "dimensions": {...}, "target_share": ...}``.
    """
    problems: list[str] = []
    declared = list(balance_dimensions)

    if not declared:
        problems.append("balance_dimensions 가 비어 있다")
    unknown_axes = [d for d in declared if d not in ALLOWED_DIMENSIONS]
    if unknown_axes:
        problems.append(f"알 수 없는 balance dimension: {sorted(unknown_axes)}")
    if len(set(declared)) != len(declared):
        problems.append("balance_dimensions 에 중복이 있다")

    if not targets:
        problems.append("target 이 하나도 없다 — 활성화해도 계산할 셀이 없다")
        return problems

    declared_set = set(declared)
    seen: dict[tuple[tuple[str, str], ...], str] = {}
    share_sum = Fraction(0)

    for target in targets:
        tid = str(target.get("target_id", "?"))
        dims = dict(target.get("dimensions") or {})
        share_sum += _as_fraction(target.get("target_share", 0))

        if set(dims) != declared_set:
            missing = sorted(declared_set - set(dims))
            extra = sorted(set(dims) - declared_set)
            problems.append(f"target {tid}: dimension 불일치 (missing={missing} extra={extra})")

        sentinel_axes = sorted(axis for axis, value in dims.items() if str(value) in AXIS_SENTINELS)
        if sentinel_axes:
            problems.append(f"target {tid}: 미분류 마커를 축 값으로 썼다 {sentinel_axes}")

        key = tuple(sorted((str(k), str(v)) for k, v in dims.items()))
        if key in seen:
            problems.append(f"target {tid}: {seen[key]} 와 같은 셀이다 (중복 deficit)")
        else:
            seen[key] = tid

    if share_sum != 1:
        problems.append(f"target_share 합이 1 이 아니다 (={share_sum})")

    return problems
