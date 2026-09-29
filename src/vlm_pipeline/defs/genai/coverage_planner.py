"""Phase F.1 — synthetic coverage planner (op + job + schedule).

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md Phase F.1 / §5.2.
계산: `lib/coverage_planner.py` (순수). 입출력: `resources/postgres_coverage.py`.

Layer 3/4: @op + @job + @schedule.

이 planner 가 **하지 않는 것** (설계서 Phase F.1 의 명시 제약)
-------------------------------------------------------------
* ComfyUI 를 직접 호출하지 않는다. HTTP 클라이언트조차 import 하지 않는다.
* GPU 를 점유하지 않고 모델을 깨우지 않는다. 쿼리와 INSERT 뿐이다.
* campaign 을 승인하지 않는다 — `auto_dispatch` policy 만 예외이며, 그 mode 는 policy 의
  `coverage_ready=TRUE` 없이는 스키마가 거부한다.
* **task 를 dispatch 하지 않는다.** task 행은 `planned`/`ready` 로만 만들고, 실제 제출은
  `synthetic_campaign_dispatch_sensor` 소관이다(F.2).

기본 STOPPED 인 이유
--------------------
설계서 Phase F.1: "기본 상태는 STOPPED 이며, `coverage_ready` 검증을 통과한 policy 만
수동으로 시작할 수 있다." 이 레포에서 STOPPED 기본값은 Dagster storage 초기화마다
리셋되므로(CLAUDE.md §Staging 초기화) 켜 두려면 매번 UI 에서 다시 켜야 한다 — 자동 생성
기능에서는 그 마찰이 **의도된 안전장치**다.

오늘 돌리면 무엇이 나오나
-------------------------
prod 는 (a) 032 가 아직 미적용이고 (b) 적용돼도 `coverage_unit_facts` 를 채우는 투영 job 이
없으며 (c) `synthetic_coverage_policies` 가 0행이다. 따라서 schedule 은 첫 게이트에서
SkipReason("계획 가능한 policy 없음") 으로 끝난다. policy 를 손으로 넣어 돌려도 셀마다
`blocked_no_finalized_facts` 가 남는다 — "비율 0" 이 아니라 **측정 불가**라는 기록이다.
"""

from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    job,
    op,
    schedule,
)

from vlm_pipeline.lib.coverage_planner import (
    MODE_DISABLED,
    campaign_decision,
    plan_policy,
)
from vlm_pipeline.resources.postgres import PostgresResource
from vlm_pipeline.resources.postgres_coverage import (
    coverage_input_hash,
    schedule_bucket_for,
)


@op(
    name="synthetic_coverage_plan",
    description="policy 별 coverage deficit 계산 → snapshot/cell/campaign 행 기록 (§5.2). DB write 만.",
)
def synthetic_coverage_plan(context, db: PostgresResource) -> dict:
    policies = db.list_plannable_policies()
    if not policies:
        context.log.info("계획 가능한 policy 없음 (active + mode<>disabled + coverage_ready).")
        return {"policies": 0, "snapshots": 0, "campaigns": 0, "planned_tasks": 0, "blocked": {}}

    bucket = schedule_bucket_for()
    summary = {
        "policies": len(policies),
        "snapshots": 0,
        "campaigns": 0,
        "planned_tasks": 0,
        "blocked": {},
    }

    for policy in policies:
        policy_id = str(policy["policy_id"])
        targets = db.list_policy_targets(policy_id)
        plan_input = db.build_policy_plan_input(policy, targets)
        plan = plan_policy(plan_input)
        input_hash = coverage_input_hash(policy, targets)

        snapshot_id, created = db.upsert_coverage_snapshot(
            policy_id=policy_id,
            schedule_bucket=bucket,
            input_config_hash=input_hash,
            balance_dimensions=list(policy.get("balance_dimensions") or []),
            horizon_finalized_total=int(policy.get("horizon_finalized_total") or 0),
            max_synthetic_share=policy.get("max_synthetic_share", 0),
            plan=plan,
        )
        if created:
            summary["snapshots"] += 1

        # 사유는 policy 마다 반드시 남긴다. "0장" 만 로그에 찍고 끝내면 (a) 사실 없음
        # (b) context 미검증 (c) 진짜 0 이 같은 모양으로 보인다.
        context.log.info(
            "policy=%s targets=%d planned=%d blocked=%s "
            "units(total=%d verified=%d unverified=%d missing=%d) references=%d snapshot=%s%s",
            policy.get("policy_key", policy_id),
            len(targets),
            plan.planned_total,
            plan.blocked_reason or "-",
            plan.eligible_units_total,
            plan.context_verified_units_total,
            plan.context_unverified_units_total,
            plan.context_missing_units_total,
            plan.reference_pool_total,
            snapshot_id,
            "" if created else " (기존 snapshot 재사용 — 같은 입력)",
        )
        for reason, count in sorted(plan.block_reason_counts.items()):
            summary["blocked"][reason] = summary["blocked"].get(reason, 0) + count

        decision = campaign_decision(
            str(policy.get("mode") or MODE_DISABLED),
            planned_total=plan.planned_total,
            blocked_reason=plan.blocked_reason,
        )
        if not decision.create_campaign:
            continue

        campaign_id, campaign_created = db.insert_campaign(
            policy_id=policy_id,
            snapshot_id=snapshot_id,
            schedule_bucket=bucket,
            snapshot_input_hash=input_hash,
            status=decision.status,
            policy_mode_at_plan=str(policy.get("mode")),
            planned_task_count=plan.planned_total,
            blocked_reason=(decision.reason if decision.status == "blocked" else None),
            approved_by=(f"auto_dispatch:{policy.get('policy_key', policy_id)}" if decision.auto_approve else None),
        )
        if campaign_created:
            summary["campaigns"] += 1
        if campaign_id is None or not campaign_created or decision.status == "blocked":
            continue

        summary["planned_tasks"] += _materialize_tasks(context, db, policy, targets, plan, campaign_id, decision)

    context.log.info("coverage planner 완료: %s", summary)
    return summary


def _materialize_tasks(context, db, policy, targets, plan, campaign_id: str, decision) -> int:
    """계획된 장수만큼 task 행을 만든다. **reference 를 고르지 않는다.**

    reference 선택은 dispatch 시점의 사실(유효기간·holdout·use_count)에 달려 있고, planner
    가 미리 못 박으면 하루 뒤 dispatch 때 이미 무효가 된 reference 를 들고 있게 된다. 그래서
    task 는 `reference_id=NULL` 인 **자리표**로 만들고 dispatcher 가 채운다.

    ⚠️ 그 결과 `(campaign_id, reference_id, workflow_id, seed)` UNIQUE 는 reference_id 가
       NULL 인 동안 중복을 막지 못한다(SQL 의 NULL 은 서로 다른 값). 중복 방지는 dispatcher 가
       reference 를 확정하는 순간부터 걸린다 — 자리표 단계의 중복은 seed 로 구분한다.
    """
    by_target = {cell.target_id: cell for cell in plan.cells}
    created = 0
    for target in targets:
        cell = by_target.get(str(target["target_id"]))
        if cell is None or cell.planned_count <= 0:
            continue
        workflow = target.get("workflow_id") or policy.get("default_workflow_id")
        for index in range(cell.planned_count):
            task_id = db.insert_generation_task(
                {
                    "campaign_id": campaign_id,
                    "target_id": cell.target_id,
                    "dimensions_hash": str(target.get("dimensions_hash")),
                    "reference_id": None,
                    "workflow_id": workflow,
                    "template_id": target.get("prompt_template_id"),
                    # 결정론: 같은 campaign/target 이면 같은 seed 열이 나온다.
                    "seed": index,
                    "state": "ready" if decision.dispatchable else "planned",
                    "priority": int(target.get("priority") or 100),
                    "max_retries": int(policy.get("max_task_retries") or 2),
                }
            )
            if task_id:
                created += 1

    if created:
        # 예약을 원장에 남긴다 — 실패·반려 시 release 로 정확히 되돌릴 수 있어야 한다(§6.3).
        db.record_budget_event(
            policy_id=str(policy["policy_id"]),
            campaign_id=campaign_id,
            event_type="reserve",
            job_count=created,
            reason="campaign_plan",
        )
        context.log.info("campaign=%s task %d건 예약", campaign_id, created)
    return created


@job(
    name="synthetic_coverage_planner_job",
    description="[Phase F.1] policy 별 coverage deficit → snapshot/campaign 초안. ComfyUI 호출 0건.",
)
def synthetic_coverage_planner_job():
    synthetic_coverage_plan()


@schedule(
    name="synthetic_coverage_planner_schedule",
    job=synthetic_coverage_planner_job,
    # off-peak. 03:00 KST 는 fiftyone_label_refresh_schedule 과 같은 시간대라 05:00 으로 둔다
    # (둘 다 DB 를 훑지만 GPU 는 안 쓴다 — 충돌은 아니고 로그 가독성 문제).
    cron_schedule="0 5 * * *",
    execution_timezone="Asia/Seoul",
    # 설계서 Phase F.1: "기본 상태는 STOPPED". 자동 생성 계통의 기본값은 언제나 꺼짐이다.
    default_status=DefaultScheduleStatus.STOPPED,
    # `db` 는 아래 함수 인자로 선언한다 — Dagster 가 두 곳 동시 선언을 금지한다
    # ("Cannot specify resource requirements in both @schedule decorator and as arguments").
    description=(
        "[Phase F.1 · 기본 STOPPED] 하루 1회 coverage deficit 계산 → snapshot/campaign 초안. "
        "ComfyUI 직접 호출 없음, GPU 미점유. 계획 가능한 policy 가 없으면 SkipReason."
    ),
)
def synthetic_coverage_planner_schedule(context: ScheduleEvaluationContext, db: PostgresResource):
    """게이트만 본다 — 실제 계산은 job 안에서 한다.

    설계서: "산출물이 없는 날은 SkipReason 으로 끝나며, 모델을 깨우거나 GPU를 점유하지
    않는다." 그래서 evaluation 에서는 작은 테이블 한 번만 읽고, 계획할 policy 가 없으면
    run 자체를 만들지 않는다. DB 가 안 되면 **예외가 아니라 skip** 이다 — 스케줄 tick 실패가
    쌓이는 것보다 사유가 남는 skip 이 낫다.

    ⚠️ `db` 를 **인자로 받는다** (`context.resources.db` 가 아니라). Dagster 의 두 경로가
       서로 다르게 주입하기 때문이다: 운영 경로(`schedule_decorator._wrapped_fn`)는 함수의
       타입 힌트에서 resource 인자를 찾아 kwarg 로 넣고, 직접 호출 경로
       (`ScheduleDefinition.__call__`)는 `required_resource_keys` 를 kwarg 로 splat 한다.
       인자로 선언하지 않으면 **테스트에서만 TypeError 가 나고 운영에서는 조용히 통과**한다
       — 둘 다 만족하는 형태가 이것뿐이다. sensor 도 같은 비대칭이 있어
       `campaign_dispatch.py` 가 같은 형태를 쓴다. (`required_resource_keys` 와 인자 선언을
       동시에 쓰면 Dagster 가 정의 시점에 거부한다.)
    """
    try:
        policies = db.list_plannable_policies()
    except Exception as exc:  # noqa: BLE001
        yield SkipReason(f"policy 조회 실패 — 이번 tick 건너뜀: {exc}")
        return

    if not policies:
        yield SkipReason(
            "계획 가능한 policy 없음 (status='active' + mode<>'disabled' + coverage_ready=true). "
            "Phase F.1 게이트 미통과 — 이것은 정상이며 오류가 아니다."
        )
        return

    bucket = schedule_bucket_for()
    keys = ", ".join(str(p.get("policy_key") or p.get("policy_id")) for p in policies)
    context.log.info("coverage planner run 요청: bucket=%s policies=[%s]", bucket, keys)
    # run_key 로 같은 날 중복 run 을 막는다 (snapshot/campaign 의 UNIQUE 와 이중 방어).
    yield RunRequest(run_key=f"coverage-plan-{bucket}", tags={"coverage_schedule_bucket": bucket})
