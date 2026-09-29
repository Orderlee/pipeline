"""Phase F.2 — synthetic_campaign_dispatch_sensor.

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md Phase F.2.

승인된 campaign 의 예약 task 를 **한 번에 최대 1건** GenAI 의 authenticated internal
endpoint 로 보낸다. Dagster 는 ComfyUI graph 를 직접 조작하지 않는다 — GenAI 가 trusted
MinIO source frame 을 materialize 하고 `comfy_local` adapter 로 submit 한 뒤 기존
poll/finalize 경로를 탄다.

"최대 1건" 은 취향이 아니다
---------------------------
ComfyUI 는 GPU0 전용 · queue=1 · `generation_gpu_leases` 단일 lease 다(설계서 §3). 한 tick 에
여러 건을 밀어 넣어도 두 번째부터는 대기하거나 실패하고, 그 사이 embedding-service 가
GPU0 에서 내려가 있는 시간만 길어진다. 그래서 상한은 코드 상수이며 env 노브로 열어 두지
않았다.

deferred 는 반드시 사유를 남긴다
--------------------------------
GPU lease / budget / reference / 승인 중 **무엇 때문에** 미뤘는지가 없으면 다음 tick 이
같은 실패를 반복한다. 033 의 `synthetic_generation_tasks_deferred_reason_check` 가 빈 사유를
거부하고, 아래 분기는 사유를 하나씩 대응시킨다.

GPU lease 는 읽기만 한다
------------------------
`lib/gpu_lease.py` 의 계약 그대로 — acquire/heartbeat/release 는 `docker/genai/db/pg.py`
전속이다. 센서가 lease 를 잡으면 comfy 가 영원히 못 잡는 역방향 교착이 생긴다.

기본 STOPPED
------------
설계서 Phase F.2 + §7 rollout 게이트. 승인 없는 campaign 이 ComfyUI 요청을 0건 만드는 것은
스키마(mode gate CHECK) · SQL(`next_dispatchable_tasks` 의 WHERE) · 이 센서의 판정 **세 겹**이
보장하지만, 센서 자체가 꺼져 있는 것이 첫 번째 방어다.
"""

from __future__ import annotations

import os
import time

import requests
from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.coverage_planner import is_dispatch_allowed
from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.gpu_lease import (
    GPU0_COMFY,
    describe_lease,
    is_lease_blocking,
    lease_from_pg_row,
    seconds_until_free,
)
from vlm_pipeline.resources.postgres import PostgresResource

#: GPU0 lease + Comfy queue=1 불변식에서 나온 상한. env 노브가 아니다(모듈 docstring).
MAX_DISPATCH_PER_TICK = 1

_SENSOR_INTERVAL = int_env("SYNTHETIC_DISPATCH_INTERVAL_SECONDS", 300)
_GENAI_INTERNAL_BASE = os.getenv("GENAI_INTERNAL_BASE", "http://genai:8088").rstrip("/")
_GENAI_INTERNAL_TIMEOUT = int_env("GENAI_INTERNAL_TIMEOUT", 60)

#: GenAI 쪽 계약. **이 endpoint 는 아직 구현되지 않았다** — docker/ 는 이 작업 범위 밖이라
#: 계약만 고정하고 부재 시 graceful defer 한다. 요구 사항은 이 파일 하단 참조.
_DISPATCH_PATH = "/internal/coverage/dispatch"

# defer 사유 — 033 의 `*_deferred_reason_domain_check` 허용값과 1:1.
_DEFER_GPU = "gpu_lease_busy"
_DEFER_BUDGET = "daily_budget_exhausted"
_DEFER_REFERENCE = "reference_unavailable"
_DEFER_NOT_APPROVED = "campaign_not_approved"
_DEFER_ENDPOINT = "endpoint_unavailable"


def _internal_token() -> str | None:
    return (os.getenv("GENAI_INTERNAL_TOKEN") or "").strip() or None


@sensor(
    name="synthetic_campaign_dispatch_sensor",
    minimum_interval_seconds=_SENSOR_INTERVAL,
    # 설계서 Phase F.2 / §7: 자동 dispatch 는 rollout 게이트를 통과한 뒤에만 켠다.
    default_status=DefaultSensorStatus.STOPPED,
    # `db` 는 함수 인자로 선언한다 (decorator 의 `required_resource_keys` 와 동시 선언 불가).
    # 인자 형태만이 운영 경로와 직접 호출 경로 양쪽에서 같게 주입된다 —
    # `coverage_planner.py` 의 schedule docstring 에 자세한 이유가 있다.
    description=(
        "[Phase F.2 · 기본 STOPPED] 승인된 campaign 의 task 를 tick 당 최대 1건 GenAI "
        "internal endpoint 로 제출. GPU lease/budget/reference 부족 시 deferred."
    ),
)
def synthetic_campaign_dispatch_sensor(context, db: PostgresResource):
    token = _internal_token()
    if not token:
        return SkipReason("GENAI_INTERNAL_TOKEN 미설정 — dispatch 비활성 (.env 확인)")

    try:
        candidates = db.next_dispatchable_tasks(limit=MAX_DISPATCH_PER_TICK)
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"dispatch 후보 조회 실패: {exc}")

    if not candidates:
        return SkipReason("dispatch 대상 task 없음 (승인된 campaign 의 ready/deferred task 0건)")

    task = candidates[0]
    task_id = str(task["task_id"])

    # ── 게이트 1: F.3 mode gate 재확인 ────────────────────────────────────
    # SQL 이 이미 걸렀지만 한 번 더 본다. 스키마 CHECK · SQL WHERE · 이 판정의 세 겹 중
    # 하나가 빠져도 나머지가 막게 하는 의도적 중복이다.
    if not is_dispatch_allowed(str(task.get("policy_mode_at_plan")), str(task.get("campaign_status"))):
        db.defer_task(task_id, _DEFER_NOT_APPROVED)
        return SkipReason(
            f"task={task_id} campaign 미승인/plan_only — deferred "
            f"(mode={task.get('policy_mode_at_plan')} status={task.get('campaign_status')})"
        )

    # ── 게이트 2: reference 확정 + 재확인 ─────────────────────────────────
    # planner 는 `reference_id=NULL` 자리표만 만든다 — 계획 시점에 못 박으면 하루 뒤
    # 유효기간이 지났거나 holdout 으로 재지정된 reference 를 들고 있게 되기 때문이다.
    # 확정은 여기, dispatch 직전에 한다. 선택 술어는 planner 의 reference 집계와 같다.
    if task.get("reference_id") is None:
        try:
            claimed = db.claim_reference_for_task(task_id)
        except Exception as exc:  # noqa: BLE001
            context.log.warning("reference 확정 실패 — 보수적으로 defer: %s", exc)
            db.defer_task(task_id, _DEFER_REFERENCE)
            return SkipReason(f"task={task_id} reference 확정 실패 — deferred: {exc}")
        if claimed is None:
            db.defer_task(task_id, _DEFER_REFERENCE)
            return SkipReason(
                f"task={task_id} 이 셀에 쓸 수 있는 reference 없음 — deferred. "
                "승인·유효기간·holdout·safe region·workflow 허용·축 검증을 모두 통과하고 "
                "아직 같은 (campaign, workflow, seed) 로 쓰이지 않은 후보가 0건이다."
            )
        task = {**task, **claimed, "reference_available": True}

    # 확정돼 있던 task 는 아직 후보인지 다시 본다 — ON DELETE SET NULL 이라 살아 있다는
    # 보장이 없고, 승인 취소·유효기간 만료·holdout 재지정으로 빠졌을 수도 있다(033 조정 5).
    if not task.get("reference_available"):
        db.defer_task(task_id, _DEFER_REFERENCE)
        return SkipReason(
            f"task={task_id} reference 사용 불가 — deferred " f"(reference_id={task.get('reference_id')})."
        )

    # ── 게이트 3: GPU0 lease ──────────────────────────────────────────────
    try:
        lease = lease_from_pg_row(db.get_generation_gpu_lease(GPU0_COMFY), resource=GPU0_COMFY)
    except Exception as exc:  # noqa: BLE001
        context.log.warning("gpu lease 조회 실패 — 보수적으로 defer: %s", exc)
        db.defer_task(task_id, _DEFER_GPU)
        return SkipReason(f"task={task_id} GPU lease 조회 실패 — deferred: {exc}")

    if is_lease_blocking(lease, now_ts=time.time()):
        db.defer_task(task_id, _DEFER_GPU)
        wait_s = seconds_until_free(lease, now_ts=time.time())
        return SkipReason(f"task={task_id} GPU0 사용 중 — deferred ({describe_lease(lease)}, <= {wait_s:.0f}s)")

    # ── 게이트 4: 일일 예산 ───────────────────────────────────────────────
    policy_id = str(task["policy_id"])
    try:
        policy = _policy_row(db, policy_id)
        remaining = db.remaining_daily_job_budget(
            policy_id,
            daily_job_budget=int((policy or {}).get("daily_job_budget") or 0),
        )
    except Exception as exc:  # noqa: BLE001
        db.defer_task(task_id, _DEFER_BUDGET)
        return SkipReason(f"task={task_id} 예산 조회 실패 — deferred: {exc}")

    if remaining <= 0:
        db.defer_task(task_id, _DEFER_BUDGET)
        return SkipReason(f"task={task_id} 일일 예산 소진 — deferred (remaining={remaining})")

    # ── 제출 ──────────────────────────────────────────────────────────────
    payload = {
        "task_id": task_id,
        "campaign_id": str(task["campaign_id"]),
        "policy_id": policy_id,
        "target_id": str(task["target_id"]),
        "dimensions_hash": str(task["dimensions_hash"]),
        "workflow_id": str(task["workflow_id"]),
        "template_id": task.get("template_id"),
        "reference_id": task.get("reference_id"),
        "reference_bucket": task.get("reference_bucket"),
        "reference_key": task.get("reference_key"),
        "seed": int(task.get("seed") or 0),
        "label_policy": "required",
        "source_type": "genai_output",
        "genai_engine": "comfy_local",
    }
    try:
        response = requests.post(
            f"{_GENAI_INTERNAL_BASE}{_DISPATCH_PATH}",
            json=payload,
            headers={"X-Internal-Token": token},
            timeout=_GENAI_INTERNAL_TIMEOUT,
        )
        response.raise_for_status()
        body = response.json()
    except Exception as exc:  # noqa: BLE001
        # endpoint 가 아직 없거나 genai 컨테이너가 죽었을 때. task 를 잃지 않고 defer 한다 —
        # 다음 tick 에서 재평가되며 retry ceiling 은 별도(실패 시에만 증가).
        db.defer_task(task_id, _DEFER_ENDPOINT)
        return SkipReason(
            f"task={task_id} GenAI dispatch endpoint 호출 실패 — deferred: {exc} "
            f"({_GENAI_INTERNAL_BASE}{_DISPATCH_PATH})"
        )

    db.mark_task_dispatched(
        task_id,
        genai_batch_id=body.get("batch_id"),
        genai_job_id=body.get("job_id"),
    )
    db.record_budget_event(
        policy_id=policy_id,
        campaign_id=str(task["campaign_id"]),
        task_id=task_id,
        event_type="consume",
        job_count=1,
        reason="dispatch",
    )
    context.log.info(
        "dispatched task=%s campaign=%s batch=%s job=%s",
        task_id,
        task["campaign_id"],
        body.get("batch_id"),
        body.get("job_id"),
    )
    return SkipReason(f"dispatched=1 task={task_id} batch={body.get('batch_id')} (remaining_budget={remaining - 1})")


def _policy_row(db, policy_id: str) -> dict | None:
    for policy in db.list_plannable_policies():
        if str(policy.get("policy_id")) == policy_id:
            return policy
    return None
