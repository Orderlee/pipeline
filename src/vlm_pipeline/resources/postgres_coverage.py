"""PG coverage control plane — 033 의 정책·snapshot·campaign·task 계층 접근.

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md §5.2 / §6.2 / §6.3.
스키마: `sql/migrations/postgres/033_coverage_control_plane.sql`.
계산: `lib/coverage_planner.py` (순수 함수 — 이 파일은 읽어 오고 써 넣기만 한다).

읽기 원천은 **032 의 두 뷰뿐**이다:
  * `v_eligible_coverage_units`          — finalized image-event 사실 (planner 의 유일한 count source)
  * `v_generation_reference_candidates`  — 승인·비홀드아웃·유효 reference

둘 다 **context 미검증 행을 걸러내지 않고 컬럼으로 노출**한다. 그래서 이 파일은
"검증된 것" 과 "관측 못 한 것" 을 각각 따로 센다 — 뷰가 조용히 걸렀다면 둘 다 0 으로
보였을 것이고, 그게 이 레포가 반복해 당한 "부재에 기댄 안전" 패턴이다.

⚠️ `generation_gpu_leases` 는 여기서 건드리지 않는다. 읽기는 `PostgresGenAIMixin.
   get_generation_gpu_lease`, 판정은 `lib/gpu_lease.py`, acquire/release 는
   `docker/genai/db/pg.py` 전속이다 (역방향 교착 방지 — lib/gpu_lease.py docstring).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2.errors

from vlm_pipeline.lib.coverage_planner import (
    ALLOWED_DIMENSIONS,
    CellObservation,
    PolicyPlanInput,
    validate_target_set,
)

#: 032 의 6 context 축. 'class' 는 축이 아니라 event class 라 따로 다룬다.
_CONTEXT_AXES: frozenset[str] = frozenset(ALLOWED_DIMENSIONS) - {"class"}

#: `schedule_bucket` 과 `v_generation_budget_daily` 의 날짜 기준. 두 곳이 같아야
#: "오늘 예산" 과 "오늘 버킷" 이 어긋나지 않는다.
_KST = timezone(timedelta(hours=9))

#: planner 입력 쿼리의 버전. 집계 방식을 바꾸면 올린다 — 같은 policy/같은 날이라도
#: 쿼리가 달라졌으면 다른 snapshot 이어야 재현성이 성립한다.
COVERAGE_QUERY_VERSION = "2026-09-21.1"


def _validated_axes(axes: list[str] | tuple[str, ...]) -> list[str]:
    """축 이름을 고정 어휘로 제한한다 — 아래에서 SQL 에 문자열로 끼워 넣기 때문이다.

    어휘 밖 이름은 ValueError 로 즉시 죽는다. 033 의 CHECK 도 같은 어휘를 쓰므로 DB 에
    들어온 값은 이미 안전하지만, 이 함수가 **두 번째 자물쇠**다.
    """
    out: list[str] = []
    for axis in axes:
        if axis not in _CONTEXT_AXES:
            raise ValueError(f"알 수 없는 context 축: {axis!r} (허용: {sorted(_CONTEXT_AXES)})")
        out.append(axis)
    return out


def _axes_verified_sql(axes: list[str], alias: str) -> str:
    """선언된 축이 **전부** verified 이고 값이 있는가.

    `context_verified`(= verification_status='verified') 를 함께 요구한다. 032 의
    'inherited' 는 §5.3 의 "reference 에서 상속받았고 아직 scene preservation 검수를 통과하지
    못한" 상태라 coverage 분자로 셀 수 없다.
    """
    if not axes:
        return f"{alias}.context_verified"
    parts = [f"{alias}.context_verified"]
    for axis in axes:
        parts.append(f"('{axis}' = ANY({alias}.context_verified_axes) AND {alias}.{axis} IS NOT NULL)")
    return "(" + " AND ".join(parts) + ")"


def _cell_key(dimensions: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """셀 정체성의 파이썬 표현. 033 의 `dimensions_hash` 와 같은 뜻이며 순서에 독립적이다."""
    return tuple(sorted((str(k), str(v)) for k, v in dimensions.items()))


def _context_key(dimensions: dict[str, Any], axes: list[str]) -> tuple[tuple[str, str], ...]:
    """셀에서 **context 축만** 뽑은 키. reference pool 은 class 를 모르기 때문에 필요하다."""
    return tuple(sorted((axis, str(dimensions[axis])) for axis in axes if axis in dimensions))


def schedule_bucket_for(now: datetime | None = None) -> str:
    """하루 1회 스케줄의 멱등 키 = KST 날짜. 같은 날 두 번 돌아도 같은 버킷이다."""
    moment = now or datetime.now(tz=_KST)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(_KST).date().isoformat()


def coverage_input_hash(policy: dict[str, Any], targets: list[dict[str, Any]]) -> str:
    """snapshot 의 `input_config_hash`. policy 설정 + target 집합 + 쿼리 버전의 함수.

    이 값이 같으면 "같은 입력" 이므로 033 의 UNIQUE 가 재계산을 막는다. 설정이 바뀌면
    해시가 바뀌어 같은 날이라도 새 snapshot 이 생긴다 — 그게 재현 가능성의 의미다.
    """
    payload = {
        "query_version": COVERAGE_QUERY_VERSION,
        "policy": {
            "policy_id": policy.get("policy_id"),
            "mode": policy.get("mode"),
            "balance_dimensions": sorted(policy.get("balance_dimensions") or []),
            "horizon_finalized_total": int(policy.get("horizon_finalized_total") or 0),
            "max_synthetic_share": str(policy.get("max_synthetic_share")),
            "max_per_campaign": int(policy.get("max_per_campaign") or 0),
            "daily_job_budget": int(policy.get("daily_job_budget") or 0),
        },
        "targets": sorted(
            [
                {
                    "dimensions": dict(t.get("dimensions") or {}),
                    "target_share": str(t.get("target_share")),
                    "min_finalized_count": int(t.get("min_finalized_count") or 0),
                }
                for t in targets
            ],
            key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False),
        ),
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class PostgresCoverageMixin:
    """synthetic coverage 제어평면 CRUD + planner 입력 조립.

    이 mixin 은 **계산하지 않는다.** deficit/share-cap 은 `lib/coverage_planner.py` 가 하고
    여기서는 그 함수에 줄 값을 읽어오고, 결과를 그대로 써 넣는다.
    """

    # ─── policy / target 읽기 ──────────────────────────────────────────────

    def list_plannable_policies(self) -> list[dict]:
        """planner 가 볼 policy. Phase F.1 의 "coverage_ready 통과 policy 만" 게이트 포함.

        `mode='disabled'` 와 `coverage_ready=false` 를 SQL 에서 거른다 — 호출자가 WHERE 를
        빠뜨려도 비활성 policy 가 계획되지 않게 한다.
        """
        sql = """
            SELECT policy_id, policy_key, version, status, mode, balance_dimensions,
                   horizon_finalized_total, max_synthetic_share, max_per_campaign,
                   daily_job_budget, daily_gpu_seconds_budget, max_task_retries,
                   default_workflow_id, holdout_scope, coverage_ready
              FROM synthetic_coverage_policies
             WHERE status = 'active'
               AND mode <> 'disabled'
               AND coverage_ready
             ORDER BY policy_key, version DESC
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_policy_targets(self, policy_id: str) -> list[dict]:
        sql = """
            SELECT target_id, dimensions_json, dimensions_hash, target_share,
                   min_finalized_count, max_per_campaign, priority, workflow_id, prompt_template_id
              FROM synthetic_coverage_targets
             WHERE policy_id = %s AND status = 'active'
             ORDER BY priority, target_id
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (policy_id,))
                cols = [c[0] for c in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        for row in rows:
            row["dimensions"] = dict(row.get("dimensions_json") or {})
        return rows

    def activate_policy(self, policy_id: str, *, approved_by: str) -> list[str]:
        """§6.2 의 활성화 검증 후 status='active'. 위반 사유 리스트를 돌려준다(비면 활성화됨).

        "share 합=1, 선언된 dimension 전부 존재, target 간 중복 없음" 중 앞 둘은 행 간/
        테이블 간 조건이라 CHECK 로 못 건다 — **스키마가 아니라 이 함수가 지키는 불변식**
        이다(033 헤더 조정 4). 세 번째(중복)는 스키마도 막는다.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT balance_dimensions, horizon_finalized_total, coverage_ready "
                    "FROM synthetic_coverage_policies WHERE policy_id = %s",
                    (policy_id,),
                )
                row = cur.fetchone()
        if row is None:
            return [f"policy {policy_id} 없음"]

        balance_dimensions, horizon, coverage_ready = row
        targets = self.list_policy_targets(policy_id)
        problems = validate_target_set(tuple(balance_dimensions or ()), targets)
        if int(horizon or 0) <= 0:
            problems.append("horizon_finalized_total 이 0 이다 — active policy 는 nonzero horizon 이 필수")
        if not coverage_ready:
            problems.append("coverage_ready 가 false 다 — Phase F.1 게이트 미통과")
        if problems:
            return problems

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE synthetic_coverage_policies
                       SET status = 'active', approved_by = %s, approved_at = CURRENT_TIMESTAMP,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE policy_id = %s
                    """,
                    (approved_by, policy_id),
                )
        return []

    # ─── 032 뷰 집계 (planner 입력) ────────────────────────────────────────

    def coverage_class_totals(self, context_axes: list[str], classes: list[str]) -> dict[str, dict[str, int]]:
        """class 별 finalized 총계 3분할.

        반환: ``{class: {"total": n, "verified": n, "unverified": n, "missing": n}}``
        세 값의 합은 total 이며, 그 분할이 곧 "0 vs 관측 불가" 의 증거다.

        ``classes`` 가 비면 전 클래스를 센다 (policy 가 class 를 balance dimension 으로
        선언하지 않은 경우 — 이때 호출자는 키 ``'*'`` 로 합산해 쓴다).
        """
        axes = _validated_axes(context_axes)
        verified = _axes_verified_sql(axes, "u")
        where = "TRUE" if not classes else "u.canonical_class = ANY(%s)"
        params: tuple = () if not classes else (list(classes),)
        sql = f"""
            SELECT u.canonical_class,
                   COUNT(*)                                                                AS total,
                   COUNT(*) FILTER (WHERE {verified})                                      AS verified,
                   COUNT(*) FILTER (WHERE NOT ({verified}) AND u.context_fact_id IS NOT NULL) AS unverified,
                   COUNT(*) FILTER (WHERE u.context_fact_id IS NULL)                       AS missing
              FROM v_eligible_coverage_units u
             WHERE {where}
             GROUP BY u.canonical_class
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return {
                    str(r[0]): {
                        "total": int(r[1]),
                        "verified": int(r[2]),
                        "unverified": int(r[3]),
                        "missing": int(r[4]),
                    }
                    for r in cur.fetchall()
                }

    def coverage_cell_counts(self, context_axes: list[str], classes: list[str]) -> dict:
        """셀 × origin_kind 의 finalized 수. **선언 축이 전부 verified 인 행만** 센다.

        반환 키는 ``(('class','falldown'), ('environment_type','outdoor'), ...)`` 형태의
        정렬 튜플이며 `lib.coverage_planner` 의 셀 키와 같은 모양이다.
        """
        axes = _validated_axes(context_axes)
        verified = _axes_verified_sql(axes, "u")
        axis_cols = "".join(f"u.{axis}, " for axis in axes)
        group_cols = ", ".join(["u.canonical_class", *[f"u.{axis}" for axis in axes], "u.origin_kind"])
        where = "TRUE" if not classes else "u.canonical_class = ANY(%s)"
        params: tuple = () if not classes else (list(classes),)
        sql = f"""
            SELECT u.canonical_class, {axis_cols}u.origin_kind, COUNT(*) AS n
              FROM v_eligible_coverage_units u
             WHERE {where} AND {verified}
             GROUP BY {group_cols}
        """
        out: dict = {}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    canonical_class = str(row[0])
                    axis_values = row[1 : 1 + len(axes)]
                    origin = str(row[-2])
                    n = int(row[-1])
                    dims = {"class": canonical_class}
                    dims.update({axis: str(value) for axis, value in zip(axes, axis_values)})
                    out.setdefault(_cell_key(dims), {"real": 0, "synthetic": 0})
                    bucket = "synthetic" if origin == "synthetic" else "real"
                    out[_cell_key(dims)][bucket] += n
        return out

    def coverage_reference_counts(self, context_axes: list[str], *, workflow_id: str | None = None) -> dict:
        """context 별 사용 가능 reference 수. reference 는 class 를 모른다(배경이므로).

        `v_generation_reference_candidates` 가 이미 승인·비홀드아웃·유효기간·safe region 을
        걸러 둔 상태이며, 여기서는 **context 검증**과 workflow 허용만 추가로 본다.
        """
        axes = _validated_axes(context_axes)
        verified = _axes_verified_sql(axes, "r")
        axis_cols = "".join(f"r.{axis}, " for axis in axes)
        group_cols = ", ".join(f"r.{axis}" for axis in axes) or "1"
        wf_clause = "AND %s = ANY(r.allowed_workflows)" if workflow_id else ""
        params: tuple = (workflow_id,) if workflow_id else ()
        sql = f"""
            SELECT {axis_cols}COUNT(*) AS n
              FROM v_generation_reference_candidates r
             WHERE {verified} {wf_clause}
             GROUP BY {group_cols}
        """
        out: dict = {}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    axis_values = row[: len(axes)]
                    n = int(row[-1])
                    key = tuple(sorted((axis, str(value)) for axis, value in zip(axes, axis_values)))
                    out[key] = out.get(key, 0) + n
        return out

    def coverage_reference_pool_total(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM v_generation_reference_candidates")
                return int(cur.fetchone()[0])

    def coverage_reservations(self, policy_id: str) -> dict[str, int]:
        """`dimensions_hash` → 살아 있는 예약 수 (§5.2 의 P)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT dimensions_hash, reserved_count FROM v_synthetic_coverage_reservations WHERE policy_id = %s",
                    (policy_id,),
                )
                return {str(r[0]): int(r[1]) for r in cur.fetchall()}

    def remaining_daily_job_budget(self, policy_id: str, *, daily_job_budget: int, bucket: str | None = None) -> int:
        """``max(0, daily_job_budget − committed_jobs(오늘 KST))``."""
        day = bucket or schedule_bucket_for()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT committed_jobs FROM v_generation_budget_daily WHERE policy_id = %s AND budget_date = %s",
                    (policy_id, day),
                )
                row = cur.fetchone()
        committed = int(row[0]) if row and row[0] is not None else 0
        return max(0, int(daily_job_budget) - committed)

    # ─── planner 입력 조립 ────────────────────────────────────────────────

    def build_policy_plan_input(self, policy: dict, targets: list[dict]) -> PolicyPlanInput:
        """032 의 두 뷰 + 033 의 예약/예산을 읽어 `PolicyPlanInput` 하나로 조립한다.

        ⚠️ target 이 0개여도 policy 단위 4개 카운터는 채운다 — 그래야 cell 행이 없는
           snapshot 에서도 "잴 대상이 없었다"(no_targets) 와 "사실이 하나도 없었다"
           (no_finalized_facts) 가 구분된다.
        """
        declared = list(policy.get("balance_dimensions") or [])
        context_axes = _validated_axes([d for d in declared if d != "class"])
        has_class = "class" in declared

        classes = sorted({str(t["dimensions"]["class"]) for t in targets if "class" in t.get("dimensions", {})})
        class_totals = self.coverage_class_totals(context_axes, classes if has_class else [])
        cell_counts = self.coverage_cell_counts(context_axes, classes if has_class else [])
        reservations = self.coverage_reservations(str(policy["policy_id"]))
        remaining_budget = self.remaining_daily_job_budget(
            str(policy["policy_id"]),
            daily_job_budget=int(policy.get("daily_job_budget") or 0),
        )

        # class 가 balance dimension 이 아니면 전 클래스를 한 scope 로 합산한다.
        if not has_class:
            merged = {"total": 0, "verified": 0, "unverified": 0, "missing": 0}
            for stats in class_totals.values():
                for key in merged:
                    merged[key] += stats[key]
            class_totals = {"*": merged}

        reference_cache: dict[str | None, dict] = {}
        cells: list[CellObservation] = []
        for target in targets:
            dims = dict(target.get("dimensions") or {})
            scope = str(dims.get("class")) if has_class else "*"
            totals = class_totals.get(scope, {"total": 0, "verified": 0, "unverified": 0, "missing": 0})

            counts = cell_counts.get(_cell_key(dims), {"real": 0, "synthetic": 0})

            workflow = target.get("workflow_id") or policy.get("default_workflow_id")
            if workflow not in reference_cache:
                reference_cache[workflow] = self.coverage_reference_counts(context_axes, workflow_id=workflow)
            references = reference_cache[workflow].get(_context_key(dims, context_axes), 0)

            cells.append(
                CellObservation(
                    target_id=str(target["target_id"]),
                    dimensions=dims,
                    target_share=target.get("target_share", 0),
                    min_finalized_count=int(target.get("min_finalized_count") or 0),
                    real_finalized_count=counts["real"],
                    synthetic_accepted_count=counts["synthetic"],
                    pending_reserved_count=int(reservations.get(str(target.get("dimensions_hash")), 0)),
                    class_finalized_total=totals["total"],
                    class_context_verified_total=totals["verified"],
                    class_context_unverified_total=totals["unverified"],
                    class_context_missing_total=totals["missing"],
                    reference_available_count=references,
                    per_target_cap=(
                        None if target.get("max_per_campaign") is None else int(target["max_per_campaign"])
                    ),
                    priority=int(target.get("priority") or 100),
                )
            )

        policy_totals = {"total": 0, "verified": 0, "unverified": 0, "missing": 0}
        for stats in class_totals.values():
            for key in policy_totals:
                policy_totals[key] += stats[key]

        return PolicyPlanInput(
            policy_id=str(policy["policy_id"]),
            mode=str(policy.get("mode") or "plan_only"),
            balance_dimensions=tuple(declared),
            horizon_finalized_total=int(policy.get("horizon_finalized_total") or 0),
            max_synthetic_share=policy.get("max_synthetic_share", 0),
            max_per_campaign=int(policy.get("max_per_campaign") or 0),
            remaining_daily_budget=remaining_budget,
            cells=tuple(cells),
            eligible_units_total=policy_totals["total"],
            context_verified_units_total=policy_totals["verified"],
            context_unverified_units_total=policy_totals["unverified"],
            context_missing_units_total=policy_totals["missing"],
            reference_pool_total=self.coverage_reference_pool_total(),
        )

    # ─── snapshot / campaign / task 쓰기 ──────────────────────────────────

    def upsert_coverage_snapshot(
        self,
        *,
        policy_id: str,
        schedule_bucket: str,
        input_config_hash: str,
        balance_dimensions: list[str],
        horizon_finalized_total: int,
        max_synthetic_share: Any,
        plan: Any,
    ) -> tuple[str, bool]:
        """snapshot 1행 + cell 행들. 반환 ``(snapshot_id, created)``.

        ``created=False`` 면 같은 (policy, bucket, input_hash) snapshot 이 이미 있다는 뜻이며
        **덮어쓰지 않는다** — snapshot 은 immutable 이라는 §6.2 의 요구다. 같은 날 두 번
        돌아도 결과가 같다는 멱등성이 여기서 나온다.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO coverage_snapshots (
                        policy_id, schedule_bucket, input_config_hash, status,
                        balance_dimensions, horizon_finalized_total, max_synthetic_share,
                        eligible_units_total, context_verified_units_total,
                        context_unverified_units_total, context_missing_units_total,
                        reference_pool_total, blocked_reason
                    ) VALUES (%s, %s, %s, 'complete', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (policy_id, schedule_bucket, input_config_hash) DO NOTHING
                    RETURNING snapshot_id
                    """,
                    (
                        policy_id,
                        schedule_bucket,
                        input_config_hash,
                        list(balance_dimensions),
                        int(horizon_finalized_total),
                        max_synthetic_share,
                        plan.eligible_units_total,
                        plan.context_verified_units_total,
                        plan.context_unverified_units_total,
                        plan.context_missing_units_total,
                        plan.reference_pool_total,
                        plan.blocked_reason,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        """
                        SELECT snapshot_id FROM coverage_snapshots
                         WHERE policy_id = %s AND schedule_bucket = %s AND input_config_hash = %s
                        """,
                        (policy_id, schedule_bucket, input_config_hash),
                    )
                    return (str(cur.fetchone()[0]), False)

                snapshot_id = str(row[0])
                for cell in plan.cells:
                    cur.execute(
                        """
                        INSERT INTO coverage_snapshot_cells (
                            snapshot_id, target_id, dimensions_json, target_share, min_finalized_count,
                            desired_count, eligible_finalized_count, deficit_count, planned_count,
                            real_finalized_count, synthetic_accepted_count, pending_reserved_count,
                            class_finalized_total, class_context_verified_total,
                            class_context_unverified_total, class_context_missing_total,
                            reference_available_count, share_headroom_count,
                            budget_headroom_count, campaign_cap_count, block_reason
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (snapshot_id, target_id) DO NOTHING
                        """,
                        (
                            snapshot_id,
                            cell.target_id,
                            json.dumps(cell.dimensions, sort_keys=True, ensure_ascii=False),
                            float(cell.target_share),
                            cell.min_finalized_count,
                            cell.desired_count,
                            cell.eligible_finalized_count,
                            cell.deficit_count,
                            cell.planned_count,
                            cell.real_finalized_count,
                            cell.synthetic_accepted_count,
                            cell.pending_reserved_count,
                            cell.class_finalized_total,
                            cell.class_context_verified_total,
                            cell.class_context_unverified_total,
                            cell.class_context_missing_total,
                            cell.reference_available_count,
                            cell.share_headroom_count,
                            cell.budget_headroom_count,
                            cell.campaign_cap_count,
                            cell.block_reason,
                        ),
                    )
                return (snapshot_id, True)

    def insert_campaign(
        self,
        *,
        policy_id: str,
        snapshot_id: str,
        schedule_bucket: str,
        snapshot_input_hash: str,
        status: str,
        policy_mode_at_plan: str,
        planned_task_count: int,
        blocked_reason: str | None = None,
        approved_by: str | None = None,
    ) -> tuple[str | None, bool]:
        """campaign 1행. 반환 ``(campaign_id, created)``.

        idempotency_key 충돌 시 기존 campaign 을 돌려준다 — 같은 bucket 을 두 번 계획해도
        campaign 이 두 개 생기지 않는다(§6.2 UNIQUE + 설계서 "idempotent schedule bucket").
        """
        approved_at = "CURRENT_TIMESTAMP" if approved_by else "NULL"
        sql = f"""
            INSERT INTO synthetic_generation_campaigns (
                policy_id, snapshot_id, schedule_bucket, snapshot_input_hash,
                status, policy_mode_at_plan, planned_task_count, blocked_reason,
                approved_by, approved_at, scheduled_for
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, {approved_at}, CURRENT_TIMESTAMP)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING campaign_id
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        policy_id,
                        snapshot_id,
                        schedule_bucket,
                        snapshot_input_hash,
                        status,
                        policy_mode_at_plan,
                        int(planned_task_count),
                        blocked_reason,
                        approved_by,
                    ),
                )
                row = cur.fetchone()
                if row is not None:
                    return (str(row[0]), True)
                cur.execute(
                    """
                    SELECT campaign_id FROM synthetic_generation_campaigns
                     WHERE idempotency_key = %s
                    """,
                    (f"{policy_id}|{schedule_bucket}|{snapshot_input_hash}",),
                )
                existing = cur.fetchone()
                return (str(existing[0]) if existing else None, False)

    def insert_generation_task(self, task: dict) -> str | None:
        """task 1행. `(campaign, reference, workflow, seed)` 중복이면 None."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO synthetic_generation_tasks (
                        campaign_id, target_id, dimensions_hash, reference_id,
                        reference_bucket, reference_key, workflow_id, template_id,
                        seed, state, priority, max_retries
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id, reference_id, workflow_id, seed) DO NOTHING
                    RETURNING task_id
                    """,
                    (
                        task["campaign_id"],
                        task["target_id"],
                        task["dimensions_hash"],
                        task.get("reference_id"),
                        task.get("reference_bucket"),
                        task.get("reference_key"),
                        task["workflow_id"],
                        task.get("template_id"),
                        int(task.get("seed", 0)),
                        task.get("state", "planned"),
                        int(task.get("priority", 100)),
                        int(task.get("max_retries", 2)),
                    ),
                )
                row = cur.fetchone()
                return str(row[0]) if row else None

    def record_budget_event(
        self,
        *,
        policy_id: str,
        event_type: str,
        job_count: int = 0,
        gpu_seconds: float = 0.0,
        campaign_id: str | None = None,
        task_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        """예산 원장 append. 부호는 `event_type` 이 지고 금액은 항상 >= 0 이다."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO generation_budget_events
                           (policy_id, campaign_id, task_id, event_type, job_count, gpu_seconds, reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        policy_id,
                        campaign_id,
                        task_id,
                        event_type,
                        max(0, int(job_count)),
                        max(0.0, float(gpu_seconds)),
                        reason,
                    ),
                )

    # ─── dispatch sensor 용 ────────────────────────────────────────────────

    def next_dispatchable_tasks(self, limit: int = 1) -> list[dict]:
        """dispatch 후보. **F.3 게이트를 SQL 에서 건다.**

        `policy_mode_at_plan NOT IN ('disabled','plan_only')` 와
        `campaign.status IN ('approved','dispatching')` 를 여기서 거르므로, 센서가 판정을
        빠뜨려도 plan_only campaign 의 task 는 애초에 후보로 나오지 않는다. 033 의 CHECK 와
        `lib.coverage_planner.is_dispatch_allowed` 와 합쳐 **세 겹**이다.

        reference 는 `v_generation_reference_candidates` 로 **다시 확인**한다 — task 행의
        reference_id 는 ON DELETE SET NULL 이라 살아 있다는 보장이 없다(033 헤더 조정 5).
        """
        sql = """
            SELECT t.task_id, t.campaign_id, t.target_id, t.dimensions_hash,
                   t.reference_id, t.reference_bucket, t.reference_key,
                   t.workflow_id, t.template_id, t.seed, t.state,
                   t.retry_count, t.max_retries, t.priority,
                   c.policy_id, c.status AS campaign_status, c.policy_mode_at_plan,
                   (r.reference_id IS NOT NULL) AS reference_available
              FROM synthetic_generation_tasks t
              JOIN synthetic_generation_campaigns c ON c.campaign_id = t.campaign_id
              LEFT JOIN v_generation_reference_candidates r ON r.reference_id = t.reference_id
             WHERE t.state IN ('ready', 'deferred')
               AND c.status IN ('approved', 'dispatching')
               AND c.policy_mode_at_plan NOT IN ('disabled', 'plan_only')
               AND t.retry_count <= t.max_retries
             ORDER BY t.priority, t.created_at
             LIMIT %s
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (max(1, int(limit)),))
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    def claim_reference_for_task(self, task_id: str) -> dict | None:
        """task 의 셀 차원에 맞는 reference 를 하나 골라 task 에 기록한다 (F.2 — 자리표 확정).

        planner 는 reference 를 고르지 않고 `reference_id=NULL` 자리표만 만든다. 계획 시점에
        못 박으면 하루 뒤 유효기간이 지났거나 holdout 으로 재지정된 reference 를 들고 있게
        되기 때문이다. 그래서 dispatch 직전인 여기서 고른다.

        ⚠️ 선택 술어는 planner 의 `coverage_reference_counts` 와 **같아야 한다.** 다르면
        planner 가 N 건을 계획했는데 dispatcher 가 0 건을 찾는 조용한 불일치가 난다 —
        둘 다 `v_generation_reference_candidates` + `_axes_verified_sql` + workflow 허용을
        쓴다. 축은 셀의 `dimensions_json` 키에서 뽑고 `_validated_axes` 로 한 번 더 조인다.

        `jsonb_build_object(...) @> dimensions_json` 이 차원 일치다. 셀이 선언하지 않은 축은
        비교하지 않고, 셀이 선언한 축의 값이 NULL 인 reference 는 containment 가 거짓이라
        자동으로 빠진다.

        반환 None 의 뜻은 **"지금 이 셀에 쓸 수 있는 reference 가 없다"** 이며, 호출자는
        `reference_unavailable` 로 defer 한다. 이미 reference 가 박힌 task 는 그대로 돌려준다.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT t.reference_id, t.reference_bucket, t.reference_key,
                           t.campaign_id, t.workflow_id, t.seed, tg.dimensions_json
                      FROM synthetic_generation_tasks t
                      JOIN synthetic_coverage_targets tg ON tg.target_id = t.target_id
                     WHERE t.task_id = %s
                    """,
                    (task_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                reference_id, bucket, key, campaign_id, workflow_id, seed, dimensions = row
                if reference_id is not None:
                    # 이미 확정된 task — 유효성은 호출자가 후보 뷰로 따로 본다.
                    return {"reference_id": reference_id, "reference_bucket": bucket, "reference_key": key}

                axes = _validated_axes(sorted(dict(dimensions or {}).keys()))
                verified = _axes_verified_sql(axes, "r")
                built = ", ".join(f"'{axis}', r.{axis}" for axis in axes) or "'_', NULL"
                cur.execute(
                    f"""
                    SELECT r.reference_id, r.reference_bucket, r.reference_key
                      FROM v_generation_reference_candidates r
                     WHERE {verified}
                       AND %(workflow)s = ANY(r.allowed_workflows)
                       AND jsonb_build_object({built}) @> %(dimensions)s::jsonb
                       AND NOT EXISTS (
                           SELECT 1
                             FROM synthetic_generation_tasks x
                            WHERE x.campaign_id = %(campaign)s
                              AND x.reference_id = r.reference_id
                              AND x.workflow_id = %(workflow)s
                              AND x.seed = %(seed)s
                       )
                     ORDER BY r.use_count, r.last_used_at NULLS FIRST, r.reference_id
                     LIMIT 1
                    """,
                    {
                        "workflow": workflow_id,
                        "dimensions": json.dumps(dimensions, sort_keys=True, ensure_ascii=False),
                        "campaign": campaign_id,
                        "seed": seed,
                    },
                )
                picked = cur.fetchone()
                if picked is None:
                    return None
                chosen_id, chosen_bucket, chosen_key = picked

                # `WHERE reference_id IS NULL` 이 동시 tick 의 이중 확정을 막는다. UNIQUE
                # (campaign, reference, workflow, seed) 위반은 위 NOT EXISTS 와 tick 당 1건
                # 처리로 사실상 안 나지만, 나면 고르지 못한 것과 같게 취급한다 — 조용히
                # 다른 값을 쓰는 것보다 defer 가 낫다.
                try:
                    cur.execute(
                        """
                        UPDATE synthetic_generation_tasks
                           SET reference_id = %s, reference_bucket = %s, reference_key = %s,
                               updated_at = CURRENT_TIMESTAMP
                         WHERE task_id = %s AND reference_id IS NULL
                        """,
                        (chosen_id, chosen_bucket, chosen_key, task_id),
                    )
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    return None
                if cur.rowcount == 0:
                    return None

                # 공평 분배용 카운터. 뷰 주석이 "dispatcher 가 갱신" 이라고 한 그 자리다.
                # ponytail: dispatch 성공이 아니라 확정 시점에 올린다 — 이 값은 회계가 아니라
                # ORDER BY 의 라운드로빈 재료라, 실패한 시도도 '썼다'로 세는 편이 편향이 적다.
                cur.execute(
                    """
                    UPDATE generation_reference_pool
                       SET use_count = use_count + 1, last_used_at = CURRENT_TIMESTAMP
                     WHERE reference_id = %s
                    """,
                    (chosen_id,),
                )
                return {
                    "reference_id": chosen_id,
                    "reference_bucket": chosen_bucket,
                    "reference_key": chosen_key,
                }

    def defer_task(self, task_id: str, reason: str) -> None:
        """task 를 `deferred` 로. 사유는 033 의 CHECK 가 요구하므로 빈 값이면 INSERT 가 막힌다."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE synthetic_generation_tasks
                       SET state = 'deferred', deferred_reason = %s, updated_at = CURRENT_TIMESTAMP
                     WHERE task_id = %s
                    """,
                    (reason, task_id),
                )

    def mark_task_dispatched(
        self,
        task_id: str,
        *,
        genai_batch_id: str | None = None,
        genai_job_id: str | None = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE synthetic_generation_tasks
                       SET state = 'dispatched', dispatched_at = CURRENT_TIMESTAMP,
                           deferred_reason = NULL, genai_batch_id = %s, genai_job_id = %s,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE task_id = %s
                    """,
                    (genai_batch_id, genai_job_id, task_id),
                )

    def mark_task_failed(self, task_id: str, *, error_message: str, retryable: bool) -> None:
        """재시도 가능하면 retry_count 증가 + `ready` 로 복귀, 아니면 `failed`.

        retry ceiling 은 `next_dispatchable_tasks` 의 `retry_count <= max_retries` 가 건다 —
        올라가다 한도를 넘으면 후보에서 자연히 빠진다.
        """
        if retryable:
            sql = """
                UPDATE synthetic_generation_tasks
                   SET retry_count = retry_count + 1,
                       state = CASE WHEN retry_count + 1 > max_retries THEN 'failed' ELSE 'ready' END,
                       deferred_reason = NULL, error_message = %s, updated_at = CURRENT_TIMESTAMP
                 WHERE task_id = %s
            """
        else:
            sql = """
                UPDATE synthetic_generation_tasks
                   SET state = 'failed', deferred_reason = NULL, error_message = %s,
                       closed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                 WHERE task_id = %s
            """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (error_message, task_id))
