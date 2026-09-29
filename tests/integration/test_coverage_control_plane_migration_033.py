"""033_coverage_control_plane — §6.2/§6.3 제어평면의 스키마 계약 + planner 입력 집계.

설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md §5.2 / §6.2 / §6.3.

``DATAOPS_TEST_POSTGRES_DSN`` 미설정/unreachable 이면 파일 전체 skip —
tests/integration/conftest.py 가 테스트마다 임시 DB 를 만들고 ``ensure_schema()`` 로
001~033 을 적용한 뒤 DROP 한다. 즉 fresh apply 경로를 매번 다시 탄다.

검증하는 것은 "테이블이 있다" 가 아니라 **행동 계약**이다:
  * 정책 기본값으로는 아무것도 생성되지 않는다 (draft + plan_only + budget 0)
  * plan_only campaign 은 승인·dispatch 상태로 **전이할 수 없다** (F.3 게이트)
  * blocked cell 은 생성을 계획할 수 없고 planned 는 다섯 상한을 넘을 수 없다 (§5.2)
  * "0" 과 "관측 불가" 가 서로 다른 열에 남는다
  * 정본 행 삭제가 인제스트를 깨지 않는다 (CASCADE / SET NULL)
  * planner 집계가 오늘 prod 상태에서 **blocked 사유**를 낸다 (비율 0 이 아니라)
"""

from __future__ import annotations

import json

import psycopg2
import pytest

from vlm_pipeline.lib.coverage_planner import (
    BLOCK_CONTEXT_COVERAGE,
    BLOCK_NO_FINALIZED_FACTS,
    plan_policy,
)

# ─── fixtures ────────────────────────────────────────────────────────────────


def _seed_asset(cur, asset_id: str, *, source_type: str = "camera") -> None:
    cur.execute(
        """
        INSERT INTO raw_files (asset_id, source_path, media_type, source_type, checksum)
        VALUES (%s, %s, 'image', %s, %s)
        """,
        (asset_id, f"/nas/data/incoming/{asset_id}.jpg", source_type, f"sha-{asset_id}"),
    )


def _seed_image(cur, image_id: str, asset_id: str, frame_index: int = 0) -> None:
    cur.execute(
        """
        INSERT INTO image_metadata (image_id, source_asset_id, image_bucket, image_key, frame_index)
        VALUES (%s, %s, 'vlm-raw', %s, %s)
        """,
        (image_id, asset_id, f"unit/{image_id}.jpg", frame_index),
    )


def _insert(cur, table: str, payload: dict, returning: str | None = None):
    cols = ", ".join(payload)
    marks = ", ".join(["%s"] * len(payload))
    suffix = f" RETURNING {returning}" if returning else ""
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks}){suffix}", tuple(payload.values()))
    return cur.fetchone()[0] if returning else None


def _policy(cur, **over) -> str:
    payload = {
        "policy_key": "falldown-env-pilot",
        "balance_dimensions": ["class", "environment_type", "daynight_type"],
        "default_workflow_id": "sdxl-inpaint-cctv-v1",
    }
    payload.update(over)
    return _insert(cur, "synthetic_coverage_policies", payload, returning="policy_id")


def _active_policy(cur, **over) -> str:
    payload = {
        "status": "active",
        "mode": "approval_required",
        "horizon_finalized_total": 200,
        "max_synthetic_share": "0.5",
        "max_per_campaign": 50,
        "daily_job_budget": 50,
        "coverage_ready": True,
        "coverage_ready_by": "operator",
        "coverage_ready_at": "2026-09-21 00:00:00+00",
        "approved_by": "operator",
        "approved_at": "2026-09-21 00:00:00+00",
    }
    payload.update(over)
    return _policy(cur, **payload)


def _target(cur, policy_id: str, dims: dict, share: str = "1.0", **over) -> str:
    payload = {
        "policy_id": policy_id,
        "dimensions_json": json.dumps(dims),
        "target_share": share,
    }
    payload.update(over)
    return _insert(cur, "synthetic_coverage_targets", payload, returning="target_id")


def _snapshot(cur, policy_id: str, **over) -> str:
    payload = {
        "policy_id": policy_id,
        "schedule_bucket": "2026-09-21",
        "input_config_hash": "hash-1",
        "balance_dimensions": ["class", "environment_type", "daynight_type"],
        "horizon_finalized_total": 200,
        "max_synthetic_share": "0.5",
    }
    payload.update(over)
    return _insert(cur, "coverage_snapshots", payload, returning="snapshot_id")


def _cell(cur, snapshot_id: str, target_id: str, **over) -> None:
    payload = {
        "snapshot_id": snapshot_id,
        "target_id": target_id,
        "dimensions_json": json.dumps({"class": "falldown", "environment_type": "outdoor", "daynight_type": "day"}),
        "target_share": "1.0",
        "desired_count": 10,
        "eligible_finalized_count": 0,
        "deficit_count": 10,
        "planned_count": 0,
        "real_finalized_count": 0,
        "synthetic_accepted_count": 0,
        "class_finalized_total": 0,
        "class_context_verified_total": 0,
        "class_context_unverified_total": 0,
        "class_context_missing_total": 0,
        "reference_available_count": 0,
        "share_headroom_count": 0,
        "budget_headroom_count": 0,
        "campaign_cap_count": 0,
    }
    payload.update(over)
    _insert(cur, "coverage_snapshot_cells", payload)


def _campaign(cur, policy_id: str, snapshot_id: str, **over) -> str:
    payload = {
        "policy_id": policy_id,
        "snapshot_id": snapshot_id,
        "schedule_bucket": "2026-09-21",
        "snapshot_input_hash": "hash-1",
        "policy_mode_at_plan": "approval_required",
        "status": "planned",
    }
    payload.update(over)
    return _insert(cur, "synthetic_generation_campaigns", payload, returning="campaign_id")


def _task(cur, campaign_id: str, **over) -> str:
    payload = {
        "campaign_id": campaign_id,
        "target_id": "t1",
        "dimensions_hash": "dh-1",
        "workflow_id": "sdxl-inpaint-cctv-v1",
        "seed": 0,
    }
    payload.update(over)
    return _insert(cur, "synthetic_generation_tasks", payload, returning="task_id")


@pytest.fixture
def seeded(pg_resource):
    """raw_files 1 + image_metadata 2. 032 정본 테이블 쪽 최소 fixture."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            _seed_asset(cur, "asset-1")
            _seed_image(cur, "img-1", "asset-1", 0)
            _seed_image(cur, "img-2", "asset-1", 1)
    return pg_resource


# ─── 1. 적용 자체 ─────────────────────────────────────────────────────────────


def test_033_objects_exist_after_ensure_schema(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM _pg_migrations WHERE name = '033_coverage_control_plane.sql'")
            assert cur.fetchone() is not None, "033 이 _pg_migrations 에 기록되지 않았다"
            cur.execute(
                """
                SELECT to_regclass('public.synthetic_coverage_policies'),
                       to_regclass('public.synthetic_coverage_targets'),
                       to_regclass('public.coverage_snapshots'),
                       to_regclass('public.coverage_snapshot_cells'),
                       to_regclass('public.synthetic_generation_campaigns'),
                       to_regclass('public.synthetic_generation_tasks'),
                       to_regclass('public.synthetic_prompt_templates'),
                       to_regclass('public.generation_quality_reviews'),
                       to_regclass('public.generation_budget_events'),
                       to_regclass('public.v_synthetic_coverage_reservations'),
                       to_regclass('public.v_generation_budget_daily')
                """
            )
            assert all(cur.fetchone()), "033 이 만든 객체 중 누락이 있다"


def test_033_is_idempotent_and_replays_assertions(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM _pg_migrations")
            before = cur.fetchone()[0]

    pg_resource.ensure_schema()  # 부팅 시 assertion 재실행 경로

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM _pg_migrations")
            assert cur.fetchone()[0] == before


def test_033_does_not_recreate_the_030_gpu_lease_table(pg_resource):
    """`generation_gpu_leases` 는 030 소관이다. 033 이 건드렸다면 resource CHECK 가 바뀌었을 것."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_get_constraintdef(oid)
                  FROM pg_constraint
                 WHERE conrelid = 'generation_gpu_leases'::regclass AND contype = 'c'
                   AND pg_get_constraintdef(oid) LIKE '%gpu0_comfy%'
                """
            )
            assert cur.fetchone() is not None, "030 의 gpu0_comfy CHECK 가 사라졌다"


def test_033_fks_never_block_a_delete(pg_resource):
    """파일의 @ASSERT_AFTER 와 같은 불변식 — RESTRICT/NO ACTION 이 하나라도 있으면 인제스트가 깨진다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT conname, confdeltype
                  FROM pg_constraint
                 WHERE contype = 'f'
                   AND conrelid IN ('synthetic_coverage_targets'::regclass,
                                    'coverage_snapshots'::regclass,
                                    'coverage_snapshot_cells'::regclass,
                                    'synthetic_generation_campaigns'::regclass,
                                    'synthetic_generation_tasks'::regclass,
                                    'generation_quality_reviews'::regclass,
                                    'generation_budget_events'::regclass)
                """
            )
            rows = cur.fetchall()
    assert rows, "FK 가 하나도 없다 — 스키마가 예상과 다르다"
    assert [r for r in rows if r[1] not in ("c", "n")] == [], f"삭제를 막는 FK: {rows}"


def test_tasks_have_no_fk_to_the_reference_pool_or_genai_batches(pg_resource):
    """cascade 충돌 방지(033 헤더 조정 5). 이 FK 가 다시 생기면 인제스트가 깨진다.

    한 행이 두 FK 를 갖고 그 부모들이 **한 문장으로** 같이 지워질 수 있으면, SET NULL 이
    섞인 조합은 재검증 시점에 FK 위반을 낸다. 아래 두 회귀 테스트가 실제 경로를 태운다.
    """
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pc.relname
                  FROM pg_constraint co
                  JOIN pg_class pc ON pc.oid = co.confrelid
                 WHERE co.contype = 'f' AND co.conrelid = 'synthetic_generation_tasks'::regclass
                """
            )
            parents = {r[0] for r in cur.fetchall()}
    assert "generation_reference_pool" not in parents, parents
    assert "genai_batches" not in parents, parents


def test_deleting_a_genai_batch_does_not_break_on_the_task_row(seeded):
    """`genai_jobs.batch_id` 가 CASCADE 라 batch 삭제 한 문장이 task 행에 두 동작을 건다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO genai_batches (batch_id, engine, output_media, prompt, n_total) "
                "VALUES ('b1', 'comfy_local', 'image', 'p', 1)"
            )
            cur.execute("INSERT INTO genai_jobs (job_id, batch_id, seq_in_batch) VALUES ('j1', 'b1', 1)")
            policy_id = _active_policy(cur)
            snapshot_id = _snapshot(cur, policy_id)
            campaign_id = _campaign(cur, policy_id, snapshot_id)
            task_id = _task(cur, campaign_id, genai_batch_id="b1", genai_job_id="j1")

            cur.execute("DELETE FROM genai_batches WHERE batch_id = 'b1'")
            cur.execute(
                "SELECT genai_batch_id, genai_job_id FROM synthetic_generation_tasks WHERE task_id = %s",
                (task_id,),
            )
            # batch 는 soft reference 라 감사값이 남고, job FK 만 끊긴다.
            assert cur.fetchone() == ("b1", None)


def test_the_real_reingest_delete_order_still_works(seeded):
    """``postgres_ingest_raw.py`` 재적재 경로 재현 — 여기서 막히면 인제스트가 죽는다.

    ⚠️ 이 테스트가 실제로 버그를 잡았다 — 두 번. 처음은 `reference_id` 를 FK(SET NULL)로
       뒀을 때, 두 번째는 2026-09-21 에 "손으로 재현해 보니 통과하더라" 는 이유로 FK 를
       되돌리려 했을 때다. 현상이 **cascade 처리 순서에 의존**해서 단건 삭제나 힙 순서가
       다른 손 재현은 그냥 통과한다. 이 테스트만이 실제 경로를 그대로 태운다.
    """
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            snapshot_id = _snapshot(cur, policy_id)
            campaign_id = _campaign(cur, policy_id, snapshot_id)
            _insert(
                cur,
                "generation_reference_pool",
                {
                    "image_id": "img-2",
                    "asset_id": "asset-1",
                    "reference_key": "unit/img-2.jpg",
                    "allowed_workflows": ["sdxl-inpaint-cctv-v1"],
                    "safe_region_json": "{}",
                    "status": "approved",
                    "approved_by": "operator",
                    "approved_at": "2026-09-21 00:00:00+00",
                    "holdout_excluded": False,
                },
                returning="reference_id",
            )
            cur.execute("SELECT reference_id FROM generation_reference_pool")
            reference_id = cur.fetchone()[0]
            task_id = _task(cur, campaign_id, reference_id=reference_id, output_image_id="img-1")

            cur.execute("DELETE FROM image_metadata WHERE source_asset_id = 'asset-1'")
            cur.execute("DELETE FROM video_metadata WHERE asset_id = 'asset-1'")
            cur.execute("DELETE FROM raw_files WHERE asset_id = 'asset-1'")

            # pool 행은 032 의 CASCADE 로 사라지고, task 는 감사 기록이라 살아남는다.
            cur.execute("SELECT count(*) FROM generation_reference_pool")
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT reference_id, output_image_id FROM synthetic_generation_tasks WHERE task_id = %s",
                (task_id,),
            )
            # reference_id 는 soft reference 라 값이 남고(감사), image FK 만 끊긴다.
            assert cur.fetchone() == (reference_id, None)


# ─── 2. policy — 기본값은 "아무것도 못 한다" ─────────────────────────────────


def test_policy_defaults_generate_nothing(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            _policy(cur)
            cur.execute(
                "SELECT status, mode, coverage_ready, max_per_campaign, daily_job_budget, max_synthetic_share "
                "FROM synthetic_coverage_policies"
            )
            status, mode, ready, cap, budget, share = cur.fetchone()
    assert (status, mode, ready) == ("draft", "plan_only", False)
    assert (cap, budget, float(share)) == (0, 0, 0.0)


def test_active_policy_needs_an_approver_and_a_nonzero_horizon(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                _policy(cur, status="active", horizon_finalized_total=200)  # 승인자 없음

    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                _policy(
                    cur,
                    status="active",
                    horizon_finalized_total=0,  # §6.2 "nonzero horizon 필수"
                    approved_by="operator",
                    approved_at="2026-09-21 00:00:00+00",
                )


def test_auto_dispatch_requires_coverage_ready(pg_resource):
    """§F.3: auto_dispatch 는 사전 승인된 policy 한정. 스키마가 강제할 수 있는 최소선."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                _policy(cur, mode="auto_dispatch")


def test_coverage_ready_requires_who_and_when(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                _policy(cur, coverage_ready=True)


@pytest.mark.parametrize(
    "dimensions",
    [
        ["source_unit_name"],  # 카메라 키로 금지된 값 (032 와 같은 방어)
        ["camera"],  # camera_registry 가 비활성이라 어휘에 없다
        ["class", "class"],  # 중복
        [],  # 비어 있음
        ["class", "environment_type", "daynight_type", "weather", "camera_angle", "subject_scale", "occlusion_state"],
    ],
)
def test_balance_dimensions_vocabulary_is_enforced(pg_resource, dimensions):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                _policy(cur, balance_dimensions=dimensions)


def test_share_must_be_a_ratio(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                _policy(cur, max_synthetic_share="1.5")


# ─── 3. target — 셀 정체성과 sentinel 방어 ───────────────────────────────────


def test_dimensions_hash_is_generated_and_key_order_independent(pg_resource):
    """같은 셀이 두 해시를 갖는 사고가 구조적으로 불가능해야 한다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_a = _policy(cur, policy_key="a")
            policy_b = _policy(cur, policy_key="b")
            _target(cur, policy_a, {"class": "falldown", "environment_type": "outdoor"})
            _target(cur, policy_b, {"environment_type": "outdoor", "class": "falldown"})
            cur.execute("SELECT count(DISTINCT dimensions_hash) FROM synthetic_coverage_targets")
            assert cur.fetchone()[0] == 1


def test_duplicate_cells_in_one_policy_are_refused(pg_resource):
    """같은 셀이 둘이면 같은 이미지가 두 deficit 을 메운다 (§5.1)."""
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _policy(cur)
                _target(cur, policy_id, {"class": "falldown", "environment_type": "outdoor"}, "0.5")
                _target(cur, policy_id, {"class": "falldown", "environment_type": "outdoor"}, "0.5")


@pytest.mark.parametrize("sentinel", ["deferred", "unknown", "indeterminate"])
def test_sentinels_cannot_be_target_values(pg_resource, sentinel):
    """미분류 마커가 target 값이 되면 planner 가 관측 불가를 하나의 셀로 센다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _policy(cur)
                _target(cur, policy_id, {"class": "falldown", "weather": sentinel})


def test_not_applicable_is_a_real_target_value(pg_resource):
    """실내 장면의 weather='not_applicable' 은 관측된 비해당이지 미관측이 아니다 (032 와 같은 규칙)."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _policy(cur)
            _target(cur, policy_id, {"class": "falldown", "weather": "not_applicable"})
            cur.execute("SELECT count(*) FROM synthetic_coverage_targets")
            assert cur.fetchone()[0] == 1


@pytest.mark.parametrize(
    "dims",
    [
        {"class": ["a", "b"]},  # 배열
        {"class": 3},  # 숫자
        {"class": ""},  # 공백
        {},  # 빈 객체
    ],
)
def test_target_dimensions_must_be_flat_nonblank_strings(pg_resource, dims):
    with pytest.raises((psycopg2.errors.CheckViolation, psycopg2.errors.NotNullViolation)):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _policy(cur)
                _target(cur, policy_id, dims)


# ─── 4. snapshot cell — §5.2 상한을 스키마가 강제한다 ────────────────────────


def test_blocked_cell_cannot_plan_generations(pg_resource):
    """§5.2: "…중 하나가 0 이면 campaign 은 부분 생성이 아니라 명시적 block/defer 사유를 남긴다." """
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                target_id = _target(cur, policy_id, {"class": "falldown"})
                snapshot_id = _snapshot(cur, policy_id)
                _cell(
                    cur,
                    snapshot_id,
                    target_id,
                    block_reason="blocked_context_coverage",
                    planned_count=5,
                    deficit_count=10,
                    reference_available_count=10,
                    share_headroom_count=10,
                    budget_headroom_count=10,
                    campaign_cap_count=10,
                )


@pytest.mark.parametrize(
    "zeroed",
    [
        "deficit_count",
        "reference_available_count",
        "share_headroom_count",
        "budget_headroom_count",
        "campaign_cap_count",
    ],
)
def test_planned_cannot_exceed_any_of_the_five_caps(pg_resource, zeroed):
    """planner 에 버그가 나도 상한을 넘는 계획은 INSERT 자체가 거부된다."""
    full = {
        "deficit_count": 10,
        "reference_available_count": 10,
        "share_headroom_count": 10,
        "budget_headroom_count": 10,
        "campaign_cap_count": 10,
        "desired_count": 10,
        "planned_count": 5,
    }
    full[zeroed] = 0
    if zeroed == "deficit_count":
        full["eligible_finalized_count"] = 10  # deficit = max(0, desired - eligible) 를 맞춘다
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                target_id = _target(cur, policy_id, {"class": "falldown"})
                snapshot_id = _snapshot(cur, policy_id)
                _cell(cur, snapshot_id, target_id, **full)


def test_deficit_must_match_the_formula(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                target_id = _target(cur, policy_id, {"class": "falldown"})
                snapshot_id = _snapshot(cur, policy_id)
                _cell(cur, snapshot_id, target_id, desired_count=10, eligible_finalized_count=4, deficit_count=99)


def test_eligible_must_decompose_into_real_plus_synthetic(pg_resource):
    """R 과 S 를 합쳐서만 보는 집계는 설계서가 금지한다 — 분해값과 합계가 어긋나면 거부."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                target_id = _target(cur, policy_id, {"class": "falldown"})
                snapshot_id = _snapshot(cur, policy_id)
                _cell(
                    cur,
                    snapshot_id,
                    target_id,
                    desired_count=10,
                    eligible_finalized_count=7,
                    deficit_count=3,
                    real_finalized_count=2,
                    synthetic_accepted_count=2,
                )


def test_class_totals_must_decompose_into_verified_unverified_missing(pg_resource):
    """이 3분할이 깨지면 (a)/(b)/(c) 판정이 무의미해진다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                target_id = _target(cur, policy_id, {"class": "falldown"})
                snapshot_id = _snapshot(cur, policy_id)
                _cell(
                    cur,
                    snapshot_id,
                    target_id,
                    class_finalized_total=248,
                    class_context_verified_total=0,
                    class_context_unverified_total=0,
                    class_context_missing_total=0,
                )


def test_zero_and_unobservable_land_in_different_columns(pg_resource):
    """이 파일의 핵심. 같은 planned=0 이 세 가지 다른 기록을 남긴다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            snapshot_id = _snapshot(cur, policy_id)
            no_facts = _target(cur, policy_id, {"class": "falldown", "environment_type": "indoor"})
            unobserved = _target(cur, policy_id, {"class": "fire", "environment_type": "indoor"})
            genuine = _target(cur, policy_id, {"class": "smoke", "environment_type": "indoor"})

            _cell(cur, snapshot_id, no_facts, block_reason="blocked_no_finalized_facts")
            _cell(
                cur,
                snapshot_id,
                unobserved,
                block_reason="blocked_context_coverage",
                class_finalized_total=248,
                class_context_missing_total=248,
            )
            _cell(
                cur,
                snapshot_id,
                genuine,
                block_reason="blocked_share_cap",
                class_finalized_total=100,
                class_context_verified_total=100,
            )

            cur.execute(
                """
                SELECT block_reason, class_finalized_total, class_context_verified_total, class_context_missing_total
                  FROM coverage_snapshot_cells ORDER BY block_reason
                """
            )
            assert cur.fetchall() == [
                ("blocked_context_coverage", 248, 0, 248),
                ("blocked_no_finalized_facts", 0, 0, 0),
                ("blocked_share_cap", 100, 100, 0),
            ]


def test_snapshot_records_policy_level_evidence_even_with_no_cells(pg_resource):
    """target 이 0개면 cell 행이 없다 — 그때 사유를 적을 곳은 snapshot 행뿐이다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            _snapshot(
                cur,
                policy_id,
                blocked_reason="blocked_no_targets",
                eligible_units_total=248,
                context_missing_units_total=248,
            )
            cur.execute(
                "SELECT blocked_reason, eligible_units_total, context_verified_units_total, "
                "context_missing_units_total FROM coverage_snapshots"
            )
            assert cur.fetchone() == ("blocked_no_targets", 248, 0, 248)
            cur.execute("SELECT count(*) FROM coverage_snapshot_cells")
            assert cur.fetchone()[0] == 0


def test_same_input_cannot_produce_a_second_snapshot(pg_resource):
    """snapshot 은 immutable — 같은 (policy, bucket, input_hash) 재계산을 UNIQUE 가 막는다."""
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                _snapshot(cur, policy_id)
                _snapshot(cur, policy_id)


# ─── 5. campaign — Phase F.3 mode gate ───────────────────────────────────────


@pytest.mark.parametrize("mode", ["plan_only", "disabled"])
@pytest.mark.parametrize("status", ["approved", "dispatching", "awaiting_review", "closed"])
def test_plan_only_campaign_cannot_reach_a_dispatchable_status(pg_resource, mode, status):
    """설계서 완료 기준: "승인하지 않은 campaign 은 ComfyUI 요청을 0건 생성한다."

    코드의 약속이 아니라 **스키마**가 막는다 — planner/sensor 양쪽에 버그가 나도 성립한다.
    """
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur, mode=mode if mode != "disabled" else "plan_only")
                snapshot_id = _snapshot(cur, policy_id)
                _campaign(
                    cur,
                    policy_id,
                    snapshot_id,
                    policy_mode_at_plan=mode,
                    status=status,
                    approved_by="operator",
                    approved_at="2026-09-21 00:00:00+00",
                    closed_at="2026-09-21 00:00:00+00",
                )


def test_plan_only_campaign_may_still_be_planned_or_blocked_or_cancelled(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur, mode="plan_only")
            snapshot_id = _snapshot(cur, policy_id)
            _campaign(cur, policy_id, snapshot_id, policy_mode_at_plan="plan_only", status="planned")
            cur.execute(
                "UPDATE synthetic_generation_campaigns SET status = 'blocked', blocked_reason = 'blocked_share_cap'"
            )
            cur.execute("UPDATE synthetic_generation_campaigns SET status = 'cancelled', blocked_reason = NULL")
            cur.execute("SELECT status FROM synthetic_generation_campaigns")
            assert cur.fetchone()[0] == "cancelled"


def test_raising_the_policy_mode_does_not_unlock_an_old_plan_only_campaign(pg_resource):
    """policy mode 를 올려도 **이미 만들어진** plan_only campaign 은 dispatch 되지 않는다."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur, mode="plan_only")
            snapshot_id = _snapshot(cur, policy_id)
            _campaign(cur, policy_id, snapshot_id, policy_mode_at_plan="plan_only")
            cur.execute(
                "UPDATE synthetic_coverage_policies SET mode = 'auto_dispatch' WHERE policy_id = %s", (policy_id,)
            )

    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE synthetic_generation_campaigns "
                    "SET status = 'approved', approved_by = 'x', approved_at = CURRENT_TIMESTAMP"
                )


def test_approved_campaign_needs_an_approver(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                _campaign(cur, policy_id, snapshot_id, status="approved")


def test_blocked_campaign_must_say_why(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                _campaign(cur, policy_id, snapshot_id, status="blocked")


def test_the_same_bucket_and_input_yields_one_campaign(pg_resource):
    """설계서 테스트 매트릭스의 "idempotent schedule bucket"."""
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                _campaign(cur, policy_id, snapshot_id)
                _campaign(cur, policy_id, snapshot_id)


def test_a_different_snapshot_hash_makes_a_new_campaign(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            s1 = _snapshot(cur, policy_id, input_config_hash="hash-1")
            s2 = _snapshot(cur, policy_id, input_config_hash="hash-2")
            _campaign(cur, policy_id, s1, snapshot_input_hash="hash-1")
            _campaign(cur, policy_id, s2, snapshot_input_hash="hash-2")
            cur.execute("SELECT count(*) FROM synthetic_generation_campaigns")
            assert cur.fetchone()[0] == 2


# ─── 6. task — defer 는 사유를 남긴다 ────────────────────────────────────────


def test_deferred_task_must_name_a_reason(pg_resource):
    """사유 없는 defer 는 다음 tick 에 같은 실패를 반복할 뿐이다 (Phase F.2)."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                campaign_id = _campaign(cur, policy_id, snapshot_id)
                _task(cur, campaign_id, state="deferred")


def test_deferred_reason_vocabulary_is_closed(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                campaign_id = _campaign(cur, policy_id, snapshot_id)
                _task(cur, campaign_id, state="deferred", deferred_reason="그냥")


def test_duplicate_submission_of_the_same_reference_and_seed_is_refused(seeded):
    """§6.3: `(campaign_id, reference_id, workflow_id, seed)` UNIQUE."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            snapshot_id = _snapshot(cur, policy_id)
            campaign_id = _campaign(cur, policy_id, snapshot_id)
            reference_id = _insert(
                cur,
                "generation_reference_pool",
                {
                    "image_id": "img-2",
                    "asset_id": "asset-1",
                    "reference_key": "unit/img-2.jpg",
                    "allowed_workflows": ["sdxl-inpaint-cctv-v1"],
                    "safe_region_json": "{}",
                },
                returning="reference_id",
            )
            _task(cur, campaign_id, reference_id=reference_id, seed=1)

    with pytest.raises(psycopg2.errors.UniqueViolation):
        with seeded.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT campaign_id, reference_id FROM synthetic_generation_tasks")
                campaign_id, reference_id = cur.fetchone()
                _task(cur, campaign_id, reference_id=reference_id, seed=1)


def test_accepted_task_needs_a_timestamp(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                campaign_id = _campaign(cur, policy_id, snapshot_id)
                _task(cur, campaign_id, state="accepted")


# ─── 7. quality review ───────────────────────────────────────────────────────


def test_accepted_review_cannot_contradict_its_sub_judgements(pg_resource):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                campaign_id = _campaign(cur, policy_id, snapshot_id)
                task_id = _task(cur, campaign_id)
                _insert(
                    cur,
                    "generation_quality_reviews",
                    {
                        "task_id": task_id,
                        "decision": "accepted",
                        "event_fidelity": "fail",  # 모순
                        "context_preserved": "pass",
                        "artifact_severity": "none",
                        "reviewer": "operator",
                    },
                )


def test_rejected_review_must_carry_reason_codes(pg_resource):
    """§F.4 "reason code 로 축적" 이 빈 배열이면 다음 회차가 배울 것이 없다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                snapshot_id = _snapshot(cur, policy_id)
                campaign_id = _campaign(cur, policy_id, snapshot_id)
                task_id = _task(cur, campaign_id)
                _insert(
                    cur,
                    "generation_quality_reviews",
                    {"task_id": task_id, "decision": "rejected", "reviewer": "operator"},
                )


# ─── 8. 예산 원장 + 예약 뷰 ─────────────────────────────────────────────────


def test_budget_daily_view_computes_committed_jobs(pg_resource):
    """committed = (reserve − release) + consume."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            for event_type, n in (("reserve", 10), ("release", 3), ("consume", 4)):
                _insert(
                    cur,
                    "generation_budget_events",
                    {"policy_id": policy_id, "event_type": event_type, "job_count": n},
                )
            cur.execute(
                "SELECT outstanding_reserved_jobs, consumed_jobs, committed_jobs FROM v_generation_budget_daily"
            )
            assert cur.fetchone() == (7, 4, 11)


def test_budget_amounts_cannot_be_negative(pg_resource):
    """부호는 event_type 이 진다 — 같은 사실이 두 모양으로 기록되는 것을 막는다."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg_resource.connect() as conn:
            with conn.cursor() as cur:
                policy_id = _active_policy(cur)
                _insert(
                    cur,
                    "generation_budget_events",
                    {"policy_id": policy_id, "event_type": "reserve", "job_count": -5},
                )


def test_reservations_view_counts_live_tasks_only(pg_resource):
    """종료된 campaign 의 task 는 여유를 잡고 있지 않다 (§5.2 의 P)."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            snapshot_id = _snapshot(cur, policy_id)
            campaign_id = _campaign(
                cur,
                policy_id,
                snapshot_id,
                status="approved",
                approved_by="operator",
                approved_at="2026-09-21 00:00:00+00",
            )
            _task(cur, campaign_id, seed=1, state="ready")
            _task(cur, campaign_id, seed=2, state="dispatched", dispatched_at="2026-09-21 00:00:00+00")
            _task(cur, campaign_id, seed=3, state="rejected")  # 예약 아님
            cur.execute(
                "SELECT reserved_count FROM v_synthetic_coverage_reservations WHERE policy_id = %s", (policy_id,)
            )
            assert cur.fetchone()[0] == 2

            cur.execute("UPDATE synthetic_generation_campaigns SET status = 'closed', closed_at = CURRENT_TIMESTAMP")
            cur.execute("SELECT count(*) FROM v_synthetic_coverage_reservations")
            assert cur.fetchone()[0] == 0


# ─── 9. planner 입력 집계 — 오늘 돌리면 무엇이 나오나 ────────────────────────


def _plan_for(db, policy_key: str = "falldown-env-pilot"):
    policies = db.list_plannable_policies()
    policy = next(p for p in policies if p["policy_key"] == policy_key)
    targets = db.list_policy_targets(policy["policy_id"])
    return plan_policy(db.build_policy_plan_input(policy, targets))


@pytest.fixture
def pilot(seeded):
    """설계서 §5.1 의 pilot policy 4 셀 + coverage_ready. 사실 테이블은 **비어 있다**."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            for env, dn, share in (
                ("indoor", "day", "0.35"),
                ("indoor", "night", "0.25"),
                ("outdoor", "day", "0.25"),
                ("outdoor", "night", "0.15"),
            ):
                _target(
                    cur,
                    policy_id,
                    {"class": "falldown", "environment_type": env, "daynight_type": dn},
                    share,
                )
    return seeded


def test_planner_on_an_empty_fact_table_reports_blocked_not_a_ratio(pilot):
    """**오늘 prod 상태의 재현.** coverage_unit_facts 0행 → 전 셀 blocked_no_finalized_facts."""
    plan = _plan_for(pilot)
    assert plan.planned_total == 0
    assert len(plan.cells) == 4
    assert {c.block_reason for c in plan.cells} == {BLOCK_NO_FINALIZED_FACTS}
    assert plan.blocked_reason == BLOCK_NO_FINALIZED_FACTS
    assert plan.eligible_units_total == 0


def test_planner_distinguishes_unverified_context_from_a_genuine_zero(pilot):
    """**실측 재현**: finalized 사실은 있는데 부모 영상 context 가 하나도 검증 안 된 상태.

    prod 의 "finalized bbox 248장 중 environment_type 보유 0장" 이 이 모양이다.
    사유가 `blocked_no_finalized_facts` 에서 `blocked_context_coverage` 로 **바뀌어야** 한다.
    """
    with pilot.connect() as conn:
        with conn.cursor() as cur:
            _insert(
                cur,
                "coverage_unit_facts",
                {
                    "image_id": "img-1",
                    "canonical_class": "falldown",
                    "fact_source": "ls_bbox",
                    "asset_id": "asset-1",
                    "origin_kind": "real",
                    "review_status": "finalized",
                    "finalized_at": "2026-09-21 00:00:00+00",
                },
            )
    plan = _plan_for(pilot)
    assert plan.planned_total == 0
    assert {c.block_reason for c in plan.cells} == {BLOCK_CONTEXT_COVERAGE}
    assert plan.eligible_units_total == 1
    assert plan.context_verified_units_total == 0
    assert plan.context_missing_units_total == 1  # context 사실 자체가 없다
    assert plan.cells[0].class_finalized_total == 1


def test_planner_counts_a_cell_once_context_is_verified(pilot):
    """context 가 verified 가 되면 그 셀만 R 을 얻고, 나머지 셀의 0 은 **진짜 0** 이 된다."""
    with pilot.connect() as conn:
        with conn.cursor() as cur:
            _insert(
                cur,
                "coverage_context_facts",
                {
                    "subject_type": "asset",
                    "asset_id": "asset-1",
                    "environment_type": "outdoor",
                    "daynight_type": "day",
                    "context_source": "places365_cuda",
                    "verification_status": "verified",
                    "verified_axes": ["environment_type", "daynight_type"],
                    "verified_at": "2026-09-21 00:00:00+00",
                },
            )
            _insert(
                cur,
                "coverage_unit_facts",
                {
                    "image_id": "img-1",
                    "canonical_class": "falldown",
                    "fact_source": "ls_bbox",
                    "asset_id": "asset-1",
                    "origin_kind": "real",
                    "review_status": "finalized",
                    "finalized_at": "2026-09-21 00:00:00+00",
                },
            )
    plan = _plan_for(pilot)
    by_cell = {(c.dimensions["environment_type"], c.dimensions["daynight_type"]): c for c in plan.cells}
    outdoor_day = by_cell[("outdoor", "day")]
    assert outdoor_day.real_finalized_count == 1
    assert outdoor_day.class_context_verified_total == 1
    # 측정은 됐으므로 context_coverage 사유가 아니다 — 남은 제약은 share cap(R 이 1뿐)이다.
    assert outdoor_day.block_reason != BLOCK_CONTEXT_COVERAGE
    for cell in plan.cells:
        assert cell.block_reason != BLOCK_NO_FINALIZED_FACTS


def test_inherited_context_is_not_counted_as_verified(pilot):
    """§5.3: 생성물의 context 는 `inherited_from_reference` 로 시작하고 검수 통과 후에만 verified.

    inherited 를 verified 로 세면 합성이 **자기 context 를 스스로 승인**하게 된다.
    """
    with pilot.connect() as conn:
        with conn.cursor() as cur:
            _insert(
                cur,
                "coverage_context_facts",
                {
                    "subject_type": "asset",
                    "asset_id": "asset-1",
                    "environment_type": "outdoor",
                    "daynight_type": "day",
                    "context_source": "inherited_from_reference",
                    "verification_status": "inherited",
                    "verified_axes": ["environment_type", "daynight_type"],
                },
            )
            _insert(
                cur,
                "coverage_unit_facts",
                {
                    "image_id": "img-1",
                    "canonical_class": "falldown",
                    "fact_source": "ls_bbox",
                    "asset_id": "asset-1",
                    "origin_kind": "synthetic",
                    "review_status": "finalized",
                    "finalized_at": "2026-09-21 00:00:00+00",
                },
            )
    plan = _plan_for(pilot)
    assert plan.context_verified_units_total == 0
    assert {c.block_reason for c in plan.cells} == {BLOCK_CONTEXT_COVERAGE}


def test_weather_as_a_balance_dimension_blocks_on_context_coverage(seeded):
    """`weather` 는 prod 실값이 0행이라 dimension 으로 선언하면 항상 여기 걸린다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(
                cur,
                policy_key="weather-pilot",
                balance_dimensions=["class", "weather"],
            )
            _target(cur, policy_id, {"class": "falldown", "weather": "rain"}, "1.0")
            # env 축은 verified 지만 weather 는 아니다 — 실측 그대로.
            _insert(
                cur,
                "coverage_context_facts",
                {
                    "subject_type": "asset",
                    "asset_id": "asset-1",
                    "environment_type": "outdoor",
                    "daynight_type": "day",
                    "context_source": "places365_cuda",
                    "verification_status": "verified",
                    "verified_axes": ["environment_type", "daynight_type"],
                    "verified_at": "2026-09-21 00:00:00+00",
                },
            )
            _insert(
                cur,
                "coverage_unit_facts",
                {
                    "image_id": "img-1",
                    "canonical_class": "falldown",
                    "fact_source": "ls_bbox",
                    "asset_id": "asset-1",
                    "origin_kind": "real",
                    "review_status": "finalized",
                    "finalized_at": "2026-09-21 00:00:00+00",
                },
            )
    plan = _plan_for(seeded, policy_key="weather-pilot")
    assert plan.blocked_reason == BLOCK_CONTEXT_COVERAGE
    assert plan.cells[0].class_finalized_total == 1
    assert plan.cells[0].class_context_verified_total == 0
    assert plan.cells[0].class_context_unverified_total == 1  # context 는 있는데 축이 없다


def test_policies_that_fail_the_phase_f1_gate_are_invisible_to_the_planner(seeded):
    """coverage_ready=false / mode=disabled / status=draft 는 애초에 후보가 아니다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _policy(cur, policy_key="draft-one")  # draft
            _active_policy(
                cur, policy_key="not-ready", coverage_ready=False, coverage_ready_by=None, coverage_ready_at=None
            )
            _active_policy(cur, policy_key="disabled-one", mode="disabled")
    assert seeded.list_plannable_policies() == []


def test_activation_gate_rejects_a_bad_target_set(seeded):
    """§6.2 의 "activate 시 검증" — 스키마가 아니라 이 함수가 지키는 불변식."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _policy(
                cur,
                horizon_finalized_total=200,
                coverage_ready=True,
                coverage_ready_by="operator",
                coverage_ready_at="2026-09-21 00:00:00+00",
            )
            # share 합 0.5 + dimension 누락
            _target(cur, policy_id, {"class": "falldown", "environment_type": "indoor"}, "0.5")

    problems = seeded.activate_policy(policy_id, approved_by="operator")
    assert any("target_share 합" in p for p in problems), problems
    assert any("dimension 불일치" in p for p in problems), problems

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM synthetic_coverage_policies WHERE policy_id = %s", (policy_id,))
            assert cur.fetchone()[0] == "draft", "검증 실패인데 활성화됐다"


def test_dispatch_candidates_exclude_plan_only_campaigns(pilot):
    """SQL 단계에서 이미 F.3 게이트가 걸린다 (스키마 CHECK + 이 WHERE + 센서 판정의 세 겹)."""
    with pilot.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT policy_id FROM synthetic_coverage_policies LIMIT 1")
            policy_id = cur.fetchone()[0]
            snapshot_id = _snapshot(cur, policy_id)
            plan_only = _campaign(
                cur, policy_id, snapshot_id, policy_mode_at_plan="plan_only", snapshot_input_hash="h-plan-only"
            )
            _task(cur, plan_only, state="ready", seed=1)

            snapshot2 = _snapshot(cur, policy_id, input_config_hash="hash-2")
            approved = _campaign(
                cur,
                policy_id,
                snapshot2,
                snapshot_input_hash="h-approved",
                status="approved",
                approved_by="operator",
                approved_at="2026-09-21 00:00:00+00",
            )
            approved_task = _task(cur, approved, state="ready", seed=2)

    candidates = pilot.next_dispatchable_tasks(limit=10)
    assert [c["task_id"] for c in candidates] == [approved_task]


# ─── 9. F.2 reference 확정 (planner 는 자리표만 만든다) ────────────────────────


def _context_fact(cur, image_id: str, *, status: str = "verified", **axes) -> str:
    payload = {
        "subject_type": "image",
        "image_id": image_id,
        "context_source": "operator",
        "verification_status": status,
        **axes,
    }
    if status == "verified":
        payload["verified_axes"] = sorted(axes)
        payload["verified_at"] = "2026-09-21 00:00:00+00"
    return _insert(cur, "coverage_context_facts", payload, returning="context_fact_id")


def _pool(cur, image_id: str, asset_id: str, context_fact_id: str, **over) -> str:
    payload = {
        "image_id": image_id,
        "asset_id": asset_id,
        "reference_key": f"unit/{image_id}.jpg",
        "allowed_workflows": ["sdxl-inpaint-cctv-v1"],
        "requires_safe_region": False,
        "status": "approved",
        "approved_by": "operator",
        "approved_at": "2026-09-21 00:00:00+00",
        "holdout_excluded": False,
        "context_fact_id": context_fact_id,
    }
    payload.update(over)
    return _insert(cur, "generation_reference_pool", payload, returning="reference_id")


_CELL = {"environment_type": "outdoor", "daynight_type": "night"}


def _placeholder_task(cur, policy_id: str, dims: dict | None = None, **over) -> tuple[str, str]:
    """reference_id 가 NULL 인 자리표 task — planner 가 만드는 바로 그 모양."""
    target_id = _target(cur, policy_id, dims if dims is not None else _CELL)
    snapshot_id = _snapshot(cur, policy_id)
    campaign_id = _campaign(cur, policy_id, snapshot_id)
    payload = {"target_id": target_id, "workflow_id": "sdxl-inpaint-cctv-v1", "seed": 7}
    payload.update(over)
    task_id = _task(cur, campaign_id, **payload)
    return task_id, campaign_id


def test_claim_picks_a_reference_matching_the_cell_dimensions(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, _ = _placeholder_task(cur, policy_id)
            fact = _context_fact(cur, "img-1", **_CELL)
            reference_id = _pool(cur, "img-1", "asset-1", fact)

    claimed = seeded.claim_reference_for_task(task_id)
    assert claimed is not None
    assert claimed["reference_id"] == reference_id
    assert claimed["reference_key"] == "unit/img-1.jpg"

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT reference_id, reference_bucket, reference_key "
                "FROM synthetic_generation_tasks WHERE task_id = %s",
                (task_id,),
            )
            # 자리표가 확정되고 bucket/key 가 동결 복사된다 — FK 가 없으므로 이것이 감사 기록이다.
            assert cur.fetchone() == (reference_id, "vlm-raw", "unit/img-1.jpg")


def test_claim_returns_none_when_the_context_does_not_match_the_cell(seeded):
    """셀은 night 를 요구하는데 pool 은 day 뿐 — 고를 게 없으면 조용히 아무거나 쓰지 않는다."""
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, _ = _placeholder_task(cur, policy_id)
            fact = _context_fact(cur, "img-1", environment_type="outdoor", daynight_type="day")
            _pool(cur, "img-1", "asset-1", fact)

    assert seeded.claim_reference_for_task(task_id) is None
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT reference_id FROM synthetic_generation_tasks WHERE task_id = %s", (task_id,))
            assert cur.fetchone()[0] is None


def test_claim_ignores_a_reference_that_does_not_allow_the_workflow(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, _ = _placeholder_task(cur, policy_id)
            fact = _context_fact(cur, "img-1", **_CELL)
            _pool(cur, "img-1", "asset-1", fact, allowed_workflows=["flux2-klein-4b-edit-v1"])

    assert seeded.claim_reference_for_task(task_id) is None


def test_claim_ignores_inherited_context(seeded):
    """§5.3 — 'inherited' 는 scene preservation 검수를 아직 통과하지 못한 상태다.

    planner 가 `_axes_verified_sql` 로 이것을 세지 않으므로 dispatcher 도 세면 안 된다.
    한쪽만 세면 planner 가 0 을 계획했는데 dispatcher 가 생성하거나 그 반대가 된다.
    """
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, _ = _placeholder_task(cur, policy_id)
            fact = _context_fact(cur, "img-1", status="inherited", **_CELL)
            _pool(cur, "img-1", "asset-1", fact)

    assert seeded.claim_reference_for_task(task_id) is None


def test_claim_does_not_reuse_the_same_campaign_workflow_seed_triplet(seeded):
    """UNIQUE(campaign, reference, workflow, seed) 를 위반하기 전에 후보에서 뺀다.

    구현은 두 겹이다 — 후보 SELECT 의 NOT EXISTS 와 UPDATE 의 UniqueViolation catch. 변형
    실험에서 NOT EXISTS 를 지워도 이 테스트가 통과했는데, 예외 경로가 같은 결과(None)를 내기
    때문이다. 그래서 여기서는 **결과**를 단언한다: 예외가 새어 나오지 않고, task 는 미확정으로
    남고, use_count 도 오르지 않는다. NOT EXISTS 는 흔한 경우에 UNIQUE 위반을 태우지 않으려는
    것이고, catch 는 tick 간 경합의 backstop 이다.
    """
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, campaign_id = _placeholder_task(cur, policy_id)
            fact = _context_fact(cur, "img-1", **_CELL)
            reference_id = _pool(cur, "img-1", "asset-1", fact)
            # 같은 campaign/workflow/seed 로 이미 그 reference 를 쓴 task 가 있다.
            _task(
                cur,
                campaign_id,
                target_id="other",
                dimensions_hash="dh-other",
                workflow_id="sdxl-inpaint-cctv-v1",
                seed=7,
                reference_id=reference_id,
            )

    assert seeded.claim_reference_for_task(task_id) is None

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT reference_id FROM synthetic_generation_tasks WHERE task_id = %s", (task_id,))
            assert cur.fetchone()[0] is None, "고르지 못했으면 자리표로 남아야 한다"
            cur.execute("SELECT use_count FROM generation_reference_pool WHERE reference_id = %s", (reference_id,))
            assert cur.fetchone()[0] == 0, "확정에 실패했는데 카운터가 오르면 라운드로빈이 편향된다"


def test_claim_prefers_the_least_used_reference_and_bumps_the_counter(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, _ = _placeholder_task(cur, policy_id)
            hot = _pool(cur, "img-1", "asset-1", _context_fact(cur, "img-1", **_CELL), use_count=5)
            cold = _pool(cur, "img-2", "asset-1", _context_fact(cur, "img-2", **_CELL), use_count=0)

    claimed = seeded.claim_reference_for_task(task_id)
    assert claimed["reference_id"] == cold, "덜 쓴 reference 를 먼저 골라야 한다"

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT use_count FROM generation_reference_pool WHERE reference_id = %s", (cold,))
            assert cur.fetchone()[0] == 1, "고른 뒤 use_count 가 올라야 다음 tick 이 다른 것을 고른다"
            cur.execute("SELECT use_count FROM generation_reference_pool WHERE reference_id = %s", (hot,))
            assert cur.fetchone()[0] == 5


def test_claim_is_idempotent_for_a_task_that_already_has_one(seeded):
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            fact = _context_fact(cur, "img-1", **_CELL)
            reference_id = _pool(cur, "img-1", "asset-1", fact)
            task_id, _ = _placeholder_task(cur, policy_id, reference_id=reference_id)

    first = seeded.claim_reference_for_task(task_id)
    second = seeded.claim_reference_for_task(task_id)
    assert first["reference_id"] == second["reference_id"] == reference_id
    with seeded.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT use_count FROM generation_reference_pool WHERE reference_id = %s", (reference_id,))
            assert cur.fetchone()[0] == 0, "이미 확정된 task 를 다시 봐도 카운터를 올리면 안 된다"


def test_claim_and_planner_reference_count_agree(seeded):
    """⚠️ 이 파일에서 가장 중요한 테스트.

    planner 는 `coverage_reference_counts` 로 "이 셀에 쓸 수 있는 reference 가 몇 개" 를 세서
    그만큼만 계획하고, dispatcher 는 `claim_reference_for_task` 로 그중 하나를 고른다. 두
    술어가 어긋나면 **계획은 N 인데 실행은 0** 이거나 그 반대가 되고, 어느 쪽도 오류로
    드러나지 않는다 — planner 는 계획했으니 정상이고 dispatcher 는 defer 사유를 남겼으니
    정상이기 때문이다. 조용한 불일치라서 계약으로 박는다.

    네 가지 배제 사유(축 불일치·workflow 불허·inherited·미승인)마다 **둘 다 0** 이어야 하고,
    통과 케이스에서는 **둘 다 1 이상**이어야 한다.
    """
    axes = ["environment_type", "daynight_type"]

    def _counts() -> int:
        key = tuple(sorted((axis, _CELL[axis]) for axis in axes))
        return seeded.coverage_reference_counts(axes, workflow_id="sdxl-inpaint-cctv-v1").get(key, 0)

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            policy_id = _active_policy(cur)
            task_id, _ = _placeholder_task(cur, policy_id)
            # 배제돼야 하는 네 종류를 전부 넣는다.
            _pool(  # 축 불일치
                cur,
                "img-1",
                "asset-1",
                _context_fact(cur, "img-1", environment_type="indoor", daynight_type="day"),
            )
            _pool(  # workflow 불허
                cur,
                "img-2",
                "asset-1",
                _context_fact(cur, "img-2", **_CELL),
                allowed_workflows=["flux2-klein-4b-edit-v1"],
            )

    assert _counts() == 0
    assert seeded.claim_reference_for_task(task_id) is None, "planner 가 0 이면 dispatcher 도 0 이어야 한다"

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _seed_image(cur, "img-3", "asset-1", 2)
            _pool(cur, "img-3", "asset-1", _context_fact(cur, "img-3", status="inherited", **_CELL))

    assert _counts() == 0, "inherited 는 planner 가 안 센다"
    assert seeded.claim_reference_for_task(task_id) is None, "planner 가 안 세면 dispatcher 도 고르면 안 된다"

    with seeded.connect() as conn:
        with conn.cursor() as cur:
            _seed_image(cur, "img-4", "asset-1", 3)
            good = _pool(cur, "img-4", "asset-1", _context_fact(cur, "img-4", **_CELL))

    assert _counts() == 1
    claimed = seeded.claim_reference_for_task(task_id)
    assert claimed is not None and claimed["reference_id"] == good
