-- 033_coverage_control_plane.sql — synthetic coverage 의 "정책·snapshot·campaign 계층"(설계서 §6.2)
-- 과 "task·provenance·품질 계층"(§6.3).
--
-- 설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md
--   Phase F (§4 Phase F.1~F.3) + §5.2 계산식 + §6.2 / §6.3 / §6.4.3.
--
-- 선행: 032_coverage_facts.sql 이 §6.1 사실·reference 계층과 §6.4.1~2 의 두 뷰를 만든다.
--   이 파일은 032 의 `generation_reference_pool` 을 FK 로 **참조한다** — 파일명 정렬상
--   032 가 먼저 적용되므로 안전하다. 반대 방향(032 가 이 파일을 참조)은 불가능하며,
--   그래서 032 는 여기 있는 테이블을 일절 언급하지 않는다.
--
-- ⚠️ `generation_gpu_leases` 는 **030_comfy_local.sql 에 이미 있다.** 설계서 §6.3 표에
--    같이 적혀 있지만 여기서 다시 만들지 않는다. 읽기 판정은 `lib/gpu_lease.py`,
--    acquire/heartbeat/release 는 `docker/genai/db/pg.py` 전속이라는 계약도 그대로다.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- 이 계층이 "0" 과 "관측 불가" 를 구분하는 방식 (설계서 §6.2: "unknown/deferred/missing
-- count 는 별 열로 표시해 숫자 0 과 관측 불가를 구분한다")
--
-- 실측 (2026-09-21, prod `vlm_pipeline`, 읽기 전용 psql):
--   * `_pg_migrations` 최신 = 031 — **032 도 아직 prod 미적용**이다(다음 이미지 재빌드
--     배포의 부팅 때 032 → 033 순으로 적용된다). 그래서 `coverage_unit_facts` 는
--     prod 에 아예 없고, 생긴 뒤에도 **투영 job 이 없어 0행**으로 시작한다.
--   * `video_metadata` 131,932 행 중 environment_type+daynight_type 실값 18,046
--     (outdoor/day 13,354 · indoor/day 4,571 · outdoor/night 103 · indoor/night 18).
--     `weather` 실값 **0행**.
--   * finalized bbox 를 가진 이미지 248장 중 부모 영상이 environment_type 을 가진 것 **0장**.
--     → (finalized event) ∩ (검증된 context) = **공집합**.
--
-- 즉 오늘 planner 를 돌리면 planned=0 이 나온다. 그 0 은 **세 가지 서로 다른 사실** 중
-- 하나일 수 있고, 이 스키마는 셋을 절대 같은 모양으로 저장하지 않는다:
--
--   (a) 사실 자체가 없다        → `class_finalized_total = 0`,  block_reason='blocked_no_finalized_facts'
--   (b) 사실은 있는데 context 미검증 → `class_finalized_total > 0` AND
--                                  `class_context_verified_total = 0`,
--                                  block_reason='blocked_context_coverage'
--   (c) 진짜로 그 셀이 0이다     → `class_context_verified_total > 0` AND
--                                  `real_finalized_count = 0`, block_reason IS NULL
--
-- (b)/(c) 의 구분이 이 파일의 존재 이유다. 세 경우를 "deficit 이 크다" 하나로 뭉개면
-- planner 는 **관측하지 못한 것을 부족분으로 읽고 생성을 지시한다.** 이 레포가 반복해 당한
-- "부재에 기댄 안전"([[project_safety_by_absence]])의 정확한 재현이라 스키마로 못 박는다.
--
-- 같은 이유로 `coverage_snapshots` 자신도 policy 단위 4개 카운터를 든다. target 이 0개면
-- cell 행도 0개인데, 그때 "아무 일도 없었다" 와 "사실이 하나도 없었다" 를 구분할 곳이
-- snapshot 행밖에 없기 때문이다.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- §5.2 계산식을 **스키마가 강제하는** 범위
--
--   desired_i  = max(min_finalized_count_i, ceil(horizon_finalized_total × target_share_i))
--   deficit_i  = max(0, desired_i − eligible_finalized_i)
--   planned_i  = min(deficit_i, max_per_campaign, remaining_daily_budget,
--                    eligible_reference_pool_i, synthetic_share_headroom_i)
--   share cap  : (S + P) / (R + S + P) ≤ a
--
-- 계산 자체는 `lib/coverage_planner.py`(L1-2 순수 함수)가 한다. 스키마는 그 **결과가
-- 계산식을 벗어날 수 없게** 상한을 CHECK 로 건다 (`coverage_snapshot_cells_planned_*_check`).
-- planner 에 버그가 나도 planned 가 deficit·reference·share·budget·cap 중 어느 하나라도
-- 넘으면 INSERT 자체가 거부된다. "계산이 맞겠지" 를 신뢰 대신 제약으로 바꾼 것.
--
-- ⚠️ share headroom 의 대수적 결과를 여기 적어 둔다 (테스트가 같은 값을 고정한다):
--     P_max_total = floor(a·R/(1−a)) − S      (a < 1),   a = 1 이면 무한
--   R=0 이면 a 가 얼마든 **P_max_total = −S ≤ 0** 이다. 즉 real finalized 가 하나도 없는
--   셀에서는 share cap 이 자동으로 생성량 0을 강제한다. 오늘 prod 가 정확히 그 상태다 —
--   별도 킬스위치가 아니라 계산식 자체가 fail-closed 다.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- 설계서와 어긋나 **조정한 지점** (검토 대상):
--
-- 1. `coverage_snapshot_cells.target_id` / `synthetic_generation_tasks.target_id` 에 FK 를
--    걸지 않았다. snapshot 은 immutable 감사 기록인데 CASCADE 면 target 한 줄 삭제가 과거
--    snapshot 을 조용히 지운다. 대신 `dimensions_json` 을 **동결 복사**하고 같은 규칙의
--    `dimensions_hash` 생성 컬럼으로 셀 정체성을 유지한다 — target 행이 사라져도 어떤
--    셀이었는지 재현된다.
--
-- 2. `dimensions_hash` 를 애플리케이션이 계산하지 않고 `md5(dimensions_json::text)` **생성
--    컬럼**으로 만들었다. jsonb 의 text 출력은 키 순서·공백이 정규화돼 있어(실측: 리터럴
--    키 순서를 바꿔 넣어도 동일 해시) 같은 셀이 두 해시를 갖는 사고가 구조적으로 불가능하다.
--    설계서는 컬럼만 요구했고 계산 주체를 정하지 않았다 — DB 로 밀어 넣은 것은 이 파일의 판단.
--
-- 3. 템플릿 `content_hash` 도 md5 다. sha256 은 `convert_to(text,name)` 이 IMMUTABLE 이 아니라
--    생성 컬럼으로 쓸 수 없다(실측: "generation expression is not immutable"). 여기 해시는
--    **내용 주소**이지 무결성 서명이 아니다 — 서명이 필요한 model/workflow checksum 은 030 의
--    `genai_job_provenance` 가 sha256 으로 따로 보관한다.
--
-- 4. §6.2 의 "policy activate 시 share 합=1, 선언된 dimension 전부 존재, target 간 중복 없음
--    검증" 중 **share 합=1 과 dimension 전부 존재는 CHECK 로 못 건다** (행 간/테이블 간 조건).
--    `(policy_id, dimensions_hash)` UNIQUE 로 중복만 스키마가 막고, 나머지 둘은
--    `resources/postgres_coverage.py` 의 `activate_policy()` 가 활성화 시점에 검증한다.
--    설계서가 애초에 "activate 시 검증"이라고 쓴 그대로이며, 스키마가 아니라 **코드가
--    지키는 불변식**이라는 점을 알고 쓸 것. 통합 테스트가 이 경로를 고정한다.
--
-- 5. `synthetic_generation_tasks.reference_id` 와 `genai_batch_id` 에는 **FK 를 걸지 않는다.**
--    처음에는 둘 다 ON DELETE SET NULL 로 만들었는데, 통합 테스트가 실제 인제스트 재적재
--    경로에서 이것이 **인제스트를 깨뜨린다**는 것을 잡아냈다.
--
--    ⚠️ **이 현상은 순서 의존적이라 손으로 재현하면 통과할 수 있다.** 2026-09-21 재검증에서
--    같은 논리적 상태인데도 pool 이 가리키는 이미지와 `output_image_id` 가 힙에서 어느
--    쪽이 먼저 삭제되느냐에 따라 통과/실패가 갈렸고, 이미지를 **하나씩** 지우면 둘 다
--    통과했다. 한 문장이 둘을 같이 지울 때만 깨진다. 따라서 "직접 해보니 되던데" 는
--    반증이 아니다 — 신뢰할 수 있는 재현은 아래 통합 테스트뿐이다:
--
--        tests/integration/test_coverage_control_plane_migration_033.py
--            ::test_the_real_reingest_delete_order_still_works
--
--    원인: 한 DELETE 문이 **같은 task 행에 두 개의 참조 동작**을 건다(pool 은 CASCADE 로
--    지워지며 `reference_id` 에 SET NULL 을, 다른 이미지가 `output_image_id` 에 SET NULL 을).
--    task 의 자식측 RI 검사 트리거는 `pg_trigger.tgattr` 가 **비어 있어**(실측) 그 행의
--    UPDATE 면 FK 컬럼을 건드리지 않아도 전부 깨어나고, 이때 부모 pool 행이 이미 다른
--    cascade 로 지워져 있으면 검사가 실패한다. PostgreSQL 은 두 cascade 의 순서를 보장하지
--    않는다. 실제 경로 `DELETE FROM image_metadata WHERE source_asset_id = %s`
--    (`postgres_ingest_raw.py:270-274`)가 정확히 이 모양이다 — coverage task 가 한 건이라도
--    쌓이면 **재적재가 FK 위반으로 실패할 수 있다.**
--
--    `genai_batch_id`/`genai_job_id` 쌍도 구조가 같다(`genai_jobs.batch_id` 가
--    `genai_batches` 에서 CASCADE 이므로 `DELETE FROM genai_batches` 한 문장이 같은 task 행에
--    두 동작을 건다). 실측에서는 우연히 통과했지만 **순서 운에 기대는 것**이라 함께 끊었다.
--    batch 는 job 에서 파생되는 값이므로 FK 를 겹쳐 걸 이유도 없었다.
--
--    → 둘 다 soft reference 로 두고 `reference_bucket`/`reference_key` 를 동결 복사해
--      감사 기록이 살아남게 했다. 그 대가로 dispatcher 는 매 tick reference 가 아직
--      후보인지 `v_generation_reference_candidates` 로 **다시 확인해야 한다** — 애초에 FK 는
--      "존재" 만 보장했지 "승인·유효기간·holdout" 은 보장하지 못했으므로 실질적 손실은 없다.
--
--    일반 규칙: **한 행이 두 개의 FK 를 갖고 그 부모들이 한 문장으로 같이 지워질 수 있으면,
--    그중 SET NULL 이 섞인 조합은 깨진다.** 이 계층에 FK 를 추가할 때 이 조건을 먼저 볼 것.
--
-- 6. §6.4.3 의 `(reference_id, status)` partial index 는 032 소관(pool 테이블)이라 여기서
--    만들지 않는다. 나머지 세 인덱스(`(policy_id, status, scheduled_for)`,
--    `(campaign_id, state, priority)`, `(snapshot_id, dimensions_hash)`)는 전부 만든다.
--
-- 7. 설계서 §6.3 은 `generation_gpu_leases` 를 이 계층 표에 넣었지만 **030 에 이미 있다.**
--    다시 만들지 않는다(위 경고).
--
-- 이 파일이 **보장하지 않는 것**:
--   * planner/dispatcher 를 만들지 않는다. 모든 테이블은 비어 있는 채로 배포된다.
--   * campaign 을 승인하지 않는다. `approval_required` 의 승인 주체는 사람이다.
--   * GenAI internal endpoint 를 정의하지 않는다 (`docker/` 범위).
--   * snapshot 의 물리적 immutability 를 트리거로 막지 않는다. `(policy_id,
--     schedule_bucket, input_config_hash)` UNIQUE 로 같은 입력의 재생성을 막고 관례로만
--     append-only 를 유지한다 — UPDATE 는 여전히 물리적으로 가능하다.
--   * LS `finalized` 와 품질 `accepted` 의 **동시 충족**을 한 CHECK 로 강제하지 않는다.
--     두 사실은 서로 다른 테이블(032 의 `coverage_unit_facts` vs 여기
--     `generation_quality_reviews`)에 있고, coverage 의 S 는 **언제나 전자에서만** 센다.
--     즉 품질 리뷰가 accepted 여도 LS finalized 가 아니면 분자에 들어가지 않는다.
--
-- Forward-only, idempotent, DO 블록 미사용(러너의 multi-DO 부분적용 quirk 회피).
-- 타임스탬프는 TIMESTAMPTZ (022/023/032 선례).
--
-- @ASSERT_AFTER 는 **이 파일이 만든 객체의 존재와 불변식**만 본다. 행 수·개수 상수를 걸지
-- 않는 이유는 032 헤더와 같다: 매 부팅마다 실행되고, 정당한 변경이 생기면 러너가 거기서
-- 죽어 이후 모든 마이그레이션이 정지한다 ([[project_postgres_migration_runner_quirk]]).
--
-- @ASSERT_AFTER: SELECT to_regclass('public.synthetic_coverage_policies') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.synthetic_coverage_targets') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.coverage_snapshots') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.coverage_snapshot_cells') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.synthetic_generation_campaigns') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.synthetic_generation_tasks') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.synthetic_prompt_templates') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.generation_quality_reviews') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.generation_budget_events') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.v_synthetic_coverage_reservations') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.v_generation_budget_daily') IS NOT NULL
-- FK 불변식: 이 파일이 만든 FK 중 **삭제를 막는 것(RESTRICT/NO ACTION)이 하나도 없어야**
-- 한다. 하나라도 있으면 그 순간부터 정본 삭제(인제스트 재적재·이미지 정리)가 FK 위반으로
-- 깨진다. 032 와 달리 개수 하한을 걸지 않는다 — 여기서 "FK 가 존재한다" 는 안전 속성이
-- 아니라 우연한 사실이고, 상수를 박으면 정당한 리팩터가 부팅을 죽인다.
-- @ASSERT_AFTER: SELECT COUNT(*) FILTER (WHERE confdeltype NOT IN ('c', 'n')) = 0 FROM pg_constraint WHERE contype = 'f' AND conrelid IN ('synthetic_coverage_targets'::regclass, 'coverage_snapshots'::regclass, 'coverage_snapshot_cells'::regclass, 'synthetic_generation_campaigns'::regclass, 'synthetic_generation_tasks'::regclass, 'generation_quality_reviews'::regclass, 'generation_budget_events'::regclass)
-- cascade 충돌 방지(위 조정 5): task 행은 `generation_reference_pool`/`genai_batches` 로 가는
-- FK 를 가지면 안 된다. 가지는 순간 `DELETE FROM image_metadata ...`(인제스트 재적재)와
-- `DELETE FROM genai_batches ...` 가 FK 위반으로 실패한다. 부모 이름을 regclass 가 아니라
-- relname 문자열로 비교하는 이유는 그 테이블이 없는 환경에서도 이 단언이 죽지 않게 하기 위함이다.
-- 이 단언이 실패하면 FK 를 되돌릴 게 아니라, 왜 다시 걸렸는지부터 볼 것.
-- @ASSERT_AFTER: SELECT NOT EXISTS (SELECT 1 FROM pg_constraint co JOIN pg_class pc ON pc.oid = co.confrelid WHERE co.contype = 'f' AND co.conrelid = 'synthetic_generation_tasks'::regclass AND pc.relname IN ('generation_reference_pool', 'genai_batches'))
-- F.3 mode gate: plan_only/disabled 로 계획된 campaign 은 승인·dispatch 상태로 갈 수 없다.
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'synthetic_generation_campaigns_mode_gate_check' AND conrelid = 'synthetic_generation_campaigns'::regclass AND contype = 'c')
-- §5.2 상한: blocked cell 은 생성을 계획할 수 없고, planned 는 다섯 상한을 넘을 수 없다.
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'coverage_snapshot_cells_planned_requires_unblocked_check' AND conrelid = 'coverage_snapshot_cells'::regclass AND contype = 'c')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'coverage_snapshot_cells_planned_within_caps_check' AND conrelid = 'coverage_snapshot_cells'::regclass AND contype = 'c')
-- 관측 불가 ≠ 0: 세 카운터가 전부 살아 있어야 (a)/(b)/(c) 를 구분할 수 있다.
-- @ASSERT_AFTER: SELECT COUNT(*) = 3 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'coverage_snapshot_cells' AND column_name IN ('class_finalized_total', 'class_context_verified_total', 'class_context_missing_total')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'synthetic_generation_campaigns' AND indexname = 'synthetic_generation_campaigns_idempotency_uniq')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'synthetic_coverage_targets' AND indexname = 'synthetic_coverage_targets_policy_dims_uniq')

BEGIN;

-- ─── 1. synthetic_prompt_templates — Comfy prompt 원장 ─────────────────────────
--
-- 먼저 만드는 이유는 아래 `synthetic_coverage_targets` 가 FK 로 가리키기 때문이다.
--
-- ⚠️ 설계서 §6.3: "Gemini용 `generation_prompts` 와 섞지 않는다." 018 의
--    `generation_prompts` 는 비디오 캡셔닝 프롬프트 계보고, 이 테이블은 **이미지 생성**
--    workflow 에 주입되는 positive/negative prompt 의 승인 원장이다. 두 테이블을 합치면
--    "라벨 생성" 과 "데이터 생성" 의 계보가 섞여 자기학습 감사가 불가능해진다.
CREATE TABLE IF NOT EXISTS synthetic_prompt_templates (
    template_id       TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    template_key      TEXT NOT NULL,
    version           INTEGER NOT NULL DEFAULT 1,
    body              TEXT NOT NULL,
    negative_body     TEXT,
    -- 변수 schema. 렌더러가 검증할 변수 이름/타입 선언이며 값이 아니다.
    variables_schema  JSONB NOT NULL DEFAULT '{}'::jsonb,
    renderer_version  TEXT NOT NULL,
    -- 내용 주소(무결성 서명 아님 — 헤더 조정 3 참조). 생성 컬럼이라 body 를 고치면 자동 갱신.
    content_hash      TEXT GENERATED ALWAYS AS (md5(body || COALESCE(negative_body, ''))) STORED,
    allowed_workflows TEXT[] NOT NULL,
    status            TEXT NOT NULL DEFAULT 'draft',
    approved_by       TEXT,
    approved_at       TIMESTAMPTZ,
    notes             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT synthetic_prompt_templates_key_version_uniq UNIQUE (template_key, version),
    CONSTRAINT synthetic_prompt_templates_key_nonblank_check CHECK (btrim(template_key) <> ''),
    CONSTRAINT synthetic_prompt_templates_body_nonblank_check CHECK (btrim(body) <> ''),
    CONSTRAINT synthetic_prompt_templates_version_check CHECK (version >= 1),
    CONSTRAINT synthetic_prompt_templates_status_check
        CHECK (status IN ('draft', 'approved', 'retired')),
    CONSTRAINT synthetic_prompt_templates_approval_check
        CHECK (status <> 'approved' OR (approved_by IS NOT NULL AND approved_at IS NOT NULL)),
    CONSTRAINT synthetic_prompt_templates_workflows_check
        CHECK (
            cardinality(allowed_workflows) BETWEEN 1 AND 16
            AND array_position(allowed_workflows, NULL::TEXT) IS NULL
        ),
    CONSTRAINT synthetic_prompt_templates_variables_schema_check
        CHECK (jsonb_typeof(variables_schema) = 'object')
);

COMMENT ON TABLE synthetic_prompt_templates IS
    'Comfy positive/negative prompt 의 승인 원장. Gemini 캡셔닝 계보(018 generation_prompts)와 의도적으로 분리돼 있다.';
COMMENT ON COLUMN synthetic_prompt_templates.content_hash IS
    'md5 내용 주소 — 무결성 서명이 아니다. workflow/model checksum 은 030 genai_job_provenance 가 sha256 으로 보관한다.';

CREATE INDEX IF NOT EXISTS synthetic_prompt_templates_approved_idx
    ON synthetic_prompt_templates (template_key, version DESC)
    WHERE status = 'approved';

-- ─── 2. synthetic_coverage_policies — 정책 버전 ────────────────────────────────
--
-- 한 policy 는 **하나의 상호배타적 balance_dimensions 집합만** 선언한다(설계서 §5.1).
-- wildcard 가 섞이거나 서로 다른 차원의 target 을 한 policy 에 섞으면 같은 이미지가 여러
-- deficit 을 메우므로 금지 — 아래 CHECK 가 어휘·중복을, targets 의 UNIQUE 가 셀 중복을 막는다.
--
-- 기본값은 전부 "아무것도 못 하는" 쪽이다:
--   status 'draft'      → planner 가 active 만 본다
--   mode   'plan_only'  → 승인·dispatch 로 갈 수 없다 (campaign 의 mode gate 가 못 박는다)
--   coverage_ready FALSE→ Phase F.1 의 "coverage_ready 검증 통과 policy 만 시작" 게이트
--   모든 budget/cap 0   → §5.2 의 min() 이 자동으로 0을 낸다
CREATE TABLE IF NOT EXISTS synthetic_coverage_policies (
    policy_id                 TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    policy_key                TEXT NOT NULL,
    version                   INTEGER NOT NULL DEFAULT 1,
    status                    TEXT NOT NULL DEFAULT 'draft',
    mode                      TEXT NOT NULL DEFAULT 'plan_only',
    balance_dimensions        TEXT[] NOT NULL,
    -- §5.2: "모든 policy 는 명시적 horizon 또는 cell 별 min_finalized_count 를 가져야 한다."
    -- §6.2: "active policy 는 nonzero horizon 이 필수" → active 일 때만 > 0 을 강제한다.
    horizon_finalized_total   INTEGER NOT NULL DEFAULT 0,
    max_synthetic_share       NUMERIC(6, 5) NOT NULL DEFAULT 0,
    max_per_campaign          INTEGER NOT NULL DEFAULT 0,
    daily_job_budget          INTEGER NOT NULL DEFAULT 0,
    weekly_job_budget         INTEGER NOT NULL DEFAULT 0,
    daily_gpu_seconds_budget  INTEGER NOT NULL DEFAULT 0,
    weekly_gpu_seconds_budget INTEGER NOT NULL DEFAULT 0,
    -- §F.4 "단순 재시도로 무한 생성하지 않으며 target/reference/workflow 별 retry ceiling".
    max_task_retries          INTEGER NOT NULL DEFAULT 2,
    default_workflow_id       TEXT NOT NULL,
    schedule_cron             TEXT,
    holdout_scope             TEXT NOT NULL DEFAULT 'camera',
    -- Phase F.1 게이트. 032 의 사실 3종(event·context·reference)이 셀 수 있는 상태임을
    -- 사람이 확인했다는 뜻이며, 스키마는 "누가 언제" 만 강제한다.
    coverage_ready            BOOLEAN NOT NULL DEFAULT FALSE,
    coverage_ready_by         TEXT,
    coverage_ready_at         TIMESTAMPTZ,
    approved_by               TEXT,
    approved_at               TIMESTAMPTZ,
    notes                     TEXT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT synthetic_coverage_policies_key_version_uniq UNIQUE (policy_key, version),
    CONSTRAINT synthetic_coverage_policies_key_nonblank_check CHECK (btrim(policy_key) <> ''),
    CONSTRAINT synthetic_coverage_policies_version_check CHECK (version >= 1),
    CONSTRAINT synthetic_coverage_policies_status_check
        CHECK (status IN ('draft', 'active', 'paused', 'retired')),
    CONSTRAINT synthetic_coverage_policies_mode_check
        CHECK (mode IN ('disabled', 'plan_only', 'approval_required', 'auto_dispatch')),
    -- 허용 어휘는 032 의 6축 + 'class'. `camera`/`site` 는 camera_registry 가 비활성이라
    -- 일부러 넣지 않았다 — 스키마가 거부한다(주석이 아니라 CHECK 로).
    -- 중복 금지: 어휘가 고정 7개라 각 이름의 등장 위치가 1개 이하면 곧 distinct 다.
    CONSTRAINT synthetic_coverage_policies_dimensions_check
        CHECK (
            cardinality(balance_dimensions) BETWEEN 1 AND 6
            AND balance_dimensions <@ ARRAY[
                'class', 'environment_type', 'daynight_type',
                'weather', 'camera_angle', 'subject_scale', 'occlusion_state'
            ]::TEXT[]
            AND array_position(balance_dimensions, NULL::TEXT) IS NULL
            AND cardinality(array_positions(balance_dimensions, 'class')) <= 1
            AND cardinality(array_positions(balance_dimensions, 'environment_type')) <= 1
            AND cardinality(array_positions(balance_dimensions, 'daynight_type')) <= 1
            AND cardinality(array_positions(balance_dimensions, 'weather')) <= 1
            AND cardinality(array_positions(balance_dimensions, 'camera_angle')) <= 1
            AND cardinality(array_positions(balance_dimensions, 'subject_scale')) <= 1
            AND cardinality(array_positions(balance_dimensions, 'occlusion_state')) <= 1
        ),
    CONSTRAINT synthetic_coverage_policies_horizon_check CHECK (horizon_finalized_total >= 0),
    CONSTRAINT synthetic_coverage_policies_share_check
        CHECK (max_synthetic_share >= 0 AND max_synthetic_share <= 1),
    CONSTRAINT synthetic_coverage_policies_budgets_check
        CHECK (
            max_per_campaign >= 0
            AND daily_job_budget >= 0
            AND weekly_job_budget >= 0
            AND daily_gpu_seconds_budget >= 0
            AND weekly_gpu_seconds_budget >= 0
            AND max_task_retries >= 0
        ),
    CONSTRAINT synthetic_coverage_policies_workflow_check CHECK (btrim(default_workflow_id) <> ''),
    CONSTRAINT synthetic_coverage_policies_holdout_scope_check
        CHECK (holdout_scope IN ('camera', 'site', 'session')),
    -- active = 승인자 + nonzero horizon (§6.2 무결성 규칙).
    CONSTRAINT synthetic_coverage_policies_active_check
        CHECK (
            status <> 'active'
            OR (approved_by IS NOT NULL AND approved_at IS NOT NULL AND horizon_finalized_total > 0)
        ),
    CONSTRAINT synthetic_coverage_policies_coverage_ready_check
        CHECK (NOT coverage_ready OR (coverage_ready_by IS NOT NULL AND coverage_ready_at IS NOT NULL)),
    -- §F.3: auto_dispatch 는 "사전 승인된 품질 기준·수용률·rollback drill 충족 policy" 한정.
    -- 스키마가 강제할 수 있는 최소선은 coverage_ready 다 — 나머지는 승인자의 책임.
    CONSTRAINT synthetic_coverage_policies_auto_dispatch_check
        CHECK (mode <> 'auto_dispatch' OR coverage_ready)
);

COMMENT ON TABLE synthetic_coverage_policies IS
    '한 policy = 하나의 상호배타적 balance_dimensions 집합(설계서 §5.1). 기본값은 draft + plan_only + 모든 budget 0 이라 아무것도 생성하지 않는다.';
COMMENT ON COLUMN synthetic_coverage_policies.balance_dimensions IS
    '허용 어휘 = class + 032 의 6 context 축. camera/site 는 camera_registry 가 비활성이라 어휘에 없다.';
COMMENT ON COLUMN synthetic_coverage_policies.max_synthetic_share IS
    '§5.2 의 a. (S+P)/(R+S+P) <= a 를 셀마다 강제한다. R=0 이면 a 와 무관하게 headroom 이 0 이다.';
COMMENT ON COLUMN synthetic_coverage_policies.coverage_ready IS
    'Phase F.1 게이트 — 032 의 event/context/reference 사실이 셀 수 있는 상태임을 사람이 확인했다는 표시.';

CREATE INDEX IF NOT EXISTS synthetic_coverage_policies_active_idx
    ON synthetic_coverage_policies (status, mode)
    WHERE status = 'active';

-- ─── 3. synthetic_coverage_targets — 상호배타적 target cell ────────────────────
--
-- `dimensions_hash` 는 **생성 컬럼**이다(헤더 조정 2). jsonb 의 text 출력이 키 순서를
-- 정규화하므로 `{"class":"falldown","environment_type":"outdoor"}` 와 키 순서를 뒤집은
-- 같은 내용이 동일 해시를 낸다 — 같은 셀이 두 해시를 갖는 사고가 구조적으로 불가능하다.
--
-- ⚠️ 축 값에 'deferred'/'unknown'/'indeterminate' 를 쓰는 것을 CHECK 로 막는다. 032 의
--    `coverage_context_facts_axis_sentinel_check` 와 같은 방어다 — 미분류 마커가 target 값
--    자리로 새면 planner 가 "deferred 라는 환경" 을 하나의 셀로 세고, 관측 불가를 비율로
--    바꿔 버린다. 'not_applicable'(실내 장면의 weather 등)은 **관측된 비해당**이라 허용한다.
CREATE TABLE IF NOT EXISTS synthetic_coverage_targets (
    target_id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    policy_id           TEXT NOT NULL REFERENCES synthetic_coverage_policies(policy_id) ON DELETE CASCADE,
    dimensions_json     JSONB NOT NULL,
    dimensions_hash     TEXT GENERATED ALWAYS AS (md5(dimensions_json::text)) STORED,
    target_share        NUMERIC(7, 6) NOT NULL,
    min_finalized_count INTEGER NOT NULL DEFAULT 0,
    -- NULL = policy.max_per_campaign 을 그대로 쓴다.
    max_per_campaign    INTEGER,
    priority            INTEGER NOT NULL DEFAULT 100,
    -- NULL = policy.default_workflow_id.
    workflow_id         TEXT,
    prompt_template_id  TEXT REFERENCES synthetic_prompt_templates(template_id) ON DELETE SET NULL,
    status              TEXT NOT NULL DEFAULT 'active',
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT synthetic_coverage_targets_status_check CHECK (status IN ('active', 'disabled')),
    CONSTRAINT synthetic_coverage_targets_share_check
        CHECK (target_share >= 0 AND target_share <= 1),
    CONSTRAINT synthetic_coverage_targets_counts_check
        CHECK (min_finalized_count >= 0 AND (max_per_campaign IS NULL OR max_per_campaign >= 0)),
    CONSTRAINT synthetic_coverage_targets_dimensions_object_check
        CHECK (jsonb_typeof(dimensions_json) = 'object' AND dimensions_json <> '{}'::jsonb),
    -- 축 개수는 policy.balance_dimensions 의 상한(6)과 같다. "선언된 dimension 전부 존재"
    -- 는 테이블 간 조건이라 여기서 못 걸고 activate_policy() 가 본다(헤더 조정 4).
    CONSTRAINT synthetic_coverage_targets_dimensions_arity_check
        CHECK (jsonb_array_length(jsonb_path_query_array(dimensions_json, '$.keyvalue().key')) BETWEEN 1 AND 6),
    CONSTRAINT synthetic_coverage_targets_dimensions_string_check
        CHECK (NOT jsonb_path_exists(dimensions_json, '$.keyvalue() ? (@.value.type() != "string")')),
    CONSTRAINT synthetic_coverage_targets_dimensions_blank_check
        CHECK (NOT jsonb_path_exists(dimensions_json, '$.keyvalue() ? (@.value == "")')),
    CONSTRAINT synthetic_coverage_targets_dimensions_sentinel_check
        CHECK (
            NOT jsonb_path_exists(
                dimensions_json,
                '$.keyvalue() ? (@.value == "deferred" || @.value == "unknown" || @.value == "indeterminate")'
            )
        )
);

COMMENT ON TABLE synthetic_coverage_targets IS
    'policy 의 상호배타적 target cell. dimensions_hash 는 jsonb 정규화 위의 생성 컬럼이라 같은 셀이 두 해시를 가질 수 없다.';
COMMENT ON COLUMN synthetic_coverage_targets.dimensions_json IS
    '축 이름 → 축 값의 flat 문자열 객체. deferred/unknown/indeterminate 는 CHECK 가 거부한다 — 관측 불가는 target 값이 될 수 없다.';

-- §6.2: "(policy_id, dimensions_hash) UNIQUE" — 한 policy 안에서 같은 셀이 두 번 선언되면
-- 같은 이미지가 두 deficit 을 메우게 된다. 셀 중복만큼은 스키마가 막는다.
CREATE UNIQUE INDEX IF NOT EXISTS synthetic_coverage_targets_policy_dims_uniq
    ON synthetic_coverage_targets (policy_id, dimensions_hash);
CREATE INDEX IF NOT EXISTS synthetic_coverage_targets_policy_priority_idx
    ON synthetic_coverage_targets (policy_id, priority, target_id)
    WHERE status = 'active';

-- ─── 4. coverage_snapshots — 계산 시점의 동결 기록 ─────────────────────────────
--
-- policy 단위 4개 카운터가 **target 이 0개여도** "사실이 하나도 없었다" 를 기록한다.
-- cell 행이 하나도 없을 때 blocked 사유를 적을 곳이 여기밖에 없기 때문이다(헤더 참조).
CREATE TABLE IF NOT EXISTS coverage_snapshots (
    snapshot_id                    TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    policy_id                      TEXT NOT NULL REFERENCES synthetic_coverage_policies(policy_id) ON DELETE CASCADE,
    -- 하루 1회 스케줄의 멱등 키(KST 날짜) 또는 'manual-<uuid>'.
    schedule_bucket                TEXT NOT NULL,
    -- policy + targets + 쿼리 버전의 해시. 같은 입력이면 같은 snapshot 이어야 한다.
    input_config_hash              TEXT NOT NULL,
    as_of                          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status                         TEXT NOT NULL DEFAULT 'computing',
    -- 재현용 동결 복사. policy 가 나중에 바뀌어도 이 snapshot 이 무엇으로 계산됐는지 남는다.
    balance_dimensions             TEXT[] NOT NULL,
    horizon_finalized_total        INTEGER NOT NULL,
    max_synthetic_share            NUMERIC(6, 5) NOT NULL,
    -- ↓ "0 vs 관측 불가" 의 policy 단위 증거. 넷은 서로 독립이며 합산해 읽지 말 것.
    eligible_units_total           INTEGER NOT NULL DEFAULT 0,
    context_verified_units_total   INTEGER NOT NULL DEFAULT 0,
    context_unverified_units_total INTEGER NOT NULL DEFAULT 0,
    context_missing_units_total    INTEGER NOT NULL DEFAULT 0,
    reference_pool_total           INTEGER NOT NULL DEFAULT 0,
    blocked_reason                 TEXT,
    notes                          TEXT,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT coverage_snapshots_bucket_input_uniq UNIQUE (policy_id, schedule_bucket, input_config_hash),
    CONSTRAINT coverage_snapshots_status_check CHECK (status IN ('computing', 'complete', 'failed')),
    CONSTRAINT coverage_snapshots_bucket_nonblank_check CHECK (btrim(schedule_bucket) <> ''),
    CONSTRAINT coverage_snapshots_hash_nonblank_check CHECK (btrim(input_config_hash) <> ''),
    CONSTRAINT coverage_snapshots_counts_check
        CHECK (
            eligible_units_total >= 0
            AND context_verified_units_total >= 0
            AND context_unverified_units_total >= 0
            AND context_missing_units_total >= 0
            AND reference_pool_total >= 0
        ),
    CONSTRAINT coverage_snapshots_blocked_reason_check
        CHECK (
            blocked_reason IS NULL
            OR blocked_reason IN (
                'blocked_no_targets',
                'blocked_no_finalized_facts',
                'blocked_context_coverage',
                'blocked_no_reference',
                'blocked_share_cap',
                'blocked_daily_budget',
                'blocked_campaign_cap',
                'blocked_retry_ceiling',
                'blocked_policy_mode'
            )
        )
);

COMMENT ON TABLE coverage_snapshots IS
    'policy 별 계산 시점의 동결 기록. (policy, bucket, input_hash) UNIQUE 로 같은 입력의 재계산을 막는다 — 물리적 immutability 는 트리거로 강제하지 않는다.';
COMMENT ON COLUMN coverage_snapshots.context_missing_units_total IS
    'context 사실 자체가 없는 finalized unit 수. verified/unverified/missing 셋은 서로 다른 사실이며 "0" 하나로 뭉치면 관측 불가가 비율로 둔갑한다.';

CREATE INDEX IF NOT EXISTS coverage_snapshots_policy_asof_idx
    ON coverage_snapshots (policy_id, as_of DESC);

-- ─── 5. coverage_snapshot_cells — snapshot × target cell ───────────────────────
--
-- 설계서 §6.2 의 "real/synthetic finalized count, pending reservation, deficit, block
-- reason, reference availability" 를 전부 **별 열**로 둔다. 그래야 planner 가 한 행만 보고
-- (a) 사실 없음 (b) context 미검증 (c) 진짜 0 을 구분한다.
--
-- ⚠️ `target_id` 에 FK 를 걸지 않는다(헤더 조정 1). snapshot 은 immutable 감사 기록인데
--    CASCADE 면 target 한 줄 삭제가 과거 기록을 지운다. `dimensions_json` 동결 복사 +
--    동일 규칙의 `dimensions_hash` 생성 컬럼이 셀 정체성을 유지한다.
CREATE TABLE IF NOT EXISTS coverage_snapshot_cells (
    snapshot_id                    TEXT NOT NULL REFERENCES coverage_snapshots(snapshot_id) ON DELETE CASCADE,
    target_id                      TEXT NOT NULL,
    dimensions_json                JSONB NOT NULL,
    dimensions_hash                TEXT GENERATED ALWAYS AS (md5(dimensions_json::text)) STORED,
    target_share                   NUMERIC(7, 6) NOT NULL,
    min_finalized_count            INTEGER NOT NULL DEFAULT 0,
    -- §5.2 desired_i / deficit_i / planned_i
    desired_count                  INTEGER NOT NULL DEFAULT 0,
    eligible_finalized_count       INTEGER NOT NULL DEFAULT 0,
    deficit_count                  INTEGER NOT NULL DEFAULT 0,
    planned_count                  INTEGER NOT NULL DEFAULT 0,
    -- 분자·분모의 구성요소를 따로 남긴다. R 과 S 를 합쳐서만 보는 집계는 설계서가 금지한다.
    real_finalized_count           INTEGER NOT NULL DEFAULT 0,   -- R
    synthetic_accepted_count       INTEGER NOT NULL DEFAULT 0,   -- S
    pending_reserved_count         INTEGER NOT NULL DEFAULT 0,   -- P (기존 예약)
    -- ↓ "0 vs 관측 불가" 의 cell 단위 증거. class 단위 값이라 같은 클래스의 셀들은 같다 —
    --   join 없이 한 행만으로 판정 가능하게 한 의도적 비정규화(032 의 asset_id 와 같은 트레이드).
    class_finalized_total          INTEGER NOT NULL DEFAULT 0,
    class_context_verified_total   INTEGER NOT NULL DEFAULT 0,
    class_context_unverified_total INTEGER NOT NULL DEFAULT 0,
    class_context_missing_total    INTEGER NOT NULL DEFAULT 0,
    -- §5.2 min() 의 나머지 항. 각각이 왜 그 값인지 재현 가능해야 하므로 전부 남긴다.
    reference_available_count      INTEGER NOT NULL DEFAULT 0,
    share_headroom_count           INTEGER NOT NULL DEFAULT 0,
    budget_headroom_count          INTEGER NOT NULL DEFAULT 0,
    campaign_cap_count             INTEGER NOT NULL DEFAULT 0,
    block_reason                   TEXT,
    notes                          TEXT,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT coverage_snapshot_cells_pkey PRIMARY KEY (snapshot_id, target_id),
    CONSTRAINT coverage_snapshot_cells_dimensions_object_check
        CHECK (jsonb_typeof(dimensions_json) = 'object' AND dimensions_json <> '{}'::jsonb),
    CONSTRAINT coverage_snapshot_cells_share_check
        CHECK (target_share >= 0 AND target_share <= 1),
    CONSTRAINT coverage_snapshot_cells_counts_nonneg_check
        CHECK (
            min_finalized_count >= 0
            AND desired_count >= 0
            AND eligible_finalized_count >= 0
            AND deficit_count >= 0
            AND planned_count >= 0
            AND real_finalized_count >= 0
            AND synthetic_accepted_count >= 0
            AND pending_reserved_count >= 0
            AND class_finalized_total >= 0
            AND class_context_verified_total >= 0
            AND class_context_unverified_total >= 0
            AND class_context_missing_total >= 0
            AND reference_available_count >= 0
            AND share_headroom_count >= 0
            AND budget_headroom_count >= 0
            AND campaign_cap_count >= 0
        ),
    -- eligible_finalized = R + S (§5.2 "event 와 context 가 모두 finalized/verified 된 고유
    -- image-event 수"). 분해값과 합계가 어긋난 행은 애초에 들어올 수 없다.
    CONSTRAINT coverage_snapshot_cells_eligible_decomposition_check
        CHECK (eligible_finalized_count = real_finalized_count + synthetic_accepted_count),
    -- class 단위 3분할은 class_finalized_total 을 정확히 나눈다. 어긋나면 (a)/(b)/(c) 판정이
    -- 무의미해지므로 스키마가 거부한다.
    CONSTRAINT coverage_snapshot_cells_class_decomposition_check
        CHECK (
            class_finalized_total
            = class_context_verified_total + class_context_unverified_total + class_context_missing_total
        ),
    CONSTRAINT coverage_snapshot_cells_deficit_check
        CHECK (deficit_count = GREATEST(0, desired_count - eligible_finalized_count)),
    CONSTRAINT coverage_snapshot_cells_block_reason_check
        CHECK (
            block_reason IS NULL
            OR block_reason IN (
                'blocked_no_finalized_facts',
                'blocked_context_coverage',
                'blocked_no_reference',
                'blocked_share_cap',
                'blocked_daily_budget',
                'blocked_campaign_cap',
                'blocked_retry_ceiling',
                'blocked_policy_mode'
            )
        ),
    -- blocked cell 은 **부분 생성이 아니라 0** 이다 (§5.2 마지막 문단).
    CONSTRAINT coverage_snapshot_cells_planned_requires_unblocked_check
        CHECK (planned_count = 0 OR block_reason IS NULL),
    -- §5.2 의 min() 을 스키마가 상한으로 강제한다. planner 에 버그가 나도 어느 한 상한을
    -- 넘는 계획은 INSERT 자체가 거부된다.
    CONSTRAINT coverage_snapshot_cells_planned_within_caps_check
        CHECK (
            planned_count <= deficit_count
            AND planned_count <= reference_available_count
            AND planned_count <= share_headroom_count
            AND planned_count <= budget_headroom_count
            AND planned_count <= campaign_cap_count
        )
);

COMMENT ON TABLE coverage_snapshot_cells IS
    'snapshot × target cell. class_finalized_total / class_context_verified_total / class_context_missing_total 셋이 "사실 없음"·"context 미검증"·"진짜 0" 을 구분한다.';
COMMENT ON COLUMN coverage_snapshot_cells.class_context_verified_total IS
    '이 값이 0 인데 class_finalized_total > 0 이면 셀의 0 은 비율이 아니라 관측 불가다 → block_reason=blocked_context_coverage.';
COMMENT ON COLUMN coverage_snapshot_cells.share_headroom_count IS
    '§5.2 (S+P)/(R+S+P)<=a 를 P 에 대해 푼 값. R=0 이면 항상 0 이며, 그것이 오늘 prod 의 상태다.';

-- §6.4.3: (snapshot_id, dimensions_hash).
CREATE INDEX IF NOT EXISTS coverage_snapshot_cells_snapshot_dims_idx
    ON coverage_snapshot_cells (snapshot_id, dimensions_hash);
-- blocked 사유 집계(운영 리포트)와 "오늘 무엇이 막혔나" 조회.
CREATE INDEX IF NOT EXISTS coverage_snapshot_cells_blocked_idx
    ON coverage_snapshot_cells (block_reason)
    WHERE block_reason IS NOT NULL;

-- ─── 6. synthetic_generation_campaigns — snapshot 에서 나온 1회 계획 ────────────
--
-- ⚠️ **F.3 mode gate 의 스키마 구현.** `policy_mode_at_plan` 은 계획 시점의 mode 동결값이며
--    `plan_only`/`disabled` 로 계획된 campaign 은 planned/blocked/cancelled 밖으로 나갈 수
--    없다. policy 의 mode 를 나중에 올려도 **이미 만들어진 plan_only campaign 은 승인·dispatch
--    대상이 되지 않는다** — 재계획을 거쳐야 한다. 설계서 완료 기준의 "승인하지 않은
--    campaign 은 ComfyUI 요청을 0건 생성한다" 를 코드가 아니라 제약으로 보장한다.
CREATE TABLE IF NOT EXISTS synthetic_generation_campaigns (
    campaign_id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    policy_id             TEXT NOT NULL REFERENCES synthetic_coverage_policies(policy_id) ON DELETE CASCADE,
    snapshot_id           TEXT NOT NULL REFERENCES coverage_snapshots(snapshot_id) ON DELETE CASCADE,
    schedule_bucket       TEXT NOT NULL,
    snapshot_input_hash   TEXT NOT NULL,
    -- §6.2 "동일 policy + schedule bucket + snapshot input hash 는 UNIQUE" 를 그대로
    -- idempotency key 로 쓴다. 생성 컬럼이라 애플리케이션이 다르게 조립할 수 없다.
    idempotency_key       TEXT GENERATED ALWAYS AS (
                              policy_id || '|' || schedule_bucket || '|' || snapshot_input_hash
                          ) STORED,
    status                TEXT NOT NULL DEFAULT 'planned',
    policy_mode_at_plan   TEXT NOT NULL,
    scheduled_for         TIMESTAMPTZ,
    planned_task_count    INTEGER NOT NULL DEFAULT 0,
    dispatched_task_count INTEGER NOT NULL DEFAULT 0,
    deferred_task_count   INTEGER NOT NULL DEFAULT 0,
    accepted_task_count   INTEGER NOT NULL DEFAULT 0,
    rejected_task_count   INTEGER NOT NULL DEFAULT 0,
    failed_task_count     INTEGER NOT NULL DEFAULT 0,
    reserved_job_count    INTEGER NOT NULL DEFAULT 0,
    reserved_gpu_seconds  NUMERIC(12, 3) NOT NULL DEFAULT 0,
    blocked_reason        TEXT,
    approved_by           TEXT,
    approved_at           TIMESTAMPTZ,
    closed_at             TIMESTAMPTZ,
    notes                 TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- §6.2 상태 전이: planned→approved→dispatching→awaiting_review→closed 또는 blocked/cancelled.
    CONSTRAINT synthetic_generation_campaigns_status_check
        CHECK (status IN ('planned', 'approved', 'dispatching', 'awaiting_review', 'closed', 'blocked', 'cancelled')),
    CONSTRAINT synthetic_generation_campaigns_mode_value_check
        CHECK (policy_mode_at_plan IN ('disabled', 'plan_only', 'approval_required', 'auto_dispatch')),
    CONSTRAINT synthetic_generation_campaigns_mode_gate_check
        CHECK (
            policy_mode_at_plan NOT IN ('disabled', 'plan_only')
            OR status IN ('planned', 'blocked', 'cancelled')
        ),
    -- 승인 없이는 dispatch 이후 상태로 갈 수 없다. auto_dispatch 도 approved_by 를 남긴다
    -- (예: 'auto_dispatch:<policy_key>') — "누가 승인했나" 가 빈 칸인 실행은 없다.
    CONSTRAINT synthetic_generation_campaigns_approval_check
        CHECK (
            status NOT IN ('approved', 'dispatching', 'awaiting_review', 'closed')
            OR (approved_by IS NOT NULL AND approved_at IS NOT NULL)
        ),
    CONSTRAINT synthetic_generation_campaigns_blocked_check
        CHECK (status <> 'blocked' OR blocked_reason IS NOT NULL),
    CONSTRAINT synthetic_generation_campaigns_closed_check
        CHECK (status <> 'closed' OR closed_at IS NOT NULL),
    CONSTRAINT synthetic_generation_campaigns_counts_check
        CHECK (
            planned_task_count >= 0
            AND dispatched_task_count >= 0
            AND deferred_task_count >= 0
            AND accepted_task_count >= 0
            AND rejected_task_count >= 0
            AND failed_task_count >= 0
            AND reserved_job_count >= 0
            AND reserved_gpu_seconds >= 0
        ),
    CONSTRAINT synthetic_generation_campaigns_blocked_reason_check
        CHECK (
            blocked_reason IS NULL
            OR blocked_reason IN (
                'blocked_no_targets',
                'blocked_no_finalized_facts',
                'blocked_context_coverage',
                'blocked_no_reference',
                'blocked_share_cap',
                'blocked_daily_budget',
                'blocked_campaign_cap',
                'blocked_retry_ceiling',
                'blocked_policy_mode'
            )
        )
);

COMMENT ON TABLE synthetic_generation_campaigns IS
    'snapshot 1개에서 나온 1회 계획. policy_mode_at_plan 이 plan_only/disabled 면 승인·dispatch 상태로 전이할 수 없다(F.3 게이트).';
COMMENT ON COLUMN synthetic_generation_campaigns.policy_mode_at_plan IS
    '계획 시점 mode 동결값. policy mode 를 나중에 올려도 이미 만들어진 plan_only campaign 은 dispatch 되지 않는다 — 재계획이 필요하다.';

CREATE UNIQUE INDEX IF NOT EXISTS synthetic_generation_campaigns_idempotency_uniq
    ON synthetic_generation_campaigns (idempotency_key);
-- §6.4.3: (policy_id, status, scheduled_for).
CREATE INDEX IF NOT EXISTS synthetic_generation_campaigns_policy_status_idx
    ON synthetic_generation_campaigns (policy_id, status, scheduled_for);
CREATE INDEX IF NOT EXISTS synthetic_generation_campaigns_snapshot_idx
    ON synthetic_generation_campaigns (snapshot_id);

-- ─── 7. synthetic_generation_tasks — campaign 의 출력 1장 ──────────────────────
--
-- dispatch 와 GenAI execution 을 잇는 행. `(campaign_id, reference_id, workflow_id, seed)`
-- UNIQUE 가 중복 제출을 막는다(§6.3).
--
-- ⚠️ `deferred` 는 반드시 사유를 남긴다 (`deferred_reason` NOT NULL). Phase F.2 의
--    "GPU lease, Comfy queue, 일/주 budget, reference availability 중 하나라도 부족하면
--    task 는 deferred 로 남는다" 에서 **어느 것 때문인지 모르는 defer** 는 다음 tick 에
--    같은 실패를 반복할 뿐이다. 사유 없는 defer 는 스키마가 거부한다.
CREATE TABLE IF NOT EXISTS synthetic_generation_tasks (
    task_id                TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    campaign_id            TEXT NOT NULL REFERENCES synthetic_generation_campaigns(campaign_id) ON DELETE CASCADE,
    -- soft reference (헤더 조정 1 — snapshot cell 과 같은 이유).
    target_id              TEXT NOT NULL,
    dimensions_hash        TEXT NOT NULL,
    -- ⚠️ **FK 없음** (헤더 조정 5 — cascade 충돌). dispatcher 는 매 tick 후보 뷰로 재확인한다.
    reference_id           TEXT,
    reference_bucket       TEXT,
    reference_key          TEXT,
    workflow_id            TEXT NOT NULL,
    template_id            TEXT REFERENCES synthetic_prompt_templates(template_id) ON DELETE SET NULL,
    template_version       INTEGER,
    template_content_hash  TEXT,
    workflow_sha256        TEXT,
    model_manifest_sha256  TEXT,
    rendered_prompt_hash   TEXT,
    negative_prompt_hash   TEXT,
    seed                   BIGINT NOT NULL,
    mask_sha256            TEXT,
    state                  TEXT NOT NULL DEFAULT 'planned',
    deferred_reason        TEXT,
    priority               INTEGER NOT NULL DEFAULT 100,
    retry_count            INTEGER NOT NULL DEFAULT 0,
    max_retries            INTEGER NOT NULL DEFAULT 2,
    -- batch 는 job 에서 파생되는 값이라 FK 를 겹쳐 걸지 않는다 (헤더 조정 5 의 같은 이유).
    genai_batch_id         TEXT,
    genai_job_id           TEXT REFERENCES genai_jobs(job_id) ON DELETE SET NULL,
    gpu_lease_token        TEXT,
    gpu_seconds            NUMERIC(12, 3),
    output_image_id        TEXT REFERENCES image_metadata(image_id) ON DELETE SET NULL,
    error_message          TEXT,
    dispatched_at          TIMESTAMPTZ,
    generated_at           TIMESTAMPTZ,
    reviewed_at            TIMESTAMPTZ,
    accepted_at            TIMESTAMPTZ,
    closed_at              TIMESTAMPTZ,
    notes                  TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT synthetic_generation_tasks_dedup_uniq UNIQUE (campaign_id, reference_id, workflow_id, seed),
    CONSTRAINT synthetic_generation_tasks_state_check
        CHECK (
            state IN (
                'planned', 'ready', 'deferred', 'dispatched',
                'awaiting_review', 'accepted', 'rejected', 'failed', 'cancelled'
            )
        ),
    CONSTRAINT synthetic_generation_tasks_deferred_reason_check
        CHECK (state <> 'deferred' OR btrim(COALESCE(deferred_reason, '')) <> ''),
    CONSTRAINT synthetic_generation_tasks_deferred_reason_domain_check
        CHECK (
            deferred_reason IS NULL
            OR deferred_reason IN (
                'gpu_lease_busy',
                'comfy_queue_full',
                'daily_budget_exhausted',
                'reference_unavailable',
                'campaign_not_approved',
                'endpoint_unavailable',
                'retry_ceiling'
            )
        ),
    CONSTRAINT synthetic_generation_tasks_seed_check CHECK (seed >= 0),
    CONSTRAINT synthetic_generation_tasks_retry_check
        CHECK (retry_count >= 0 AND max_retries >= 0 AND retry_count <= max_retries + 1),
    CONSTRAINT synthetic_generation_tasks_gpu_seconds_check
        CHECK (gpu_seconds IS NULL OR gpu_seconds >= 0),
    CONSTRAINT synthetic_generation_tasks_dispatched_at_check
        CHECK (state <> 'dispatched' OR dispatched_at IS NOT NULL),
    CONSTRAINT synthetic_generation_tasks_accepted_at_check
        CHECK (state <> 'accepted' OR accepted_at IS NOT NULL),
    CONSTRAINT synthetic_generation_tasks_workflow_check CHECK (btrim(workflow_id) <> '')
);

COMMENT ON TABLE synthetic_generation_tasks IS
    'campaign 의 출력 1장. (campaign, reference, workflow, seed) UNIQUE 로 중복 제출을 막고, deferred 는 반드시 사유를 남긴다.';
COMMENT ON COLUMN synthetic_generation_tasks.reference_id IS
    'ON DELETE SET NULL — 원본 이미지 삭제가 인제스트를 깨지 않게 하기 위함. 유효성은 매 tick v_generation_reference_candidates 로 재확인해야 한다.';
COMMENT ON COLUMN synthetic_generation_tasks.deferred_reason IS
    '사유 없는 defer 는 다음 tick 에 같은 실패를 반복할 뿐이라 CHECK 로 금지한다.';

-- §6.4.3: (campaign_id, state, priority).
CREATE INDEX IF NOT EXISTS synthetic_generation_tasks_campaign_state_idx
    ON synthetic_generation_tasks (campaign_id, state, priority);
-- dispatcher 가 매 tick 1건만 고르는 방향(우선순위 → 생성순).
CREATE INDEX IF NOT EXISTS synthetic_generation_tasks_dispatchable_idx
    ON synthetic_generation_tasks (priority, created_at)
    WHERE state IN ('ready', 'deferred');
CREATE INDEX IF NOT EXISTS synthetic_generation_tasks_reference_idx
    ON synthetic_generation_tasks (reference_id)
    WHERE reference_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS synthetic_generation_tasks_genai_job_idx
    ON synthetic_generation_tasks (genai_job_id)
    WHERE genai_job_id IS NOT NULL;

-- ─── 8. generation_quality_reviews — 생성 전용 보조 감사 ───────────────────────
--
-- ⚠️ 이것은 **LS 최종 annotation 을 대체하지 않는다**(설계서 §6.3). coverage 의 S 는
--    언제나 032 의 `coverage_unit_facts`(=LS finalized)에서만 센다. 여기 'accepted' 는
--    "생성물이 쓸 만하다" 는 판정일 뿐 분자에 들어가는 근거가 아니다.
--
-- 'accepted' 는 하위 판정과 모순될 수 없다: event fidelity 와 scene preservation 이 pass 고
-- artifact 가 none/minor 여야만 accepted 다. 'rejected' 는 reason code 없이 남길 수 없다 —
-- §F.4 의 "reason code 로 축적" 이 빈 배열이면 다음 회차가 배울 것이 없다.
CREATE TABLE IF NOT EXISTS generation_quality_reviews (
    review_id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    task_id            TEXT NOT NULL REFERENCES synthetic_generation_tasks(task_id) ON DELETE CASCADE,
    decision           TEXT NOT NULL,
    event_fidelity     TEXT,
    context_preserved  TEXT,
    artifact_severity  TEXT,
    duplicate_result   TEXT,
    reason_codes       TEXT[] NOT NULL DEFAULT '{}',
    reviewer           TEXT NOT NULL,
    -- LS 앱 DB 는 다른 인스턴스라 FK 없음 (032 헤더 2 / 028 al_selections 선례).
    ls_task_id         INTEGER,
    ls_project_id      INTEGER,
    coverage_image_id  TEXT REFERENCES image_metadata(image_id) ON DELETE SET NULL,
    reviewed_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT generation_quality_reviews_decision_check
        CHECK (decision IN ('accepted', 'rejected', 'rework')),
    CONSTRAINT generation_quality_reviews_reviewer_check CHECK (btrim(reviewer) <> ''),
    CONSTRAINT generation_quality_reviews_event_fidelity_check
        CHECK (event_fidelity IS NULL OR event_fidelity IN ('pass', 'fail', 'unclear')),
    CONSTRAINT generation_quality_reviews_context_preserved_check
        CHECK (context_preserved IS NULL OR context_preserved IN ('pass', 'fail', 'unclear')),
    CONSTRAINT generation_quality_reviews_artifact_severity_check
        CHECK (artifact_severity IS NULL OR artifact_severity IN ('none', 'minor', 'major', 'blocking')),
    CONSTRAINT generation_quality_reviews_duplicate_result_check
        CHECK (duplicate_result IS NULL OR duplicate_result IN ('unique', 'near_duplicate', 'duplicate', 'not_evaluated')),
    CONSTRAINT generation_quality_reviews_accepted_consistency_check
        CHECK (
            decision <> 'accepted'
            OR (
                event_fidelity = 'pass'
                AND context_preserved = 'pass'
                AND artifact_severity IN ('none', 'minor')
            )
        ),
    CONSTRAINT generation_quality_reviews_rejected_reason_check
        CHECK (decision <> 'rejected' OR cardinality(reason_codes) > 0),
    CONSTRAINT generation_quality_reviews_reason_codes_check
        CHECK (array_position(reason_codes, NULL::TEXT) IS NULL)
);

COMMENT ON TABLE generation_quality_reviews IS
    'task 별 생성 전용 보조 감사. LS 최종 annotation 을 대체하지 않으며 coverage 의 S 는 언제나 coverage_unit_facts(LS finalized)에서만 센다.';
COMMENT ON COLUMN generation_quality_reviews.reason_codes IS
    'rejected 는 빈 배열로 남길 수 없다 — 사유가 없으면 다음 회차가 배울 것이 없다(§F.4).';

CREATE INDEX IF NOT EXISTS generation_quality_reviews_task_idx
    ON generation_quality_reviews (task_id, reviewed_at DESC);
CREATE INDEX IF NOT EXISTS generation_quality_reviews_decision_idx
    ON generation_quality_reviews (decision, reviewed_at DESC);

-- ─── 9. generation_budget_events — append-only 예산 원장 ───────────────────────
--
-- 부호는 숫자가 아니라 `event_type` 이 진다 (`job_count`/`gpu_seconds` 는 항상 >= 0).
-- "음수 reserve" 같은 표현이 가능하면 같은 사실이 두 모양으로 기록돼 집계가 갈린다.
--
-- 일/주 cap 재집계는 아래 `v_generation_budget_daily` 가 한다:
--   committed = (reserve − release) + consume
-- 실패·반려 시 release 를 남기면 예약이 정확히 반환된다(§6.3).
CREATE TABLE IF NOT EXISTS generation_budget_events (
    event_id    BIGSERIAL PRIMARY KEY,
    policy_id   TEXT NOT NULL REFERENCES synthetic_coverage_policies(policy_id) ON DELETE CASCADE,
    campaign_id TEXT REFERENCES synthetic_generation_campaigns(campaign_id) ON DELETE SET NULL,
    task_id     TEXT REFERENCES synthetic_generation_tasks(task_id) ON DELETE SET NULL,
    event_type  TEXT NOT NULL,
    job_count   INTEGER NOT NULL DEFAULT 0,
    gpu_seconds NUMERIC(12, 3) NOT NULL DEFAULT 0,
    reason      TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT generation_budget_events_type_check
        CHECK (event_type IN ('reserve', 'consume', 'release')),
    CONSTRAINT generation_budget_events_amounts_check
        CHECK (job_count >= 0 AND gpu_seconds >= 0)
);

COMMENT ON TABLE generation_budget_events IS
    'append-only 예산 원장. 부호는 event_type 이 지며 금액 컬럼은 항상 >= 0 이다 — 같은 사실이 두 모양으로 기록되는 것을 막는다.';

CREATE INDEX IF NOT EXISTS generation_budget_events_policy_time_idx
    ON generation_budget_events (policy_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS generation_budget_events_campaign_idx
    ON generation_budget_events (campaign_id)
    WHERE campaign_id IS NOT NULL;

-- ─── 10. v_synthetic_coverage_reservations — §5.2 의 P ─────────────────────────
--
-- "예약 상태도 P 에 포함해 동시에 여러 planner tick 이 같은 여유를 초과 배정하지 못하게
-- 한다"(§5.2). 그 P 를 task 상태에서 파생한다 — 별도 카운터를 두면 task 와 어긋난다.
--
-- 종료된 campaign(closed/cancelled/blocked)의 task 는 여유를 잡고 있지 않으므로 제외한다.
-- grain 은 `(policy_id, dimensions_hash)` 다: 셀 정체성은 target_id 가 아니라 해시이며,
-- policy 버전이 바뀌어도 같은 셀의 예약은 같은 해시 아래 모인다.
CREATE OR REPLACE VIEW v_synthetic_coverage_reservations AS
SELECT
    c.policy_id                                                      AS policy_id,
    t.dimensions_hash                                                AS dimensions_hash,
    COUNT(*)                                                         AS reserved_count,
    COUNT(*) FILTER (WHERE t.state = 'planned')                      AS planned_count,
    COUNT(*) FILTER (WHERE t.state = 'ready')                        AS ready_count,
    COUNT(*) FILTER (WHERE t.state = 'deferred')                     AS deferred_count,
    COUNT(*) FILTER (WHERE t.state = 'dispatched')                   AS dispatched_count,
    COUNT(*) FILTER (WHERE t.state = 'awaiting_review')              AS awaiting_review_count
FROM synthetic_generation_tasks t
JOIN synthetic_generation_campaigns c ON c.campaign_id = t.campaign_id
WHERE t.state IN ('planned', 'ready', 'deferred', 'dispatched', 'awaiting_review')
  AND c.status NOT IN ('closed', 'cancelled', 'blocked')
GROUP BY c.policy_id, t.dimensions_hash;

COMMENT ON VIEW v_synthetic_coverage_reservations IS
    '§5.2 의 P. task 상태에서 파생하므로 별도 카운터와 어긋날 수 없다. 종료 campaign 의 task 는 여유를 잡지 않는다.';

-- ─── 11. v_generation_budget_daily — 일 단위 cap 재집계 ────────────────────────
--
-- 날짜 버킷은 **KST** 다 (`schedule_bucket` 과 같은 기준). 서버 TZ 는 UTC 라
-- `AT TIME ZONE 'Asia/Seoul'` 이 필요하며, 이 표현식은 STABLE 이라 생성 컬럼으로는 쓸 수
-- 없어 뷰에서 계산한다.
--
-- committed = 미반환 예약 + 실제 소비. planner 의 remaining_daily_budget 은
-- `max(0, policy.daily_job_budget − committed_jobs)` 로 구한다.
CREATE OR REPLACE VIEW v_generation_budget_daily AS
SELECT
    e.policy_id                                                                    AS policy_id,
    (e.occurred_at AT TIME ZONE 'Asia/Seoul')::date                                AS budget_date,
    COALESCE(SUM(e.job_count) FILTER (WHERE e.event_type = 'reserve'), 0)
      - COALESCE(SUM(e.job_count) FILTER (WHERE e.event_type = 'release'), 0)      AS outstanding_reserved_jobs,
    COALESCE(SUM(e.job_count) FILTER (WHERE e.event_type = 'consume'), 0)          AS consumed_jobs,
    COALESCE(SUM(e.job_count) FILTER (WHERE e.event_type = 'reserve'), 0)
      - COALESCE(SUM(e.job_count) FILTER (WHERE e.event_type = 'release'), 0)
      + COALESCE(SUM(e.job_count) FILTER (WHERE e.event_type = 'consume'), 0)      AS committed_jobs,
    COALESCE(SUM(e.gpu_seconds) FILTER (WHERE e.event_type = 'reserve'), 0)
      - COALESCE(SUM(e.gpu_seconds) FILTER (WHERE e.event_type = 'release'), 0)    AS outstanding_reserved_gpu_seconds,
    COALESCE(SUM(e.gpu_seconds) FILTER (WHERE e.event_type = 'consume'), 0)        AS consumed_gpu_seconds,
    COALESCE(SUM(e.gpu_seconds) FILTER (WHERE e.event_type = 'reserve'), 0)
      - COALESCE(SUM(e.gpu_seconds) FILTER (WHERE e.event_type = 'release'), 0)
      + COALESCE(SUM(e.gpu_seconds) FILTER (WHERE e.event_type = 'consume'), 0)    AS committed_gpu_seconds
FROM generation_budget_events e
GROUP BY e.policy_id, (e.occurred_at AT TIME ZONE 'Asia/Seoul')::date;

COMMENT ON VIEW v_generation_budget_daily IS
    'KST 일 단위 예산 재집계. committed = (reserve - release) + consume 이며 planner 의 remaining_daily_budget 입력이다.';

COMMIT;
