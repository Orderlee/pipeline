-- 032_coverage_facts.sql — synthetic coverage 의 "사실·reference 계층" (설계서 §6.1 / §6.4).
--
-- 설계 정본: docs/exec-plans/active/comfyui-local-genai-pipeline-plan.md
--   Phase A0 §6.1 (사실·reference 테이블) + §6.4.1~2 (두 뷰).
--   §6.2 정책·snapshot·campaign 계층과 §6.3 task·provenance·품질 계층은 **이 파일 범위 밖**이다
--   (별도 migration). 그래서 이 파일은 그쪽 테이블을 일절 참조하지 않는다 — 참조했다면
--   파일명 정렬 순서상 뒤에 오는 migration 을 앞에서 요구하게 되어 러너가 거기서 죽는다.
--
-- 이 테이블들은 **제어·감사 계층**이다. 라벨의 정본은 여전히
-- `raw_files` / `image_metadata` / `labels` / `image_labels` / `image_label_annotations` +
-- MinIO 의 events JSON · COCO JSON 이다. 여기 있는 행은 전부 그 정본의 투영이며,
-- 정본과 어긋나면 **정본이 이긴다**. planner 가 셀 수 있는 형태로 옮겨 적은 것뿐이다.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- 실측 (2026-09-21, prod `vlm_pipeline`, 읽기 전용 psql). 설계서 상단 경고의 실제 값:
--
--   video_metadata                131,932 행
--     environment_type / daynight_type 실값  18,046  (env_method='places365_cuda')
--       outdoor/day 13,354 · indoor/day 4,571 · outdoor/night 103 · indoor/night 18
--     weather / subject_scale / occlusion_state 실값 **0**
--     camera_angle 실값 871 (설계서 §3: 라벨러 bin 붕괴로 사용 불가 판정)
--     angle_method='deferred' 113,710 · env_method='deferred' 113,710
--   image_metadata                598,474 행 (부모 영상이 env+daynight 을 가진 것 9,785)
--   image_labels                  finalized 288 / auto_generated 474,110
--   image_label_annotations       1,558 행 — category 는 patient 1,219 · person 339 **뿐**
--   labels                        finalized 1,369 / auto_generated 11,978, category 컬럼 없음
--   v_finalized_labels            bbox 1,558 + timestamp 1,338 (caption 0)
--   label_classes                 15 canonical (022 의 13 + 026 의 intrusion·no_harness)
--
--   ⚠️ **결정적 실측**: finalized bbox 를 가진 이미지 248장 중 부모 영상이 environment_type 을
--      가진 것은 **0장**이다. 즉 (a) finalized event 사실과 (b) 검증된 context 의 **교집합이
--      현재 공집합**이다. `v_eligible_coverage_units` 가 오늘 0행을 내는 것은 버그가 아니라
--      이 사실의 정확한 반영이다. Phase A0 완료 기준("셋 중 하나라도 0이면 생성 금지")이
--      지금 그대로 걸린다.
--
--   ⚠️ **canonical event class 의 소스가 아직 없다.** 설계서 A0.1 이 경고한 그대로,
--      finalized bbox 는 객체 클래스(patient/person)이지 이벤트 클래스가 아니고 `labels` 에는
--      category 컬럼 자체가 없다. 그래서 `fact_source='ls_image_event'` 투영은 LS form/API 에
--      event taxonomy 가 구조화된 뒤에야 행을 만들 수 있다. **0행을 "비율 0"으로 읽지 마라** —
--      이 스키마가 그 구분을 보장하는 방식은 아래 "0 vs 관측불가" 항목 참조.
-- ─────────────────────────────────────────────────────────────────────────────
--
-- 설계서와 어긋나 **실측에 맞춘 지점** (검토 대상):
--
-- 1. `label_id` 에 FK 를 걸지 않는다. `src/gemini/ls_sync_db.py:216` 이 LS 동기화마다
--    `DELETE FROM labels WHERE labels_key = %s` 후 재삽입하고, 같은 파일 :369 가
--    `DELETE FROM image_label_annotations WHERE image_label_id = %s` 후 재삽입한다. 즉
--    `labels.label_id` 와 `annotation_id` 는 **재검수마다 바뀌는 비영속 키**다. CASCADE 를
--    걸면 재동기화가 coverage 사실을 조용히 지우고, RESTRICT 를 걸면 LS 동기화가 깨진다.
--    → soft reference(FK 없음)로 두고, 영속 자연키 `source_labels_key`(MinIO object key) +
--      `source_event_index` 를 함께 적는다. `(labels_key, event_index)` 는 005 의 UNIQUE 라
--      DELETE+INSERT 를 건너 살아남는다.
--
-- 2. `ls_task_id` 도 FK 없음 — Label Studio 앱 DB 는 `pipeline-postgres-1` 의 `airflow` DB 로
--    **다른 인스턴스**다(CLAUDE.md §Label Studio). 028 의 `al_selections.ls_task_id integer`
--    선례와 동일하게 FK 없는 integer 로 둔다.
--
-- 3. image/asset FK 는 **반드시 ON DELETE CASCADE** 다. `postgres_ingest_raw.py:270-274` 의
--    재적재 경로가 image_metadata → video_metadata → raw_files 순으로 실제 DELETE 를 하고
--    `cleanup_duplicate_assets.py` / `purge_pipeline_data.py` 도 같은 일을 한다. RESTRICT 였다면
--    coverage 사실이 쌓인 순간부터 **인제스트가 FK 위반으로 깨진다**. 의미상으로도 이미지가
--    사라지면 그 이미지에 대한 coverage 사실은 무의미하다.
--
-- 4. `coverage_context_facts` 의 6축은 `video_metadata` 의 6개 컬럼과 같은 이름·같은 어휘를
--    쓰되 **값을 복제하지 않는다**. verified 로 승격된 것만 여기 들어온다. 실측상 지금
--    투영 가능한 것은 environment_type/daynight_type 2축 18,046 asset 뿐이고 weather 는
--    한 행도 못 채운다 — 컬럼은 만들되 전부 NULL 이 되고 `verified_axes` 가 그 사실을 드러낸다.
--
-- 5. `camera_registry` / `asset_camera_map` 은 만들되 **채우는 경로를 만들지 않는다**
--    (설계서 §6.1 "첫 pilot 에서 불필요하면 비활성"). `source_unit_name` 을 카메라 키로
--    쓰는 것은 이 레포가 반복해서 경고한 오류라 `assignment_source` CHECK 의 허용값에서
--    아예 뺐다 — 스키마가 거부한다.
--
-- 6. `v_generation_reference_candidates` 는 설계서가 말한 "target cell 과 join" 을 **하지
--    않는다.** `synthetic_coverage_targets` 는 §6.2(다른 파일) 소관이라 이 시점에 존재하지
--    않는다. 대신 reference × 해석된 context 를 내보내고 cell 매칭은 planner 쿼리가 한다.
--    "최근 사용 횟수"도 §6.3 의 `synthetic_generation_tasks` 대신 pool 행 자신의
--    `use_count`/`last_used_at` 로 제공한다(dispatcher 가 갱신). "duplicate risk" 는
--    임베딩이 필요해 이 계층에서 계산하지 않는다 — 계산하는 척하지 않는다.
--
-- 7. 설계서 §6.4.3 의 `(reference_id, status)` partial index 는 만들지 않았다 —
--    `reference_id` 가 PK 라 선행 컬럼으로 둔 인덱스는 중복이다. 실제로 쓰이는 방향
--    (승인·비홀드아웃 pool 을 context 로 뒤지는 것)으로 partial index 를 걸었다.
--
-- 8. `coverage_unit_facts.asset_id` 는 `image_metadata.source_asset_id` 와 같은 값이다
--    (join 1 hop 절약용 비정규화). 복합 FK 로 강제하려면 image_metadata 에 UNIQUE 인덱스를
--    추가해야 하는데, 598k 행 정본 테이블에 부팅 중 쓰기 락을 거는 것은 이 파일의 권한 밖이다.
--    → 대신 `tests/integration/test_coverage_facts_migration_032.py` 가 이 정합성을 검증한다.
--      **스키마가 아니라 테스트가 지키는 불변식**이라는 점을 알고 쓸 것.
--
-- "0 vs 관측불가" 를 구분하는 방식 (설계서 §6.2 의 요구를 이 계층에서 지키는 법):
--   `v_eligible_coverage_units` 는 **context 로 걸러내지 않는다.** finalized unit 을 전부
--   한 행씩 내보내고 context 상태를 `context_fact_id`(NULL = 사실 자체가 없음),
--   `context_verification_status`, `context_verified`(NULL 없음, 항상 t/f) 컬럼으로 노출한다.
--   그래서 planner 는 한 뷰에서 (a) 셀의 eligible count 와 (b) context 미관측 count 를 따로
--   셀 수 있다. context 미검증을 뷰에서 조용히 걸러냈다면 그 둘이 똑같이 "0" 으로 보였을 것이다
--   — 이 레포가 반복해 겪은 "부재에 기댄 안전" 패턴 그대로다.
--
-- 이 파일이 **보장하지 않는 것**:
--   * 투영 job 을 만들지 않는다. 테이블은 비어 있는 채로 배포된다.
--   * 라벨 정본을 읽어 자동으로 채우지 않는다 (트리거·view materialization 없음).
--   * canonical event class 의 부재를 해결하지 않는다 — LS form/API mapping 이 선행이다.
--   * `holdout_excluded` 가 실제 eval holdout 과 일치하는지 검사하지 않는다. 그 판정은
--     승인자의 책임이고 스키마는 **fail-closed 기본값(TRUE=사용 금지)** 만 보장한다.
--
-- Forward-only, idempotent, DO 블록 미사용(러너의 multi-DO 부분적용 quirk 회피).
-- 타임스탬프는 TIMESTAMPTZ (022/023 선례). prod/staging/CI 모두 서버 TZ = UTC 라
-- TIMESTAMP 정본 컬럼에서 투영할 때 값 이동이 없다.
--
-- @ASSERT_AFTER 는 **이 파일이 만든 객체의 존재**만 본다. 행 수·기본값을 걸지 않는 이유는
-- 두 가지다: (a) 매 부팅마다 실행되므로 큰 테이블 스캔은 부팅 비용이고, (b) 나중에 정당한
-- 변경이 생기면 러너가 거기서 죽어 **이후 모든 마이그레이션이 정지**한다
-- ([[project_postgres_migration_runner_quirk]]).
--
-- @ASSERT_AFTER: SELECT to_regclass('public.coverage_unit_facts') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.coverage_context_facts') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.camera_registry') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.asset_camera_map') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.generation_reference_pool') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.v_eligible_coverage_units') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.v_generation_reference_candidates') IS NOT NULL
-- 아래 FK 단언은 개수를 박지 않고 **불변식**으로 쓴다: 정본 테이블을 가리키는 FK 가 하나라도
-- 살아 있고(>=6), 그중 ON DELETE CASCADE 가 아닌 것이 하나도 없어야 한다. CASCADE 가 아니면
-- 인제스트 재적재 경로(postgres_ingest_raw.py:270-274)가 FK 위반으로 깨진다. 개수를 상수로
-- 박으면 나중에 FK 를 하나 더 추가하는 정당한 변경이 부팅을 죽인다.
-- @ASSERT_AFTER: SELECT (COUNT(*) >= 6 AND COUNT(*) FILTER (WHERE confdeltype <> 'c') = 0) FROM pg_constraint WHERE contype = 'f' AND conrelid IN ('coverage_unit_facts'::regclass, 'coverage_context_facts'::regclass, 'generation_reference_pool'::regclass, 'asset_camera_map'::regclass) AND confrelid IN ('image_metadata'::regclass, 'raw_files'::regclass)
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'coverage_unit_facts_review_status_check' AND conrelid = 'coverage_unit_facts'::regclass AND contype = 'c')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'generation_reference_pool_safe_region_required_check' AND conrelid = 'generation_reference_pool'::regclass AND contype = 'c')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'coverage_context_facts' AND indexname = 'coverage_context_facts_image_subject_uniq')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'coverage_context_facts' AND indexname = 'coverage_context_facts_asset_subject_uniq')

BEGIN;

-- ─── 1. coverage_unit_facts — 사람이 확정한 image-event 단위 ────────────────────
--
-- grain = (image_id, canonical_class, fact_source). 설계서 §6.1 그대로이며 대리키를 두지
-- 않았다 — 같은 이미지·같은 클래스가 두 경로(bbox 투영 / event 투영)로 들어오는 것은
-- 정상이고, 같은 경로로 두 번 들어오는 것은 중복이다. PK 가 정확히 그 선을 긋는다.
--
-- review_status 는 ('reviewed','finalized') 만 허용한다. auto_generated 라벨은 **이 테이블에
-- 들어올 수 없다** — 설계서의 "모델 파생 라벨을 부족분 계산에 쓰지 않는다"와 MLOps 불변식의
-- "자기학습 금지"를 스키마로 못 박는 것. 소비자 쿼리가 WHERE 를 빠뜨려도 오염되지 않는다.
CREATE TABLE IF NOT EXISTS coverage_unit_facts (
    image_id            TEXT NOT NULL REFERENCES image_metadata(image_id) ON DELETE CASCADE,
    canonical_class     TEXT NOT NULL REFERENCES label_classes(canonical) ON UPDATE CASCADE,
    fact_source         TEXT NOT NULL,
    asset_id            TEXT NOT NULL REFERENCES raw_files(asset_id) ON DELETE CASCADE,
    origin_kind         TEXT NOT NULL,
    review_status       TEXT NOT NULL,
    finalized_at        TIMESTAMPTZ,
    -- soft references (FK 없음 — 위 헤더 1·2 참조)
    label_id            TEXT,
    source_labels_key   TEXT,
    source_event_index  INTEGER,
    ls_task_id          INTEGER,
    ls_project_id       INTEGER,
    -- synthetic 출처. genai_jobs 삭제 시 사실 자체는 남기되 링크만 끊는다.
    genai_job_id        TEXT REFERENCES genai_jobs(job_id) ON DELETE SET NULL,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT coverage_unit_facts_pkey PRIMARY KEY (image_id, canonical_class, fact_source),
    -- ls_bbox        : image_label_annotations(finalized image_labels 경유) — 현재 유일하게 행이 나오는 경로
    -- ls_event       : labels 의 video event 를 프레임에 투영 (프레임 매핑 선행 필요)
    -- ls_image_event : LS 이미지 폼의 event taxonomy (폼/API mapping 미구현 — 현재 0행)
    -- manual_backfill: 운영자가 근거를 남기고 손으로 넣은 사실
    CONSTRAINT coverage_unit_facts_fact_source_check
        CHECK (fact_source IN ('ls_bbox', 'ls_event', 'ls_image_event', 'manual_backfill')),
    CONSTRAINT coverage_unit_facts_origin_kind_check
        CHECK (origin_kind IN ('real', 'synthetic')),
    CONSTRAINT coverage_unit_facts_review_status_check
        CHECK (review_status IN ('reviewed', 'finalized')),
    CONSTRAINT coverage_unit_facts_finalized_at_check
        CHECK (review_status <> 'finalized' OR finalized_at IS NOT NULL),
    CONSTRAINT coverage_unit_facts_event_index_check
        CHECK (source_event_index IS NULL OR source_event_index >= 0)
);

COMMENT ON TABLE coverage_unit_facts IS
    'Coverage planner 가 세는 image-event 단위 투영. 정본은 image_labels/image_label_annotations/labels + MinIO JSON.';
COMMENT ON COLUMN coverage_unit_facts.origin_kind IS
    'real | synthetic. synthetic 을 real 과 합쳐서만 보는 집계는 설계서가 금지한다.';
COMMENT ON COLUMN coverage_unit_facts.label_id IS
    'soft reference. labels/image_label_annotations 는 LS 재동기화마다 DELETE+INSERT 되므로 FK 를 걸 수 없다.';

-- 셀 카운팅용. finalized 만 세므로 partial 로 좁힌다.
CREATE INDEX IF NOT EXISTS coverage_unit_facts_class_origin_idx
    ON coverage_unit_facts (canonical_class, origin_kind)
    WHERE review_status = 'finalized';
-- asset 단위 context 해석(뷰의 LATERAL)과 asset 기준 감사용.
CREATE INDEX IF NOT EXISTS coverage_unit_facts_asset_idx
    ON coverage_unit_facts (asset_id);
-- LS 피드백 루프(검수 결과 → 사실 갱신) 역조회.
CREATE INDEX IF NOT EXISTS coverage_unit_facts_ls_task_idx
    ON coverage_unit_facts (ls_task_id)
    WHERE ls_task_id IS NOT NULL;

-- ─── 2. coverage_context_facts — subject 당 현재 context 1행 ────────────────────
--
-- subject 는 image 또는 asset 이다(설계서 §6.1 "image 또는 asset당 1 context version").
-- NULL 은 UNIQUE 에서 서로 다른 값 취급이라 단일 UNIQUE(image_id, asset_id) 로는 "당 1행" 을
-- 보장하지 못한다 → subject_type 별 partial UNIQUE 인덱스 2개로 강제한다.
--
-- "1 version" 이므로 재분류는 append 가 아니라 **UPDATE** 다. 이력이 필요해지면 별도
-- history 테이블을 새 migration 으로 추가할 것 — 여기서 grain 을 바꾸면 뷰가 중복 카운트한다.
--
-- ⚠️ 축 값에 'deferred'/'unknown'/'indeterminate' 를 문자열로 넣는 것을 CHECK 로 막는다.
--    `video_metadata` 는 미분류를 `env_method='deferred'` 마커로 표현하는데, 그 마커가 축 값
--    자리로 새어 들어오면 planner 가 "deferred 라는 환경" 을 하나의 셀로 세게 된다. 관측 못 한
--    축은 **NULL** 이고, NULL 이 아닌데 verified 가 아니면 `verified_axes` 가 그 사실을 말한다.
--    'not_applicable' 은 막지 않는다 — 실내 장면의 weather 처럼 **관측된 비해당**이다.
CREATE TABLE IF NOT EXISTS coverage_context_facts (
    context_fact_id     TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    subject_type        TEXT NOT NULL,
    image_id            TEXT REFERENCES image_metadata(image_id) ON DELETE CASCADE,
    asset_id            TEXT REFERENCES raw_files(asset_id) ON DELETE CASCADE,
    -- 6축. 이름·어휘는 video_metadata 와 동일하게 맞췄다(017 참조).
    -- 실측: pilot 가능 축은 environment_type/daynight_type 둘뿐이다.
    environment_type    TEXT,
    daynight_type       TEXT,
    weather             TEXT,   -- 실측 prod 0행 — 컬럼만 있고 당분간 전부 NULL
    camera_angle        TEXT,   -- 설계서 §3: 라벨러 bin 붕괴로 현재 사용 불가
    subject_scale       TEXT,   -- 실측 prod 0행
    occlusion_state     TEXT,   -- 실측 prod 0행
    context_source      TEXT NOT NULL,   -- places365_cuda | gemini_video_scene | inherited_from_reference | human | ...
    model_version       TEXT,
    verification_status TEXT NOT NULL DEFAULT 'unverified',
    -- verified 가 **어느 축까지** 커버하는지. 한 행 안에서 축마다 신뢰도가 다르다는 실측
    -- (env/daynight 은 실값, weather 는 전무)을 숨기지 않으려고 둔 컬럼이다.
    verified_axes       TEXT[] NOT NULL DEFAULT '{}',
    verified_by         TEXT,
    verified_at         TIMESTAMPTZ,
    -- §5.3 "생성 이미지의 context 는 reference 에서 상속된다". soft reference(FK 없음) —
    -- reference 가 은퇴·삭제돼도 상속 사실 자체는 감사 기록으로 남아야 한다.
    inherited_from_reference_id TEXT,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT coverage_context_facts_subject_type_check
        CHECK (subject_type IN ('image', 'asset')),
    CONSTRAINT coverage_context_facts_subject_binding_check
        CHECK (
            (subject_type = 'image' AND image_id IS NOT NULL AND asset_id IS NULL)
            OR (subject_type = 'asset' AND asset_id IS NOT NULL AND image_id IS NULL)
        ),
    CONSTRAINT coverage_context_facts_source_check
        CHECK (
            btrim(context_source) <> ''
            AND context_source NOT IN ('deferred', 'unknown', 'indeterminate')
        ),
    CONSTRAINT coverage_context_facts_verification_status_check
        CHECK (verification_status IN ('unverified', 'inherited', 'verified', 'rejected')),
    CONSTRAINT coverage_context_facts_verified_at_check
        CHECK (verification_status <> 'verified' OR verified_at IS NOT NULL),
    CONSTRAINT coverage_context_facts_verified_axes_domain_check
        CHECK (
            verified_axes <@ ARRAY[
                'environment_type', 'daynight_type', 'weather',
                'camera_angle', 'subject_scale', 'occlusion_state'
            ]::TEXT[]
            AND array_position(verified_axes, NULL::TEXT) IS NULL
        ),
    -- 축을 하나도 명시하지 않은 'verified' 는 아무것도 말하지 않는다 → 금지.
    CONSTRAINT coverage_context_facts_verified_axes_nonempty_check
        CHECK (verification_status <> 'verified' OR cardinality(verified_axes) > 0),
    CONSTRAINT coverage_context_facts_axis_sentinel_check
        CHECK (
            COALESCE(environment_type, '') NOT IN ('deferred', 'unknown', 'indeterminate')
            AND COALESCE(daynight_type, '') NOT IN ('deferred', 'unknown', 'indeterminate')
            AND COALESCE(weather, '') NOT IN ('deferred', 'unknown', 'indeterminate')
            AND COALESCE(camera_angle, '') NOT IN ('deferred', 'unknown', 'indeterminate')
            AND COALESCE(subject_scale, '') NOT IN ('deferred', 'unknown', 'indeterminate')
            AND COALESCE(occlusion_state, '') NOT IN ('deferred', 'unknown', 'indeterminate')
        )
);

COMMENT ON TABLE coverage_context_facts IS
    'subject(image 또는 asset) 당 현재 context 1행. video_metadata 의 값을 검증 상태와 함께 투영한 것이며 정본이 아니다.';
COMMENT ON COLUMN coverage_context_facts.verified_axes IS
    'verification_status=verified 가 실제로 커버하는 축 이름들. 비어 있으면 verified 가 될 수 없다.';

-- "subject 당 1 context version" 을 NULL-safe 하게 강제. 조회 인덱스 역할도 겸한다.
CREATE UNIQUE INDEX IF NOT EXISTS coverage_context_facts_image_subject_uniq
    ON coverage_context_facts (image_id)
    WHERE subject_type = 'image';
CREATE UNIQUE INDEX IF NOT EXISTS coverage_context_facts_asset_subject_uniq
    ON coverage_context_facts (asset_id)
    WHERE subject_type = 'asset';
-- pilot 2축 셀 카운팅.
CREATE INDEX IF NOT EXISTS coverage_context_facts_pilot_axes_idx
    ON coverage_context_facts (environment_type, daynight_type)
    WHERE verification_status = 'verified';

-- ─── 3. camera_registry / asset_camera_map — 만들되 비활성 ─────────────────────
--
-- 설계서 §6.1: "첫 pilot 에서 불필요하면 비활성이다." 이 파일은 **테이블만** 만들고 채우는
-- 경로(투영 job·백필·센서)를 일절 만들지 않는다. 두 테이블은 배포 후에도 0행으로 남는다.
--
-- ⚠️ `source_unit_name` 을 camera_id 로 쓰는 것은 금지다 — 처리 단계·여러 카메라가 섞인 값이고
--    설계서 §3·§A0.2 가 명시적으로 배제했다. `assignment_source` 의 허용값에서 아예 빼서
--    스키마가 거부하게 했다(주석이 아니라 CHECK 로).
CREATE TABLE IF NOT EXISTS camera_registry (
    camera_id      TEXT PRIMARY KEY,
    site           TEXT,
    display_label  TEXT,
    -- holdout leakage 방지용 그룹키. al_frames.group_key 와 같은 역할이며, 카메라보다
    -- 넓은 단위(세션 등)가 필요할 수 있다 — certbody sitej 는 카메라 홀드아웃조차 28% 누수했다.
    holdout_group  TEXT,
    status         TEXT NOT NULL DEFAULT 'inactive',
    notes          TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT camera_registry_camera_id_nonblank_check CHECK (btrim(camera_id) <> ''),
    CONSTRAINT camera_registry_status_check CHECK (status IN ('inactive', 'active', 'retired'))
);

COMMENT ON TABLE camera_registry IS
    '안정 camera_id 원장. 첫 pilot 에서는 비활성 — 채우는 코드 경로가 의도적으로 없다.';

CREATE TABLE IF NOT EXISTS asset_camera_map (
    asset_id          TEXT PRIMARY KEY REFERENCES raw_files(asset_id) ON DELETE CASCADE,
    camera_id         TEXT NOT NULL REFERENCES camera_registry(camera_id) ON UPDATE CASCADE,
    assignment_source TEXT NOT NULL,
    confidence        DOUBLE PRECISION,
    assigned_by       TEXT,
    assigned_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes             TEXT,
    -- 'source_unit_name' 은 의도적으로 허용값에 없다 (위 경고).
    CONSTRAINT asset_camera_map_assignment_source_check
        CHECK (assignment_source IN ('operator', 'exif_device', 'stream_url', 'filename_rule')),
    CONSTRAINT asset_camera_map_confidence_check
        CHECK (confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0))
);

COMMENT ON COLUMN asset_camera_map.assignment_source IS
    'source_unit_name 은 허용값이 아니다 — 처리 단계/복수 카메라가 섞인 값이라 카메라 키가 될 수 없다.';

CREATE INDEX IF NOT EXISTS asset_camera_map_camera_idx
    ON asset_camera_map (camera_id);

-- ─── 4. generation_reference_pool — 승인된 배경 풀 ──────────────────────────────
--
-- §5.3: 자동화는 배경을 새로 발명하지 않고 **이미 검증된 CCTV frame** 을 고른다.
-- 그래서 이 테이블의 기본 상태는 전부 "사용 불가" 다:
--   status        기본 'draft'  → 뷰가 'approved' 만 통과시킨다
--   holdout_excluded 기본 TRUE  → TRUE = **이 reference 를 생성에 쓰지 않는다**
--                                 (holdout/누수 위험으로 제외됨). 승인자가 홀드아웃과
--                                 겹치지 않음을 확인해야만 FALSE 가 된다.
--   requires_safe_region 기본 TRUE → safe_region_json 없이는 INSERT 자체가 거부된다
--
-- safe region 을 workflow 이름으로 판별(예: id 에 'inpaint' 포함)하지 않는 이유: workflow id 는
-- 버전이 붙는다(`sdxl-inpaint-cctv-v1`). 이름 규칙에 기대면 v2 에서 **조용히 검사가 사라진다**.
-- boolean 플래그 + CHECK 가 명시적이고 버전에 독립적이다.
CREATE TABLE IF NOT EXISTS generation_reference_pool (
    reference_id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    image_id             TEXT NOT NULL REFERENCES image_metadata(image_id) ON DELETE CASCADE,
    asset_id             TEXT NOT NULL REFERENCES raw_files(asset_id) ON DELETE CASCADE,
    -- context 가 사라지면 링크만 끊고 행은 남긴다. RESTRICT 면 인제스트 재적재 경로가 막힌다.
    -- NULL 이 된 reference 는 뷰에서 context_verified=false 가 되어 자동으로 후보에서 빠진다.
    context_fact_id      TEXT REFERENCES coverage_context_facts(context_fact_id) ON DELETE SET NULL,
    -- raw path 가 아니라 trusted MinIO object 로 materialize 한다(설계서 §6.1).
    reference_bucket     TEXT NOT NULL DEFAULT 'vlm-raw',
    reference_key        TEXT NOT NULL,
    allowed_workflows    TEXT[] NOT NULL,
    requires_safe_region BOOLEAN NOT NULL DEFAULT TRUE,
    safe_region_json     JSONB,
    holdout_excluded     BOOLEAN NOT NULL DEFAULT TRUE,
    holdout_reason       TEXT,
    status               TEXT NOT NULL DEFAULT 'draft',
    approved_by          TEXT,
    approved_at          TIMESTAMPTZ,
    valid_until          TIMESTAMPTZ,
    -- §6.4.2 의 "최근 사용 횟수". §6.3 task 테이블(다른 파일)에 의존하지 않도록 pool 행이
    -- 자기 카운터를 들고 있는다. dispatcher 가 갱신한다.
    use_count            INTEGER NOT NULL DEFAULT 0,
    last_used_at         TIMESTAMPTZ,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- reference image 당 1행 (설계서 §6.1 grain).
    CONSTRAINT generation_reference_pool_image_uniq UNIQUE (image_id),
    CONSTRAINT generation_reference_pool_status_check
        CHECK (status IN ('draft', 'approved', 'suspended', 'retired')),
    CONSTRAINT generation_reference_pool_approval_check
        CHECK (status <> 'approved' OR (approved_by IS NOT NULL AND approved_at IS NOT NULL)),
    CONSTRAINT generation_reference_pool_workflows_check
        CHECK (
            cardinality(allowed_workflows) BETWEEN 1 AND 16
            AND array_position(allowed_workflows, NULL::TEXT) IS NULL
        ),
    CONSTRAINT generation_reference_pool_safe_region_type_check
        CHECK (safe_region_json IS NULL OR jsonb_typeof(safe_region_json) IN ('object', 'array')),
    CONSTRAINT generation_reference_pool_safe_region_required_check
        CHECK (NOT requires_safe_region OR safe_region_json IS NOT NULL),
    CONSTRAINT generation_reference_pool_reference_key_check
        CHECK (btrim(reference_key) <> ''),
    CONSTRAINT generation_reference_pool_use_count_check
        CHECK (use_count >= 0)
);

COMMENT ON TABLE generation_reference_pool IS
    '자동 생성이 배경으로 쓸 수 있는 승인 CCTV frame 풀. 기본 상태는 draft + holdout_excluded=true 로 사용 불가다.';
COMMENT ON COLUMN generation_reference_pool.holdout_excluded IS
    'TRUE = 이 reference 를 생성에서 제외한다(평가 holdout/누수 위험). 기본값 TRUE 는 fail-closed 이며, 승인자의 명시적 판단으로만 FALSE 가 된다.';
COMMENT ON COLUMN generation_reference_pool.requires_safe_region IS
    'TRUE 면 safe_region_json 없이는 행이 존재할 수 없다. inpaint 계열 workflow 는 반드시 TRUE 여야 한다.';

CREATE INDEX IF NOT EXISTS generation_reference_pool_status_idx
    ON generation_reference_pool (status, holdout_excluded);
-- 실제 후보 조회 방향: 승인·비홀드아웃 풀을 context 로 좁힌다.
CREATE INDEX IF NOT EXISTS generation_reference_pool_candidate_idx
    ON generation_reference_pool (context_fact_id)
    WHERE status = 'approved' AND NOT holdout_excluded;
CREATE INDEX IF NOT EXISTS generation_reference_pool_asset_idx
    ON generation_reference_pool (asset_id);

-- ─── 5. §6.4.1 v_eligible_coverage_units ───────────────────────────────────────
--
-- planner 의 유일한 count source. **finalized unit 만** 내보내되 context 로는 거르지 않는다
-- (위 헤더 "0 vs 관측불가" 참조). context 는 image 단위 사실을 우선하고 없으면 부모 asset
-- 단위 사실로 떨어진다 — 실측상 context 는 전부 `video_metadata`(asset 단위)에서 오므로
-- image 단위만 봤다면 이 뷰는 영원히 context 없는 행만 냈을 것이다.
--
-- ⚠️ `CREATE OR REPLACE VIEW` 는 컬럼 이름/순서/타입 변경을 허용하지 않는다. 컬럼을
--    추가·재정렬하려면 새 migration 에서 DROP VIEW 후 재생성할 것(012 와 같은 제약).
CREATE OR REPLACE VIEW v_eligible_coverage_units AS
SELECT
    f.image_id                                             AS image_id,
    f.asset_id                                             AS asset_id,
    f.canonical_class                                      AS canonical_class,
    f.fact_source                                          AS fact_source,
    f.origin_kind                                          AS origin_kind,
    f.finalized_at                                         AS finalized_at,
    f.ls_task_id                                           AS ls_task_id,
    f.genai_job_id                                         AS genai_job_id,
    ctx.context_fact_id                                    AS context_fact_id,
    ctx.subject_type                                       AS context_subject_type,
    ctx.environment_type                                   AS environment_type,
    ctx.daynight_type                                      AS daynight_type,
    ctx.weather                                            AS weather,
    ctx.camera_angle                                       AS camera_angle,
    ctx.subject_scale                                      AS subject_scale,
    ctx.occlusion_state                                    AS occlusion_state,
    ctx.context_source                                     AS context_source,
    ctx.verification_status                                AS context_verification_status,
    COALESCE(ctx.verified_axes, '{}'::TEXT[])              AS context_verified_axes,
    -- NULL 을 내보내지 않는다. WHERE 절에서 NULL 이 false 로 접히며 조용히 새는 사고를 막는다.
    COALESCE(ctx.verification_status = 'verified', FALSE)  AS context_verified,
    acm.camera_id                                          AS camera_id
FROM coverage_unit_facts f
LEFT JOIN LATERAL (
    SELECT c.*
      FROM coverage_context_facts c
     WHERE (c.subject_type = 'image' AND c.image_id = f.image_id)
        OR (c.subject_type = 'asset' AND c.asset_id = f.asset_id)
     ORDER BY (c.subject_type = 'image') DESC
     LIMIT 1
) ctx ON TRUE
LEFT JOIN asset_camera_map acm ON acm.asset_id = f.asset_id
WHERE f.review_status = 'finalized';

COMMENT ON VIEW v_eligible_coverage_units IS
    'planner 의 유일한 count source. finalized unit 전부를 내보내고 context 검증 여부는 컬럼으로 노출한다 — 미검증을 뷰에서 걸러내면 "비율 0" 과 "관측 불가" 가 구분되지 않는다.';

-- ─── 6. §6.4.2 v_generation_reference_candidates ───────────────────────────────
--
-- 승인·유효·비홀드아웃·safe-region 충족 reference 만 통과시키고, context 는 컬럼으로 노출한다
-- (context 미검증 reference 를 여기서 제거하지 않는 이유는 위와 같다 — planner 가 "후보 0" 과
-- "context 미검증 때문에 0" 을 구분할 수 있어야 한다).
--
-- 내보내지 **않는** 것: target cell 매칭(§6.2 테이블 부재), duplicate risk(임베딩 필요).
-- 둘 다 계산하는 척하지 않고 소비자에게 남긴다.
CREATE OR REPLACE VIEW v_generation_reference_candidates AS
SELECT
    r.reference_id                                       AS reference_id,
    r.image_id                                           AS image_id,
    r.asset_id                                           AS asset_id,
    r.reference_bucket                                   AS reference_bucket,
    r.reference_key                                      AS reference_key,
    r.allowed_workflows                                  AS allowed_workflows,
    r.requires_safe_region                               AS requires_safe_region,
    (r.safe_region_json IS NOT NULL)                     AS has_safe_region,
    r.safe_region_json                                   AS safe_region_json,
    r.valid_until                                        AS valid_until,
    r.use_count                                          AS use_count,
    r.last_used_at                                       AS last_used_at,
    c.context_fact_id                                    AS context_fact_id,
    c.environment_type                                   AS environment_type,
    c.daynight_type                                      AS daynight_type,
    c.weather                                            AS weather,
    c.camera_angle                                       AS camera_angle,
    c.subject_scale                                      AS subject_scale,
    c.occlusion_state                                    AS occlusion_state,
    c.context_source                                     AS context_source,
    c.verification_status                                AS context_verification_status,
    COALESCE(c.verified_axes, '{}'::TEXT[])              AS context_verified_axes,
    COALESCE(c.verification_status = 'verified', FALSE)  AS context_verified,
    acm.camera_id                                        AS camera_id,
    cr.holdout_group                                     AS camera_holdout_group
FROM generation_reference_pool r
LEFT JOIN coverage_context_facts c ON c.context_fact_id = r.context_fact_id
LEFT JOIN asset_camera_map acm ON acm.asset_id = r.asset_id
LEFT JOIN camera_registry cr ON cr.camera_id = acm.camera_id
WHERE r.status = 'approved'
  AND NOT r.holdout_excluded
  AND (r.valid_until IS NULL OR r.valid_until > CURRENT_TIMESTAMP)
  -- CHECK 와 중복이지만 뷰만 읽는 사람에게 규칙을 드러낸다.
  AND (NOT r.requires_safe_region OR r.safe_region_json IS NOT NULL);

COMMENT ON VIEW v_generation_reference_candidates IS
    '생성에 쓸 수 있는 승인 reference. target cell 매칭과 duplicate risk 는 계산하지 않는다 — 각각 §6.2 테이블과 임베딩이 필요하다.';

COMMIT;
