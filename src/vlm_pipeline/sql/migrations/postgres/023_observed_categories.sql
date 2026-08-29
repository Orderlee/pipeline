-- 023_observed_categories.sql — 정본 밖 카테고리 원문을 판단 유예 원장에 축적한다.
--
-- Gemini 이벤트 스키마는 category 를 필수로 요구하지만 labels 11,978행에는 이를 담는 컬럼이
-- 없어서, 지금까지의 값은 vlm-labels/<source>/events/*.json 에만 남았다. dispatch 이력에서도
-- 10개 문자열 중 etc, safety_equipment, 연기(smoke), 화재(fire), 쓰러짐(falldown) 5개가
-- 13개 canonical 정본 밖에서 관측됐다.
--
-- 미상 값은 한 경로에서는 검증 없이 SAM3 text prompt 로 흘러 검출 어휘를 오염시키고, 다른
-- 경로에서는 Label Studio prediction 정규화 중 조용히 사라졌다(실측 515 events 중 18개만
-- 생존). 이 테이블은 그 값을 canonical 정본에 자동 편입하지 않고, 기계가 낸 값을 바깥
-- 공백만 제거한 형태와 출처로 모아 사람이 승격·매핑·거절을 결정하도록 하는 판단 유예
-- 원장이다. 관측 기록 실패는 본 파이프라인을 막지 않아야 하며, 이 migration 은
-- seed/backfill 없이 빈 테이블만 만든다.
--
-- 애플리케이션은 raw_value 의 바깥 공백만 제거한다. 저장된 값의 대소문자·내부 공백·그 밖의
-- 문자는 그대로 보존한다. category label 이라는 도메인 경계를 명시하기 위해 최대 200자로
-- 제한하며, 이를 넘는 값은 애플리케이션이 truncate 하지 않고 WARNING 후 관측 대상에서 제외한다.
-- source_units 는 최대 32개 distinct sample 만 유지하며 observation_count 는 그 이후에도 증가한다.
--
-- 런타임 관측 UPSERT (psycopg2 named placeholders). raw_value 는 바깥 공백 제거와 200자
-- 상한 검사를 마친 값이다. conflict 시 사람이 관리하는 status/mapped_to/notes 는 갱신하지 않는다.
--
-- INSERT INTO observed_categories AS oc (
--     source,
--     raw_value,
--     observation_count,
--     source_units,
--     first_seen,
--     last_seen
-- )
-- VALUES (
--     %(source)s,
--     %(raw_value)s,
--     1,
--     CASE
--         WHEN %(source_unit)s::TEXT ~ '[^[:space:]]'
--         THEN ARRAY[%(source_unit)s]::TEXT[]
--         ELSE '{}'::TEXT[]
--     END,
--     statement_timestamp(),
--     statement_timestamp()
-- )
-- ON CONFLICT (source, raw_value) DO UPDATE
-- SET observation_count = oc.observation_count + 1,
--     source_units = CASE
--         WHEN cardinality(EXCLUDED.source_units) = 0
--           OR array_position(oc.source_units, (EXCLUDED.source_units)[1]) IS NOT NULL
--           OR cardinality(oc.source_units) >= 32
--         THEN oc.source_units
--         ELSE array_append(oc.source_units, (EXCLUDED.source_units)[1])
--     END,
--     last_seen = GREATEST(oc.last_seen, EXCLUDED.last_seen)
-- RETURNING oc.source, oc.raw_value;
--
-- @ASSERT_AFTER: SELECT to_regclass('public.observed_categories') IS NOT NULL
-- @ASSERT_AFTER: SELECT NOT EXISTS (SELECT 1 FROM (VALUES ('source'), ('raw_value'), ('observation_count'), ('source_units'), ('first_seen'), ('last_seen'), ('status'), ('mapped_to'), ('notes')) AS required(column_name) WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns AS actual WHERE actual.table_schema = 'public' AND actual.table_name = 'observed_categories' AND actual.column_name = required.column_name))
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint AS c WHERE c.conname = 'observed_categories_pkey' AND c.conrelid = to_regclass('public.observed_categories') AND c.contype = 'p' AND c.convalidated AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (source, raw_value)')
-- @ASSERT_AFTER: SELECT NOT EXISTS (SELECT 1 FROM (VALUES ('observed_categories_source_check'), ('observed_categories_raw_value_nonblank_check'), ('observed_categories_raw_value_length_check'), ('observed_categories_observation_count_positive_check'), ('observed_categories_source_units_cap_check'), ('observed_categories_source_units_no_null_check'), ('observed_categories_source_units_one_dimensional_check'), ('observed_categories_seen_order_check'), ('observed_categories_status_check')) AS required(conname) WHERE NOT EXISTS (SELECT 1 FROM pg_constraint AS c WHERE c.conname = required.conname AND c.conrelid = to_regclass('public.observed_categories') AND c.contype = 'c' AND c.convalidated))
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint AS c WHERE c.conname = 'observed_categories_mapped_to_fkey' AND c.conrelid = to_regclass('public.observed_categories') AND c.confrelid = to_regclass('public.label_classes') AND c.contype = 'f' AND c.confupdtype = 'c' AND c.confdeltype = 'a' AND c.convalidated)
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_index AS i WHERE i.indexrelid = to_regclass('public.observed_categories_promotion_review_idx') AND i.indrelid = to_regclass('public.observed_categories') AND i.indisvalid AND i.indisready AND i.indpred IS NOT NULL)

BEGIN;

CREATE TABLE IF NOT EXISTS observed_categories (
    source             TEXT NOT NULL,
    raw_value          TEXT NOT NULL,
    observation_count BIGINT NOT NULL DEFAULT 1,
    source_units       TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    first_seen         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status             TEXT NOT NULL DEFAULT 'observed',
    mapped_to          TEXT,
    notes              TEXT,
    CONSTRAINT observed_categories_pkey PRIMARY KEY (source, raw_value),
    CONSTRAINT observed_categories_source_check CHECK (
        source IN ('gemini_event', 'dispatch_request', 'sam3_label', 'prompt_bank')
    ),
    CONSTRAINT observed_categories_raw_value_nonblank_check CHECK (
        raw_value ~ '[^[:space:]]'
    ),
    CONSTRAINT observed_categories_raw_value_length_check CHECK (
        length(raw_value) BETWEEN 1 AND 200
    ),
    CONSTRAINT observed_categories_observation_count_positive_check CHECK (
        observation_count > 0
    ),
    CONSTRAINT observed_categories_source_units_cap_check CHECK (
        cardinality(source_units) <= 32
    ),
    CONSTRAINT observed_categories_source_units_no_null_check CHECK (
        array_position(source_units, NULL) IS NULL
    ),
    CONSTRAINT observed_categories_source_units_one_dimensional_check CHECK (
        cardinality(source_units) = 0 OR array_ndims(source_units) = 1
    ),
    CONSTRAINT observed_categories_seen_order_check CHECK (
        last_seen >= first_seen
    ),
    CONSTRAINT observed_categories_status_check CHECK (
        status IN ('observed', 'candidate', 'promoted', 'rejected')
    ),
    CONSTRAINT observed_categories_mapped_to_fkey
        FOREIGN KEY (mapped_to) REFERENCES label_classes(canonical)
        ON UPDATE CASCADE
        ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS observed_categories_promotion_review_idx
    ON observed_categories (observation_count DESC, last_seen DESC)
    WHERE status = 'observed' AND cardinality(source_units) >= 2;

COMMIT;
