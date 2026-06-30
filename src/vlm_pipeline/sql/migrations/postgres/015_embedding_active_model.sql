-- 015_embedding_active_model.sql — AL/검색이 읽는 '활성 임베딩 model_name' 포인터 (design §8.1-A / §5.3).
--
-- 기존: DEFAULT_MODEL 상수 (docker/analysis/fiftyone_pgvector.py:23, defs/embed/assets.py:19).
-- 변경: 단일행 PG 설정으로 빼서 PE-Core 승격(H4)이 포인터를 원자 UPDATE 로 전환 가능하게.
-- scope='frame_search' = AL 큐 + text→image 검색이 읽는 활성 frame model_name.
-- 기본값(seed) = 현 stock model_name → 마이그레이션만으로는 동작 무변경 (미승격).
--
-- pgvector-gated (_OPTIONAL_MIGRATIONS): image_embeddings 가 있는 환경에서만 의미.
-- 비-pgvector prod 부팅을 깨지 않도록 008/009 과 동일한 전제조건으로 등재 (H2.4).
-- DO $$ block 미사용 (runner multi-statement DO 한계 회피). 모든 DDL 멱등.
BEGIN;

CREATE TABLE IF NOT EXISTS embedding_active_model (
    scope       TEXT        NOT NULL,
    model_name  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT,
    CONSTRAINT embedding_active_model_pk        PRIMARY KEY (scope),
    CONSTRAINT embedding_active_model_scope_chk CHECK (scope IN ('frame_search'))
);

-- 활성 포인터 기본값 = stock model_name. 재적용 시 덮어쓰지 않음 (승격이 바꾼 값 보존).
INSERT INTO embedding_active_model (scope, model_name, updated_by)
VALUES ('frame_search', 'facebook/PE-Core-L14-336', 'migration_015')
ON CONFLICT (scope) DO NOTHING;

COMMIT;

-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'embedding_active_model_scope_chk' AND conrelid = 'embedding_active_model'::regclass)
-- 시드 행 존재만 단언 — value-equality 금지. _verify_assertions 는 매 부팅 재검증하므로(이미 적용된
-- migration 도), 승격(promote_pe_core)이 포인터를 @ft-<ver> 로 바꾼 뒤 equality 단언은 영구 부팅실패를
-- 일으킨다 (Codex review BUG1). non-null 존재 단언이 seed 적용을 확인하면서 승격값도 허용.
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM embedding_active_model WHERE scope='frame_search' AND model_name IS NOT NULL)
