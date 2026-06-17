-- 006_image_embeddings.sql — pgvector 임베딩 저장 (frame + detection)
--
-- PE-Core-L14-336 (1024-d) 임베딩을 native PG(pgvector)에 저장.
-- entity_type='frame'|'detection' 의 polymorphic 단일 테이블. model_name 으로 모델 구분
-- (현재 단일 모델 → 단일 HNSW; 추후 모델 추가 시 partial-by-model index 로 확장).
--
-- ⚠️ 이 마이그레이션은 pgvector(`vector`) 확장이 있는 PG 이미지에서만 적용 가능.
--    prod 는 파생 PG 이미지 배포 후 적용. _REQUIRED_MIGRATIONS 에는 넣지 않는다
--    (pgvector 미적용 환경 부팅 실패 방지).
-- 주의: DO $$ block 미사용 (multi-statement DO 일부만 적용되는 runner quirk 회피).

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS image_embeddings (
  embedding_id  TEXT PRIMARY KEY,
  entity_type   TEXT NOT NULL,
  entity_id     TEXT NOT NULL,
  image_id      TEXT NOT NULL,
  model_name    TEXT NOT NULL,
  dim           INTEGER NOT NULL,
  embedding     vector(1024) NOT NULL,
  source_bucket TEXT,
  source_key    TEXT,
  bbox          JSONB,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_id, model_name)
);

CREATE INDEX IF NOT EXISTS image_embeddings_entity_idx ON image_embeddings (entity_type, model_name);
CREATE INDEX IF NOT EXISTS image_embeddings_image_idx  ON image_embeddings (image_id);
CREATE INDEX IF NOT EXISTS image_embeddings_hnsw
  ON image_embeddings USING hnsw (embedding vector_cosine_ops);

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM pg_extension WHERE extname='vector'
-- @ASSERT_AFTER: SELECT 1 FROM information_schema.tables WHERE table_name='image_embeddings'
