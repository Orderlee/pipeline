-- 008_embedding_partial_indexes.sql — entity_type 별 partial HNSW 인덱스
--
-- 문제: 단일 통합 HNSW(006 의 image_embeddings_hnsw)는 filtered 쿼리
--   (WHERE entity_type='frame' ORDER BY embedding <=> q)에서 다른 entity_type 후보를
--   먼저 잡고 필터로 제거 → 0건 반환. 특히 **cross-modal**(텍스트 쿼리 → frame): 텍스트
--   벡터는 caption 임베딩과 가까워 top-N 후보가 전부 caption → frame 필터 후 빈 결과.
--   (2026-06-16 staging 트라이얼에서 실측 확인)
-- 해결: entity_type 별 partial HNSW 인덱스 → planner 가 해당 type 인덱스만 타 contamination 제거.
-- ⚠️ pgvector-gated (_OPTIONAL_MIGRATIONS). DO $$ block 미사용.

BEGIN;

CREATE INDEX IF NOT EXISTS image_embeddings_hnsw_frame
  ON image_embeddings USING hnsw (embedding vector_cosine_ops) WHERE entity_type = 'frame';

CREATE INDEX IF NOT EXISTS image_embeddings_hnsw_caption
  ON image_embeddings USING hnsw (embedding vector_cosine_ops) WHERE entity_type = 'caption';

-- 통합 인덱스는 filtered 검색에 부적합 → 제거 (모든 검색은 entity_type 필터 사용).
DROP INDEX IF EXISTS image_embeddings_hnsw;

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM pg_indexes WHERE indexname='image_embeddings_hnsw_frame'
-- @ASSERT_AFTER: SELECT 1 FROM pg_indexes WHERE indexname='image_embeddings_hnsw_caption'
