-- 009_video_embeddings.sql — video entity_type partial HNSW 인덱스
--
-- image_embeddings 테이블에 entity_type='video' row 가 추가됨 (video-level embedding).
-- 테이블 스키마 변경 없음 — entity_type='video', entity_id=source_asset_id(video) 로 사용.
-- method A(frame_pool): model_name='facebook/PE-Core-L14-336/framepool', L2-normalized mean of frame embeddings.
-- method B(video_model): 미래 전용 video encoder 이름 사용 (현재 미구현).
--
-- partial HNSW 인덱스를 추가해 entity_type='video' 검색이 다른 entity_type 후보를
-- 오염시키지 않도록 한다 (008 과 동일한 이유).
--
-- ⚠️ pgvector-gated (_OPTIONAL_MIGRATIONS). DO $$ block 미사용.

BEGIN;

CREATE INDEX IF NOT EXISTS image_embeddings_hnsw_video
  ON image_embeddings USING hnsw (embedding vector_cosine_ops) WHERE entity_type = 'video';

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM pg_indexes WHERE indexname='image_embeddings_hnsw_video'
