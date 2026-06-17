-- 007_caption_embeddings.sql — caption 임베딩 지원 컬럼 추가
--
-- image_embeddings 테이블을 캡션(텍스트) 임베딩도 수용하도록 확장.
-- entity_type='caption' 행은 image_id=NULL, asset_id+text_content 사용.
-- entity_type='frame'|'detection' 행은 기존 schema 그대로 유지.
--
-- ⚠️ 006_image_embeddings.sql 이 적용된 pgvector 환경에서만 의미 있음.
--    _OPTIONAL_MIGRATIONS 에 등록하고 동일 precondition(vector extension) 사용.
-- 주의: DO $$ block 미사용 (multi-statement DO 일부만 적용되는 runner quirk 회피).

BEGIN;

ALTER TABLE image_embeddings ALTER COLUMN image_id DROP NOT NULL;

ALTER TABLE image_embeddings ADD COLUMN IF NOT EXISTS asset_id TEXT;

ALTER TABLE image_embeddings ADD COLUMN IF NOT EXISTS text_content TEXT;

CREATE INDEX IF NOT EXISTS image_embeddings_asset_idx ON image_embeddings (asset_id);

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM information_schema.columns WHERE table_name='image_embeddings' AND column_name='asset_id'
-- @ASSERT_AFTER: SELECT 1 FROM information_schema.columns WHERE table_name='image_embeddings' AND column_name='text_content'
-- @ASSERT_AFTER: SELECT 1 FROM information_schema.columns WHERE table_name='image_embeddings' AND column_name='image_id' AND is_nullable='YES'
