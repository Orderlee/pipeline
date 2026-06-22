-- 010_caption_trgm_index.sql — 캡션 키워드(lexical) 검색용 pg_trgm 확장 + GIN 인덱스
--
-- 하이브리드 캡션 검색(search_captions mode='keyword'|'hybrid')의 lexical 절반은
-- labels.caption_text 에 대한 pg_trgm(ILIKE 부분일치 + trigram 유사도) 매칭을 사용한다.
-- 이 확장/인덱스가 없으면 keyword 절반이 빈 결과(graceful) → hybrid 가 semantic-only 로 강등.
-- ⚠️ pg_trgm-gated (_OPTIONAL_MIGRATIONS). DO $$ block 미사용. CONCURRENTLY 미사용(txn 내, 정식배포 시 짧은 lock 허용).
--    prod 는 2026-06-22 CREATE INDEX CONCURRENTLY 로 무중단 in-place 선적용됨 — 이 파일은 IF NOT EXISTS 로 재현/정렬용.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS labels_caption_trgm
  ON labels USING gin (caption_text gin_trgm_ops);

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM pg_extension WHERE extname='pg_trgm'
-- @ASSERT_AFTER: SELECT 1 FROM pg_indexes WHERE indexname='labels_caption_trgm'
