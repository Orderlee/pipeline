-- 021_prompt_embedding_index.sql — 뱅크 문장 벡터용 partial HNSW.
--
-- 019 로 문장 텍스트가 DB 정본이 된 뒤, 고유 문장(content_hash 단위)의 PE-Core 벡터를
-- image_embeddings(entity_type='prompt') 에 흡수한다. 그래야 Phase 2 창이 "이 문장의
-- 최근접 프레임"을 SQL `<=>` 한 방으로 조회한다.
--
-- entity_id = content_hash (뱅크 간 공유 문장은 벡터도 하나면 충분 — 벡터는 텍스트만의
-- 함수이고 클래스와 무관하다. 클래스는 bank_sentences 쪽 멤버십 속성이다).
--
-- ⚠️ 통합 인덱스가 아니라 **entity_type 별 partial HNSW** 관례를 따른다(008 과 동일).
--    통합 인덱스는 제거된 설계다 — 코호트가 섞이면 recall 이 무너진다.
--
-- 020 은 이 migration 과 무관하다(generation_prompts 계열 VIEW, 별도 트랙).
-- 018/020 은 video_metadata 를 ALTER 하므로 라벨링 중 적용하지 않는다 — 별도 배포 윈도우.
--
-- Optional 마이그레이션: pgvector 가 없는 이미지(CI/vanilla postgres)에서는 skip 된다.
-- `PostgresMigrationMixin._OPTIONAL_MIGRATIONS` 에 전제조건이 등록돼 있다.
--
-- ⚠️ **트랜잭션 블록 없음 + CONCURRENTLY**: image_embeddings 는 라이브 쓰기 경로다
--    (임베딩 asset). 일반 CREATE INDEX 는 빌드 내내 쓰기를 막으므로 121K 벡터에서 수 분간
--    파이프라인이 멈춘다. CONCURRENTLY 는 트랜잭션 안에서 실행할 수 없어 BEGIN/COMMIT 을
--    두지 않는다 — 러너가 AUTOCOMMIT 커넥션으로 실행하므로 성립한다.
--    실패 시 INVALID 인덱스가 남고 `IF NOT EXISTS` 가 그걸 건너뛰므로, 아래 검증은
--    존재가 아니라 **indisvalid** 를 본다 (조용히 죽은 인덱스 방지).
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = 'image_embeddings_hnsw_prompt'

CREATE INDEX CONCURRENTLY IF NOT EXISTS image_embeddings_hnsw_prompt
    ON image_embeddings USING hnsw (embedding vector_cosine_ops)
    WHERE entity_type = 'prompt';
