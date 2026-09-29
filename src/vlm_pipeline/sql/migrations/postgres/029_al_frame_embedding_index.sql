-- 029_al_frame_embedding_index.sql — al_frame 벡터용 partial HNSW.
--
-- 027 에서 분리해 나온 파일이다. 027 이 `al_frames` 테이블(pgvector 무관)과 이 인덱스
-- (pgvector 필요)를 한 파일에 담았다가, CI 의 vanilla postgres 에서
-- `CREATE INDEX ON image_embeddings` 가 실패하며 **러너가 거기서 죽어 이후 마이그레이션이
-- 전부 멈추는** 사고를 냈다(2026-09-14, unit test 29 errors → 배포 중단).
-- 전제조건이 다른 DDL 을 한 파일에 섞으면 안 된다는 사례다.
--
-- Optional 마이그레이션: pgvector 가 없는 이미지(CI/vanilla postgres)에서는 skip 된다.
-- `PostgresMigrationMixin._OPTIONAL_MIGRATIONS` 에 전제조건이 등록돼 있다 (008/021 과 동일).
--
-- ⚠️ 통합 인덱스가 아니라 **entity_type 별 partial HNSW** 관례를 따른다(008 과 동일).
--    통합 인덱스는 제거된 설계다 — 코호트가 섞이면 recall 이 무너진다.
--
-- ⚠️ 021 과 달리 CONCURRENTLY 를 쓰지 않는다: al_frame 벡터는 배치 적재 대상이라
--    라이브 쓰기 경로가 아니고, CONCURRENTLY 는 실패 시 INVALID 인덱스를 남겨
--    `IF NOT EXISTS` 가 그걸 건너뛰는 함정이 있다(021 주석 참조).
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT to_regclass('public.image_embeddings_hnsw_al_frame') IS NOT NULL

CREATE INDEX IF NOT EXISTS image_embeddings_hnsw_al_frame
  ON image_embeddings USING hnsw (embedding vector_cosine_ops)
  WHERE entity_type = 'al_frame';
