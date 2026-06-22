-- =====================================================================
-- VLM 파이프라인 Vector DB (pgvector) 자주 쓰는 SQL 모음
--   대상 테이블: image_embeddings (embedding vector(1024), entity_type{frame|caption|video}, model_name, image_id, asset_id, text_content)
--   조인: frame  → image_embeddings.image_id = image_metadata.image_id → image_metadata.source_asset_id = video_metadata.asset_id
--         caption→ image_embeddings.entity_id = labels.label_id (writer 규약), image_embeddings.asset_id 채워짐
--   ※ frame 행은 image_embeddings.asset_id 가 NULL → daynight/environment 는 image_metadata→video_metadata 경유 조인
--   유사도 연산자: <=> cosine거리(0=동일), <-> L2, <#> 내적.  cosine_similarity = 1 - (a <=> b)
--
-- 실행:
--   prod    psql:  docker exec -it docker-postgres-1        psql -U airflow -d vlm_pipeline
--   staging psql:  docker exec -it pipeline-test-postgres-1 psql -U airflow -d vlm_pipeline_staging
--   또는 analysis 컨테이너에서 python: fp._pg_conn() (DATAOPS_POSTGRES_DSN)
--
-- ⚠️ 환경 차이:
--   - pg_trgm(캡션 키워드, E절): staging 에만 설치됨. prod 는 migration(pg_trgm ext+GIN) 후 사용.
--   - daynight_type/environment_type(C/D절 필터): prod frame 들은 현재 전부 NULL(env 분류 미실행). source/role/키워드 필터부터 활용.
-- =====================================================================


-- ============ A. 기본 현황/통계 (순수 SQL) ============

-- A1. entity_type · model 별 임베딩 수
SELECT entity_type, model_name, count(*) AS n
FROM image_embeddings GROUP BY 1,2 ORDER BY 3 DESC;

-- A2. 소스 데이터셋별 frame 임베딩 분포
SELECT split_part(im.image_key,'/',1) AS source, count(*) AS n
FROM image_embeddings e JOIN image_metadata im ON im.image_id = e.image_id
WHERE e.entity_type='frame'
GROUP BY 1 ORDER BY 2 DESC;

-- A3. 커버리지: 라벨(bbox) 이미지 중 아직 임베딩 안 된 수
SELECT count(*) AS labeled_not_embedded
FROM image_metadata im
WHERE im.image_role = ANY(ARRAY['raw_video_frame','processed_clip_frame','source_image'])
  AND EXISTS (SELECT 1 FROM image_labels il WHERE il.image_id = im.image_id)
  AND NOT EXISTS (SELECT 1 FROM image_embeddings e
                  WHERE e.image_id = im.image_id AND e.entity_type='frame');


-- ============ B. 이미지→이미지 유사검색 (순수 SQL, image_id 시드) ============

-- B1. 주어진 image_id 와 가장 비슷한 프레임 top-10 (cosine)
WITH q AS (
  SELECT embedding FROM image_embeddings
  WHERE entity_type='frame' AND image_id = :'image_id' LIMIT 1
)
SELECT im.image_key,
       round((e.embedding <=> (SELECT embedding FROM q))::numeric, 4) AS cosine_dist
FROM image_embeddings e JOIN image_metadata im ON im.image_id = e.image_id
WHERE e.entity_type='frame'
ORDER BY e.embedding <=> (SELECT embedding FROM q)
LIMIT 10;
-- 사용: psql 에서  \set image_id '00012f21-ff0c-4b0e-89cb-1049d71bfffc'  후 실행
-- (텍스트→이미지 검색은 쿼리 텍스트를 embedding-service /embed_text 로 먼저 벡터화해야 하므로 순수 SQL 불가 → fp.search_by_text 사용)


-- ============ C. 하이브리드: 메타데이터 필터 + 벡터 (iterative_scan) ============
-- ⚠️ 필터가 강하면 plain HNSW 는 k개 못 채울 수 있음 → iterative_scan 필수. 아래 3개 SET 을 같은 트랜잭션에서 먼저.

-- C1. 특정 소스 안에서만 유사 프레임 (필터 + 벡터)
SET LOCAL hnsw.iterative_scan = 'relaxed_order';
SET LOCAL hnsw.ef_search = 100;
SET LOCAL hnsw.max_scan_tuples = 50000;
WITH q AS (SELECT embedding FROM image_embeddings WHERE entity_type='frame' AND image_id = :'image_id' LIMIT 1)
SELECT im.image_key,
       round((e.embedding <=> (SELECT embedding FROM q))::numeric,4) AS cosine_dist
FROM image_embeddings e
JOIN image_metadata im ON im.image_id = e.image_id
WHERE e.entity_type='frame'
  AND im.image_key LIKE 'vietnam_data%'          -- source 필터(prefix)
  -- AND im.image_role = 'raw_video_frame'        -- (선택) role 필터
ORDER BY e.embedding <=> (SELECT embedding FROM q)
LIMIT 20;

-- C2. 야간(night) 영상 프레임만 유사검색 (video_metadata 조인 필터)
--     ※ prod 는 daynight_type 미populate(전부 NULL) → env 분류 후 사용. staging 도 데이터 없음.
SET LOCAL hnsw.iterative_scan = 'relaxed_order';
SET LOCAL hnsw.ef_search = 100;
SET LOCAL hnsw.max_scan_tuples = 50000;
WITH q AS (SELECT embedding FROM image_embeddings WHERE entity_type='frame' AND image_id = :'image_id' LIMIT 1)
SELECT im.image_key, vm.daynight_type, vm.environment_type,
       round((e.embedding <=> (SELECT embedding FROM q))::numeric,4) AS cosine_dist
FROM image_embeddings e
JOIN image_metadata im ON im.image_id = e.image_id
JOIN video_metadata vm ON vm.asset_id = im.source_asset_id
WHERE e.entity_type='frame'
  AND vm.daynight_type = 'night'
ORDER BY e.embedding <=> (SELECT embedding FROM q)
LIMIT 20;


-- ============ D. 관계형 조인: 임베딩 × 라벨/메타 ============

-- D1. caption 임베딩 + 캡션 텍스트 + (영상) 환경 메타
SELECT e.entity_id AS label_id, l.event_index, left(l.caption_text,40) AS caption,
       vm.daynight_type, vm.environment_type
FROM image_embeddings e
JOIN labels l ON l.label_id = e.entity_id
LEFT JOIN video_metadata vm ON vm.asset_id = e.asset_id
WHERE e.entity_type='caption'
LIMIT 20;

-- D2. frame 임베딩 × 영상 daynight/environment 교차표 (분포 파악)
SELECT vm.daynight_type, vm.environment_type, count(*) AS n
FROM image_embeddings e
JOIN image_metadata im ON im.image_id = e.image_id
JOIN video_metadata vm ON vm.asset_id = im.source_asset_id
WHERE e.entity_type='frame'
GROUP BY 1,2 ORDER BY 3 DESC;


-- ============ E. 캡션 키워드 검색 (pg_trgm) — staging 전용(prod 는 migration 후) ============

-- E0. (1회) 확장/인덱스 설치
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX IF NOT EXISTS labels_caption_trgm ON labels USING gin (caption_text gin_trgm_ops);

-- E1. 한국어 캡션 키워드 검색 (ILIKE 부분일치 + trigram 유사도)
SET LOCAL pg_trgm.similarity_threshold = 0.05;
SELECT label_id, asset_id, left(caption_text,60) AS caption,
       (caption_text ILIKE '%넘어%') AS substring_hit,
       round(similarity(caption_text, '넘어')::numeric,3) AS sim
FROM labels
WHERE caption_text IS NOT NULL AND caption_text <> ''
  AND (caption_text ILIKE '%넘어%' OR caption_text % '넘어')
ORDER BY substring_hit DESC, sim DESC
LIMIT 20;


-- ============ F. 중복/근접중복 (dedup — 학습데이터 정제) ============

-- F1. 특정 프레임과 거의 동일한 near-duplicate (cosine_dist < 0.05)
WITH q AS (SELECT embedding FROM image_embeddings WHERE entity_type='frame' AND image_id = :'image_id' LIMIT 1)
SELECT im.image_key, round((e.embedding <=> (SELECT embedding FROM q))::numeric,4) AS dist
FROM image_embeddings e JOIN image_metadata im ON im.image_id = e.image_id
WHERE e.entity_type='frame'
  AND (e.embedding <=> (SELECT embedding FROM q)) < 0.05
ORDER BY 2
LIMIT 50;


-- ============ G. 캡션↔이미지 정합 (cross-modal, 같은 asset) ============

-- G1. 캡션 임베딩 ↔ 자기 영상 프레임 임베딩 최소 cosine거리 (작을수록 정합 좋음)
--     ※ 캡션이 한국어로 임베딩된 동안은 거리 큼(텍스트 인코더 degenerate). 영어 재임베딩 후 개선.
SELECT cap.entity_id AS label_id,
       round(min(cap.embedding <=> frm.embedding)::numeric,4) AS best_dist,
       count(*) AS n_frames
FROM image_embeddings cap
JOIN image_metadata im  ON im.source_asset_id = cap.asset_id
JOIN image_embeddings frm ON frm.image_id = im.image_id AND frm.entity_type='frame'
WHERE cap.entity_type='caption' AND cap.asset_id IS NOT NULL
GROUP BY 1
ORDER BY best_dist ASC
LIMIT 20;


-- ============ H. 유지보수 / 헬스 ============

-- H1. 인덱스 목록 + 크기 + 정의 (HNSW partial 확인)
SELECT i.relname AS index_name, pg_size_pretty(pg_relation_size(i.oid)) AS size, pg_get_indexdef(i.oid) AS def
FROM pg_class t JOIN pg_index ix ON ix.indrelid=t.oid JOIN pg_class i ON i.oid=ix.indexrelid
WHERE t.relname='image_embeddings'
ORDER BY pg_relation_size(i.oid) DESC;

-- H2. HNSW 인덱스 실제 사용 확인 (EXPLAIN — 'Index Scan using image_embeddings_hnsw_frame' 떠야 정상)
--     (벡터 리터럴 자리에 실제 1024차원 벡터 필요 — fp.search_by_text 등에서 캡처해 사용)
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT image_id FROM image_embeddings WHERE entity_type='frame' AND model_name='facebook/PE-Core-L14-336'
-- ORDER BY embedding <=> '[...]'::vector LIMIT 10;

-- H3. 통계/ANALYZE 상태 (대량 적재 후 NULL 이면 통계 없음 → 플래너 비효율)
SELECT n_live_tup, n_dead_tup, last_analyze, last_autoanalyze, last_vacuum, last_autovacuum
FROM pg_stat_user_tables WHERE relname='image_embeddings';

-- H4. 대량 적재(backfill) 후 통계 갱신 (안전, 빠름) — 필요 시
-- ANALYZE image_embeddings;
