-- 024_query_path_indexes.sql — 라벨링 hot path 3개 쿼리의 full scan 제거.
--
-- 배경: `pg_stat_user_tables` 실측에서 raw_files(seq_scan 10,706 / 누적 465M행)와
-- video_metadata(3,586 / 464M행)가 반복 풀스캔을 받고 있었다. 원인을 술어 단위로
-- 분해해보니 **인덱스가 없어서가 아니라 인덱스가 붙을 수 없는 술어**였다:
--
--   raw_files.media_type='video'          → 129,970/129,970 = 100.0%  (인덱스 무의미)
--   raw_files.ingest_status='completed'   → 129,089/129,970 =  99.3%  (인덱스 무의미)
--   video_metadata.caption_status='pending'   → 100.0%               (인덱스 무의미)
--   video_metadata.auto_label_status='pending'→  80.9%               (인덱스 무의미)
--
-- 반면 실제로 선택적인 술어는 셋뿐이고, 여기에만 인덱스를 붙인다:
--
--   video_metadata.auto_label_status='generated'          → 2,093 / 1.61%
--   COALESCE(video_metadata.timestamp_status,'pending')='completed' → 1,314 / 1.01%
--   labels.asset_id (FK인데 인덱스 부재 — anti-join 이 매번 12,608행 Materialize)
--
-- 실측 효과 (BEGIN/CREATE/EXPLAIN/ROLLBACK 으로 사전 검증):
--   postgres_labeling.py:499 자동라벨 미투영 조회
--     143.3ms / 21,274 buffers  →  0.68ms / 326 buffers   (211x, 65x)
--   postgres_labeling.py:472 timestamp 완료분 조회
--      40.1ms / 16,241 buffers  →  7.57ms / 5,601 buffers (5.3x, 2.9x)
--
-- ⚠️ 버퍼 감소가 지연시간보다 중요하다: shared_buffers 가 128MB 인데 위 쿼리 하나가
--    166MB 를 훑어 **매 실행마다 버퍼풀을 전량 축출**했다. 같은 풀을 쓰는 파이프라인
--    본체(btree idx_scan 278,796회)가 그 부수피해를 받고 있었다.
--
-- 기각한 후보: raw_files(source_unit_name) — 플래너가 vm 쪽 구동을 선호해 실측에서
--    인덱스를 아예 타지 않았다(40.1ms → 33.9ms, 계획 동일 = 노이즈). 만들지 않는다.
--
-- 표현식 인덱스를 쓰는 이유: 쿼리가 `COALESCE(timestamp_status,'pending')` 형태라
--    컬럼 인덱스로는 매칭되지 않는다. 쿼리를 고치면(=COALESCE 제거, 의미 동일) 일반
--    부분 인덱스로 충분하지만 그건 코드 변경 → 배포 → 라벨링 중단이다. 표현식 부분
--    인덱스는 32kB 짜리라 코드를 안 건드리는 쪽이 싸다.
--
-- ⚠️ **트랜잭션 블록 없음 + CONCURRENTLY**: 세 테이블 다 라이브 쓰기 경로다(021 과 동일
--    이유). CONCURRENTLY 는 트랜잭션 안에서 실행 불가라 BEGIN/COMMIT 을 두지 않는다 —
--    러너가 AUTOCOMMIT 커넥션으로 실행하므로 성립한다. 실패 시 INVALID 인덱스가 남고
--    `IF NOT EXISTS` 가 그걸 건너뛰므로, 검증은 존재가 아니라 **indisvalid** 를 본다.
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = 'labels_asset_id_idx'
-- @ASSERT_AFTER: SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = 'video_metadata_autolabel_generated_idx'
-- @ASSERT_AFTER: SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = 'video_metadata_ts_completed_idx'

-- FK 인덱스 누락 보강. labels.asset_id → raw_files.asset_id 인데 인덱스가 없어
-- NOT EXISTS anti-join 이 매번 전체 Materialize 로 풀렸다. DELETE FROM raw_files 의
-- FK 검사도 같이 덕을 본다.
CREATE INDEX CONCURRENTLY IF NOT EXISTS labels_asset_id_idx
    ON labels (asset_id);

-- auto_labeled_at 을 키로 두어 필터와 ORDER BY 를 한 인덱스로 처리한다
-- (LIMIT 이 정렬 없이 조기 종료 → top-N heapsort 소멸).
CREATE INDEX CONCURRENTLY IF NOT EXISTS video_metadata_autolabel_generated_idx
    ON video_metadata (auto_labeled_at)
    WHERE auto_label_status = 'generated';

-- 부분 표현식 인덱스: 쿼리의 COALESCE 술어와 정확히 일치시키면서 1,314행만 인덱싱.
CREATE INDEX CONCURRENTLY IF NOT EXISTS video_metadata_ts_completed_idx
    ON video_metadata ((COALESCE(timestamp_status, 'pending')))
    WHERE COALESCE(timestamp_status, 'pending') = 'completed';
