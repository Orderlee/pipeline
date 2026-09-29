-- 024: 라벨링 조회 경로 인덱스 3종
--
-- 이 파일은 2026-08-27 에 작성돼 prod DB 에는 적용·기록됐지만 **커밋되지 않은 상태**로 남아 있다가
-- 2026-09-03 배포의 `rsync -a --delete`(체크아웃본에 없는 파일 삭제)로 작업 트리에서 사라졌다.
-- 여기 DDL 은 그때 실제로 만들어진 prod 인덱스 정의(`pg_indexes.indexdef`)를 그대로 옮긴 것이다 —
-- 원본의 설명 주석은 복구 불가라 없다. prod 는 `_pg_migrations` 에 이미 기록돼 있어 skip 되고,
-- staging·CI·새 DB 에서만 실제로 생성된다.
--
-- ⚠️ **트랜잭션 블록 없음 + CONCURRENTLY**: 세 테이블 다 라이브 쓰기 경로다. 일반 CREATE INDEX 는
--    ACCESS EXCLUSIVE 락으로 인제스트/라벨링을 멈춘다. CONCURRENTLY 는 트랜잭션 안에서 실행할 수
--    없으므로 BEGIN/COMMIT 을 두지 않는다 — 러너(postgres_migration.py)가 이런 파일만 문장 단위로
--    나눠 AUTOCOMMIT 커넥션에 보낸다(전체를 한 번에 보내면 암시적 트랜잭션이 걸려 실패한다).
--    실패 시 INVALID 인덱스가 남을 수 있으므로 `DROP INDEX` 후 재적용할 것.

CREATE INDEX CONCURRENTLY IF NOT EXISTS labels_asset_id_idx
    ON labels (asset_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS video_metadata_autolabel_generated_idx
    ON video_metadata (auto_labeled_at)
    WHERE auto_label_status = 'generated';

CREATE INDEX CONCURRENTLY IF NOT EXISTS video_metadata_ts_completed_idx
    ON video_metadata ((COALESCE(timestamp_status, 'pending')))
    WHERE COALESCE(timestamp_status, 'pending') = 'completed';

-- @ASSERT_AFTER: SELECT count(*) = 3 FROM pg_indexes WHERE indexname IN ('labels_asset_id_idx', 'video_metadata_autolabel_generated_idx', 'video_metadata_ts_completed_idx')
