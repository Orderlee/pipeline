-- 005_labels_unique.sql — duplicate (labels_key, event_index) 정리 + UNIQUE 보장.
--
-- Codex follow-up: ls_sync 의 finalized guard 가 추가되기 전 prod 에서 발생한
-- 158건 duplicate 정리. 분석 결과 모든 duplicate 가 (finalized 1 + non-finalized 1)
-- 패턴 → finalized 우선 유지, 나머지 삭제.
--
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'labels_key_event_idx_unique' AND conrelid = 'labels'::regclass)
-- @ASSERT_AFTER: SELECT NOT EXISTS (SELECT 1 FROM (SELECT labels_key, event_index FROM labels GROUP BY labels_key, event_index HAVING COUNT(*) > 1) t)

BEGIN;

-- 1. duplicate 정리: 각 (labels_key, event_index) group 에서 non-finalized row 삭제.
--    조건: 같은 group 에 review_status='finalized' 인 row 가 존재하는 경우에만 삭제.
--    재실행 시 duplicate 없음 → 0 rows deleted (idempotent).
DELETE FROM labels a
USING labels b
WHERE a.labels_key = b.labels_key
  AND a.event_index = b.event_index
  AND a.label_id != b.label_id
  AND a.review_status != 'finalized'
  AND b.review_status = 'finalized';

-- 2. 정리 검증 — duplicate 가 0 인지 확인 (남아 있으면 ROLLBACK).
DO $$
DECLARE
  remaining_dups INTEGER;
BEGIN
  SELECT COUNT(*) INTO remaining_dups
  FROM (
    SELECT labels_key, event_index FROM labels
    GROUP BY labels_key, event_index HAVING COUNT(*) > 1
  ) t;
  IF remaining_dups > 0 THEN
    RAISE EXCEPTION 'duplicate cleanup 후에도 % groups 남음 — migration ROLLBACK', remaining_dups;
  END IF;
END $$;

-- 3. UNIQUE constraint 추가 — 향후 race / regression 방지.
--    IF NOT EXISTS guard 로 멱등성 보장 (재실행 시 에러 없이 skip).
--    conrelid 가드: 다른 테이블에 동명 constraint 가 있어도 false-skip 방지 (codex follow-up).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'labels_key_event_idx_unique'
      AND conrelid = 'labels'::regclass
  ) THEN
    ALTER TABLE labels
      ADD CONSTRAINT labels_key_event_idx_unique UNIQUE (labels_key, event_index);
  END IF;
END $$;

COMMIT;
