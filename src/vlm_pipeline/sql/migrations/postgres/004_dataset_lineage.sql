-- migration 004_dataset_lineage: stack-candidates #11 첫 단계 — dataset 재현성 메타.
--
-- 현재 dataset row 는 dataset_id(uuid)+version="v1" 하드코딩으로 동일 spec 재빌드 시
-- 추적이 끊긴다. 학습 결과 회귀 시 "어떤 dataset 으로 학습했는지" 못 알아냄.
--
-- 이 마이그레이션은 *컬럼만 추가* — 기존 path / API / consumer 영향 0:
--   - spec_hash: build 의 결정적 hash (config + 입력 set). 같은 spec 재빌드 ↔ 같은 hash.
--   - git_sha: build asset 실행 시점의 코드 commit (lineage 추적용).
--   - build_started_at: 실제 build asset 시작 시각 (created_at 과 별개 — created_at 은
--     row insert 시각이지만 이 컬럼은 build 시작 시각 = build_status='building' 진입 시점).
--
-- 후속 PR 에서 dataset_prefix 를 spec_hash/build_ts 기반으로 변경하기 위한 사전 단계.
-- 그 때까지 path 는 folder_prefix 그대로 유지 (consumer 호환 보존).

BEGIN;

ALTER TABLE datasets
  ADD COLUMN IF NOT EXISTS spec_hash TEXT,
  ADD COLUMN IF NOT EXISTS git_sha TEXT,
  ADD COLUMN IF NOT EXISTS build_started_at TIMESTAMP;

-- spec_hash 로 retrieval — 같은 spec 의 build 들 찾기.
CREATE INDEX IF NOT EXISTS idx_datasets_spec_hash ON datasets(spec_hash);

COMMIT;
