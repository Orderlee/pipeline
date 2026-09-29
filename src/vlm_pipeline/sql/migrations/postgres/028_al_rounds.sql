-- 028_al_rounds.sql — 능동학습 라운드를 추적 가능하게 만든다.
--
-- 왜 필요한가: 지금까지 선별 근거는 CSV 파일 하나뿐이라 **어느 라운드에서 어떤 모델로 왜 이
-- 프레임을 골랐는지** 추적이 안 됐다. 그래서 (a) 라벨러가 확정한 결과가 어느 선별에서 왔는지
-- 되짚을 수 없고 (b) "AL 이 실제로 도움됐나"를 사후에 채점할 수 없다.
--
-- 이 두 테이블이 루프의 뼈대다:
--   선별 → al_selections 기록 → LS 태스크 생성(ls_task_id 기록) → 사람 검수
--        → labeled_cls 채움(루프 닫힘) → al_frames 재유입 → 다음 라운드 재학습
--
-- ⚠️ `al_selections.score` 의 의미는 strategy 마다 다르다 — margin 은 top1-top2 확률차(작을수록
--    헷갈림), coverage 는 라벨셋과의 최대 코사인(작을수록 안 덮임). 부호를 섞지 말 것.
-- ⚠️ `labeled_cls` 가 NULL 인 행은 "아직 라벨 안 됨"이지 "라벨이 없다고 확정"이 아니다.
--    사람이 보고 아무 클래스도 아니라고 판정하면 'normal' 처럼 실제 값이 들어간다.
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT to_regclass('public.al_rounds') IS NOT NULL
-- @ASSERT_AFTER: SELECT to_regclass('public.al_selections') IS NOT NULL

BEGIN;

CREATE TABLE IF NOT EXISTS al_rounds (
  round_id      text PRIMARY KEY,                 -- '<pool>__<strategy>__<YYYYMMDDHHMM>'
  pool_cohort   text NOT NULL,                    -- 점수 매긴 미라벨 풀 (al_frames.cohort)
  gt_cohorts    text[] NOT NULL,                  -- 프로브 학습에 쓴 GT 코호트들
  strategy      text NOT NULL,                    -- margin | coverage | rare | random
  model_desc    jsonb NOT NULL DEFAULT '{}'::jsonb,  -- 클래스·표본수·그룹키 등 재현 정보
  n_pool        integer,
  n_selected    integer,
  status        text NOT NULL DEFAULT 'selected', -- selected | sent | labeled | scored
  ls_project_id integer,
  created_at    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  notes         text
);

CREATE TABLE IF NOT EXISTS al_selections (
  round_id    text NOT NULL REFERENCES al_rounds(round_id) ON DELETE CASCADE,
  cohort      text NOT NULL,
  frame_key   text NOT NULL,
  rank        integer NOT NULL,
  score       double precision,      -- 의미는 strategy 마다 다르다(위 주석)
  pred        text,                  -- 선별 시점 모델의 예측(사후 비교용)
  approved    boolean,               -- 대시보드 검토 결과. NULL = 미검토
  ls_task_id  integer,
  labeled_cls text,                  -- ★ 사람이 확정한 클래스 = 루프가 닫힌 지점
  labeled_at  timestamp,
  PRIMARY KEY (round_id, cohort, frame_key)
);

CREATE INDEX IF NOT EXISTS al_selections_frame_idx    ON al_selections (cohort, frame_key);
CREATE INDEX IF NOT EXISTS al_selections_task_idx     ON al_selections (ls_task_id);
CREATE INDEX IF NOT EXISTS al_selections_unlabeled_idx ON al_selections (round_id) WHERE labeled_cls IS NULL;
CREATE INDEX IF NOT EXISTS al_rounds_pool_idx         ON al_rounds (pool_cohort, created_at DESC);

COMMIT;
