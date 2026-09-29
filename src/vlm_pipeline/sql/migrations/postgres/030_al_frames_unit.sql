-- 030_al_frames_unit.sql — al_frames 에 unit(frame|video|event) 구분자 + 이벤트 종료시각 추가
--
-- ⚠️ RECONSTRUCTED FILE (2026-09-21). prod DB `_pg_migrations` 에 `applied_at=2026-09-15
-- 02:13:44` 로 기록돼 있으나 어느 브랜치에도 커밋된 적이 없다(원본 미발견 — staging clone /
-- docker/data/al_scripts / /tmp / git stash·reflog(양쪽 repo) / analysis 컨테이너 확인 완료).
-- ⚠️ **번호 충돌**: `030_comfy_local.sql` 이 동일 030 번호로 2026-09-18 에 별도 적용됐다.
-- 지시에 따라 **번호를 재배정하지 않고 그대로 030 을 유지**한다 — 파일명이 러너의 유일한 키라,
-- 다른 번호를 쓰면 prod 는 (이미 이 이름으로 기록돼 있으므로) 재실행하지 않지만 fresh DB 는
-- 영원히 `unit`/`t_end_sec` 컬럼을 못 얻는다. 두 030 파일이 서로 다른 테이블(al_frames vs
-- genai_batches/raw_files/genai_job_provenance/generation_gpu_leases)을 건드려 충돌은 없다.
--
-- 아래 컬럼 정의는 추측이 아니라 prod 실측이다 — 2026-09-21 `\d al_frames` 로 확인한 결과
-- `unit text NOT NULL DEFAULT 'frame'` + `t_end_sec double precision` 컬럼과
-- `al_frames_unit_idx (unit, cohort)` / `al_frames_unit_cls_idx (unit, cls) WHERE cls IS NOT NULL`
-- 인덱스가 이미 존재한다. **불확실한 부분(추측)은 주석 문구와 파일 서두 설명뿐**이다.
-- pg_constraint 조회 결과 `unit` 에 CHECK 제약은 없다(label_source 와 동일하게 자유 텍스트 +
-- DEFAULT 만으로 관례를 강제) — 아래도 그 관례를 그대로 따른다.
--
-- 왜: timestamp 라벨링의 단위는 프레임이 아니라 **영상 안의 이벤트 구간**이다(`labels` 테이블이
-- per-event 인 것과 같은 이유). 구분 컬럼 없이 섞으면 소비자가 프레임과 이벤트를 함께 세어
-- 조용히 틀린 수치를 낸다. `t_sec IS NULL` 로 단위를 유추하지 않는다.
--
-- `unit='event'` 행 규약(관례, DB 강제 아님):
--   - `frame_key` = `<site>/<video_stem>#ev<NN>` — site 를 빼면 프로젝트 간 파일명이 겹쳐
--     `ON CONFLICT DO UPDATE command cannot affect row a second time` 로 죽는다(실측 사례:
--     loc-c 두 프로젝트가 동일 stem 을 공유).
--   - `t_sec`/`t_end_sec` = 이벤트 시작·끝(초)
--   - `media_uri` = 영상 경로(프레임 아님)
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'al_frames' AND column_name = 'unit')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'al_frames' AND column_name = 't_end_sec')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'al_frames' AND indexname = 'al_frames_unit_idx')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'al_frames' AND indexname = 'al_frames_unit_cls_idx')

BEGIN;

ALTER TABLE al_frames ADD COLUMN IF NOT EXISTS unit text NOT NULL DEFAULT 'frame';
ALTER TABLE al_frames ADD COLUMN IF NOT EXISTS t_end_sec double precision;

CREATE INDEX IF NOT EXISTS al_frames_unit_idx ON al_frames (unit, cohort);
CREATE INDEX IF NOT EXISTS al_frames_unit_cls_idx ON al_frames (unit, cls) WHERE cls IS NOT NULL;

COMMIT;
