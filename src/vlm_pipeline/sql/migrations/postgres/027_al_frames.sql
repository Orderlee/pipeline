-- 027_al_frames.sql — 능동학습 후보 프레임을 한 테이블로 모은다.
--
-- 왜 새 테이블인가: AL 코호트는 라벨(클래스)·그룹키(홀드아웃 단위)·라벨출처를 함께 가져야 하는데
-- 기존 스키마에 담을 자리가 없다. `labels` 는 per-event 라 `category` 컬럼이 아예 없고(025 까지),
-- `image_label_annotations.category` 는 `CHECK (bbox_w > 0)` 라 박스 없는 클래스를 못 담는다.
-- `image_metadata` 는 `source_asset_id NOT NULL` 이라 부모 영상이 없는 썸네일을 못 받는다.
--
-- 왜 raw_files/image_metadata 에 밀어넣지 않는가: 그러면 dedup·dispatch·프레임추출 등
-- 운영 파이프라인이 이 행들을 집어간다. AL 코호트는 **분석용 별도 레인**이라 부작용을 만들면 안 된다.
--
-- ⚠️ `label_source` 는 자기학습 금지 불변식의 판정 근거다.
--    'human'  = LS finalized / 사람 검수 확정 → 학습 GT 로 사용 가능
--    'model'  = 탐지기 알람·Gemini 캡션 파생 → **학습/eval 금지**, 후보 랭킹에만
--    'derived'= 사람 GT 의 구간을 프레임에 투영한 것(원본은 human, 투영 규칙은 기계)
--
-- ⚠️ `group_key` 가 홀드아웃 단위다. 코호트마다 다르다 —
--    certbody sitej 는 **session**(연출 동시녹화라 카메라로 나누면 같은 이벤트가 학습에 남는다),
--    sourcei 는 camera. 이 컬럼을 안 보고 카메라로 나누면 조용히 누수된 수치가 나온다.
--
-- 임베딩은 이 테이블에 두지 않는다 — `image_embeddings` 에 entity_type='al_frame',
-- entity_id = cohort || '/' || frame_key 로 넣는다.
--
-- ⚠️ **partial HNSW 인덱스는 029 로 분리했다** (2026-09-14 수정). 처음엔 이 파일에 같이
--    뒀는데, `image_embeddings` 는 pgvector 가 있어야 존재하는 테이블(006 이 optional)이라
--    CI 의 vanilla postgres 에서 `CREATE INDEX ON image_embeddings` 가 실패하고
--    **러너가 거기서 죽어 이후 마이그레이션이 전부 멈췄다**(unit test 29 errors → 배포 중단).
--    al_frames 자체는 pgvector 와 무관하므로 무조건 적용돼야 한다 → 파일을 갈랐다.
--    교훈: **전제조건이 다른 DDL 을 한 파일에 섞지 말 것.**
--
-- Forward-only, idempotent, DO 블록 미사용.
--
-- @ASSERT_AFTER: SELECT to_regclass('public.al_frames') IS NOT NULL

BEGIN;

CREATE TABLE IF NOT EXISTS al_frames (
  cohort        text NOT NULL,                    -- 'sitej_certbody' | 'sourcea_thumb' | 'sourcei' ...
  frame_key     text NOT NULL,                    -- 코호트 내 고유. 한글 경로는 NFC 정규화 필수
  media_uri     text NOT NULL,                    -- 파일 경로 또는 minio://<bucket>/<key>
  site          text,
  cls           text,                             -- 라벨(미라벨이면 NULL)
  label_rule    text,                             -- 라벨이 만들어진 규칙(재현용)
  label_source  text NOT NULL DEFAULT 'unknown',  -- human | model | derived | unknown
  group_key     text,                             -- ★ 홀드아웃 단위 (코호트마다 다름)
  camera        text,
  session       text,
  video_stem    text,
  t_sec         double precision,
  ambiguous     boolean NOT NULL DEFAULT false,   -- 멀티라벨 → 단일라벨 채점에서 제외
  boundary      boolean NOT NULL DEFAULT false,   -- 구간 경계 ±margin → 채점에서 제외
  n_passes      integer,                          -- 같은 원본의 어노테이션 패스 수(불일치 추정용)
  asset_id      text,                             -- 알면 raw_files 연결(FK 는 걸지 않는다 — 코호트가 더 넓다)
  extra         jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cohort, frame_key)
);

CREATE INDEX IF NOT EXISTS al_frames_cohort_cls_idx   ON al_frames (cohort, cls);
CREATE INDEX IF NOT EXISTS al_frames_group_idx        ON al_frames (cohort, group_key);
CREATE INDEX IF NOT EXISTS al_frames_label_source_idx ON al_frames (label_source);
CREATE INDEX IF NOT EXISTS al_frames_asset_idx        ON al_frames (asset_id);

COMMIT;
