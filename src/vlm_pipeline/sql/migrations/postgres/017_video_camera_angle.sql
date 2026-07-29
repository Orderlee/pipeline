-- 017_video_camera_angle.sql — 카메라 씬 6축(camera_angle 계열 + weather) + provenance
-- (design: docs/design-docs/camera-angle-grouping-2026-07-29.md §1, §3.1).
--
-- video_metadata 에 per-video 서술자 5컬럼을 추가한다 — environment_type/daynight_type 계열과
-- grain·성격이 동일한 형제 컬럼(§3.1 근거). 신규 테이블이 아니라 컬럼인 이유는 설계 문서 참조.
--   * camera_angle    : plan_view|non_plan|indeterminate — DAv2(Depth Anything V2-S + 바닥
--                       평면 피팅) 서비스가 산출하는 2(+1)-bin. level_view/oblique_view 는
--                       폐기됐다 — 어떤 모델도 분리하지 못했다(2026-07-29 실측, level AUC
--                       ≤ 0.68).
--   * subject_scale   : subject_legible|subject_marginal|not_applicable — 피사체 크기, tilt 와
--                       별도 축.
--   * occlusion_state : unoccluded|partially_occluded|truncated|not_applicable — 폐색은
--                       각도와 무관한 별도 원인이라 등치 취급 금지(§1).
--   * weather         : clear|cloudy|rain|snow|fog|not_applicable|indeterminate — Gemini 통합
--                       호출(lib/video_scene.py)로 흡수한 날씨 축. Places365 는 다루지 않던
--                       신규 축이라 001 에 없다.
--   * angle_method    : provenance — 모델 식별자 성공값 | 'deferred'(ingest 시 기본값 —
--                       백필 큐 등록 메커니즘) | 'deferred_missing_archive'|'deferred_no_frames'
--                       (터미널 마커, env_backfill 패턴 복제).
--
-- environment_type/daynight_type/env_method 는 001_init.sql 에 이미 존재 — 여기서 손대지 않는다.
-- lib/video_scene.classify_video_scene() 은 위 3개도 함께 반환하지만(Gemini 1회 호출로 통합),
-- 그 값은 video_scene_backfill 이 update_video_scene() 을 통해 기존 environment_type/
-- daynight_type/env_method 컬럼에 쓴다 — 스키마 변경이 필요 없다.
--
-- Forward-only, idempotent. DO block 미사용 (runner 의 multi-statement DO 부분적용 quirk 회피).
--
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'video_metadata' AND column_name = 'camera_angle')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'video_metadata' AND column_name = 'subject_scale')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'video_metadata' AND column_name = 'occlusion_state')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'video_metadata' AND column_name = 'weather')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'video_metadata' AND column_name = 'angle_method')
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'video_metadata_camera_angle_idx')

BEGIN;

ALTER TABLE video_metadata
  ADD COLUMN IF NOT EXISTS camera_angle    TEXT,   -- plan_view|non_plan|indeterminate
  ADD COLUMN IF NOT EXISTS subject_scale   TEXT,   -- subject_legible|subject_marginal|not_applicable
  ADD COLUMN IF NOT EXISTS occlusion_state TEXT,   -- unoccluded|partially_occluded|truncated|not_applicable
  ADD COLUMN IF NOT EXISTS weather         TEXT,   -- clear|cloudy|rain|snow|fog|not_applicable|indeterminate
  ADD COLUMN IF NOT EXISTS angle_method    TEXT;   -- 'dav2-s+plane' | 'deferred' | 'deferred_*'

-- 기존 행을 백필 큐에 등록한다. ADD COLUMN 은 NULL 로만 채우고, 선택 쿼리
-- (find_deferred_scene_videos) 는 angle_method='deferred' OR env_method='deferred' 를 보므로
-- 이 UPDATE 가 없으면 마이그레이션 이전에 적재된 영상 전량(129,089편)이 영원히 대상에서 빠진다.
-- WHERE 절 덕에 idempotent — 재실행해도 이미 분류된 행을 되돌리지 않는다.
-- weather 는 큐 등록 마커가 아니라 데이터 컬럼이라 별도 UPDATE 가 필요 없다 — angle_method
-- 를 통해 큐에 남은 행은 scene backfill 이 한 번에 camera_angle/.../weather 를 함께 채운다.
UPDATE video_metadata SET angle_method = 'deferred' WHERE angle_method IS NULL;

CREATE INDEX IF NOT EXISTS video_metadata_camera_angle_idx
  ON video_metadata (camera_angle) WHERE camera_angle IS NOT NULL;

-- 백필 큐 선택을 위한 partial index (find_deferred_scene_videos 의 WHERE 절).
CREATE INDEX IF NOT EXISTS video_metadata_angle_deferred_idx
  ON video_metadata (asset_id) WHERE angle_method = 'deferred';

COMMIT;
