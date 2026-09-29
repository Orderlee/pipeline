-- 025: labels.caption_text_en — Gemini 영문 캡션 적재 컬럼
--
-- Gemini VIDEO_EVENT_SCHEMA 는 `ko_caption` 과 `en_caption` 을 **둘 다 required** 로 받아왔으나
-- (`lib/gemini_prompts.py`), 적재 시점에 `caption_text = ko_caption or en_caption` 으로 접혀서
-- 한국어가 있으면 영문이 버려졌다. events JSON(MinIO SoT)에는 둘 다 남지만 DB 에는 하나만
-- 남아, 영문이 필요한 소비자가 매번 다시 번역해야 했다:
--   * `defs/embed/assets.py` 의 caption_embedding 이 `lib/gemini_translate.py` 로 ko→en 재번역
--     (PE-Core-L14-336 텍스트 인코더가 영어 중심이라 ko 원문은 cross-modal 정렬이 거의 0)
--   * FiftyOne `caption_en` 필드도 `docker/analysis/reembed_captions_en.py` 의 별도 번역 산출물
--
-- 이 컬럼이 생기면 그 재번역이 불필요해진다. `gemini_translate.py` 는 이 컬럼이 NULL 인
-- **구 행 백필용 fallback** 으로만 남는다.
--
-- 컬럼 의미:
--   caption_text     — 표시용. 기존 동작 유지(`ko_caption` 우선, 없으면 `en_caption`).
--                      **한국어임이 보장되지 않는다** — ko 가 비어 있으면 영문이 들어온다.
--   caption_text_en  — `en_caption` 원문만. 없으면 NULL. 폴백하지 않는다(언어를 섞지 않기 위해).
--
-- ⚠️ 기존 행(2026-09-08 실측 11,978건 caption 보유)은 **백필 불가**다. 영문은 events JSON 에만
--    있었고 prod MinIO 5개 버킷이 전량 비어 있다(NAS_primary 재구축 2026-08-31). 새로 도는
--    `clip_captioning` / `clip_timestamp` run 부터 채워진다.
--
-- ⚠️ 트랜잭션 안전성: nullable + DEFAULT 없는 ADD COLUMN 은 PG 11+ 에서 테이블 rewrite 가 없는
--    카탈로그 전용 변경이다. ACCESS EXCLUSIVE 락을 잡지만 즉시 끝나므로 라벨링 중 적용해도
--    안전하다 (018 의 video_metadata ALTER 와 달리 기존 컬럼을 건드리지 않는다).
--    단 장기 트랜잭션 뒤에 큐잉되면 그 트랜잭션이 끝날 때까지 대기한다.
--
-- @ASSERT_AFTER: SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='labels' AND column_name='caption_text_en')

BEGIN;

ALTER TABLE labels ADD COLUMN IF NOT EXISTS caption_text_en text;

COMMIT;
