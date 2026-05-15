-- migration 003_genai_veo: GenAI engine enum 에 'veo' (Vertex AI) 추가
-- Phase 5+: VeoAdapter 신설. 002_genai 의 CHECK constraint 가 4 엔진만 허용해
-- genai_batches INSERT 시 violation. forward-only 정책 — constraint DROP + ADD
-- (데이터 손실 없음).

BEGIN;

-- genai_batches.engine
ALTER TABLE genai_batches DROP CONSTRAINT IF EXISTS genai_batches_engine_check;
ALTER TABLE genai_batches
  ADD CONSTRAINT genai_batches_engine_check
  CHECK (engine IN ('kling','higgsfield','veo','nanobanana','gpt_image'));

-- raw_files.genai_engine
ALTER TABLE raw_files DROP CONSTRAINT IF EXISTS chk_raw_files_genai_engine;
ALTER TABLE raw_files
  ADD CONSTRAINT chk_raw_files_genai_engine
  CHECK (genai_engine IN ('kling','higgsfield','veo','nanobanana','gpt_image'));

COMMIT;
