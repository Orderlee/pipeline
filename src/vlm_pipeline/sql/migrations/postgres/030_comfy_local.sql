-- migration 030_comfy_local: local ComfyUI image engine + per-job provenance/GPU lease
--
-- Forward-only: existing rows are preserved. CHECK constraints are replaced only to
-- append the new engine value.
--
-- @ASSERT_AFTER: SELECT COUNT(*) = 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='genai_job_provenance'
-- @ASSERT_AFTER: SELECT COUNT(*) = 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='generation_gpu_leases'
-- @ASSERT_AFTER: SELECT pg_get_constraintdef(oid) LIKE '%comfy_local%' FROM pg_constraint WHERE conname='genai_batches_engine_check' AND conrelid='genai_batches'::regclass
-- @ASSERT_AFTER: SELECT pg_get_constraintdef(oid) LIKE '%comfy_local%' FROM pg_constraint WHERE conname='chk_raw_files_genai_engine' AND conrelid='raw_files'::regclass

BEGIN;

ALTER TABLE genai_batches DROP CONSTRAINT IF EXISTS genai_batches_engine_check;
ALTER TABLE genai_batches
  ADD CONSTRAINT genai_batches_engine_check
  CHECK (engine IN ('kling','higgsfield','veo','nanobanana','gpt_image','comfy_local'));

ALTER TABLE raw_files DROP CONSTRAINT IF EXISTS chk_raw_files_genai_engine;
ALTER TABLE raw_files
  ADD CONSTRAINT chk_raw_files_genai_engine
  CHECK (genai_engine IN ('kling','higgsfield','veo','nanobanana','gpt_image','comfy_local'));

CREATE TABLE IF NOT EXISTS genai_job_provenance (
    job_id                    TEXT PRIMARY KEY
                                REFERENCES genai_jobs(job_id) ON DELETE CASCADE,
    workflow_id               TEXT NOT NULL,
    workflow_sha256           TEXT NOT NULL CHECK (length(workflow_sha256) = 64),
    model_manifest_sha256     TEXT NOT NULL CHECK (length(model_manifest_sha256) = 64),
    prompt_sha256             TEXT NOT NULL CHECK (length(prompt_sha256) = 64),
    negative_prompt_sha256    TEXT CHECK (negative_prompt_sha256 IS NULL OR length(negative_prompt_sha256) = 64),
    seed                      BIGINT NOT NULL CHECK (seed >= 0),
    input_sha256              TEXT NOT NULL CHECK (length(input_sha256) = 64),
    mask_sha256               TEXT CHECK (mask_sha256 IS NULL OR length(mask_sha256) = 64),
    provider_prompt_id        TEXT,
    output_sha256             TEXT CHECK (output_sha256 IS NULL OR length(output_sha256) = 64),
    params_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
    gpu_started_at            TIMESTAMP,
    gpu_completed_at          TIMESTAMP,
    created_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_genai_job_provenance_workflow
  ON genai_job_provenance(workflow_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_genai_job_provenance_provider
  ON genai_job_provenance(provider_prompt_id)
  WHERE provider_prompt_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS generation_gpu_leases (
    resource            TEXT PRIMARY KEY,
    owner_job_id        TEXT REFERENCES genai_jobs(job_id) ON DELETE SET NULL,
    lease_token         TEXT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'active'
                          CHECK (state IN ('active','released','expired')),
    acquired_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    heartbeat_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at          TIMESTAMP NOT NULL,
    released_at         TIMESTAMP,
    release_reason      TEXT,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (resource IN ('gpu0_comfy'))
);

CREATE INDEX IF NOT EXISTS idx_generation_gpu_leases_active
  ON generation_gpu_leases(state, expires_at)
  WHERE state = 'active';

COMMIT;
