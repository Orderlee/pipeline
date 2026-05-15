-- migration 002_genai: GenAI Studio fan-out batch tracking + raw_files provenance columns

BEGIN;

-- ============================================================
-- A. raw_files: provenance and GenAI labeling guards
-- ============================================================
ALTER TABLE IF EXISTS raw_files ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'camera';
ALTER TABLE IF EXISTS raw_files ADD COLUMN IF NOT EXISTS genai_engine TEXT;
ALTER TABLE IF EXISTS raw_files ADD COLUMN IF NOT EXISTS label_policy TEXT DEFAULT 'required';

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_raw_files_source_type'
      AND conrelid = 'raw_files'::regclass
  ) THEN
    ALTER TABLE raw_files
      ADD CONSTRAINT chk_raw_files_source_type
      CHECK (source_type IN ('camera','nas_upload','genai_source','genai_output'));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_raw_files_genai_engine'
      AND conrelid = 'raw_files'::regclass
  ) THEN
    ALTER TABLE raw_files
      ADD CONSTRAINT chk_raw_files_genai_engine
      CHECK (genai_engine IN ('kling','higgsfield','nanobanana','gpt_image'));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_raw_files_label_policy'
      AND conrelid = 'raw_files'::regclass
  ) THEN
    ALTER TABLE raw_files
      ADD CONSTRAINT chk_raw_files_label_policy
      CHECK (label_policy IN ('required','none'));
  END IF;
END $$;

-- ============================================================
-- B. genai_batches: batch-level GenAI request tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS genai_batches (
    batch_id       TEXT PRIMARY KEY,
    engine         TEXT NOT NULL
                     CHECK (engine IN ('kling','higgsfield','nanobanana','gpt_image')),
    output_media   TEXT NOT NULL
                     CHECK (output_media IN ('video','image')),
    prompt         TEXT NOT NULL,
    options_json   TEXT,
    requested_by   TEXT,
    status         TEXT DEFAULT 'pending'
                     CHECK (status IN ('pending','running','succeeded','partial_success','failed','cancelled')),
    n_total        INTEGER NOT NULL CHECK (n_total >= 1),
    n_succeeded    INTEGER DEFAULT 0,
    n_failed       INTEGER DEFAULT 0,
    submitted_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_genai_batches_status ON genai_batches(status);

-- ============================================================
-- C. genai_jobs: per-output GenAI job tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS genai_jobs (
    job_id           TEXT PRIMARY KEY,
    batch_id         TEXT NOT NULL REFERENCES genai_batches(batch_id) ON DELETE CASCADE,
    seq_in_batch     INTEGER NOT NULL CHECK (seq_in_batch >= 1),
    input_asset_id   TEXT REFERENCES raw_files(asset_id),
    output_asset_id  TEXT REFERENCES raw_files(asset_id),
    provider_job_id  TEXT,
    status           TEXT DEFAULT 'pending'
                       CHECK (status IN ('pending','submitted','running','done','failed')),
    error_message    TEXT,
    cost_units       DOUBLE PRECISION,
    submitted_at     TIMESTAMP,
    completed_at     TIMESTAMP,
    UNIQUE(batch_id, seq_in_batch)
);

CREATE INDEX IF NOT EXISTS idx_genai_jobs_batch ON genai_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_genai_jobs_status ON genai_jobs(status);

-- ============================================================
-- D. raw_files: provenance column indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_raw_files_source_type   ON raw_files(source_type);
CREATE INDEX IF NOT EXISTS idx_raw_files_label_policy  ON raw_files(label_policy);
CREATE INDEX IF NOT EXISTS idx_raw_files_genai_engine  ON raw_files(genai_engine);

COMMIT;
