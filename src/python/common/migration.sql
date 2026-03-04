-- 6 -> 8 table migration (safe additive)
-- run inside docker container:
--   duckdb /data/pipeline.duckdb < /src/python/common/migration.sql

BEGIN TRANSACTION;

-- Backup snapshots (one-time)
CREATE TABLE IF NOT EXISTS _bak_raw_files AS SELECT * FROM raw_files;
CREATE TABLE IF NOT EXISTS _bak_processed_clips AS SELECT * FROM processed_clips;
CREATE TABLE IF NOT EXISTS _bak_labels AS SELECT * FROM labels;

-- Strict 8: clip_metadata는 운영 대상에서 제외하고 _bak_로 보관
ALTER TABLE IF EXISTS clip_metadata RENAME TO _bak_clip_metadata;

-- Additive columns for raw_files
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS original_name VARCHAR;
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS phash VARCHAR;
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS dup_group_id VARCHAR;
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE raw_files ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

UPDATE raw_files
SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP);

-- Additive columns for processed_clips
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS width INTEGER;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS height INTEGER;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS codec VARCHAR;

-- New metadata tables
CREATE TABLE IF NOT EXISTS image_metadata (
    asset_id        VARCHAR PRIMARY KEY,
    width           INTEGER,
    height          INTEGER,
    color_mode      VARCHAR,
    bit_depth       INTEGER,
    codec           VARCHAR,
    has_alpha       BOOLEAN DEFAULT FALSE,
    orientation     INTEGER,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS video_metadata (
    asset_id        VARCHAR PRIMARY KEY,
    width           INTEGER,
    height          INTEGER,
    duration_sec    DOUBLE,
    fps             DOUBLE,
    codec           VARCHAR,
    bitrate         BIGINT,
    frame_count     INTEGER,
    has_audio       BOOLEAN DEFAULT FALSE,
    environment_type VARCHAR,
    daynight_type    VARCHAR,
    outdoor_score    DOUBLE,
    avg_brightness   DOUBLE,
    env_method       VARCHAR,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS environment_type VARCHAR;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS daynight_type VARCHAR;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS outdoor_score DOUBLE;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS avg_brightness DOUBLE;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS env_method VARCHAR;

COMMIT;
