-- ============================================================
-- VLM Data Pipeline — MVP 8테이블 DDL (Strict 8)
-- ============================================================
-- 운영 테이블: raw_files, image_metadata, video_metadata, labels,
--            processed_clips, datasets, dataset_clips
-- 백업 테이블은 migration.sql에서 _bak_* 형태로 관리

-- ============================================================
-- 1. raw_files: NAS 파일 스캔 및 수집 상태 관리
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_files (
    asset_id        VARCHAR PRIMARY KEY,
    source_path     VARCHAR NOT NULL,
    original_name   VARCHAR,
    media_type      VARCHAR DEFAULT 'image',
    file_size       BIGINT,
    checksum        VARCHAR UNIQUE,
    phash           VARCHAR,
    dup_group_id    VARCHAR,
    archive_path    VARCHAR,
    raw_bucket      VARCHAR DEFAULT 'vlm-raw',
    raw_key         VARCHAR,
    ingest_batch_id VARCHAR,
    transfer_tool   VARCHAR DEFAULT 'manual',
    ingest_status   VARCHAR DEFAULT 'pending',
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. image_metadata: 원본 이미지 메타데이터 (INGEST 1-pass)
-- ============================================================
CREATE TABLE IF NOT EXISTS image_metadata (
    asset_id        VARCHAR PRIMARY KEY REFERENCES raw_files(asset_id),
    width           INTEGER,
    height          INTEGER,
    color_mode      VARCHAR DEFAULT 'RGB',
    bit_depth       INTEGER DEFAULT 8,
    codec           VARCHAR,
    has_alpha       BOOLEAN DEFAULT FALSE,
    orientation     INTEGER DEFAULT 1,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 3. video_metadata: 원본 비디오 메타데이터 (ffprobe)
-- ============================================================
CREATE TABLE IF NOT EXISTS video_metadata (
    asset_id        VARCHAR PRIMARY KEY REFERENCES raw_files(asset_id),
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

-- ============================================================
-- 4. labels: 라벨 데이터 관리 (LABEL 단계에서 INSERT)
-- ============================================================
CREATE TABLE IF NOT EXISTS labels (
    label_id        VARCHAR PRIMARY KEY,
    asset_id        VARCHAR REFERENCES raw_files(asset_id),
    labels_bucket   VARCHAR DEFAULT 'vlm-labels',
    labels_key      VARCHAR,
    label_format    VARCHAR,
    label_tool      VARCHAR DEFAULT 'pre-built',
    event_count     INTEGER,
    label_status    VARCHAR DEFAULT 'completed',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 5. processed_clips: 전처리 결과 (clip_metadata 흡수)
-- ============================================================
CREATE TABLE IF NOT EXISTS processed_clips (
    clip_id          VARCHAR PRIMARY KEY,
    source_asset_id  VARCHAR REFERENCES raw_files(asset_id),
    source_label_id  VARCHAR REFERENCES labels(label_id),
    event_index      INTEGER DEFAULT 0,
    checksum         VARCHAR UNIQUE,
    file_size        BIGINT,
    processed_bucket VARCHAR DEFAULT 'vlm-processed',
    clip_key         VARCHAR,
    label_key        VARCHAR,
    width            INTEGER,
    height           INTEGER,
    codec            VARCHAR,
    process_status   VARCHAR DEFAULT 'pending',
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 6. datasets: 데이터셋 구성 관리
-- ============================================================
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id      VARCHAR PRIMARY KEY,
    name            VARCHAR,
    version         VARCHAR,
    config          JSON,
    split_ratio     JSON DEFAULT '{"train":0.8,"val":0.1,"test":0.1}',
    dataset_bucket  VARCHAR DEFAULT 'vlm-dataset',
    dataset_prefix  VARCHAR,
    build_status    VARCHAR DEFAULT 'pending',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 7. dataset_clips: 데이터셋-클립 연결 (M:N)
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_clips (
    dataset_id      VARCHAR REFERENCES datasets(dataset_id),
    clip_id         VARCHAR REFERENCES processed_clips(clip_id),
    split           VARCHAR,
    dataset_key     VARCHAR,
    PRIMARY KEY (dataset_id, clip_id)
);
