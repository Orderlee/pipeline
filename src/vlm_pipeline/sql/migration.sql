-- ============================================================
-- asset_catalog → raw_files 데이터 이관
-- ============================================================
-- 실행 전 반드시 pipeline.duckdb 백업:
--   cp ./docker/data/pipeline.duckdb ./docker/data/pipeline.duckdb.bak.$(date +%Y%m%d)

-- Step 1: asset_catalog → raw_files 이관
INSERT INTO raw_files (
    asset_id, source_path, original_name, media_type,
    file_size, checksum, raw_bucket, raw_key,
    ingest_batch_id, transfer_tool, ingest_status,
    created_at, updated_at
)
SELECT
    asset_id,
    source_path,
    regexp_extract(source_path, '[^/]+$'),  -- original_name = basename
    CASE
        WHEN lower(source_path) LIKE '%.mp4'
          OR lower(source_path) LIKE '%.avi'
          OR lower(source_path) LIKE '%.mov'
          OR lower(source_path) LIKE '%.mkv'
          OR lower(source_path) LIKE '%.webm'
          OR lower(source_path) LIKE '%.mts'
          OR lower(source_path) LIKE '%.m2ts'
        THEN 'video'
        ELSE 'image'
    END,
    file_size,
    checksum,
    raw_bucket,
    raw_key,
    'legacy_migration',
    'unknown',
    CASE
        WHEN ingest_status = 'scanned' THEN 'pending'
        WHEN ingest_status IN ('uploaded', 'verified') THEN 'completed'
        ELSE COALESCE(ingest_status, 'pending')
    END,
    created_at,
    CURRENT_TIMESTAMP
FROM asset_catalog
WHERE asset_id NOT IN (SELECT asset_id FROM raw_files);

-- Step 2: 레거시 테이블 백업 (RENAME)
-- ALTER TABLE asset_catalog RENAME TO _bak_asset_catalog;
-- ALTER TABLE scan_runs     RENAME TO _bak_scan_runs;
-- ALTER TABLE dir_cache     RENAME TO _bak_dir_cache;

-- Step 3: 검증
-- SELECT count(*) FROM raw_files;
-- SELECT count(*) FROM _bak_asset_catalog;

-- ============================================================
-- Strict 7 → Strict 8 마이그레이션
-- ============================================================
-- 실행 전 반드시 pipeline.duckdb 백업:
--   cp ./docker/data/pipeline.duckdb ./docker/data/pipeline.duckdb.bak.$(date +%Y%m%d)

-- 재인코딩 품질 검사 컬럼 추가
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS original_codec VARCHAR;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS original_profile VARCHAR;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS original_has_b_frames BOOLEAN;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS original_level_int INTEGER;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS reencode_required BOOLEAN DEFAULT FALSE;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS reencode_reason VARCHAR;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS reencode_applied BOOLEAN DEFAULT FALSE;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS reencode_preset VARCHAR;

-- video_metadata: Gemini auto-label 상태 컬럼 추가
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS auto_label_status VARCHAR DEFAULT 'pending';
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS auto_label_error TEXT;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS auto_label_key VARCHAR;
ALTER TABLE video_metadata ADD COLUMN IF NOT EXISTS auto_labeled_at TIMESTAMP;

-- processed_clips: clip 미디어 메타 + 이미지 추출 상태 컬럼 추가
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS duration_sec DOUBLE;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS fps DOUBLE;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS frame_count INTEGER;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS image_extract_status VARCHAR DEFAULT 'pending';
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS image_extract_count INTEGER DEFAULT 0;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS image_extract_error TEXT;
ALTER TABLE processed_clips ADD COLUMN IF NOT EXISTS image_extracted_at TIMESTAMP;

-- image_labels: YOLO-world 등 이미지 detection 라벨 테이블
CREATE TABLE IF NOT EXISTS image_labels (
    image_label_id   VARCHAR PRIMARY KEY,
    image_id         VARCHAR REFERENCES image_metadata(image_id),
    source_clip_id   VARCHAR REFERENCES processed_clips(clip_id),
    labels_bucket    VARCHAR DEFAULT 'vlm-labels',
    labels_key       VARCHAR,
    label_format     VARCHAR,
    label_tool       VARCHAR,
    label_source     VARCHAR,
    review_status    VARCHAR DEFAULT 'pending',
    label_status     VARCHAR DEFAULT 'pending',
    object_count     INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
