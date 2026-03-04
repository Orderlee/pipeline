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
