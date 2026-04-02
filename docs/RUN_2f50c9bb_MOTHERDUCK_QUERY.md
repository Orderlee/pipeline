# Run 2f50c9bb MotherDuck Query

Dagster run `2f50c9bb-3da6-4d8f-94a8-b9e4e8c316b0` 의 MotherDuck 조회 기준 키와 실행용 SQL을 정리한 문서입니다.

MotherDuck에는 Dagster `run_id`가 직접 동기화되지 않으므로 `run_id`로 바로 조회할 수 없습니다. 이 run은 `dispatch_request_id`, `folder_name`, `ingest_batch_id`를 기준으로 추적해야 합니다.

## Run Mapping

- `dispatch_request_id`: `prod_tmp_data_2_retry_20260331_152111`
- `folder_name`: `tmp_data_2`
- `ingest_batch_id`: `dispatch_prod_tmp_data_2_retry_20260331_152111_20260331_062216`
- MotherDuck 동기화 대상은 운영 8개 테이블뿐입니다:
  - `raw_files`
  - `image_metadata`
  - `video_metadata`
  - `labels`
  - `processed_clips`
  - `datasets`
  - `dataset_clips`
  - `image_labels`
- 근거 파일:
  - [local_duckdb_to_motherduck_sync.py](/home/pia/work_p/Datapipeline-Data-data_pipeline/src/python/local_duckdb_to_motherduck_sync.py#L41)
  - [service.py](/home/pia/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/defs/dispatch/service.py#L206)
  - [ops.py](/home/pia/work_p/Datapipeline-Data-data_pipeline/src/vlm_pipeline/defs/ingest/ops.py#L109)

## Main Query

```sql
WITH target_assets AS (
    SELECT
        r.asset_id,
        r.raw_key,
        r.source_path,
        r.media_type,
        r.ingest_batch_id,
        r.ingest_status,
        r.created_at AS raw_created_at
    FROM raw_files r
    WHERE r.raw_key LIKE 'tmp_data_2/%'
),
label_counts AS (
    SELECT l.asset_id, COUNT(*) AS label_rows, MIN(l.created_at) AS first_label_at, MAX(l.created_at) AS last_label_at
    FROM labels l
    GROUP BY l.asset_id
),
clip_counts AS (
    SELECT pc.source_asset_id AS asset_id, COUNT(*) AS clip_rows, MIN(pc.created_at) AS first_clip_at, MAX(pc.created_at) AS last_clip_at
    FROM processed_clips pc
    GROUP BY pc.source_asset_id
),
frame_counts AS (
    SELECT
        im.source_asset_id AS asset_id,
        COUNT(*) AS frame_rows,
        SUM(CASE WHEN COALESCE(TRIM(im.image_caption_text), '') <> '' THEN 1 ELSE 0 END) AS captioned_frames,
        MIN(im.extracted_at) AS first_frame_at,
        MAX(im.extracted_at) AS last_frame_at
    FROM image_metadata im
    GROUP BY im.source_asset_id
),
yolo_counts AS (
    SELECT
        im.source_asset_id AS asset_id,
        COUNT(DISTINCT il.image_label_id) AS bbox_rows,
        SUM(COALESCE(il.object_count, 0)) AS bbox_objects,
        MIN(il.created_at) AS first_bbox_at,
        MAX(il.created_at) AS last_bbox_at
    FROM image_labels il
    JOIN image_metadata im ON im.image_id = il.image_id
    GROUP BY im.source_asset_id
)
SELECT
    ta.raw_key,
    ta.asset_id,
    CASE WHEN ta.ingest_batch_id = 'dispatch_prod_tmp_data_2_retry_20260331_152111_20260331_062216' THEN 'new_in_this_run' ELSE 'reused_existing_asset' END AS asset_origin_in_run,
    ta.ingest_status,
    vm.timestamp_status,
    vm.caption_status,
    vm.frame_status,
    vm.bbox_status,
    COALESCE(lc.label_rows, 0) AS label_rows,
    COALESCE(cc.clip_rows, 0) AS clip_rows,
    COALESCE(fc.frame_rows, 0) AS frame_rows,
    COALESCE(fc.captioned_frames, 0) AS captioned_frames,
    COALESCE(yc.bbox_rows, 0) AS bbox_rows,
    COALESCE(yc.bbox_objects, 0) AS bbox_objects,
    vm.timestamp_error,
    vm.caption_error,
    vm.frame_error,
    vm.bbox_error,
    ta.raw_created_at,
    lc.last_label_at,
    cc.last_clip_at,
    fc.last_frame_at,
    yc.last_bbox_at
FROM target_assets ta
LEFT JOIN video_metadata vm ON vm.asset_id = ta.asset_id
LEFT JOIN label_counts lc ON lc.asset_id = ta.asset_id
LEFT JOIN clip_counts cc ON cc.asset_id = ta.asset_id
LEFT JOIN frame_counts fc ON fc.asset_id = ta.asset_id
LEFT JOIN yolo_counts yc ON yc.asset_id = ta.asset_id
ORDER BY ta.raw_key;
```

## Event Detail Query

```sql
WITH target_assets AS (
    SELECT r.asset_id, r.raw_key, r.ingest_batch_id
    FROM raw_files r
    WHERE r.raw_key LIKE 'tmp_data_2/%'
)
SELECT
    ta.raw_key,
    ta.asset_id,
    CASE WHEN ta.ingest_batch_id = 'dispatch_prod_tmp_data_2_retry_20260331_152111_20260331_062216' THEN 'new_in_this_run' ELSE 'reused_existing_asset' END AS asset_origin_in_run,
    l.event_index,
    ROUND(l.timestamp_start_sec, 3) AS start_sec,
    ROUND(l.timestamp_end_sec, 3) AS end_sec,
    l.caption_text,
    l.labels_key,
    pc.clip_id,
    pc.clip_key,
    COUNT(DISTINCT im.image_id) AS frame_rows,
    COUNT(DISTINCT il.image_label_id) AS bbox_rows,
    COALESCE(SUM(il.object_count), 0) AS bbox_objects,
    l.created_at AS label_created_at,
    MAX(il.created_at) AS last_bbox_at
FROM target_assets ta
LEFT JOIN labels l ON l.asset_id = ta.asset_id
LEFT JOIN processed_clips pc ON pc.source_label_id = l.label_id
LEFT JOIN image_metadata im ON im.source_clip_id = pc.clip_id
LEFT JOIN image_labels il ON il.image_id = im.image_id
GROUP BY
    ta.raw_key, ta.asset_id, ta.ingest_batch_id,
    l.event_index, l.timestamp_start_sec, l.timestamp_end_sec,
    l.caption_text, l.labels_key, l.created_at,
    pc.clip_id, pc.clip_key
ORDER BY ta.raw_key, l.event_index, pc.clip_id;
```

## Notes

신규 ingest row만 따로 확인할 때는 아래 쿼리를 사용합니다.

```sql
SELECT * FROM raw_files
WHERE ingest_batch_id = 'dispatch_prod_tmp_data_2_retry_20260331_152111_20260331_062216'
ORDER BY raw_key;
```

이 run은 확인 시점 기준 `STARTED` 상태였고 2026-03-31 15:51 KST까지 로그가 진행 중이라 결과 건수는 변할 수 있음.
