# Production source-a/source-b Label·Preprocess Cleanup Runbook

Use this runbook when cleaning up only the label·preprocess traces for the two prefixes below in production.

- `source-a-rtsp-bucket/`
- `source-b-202512/`

## Purpose

Delete only the following:

- `labels`
- `processed_clips`
- related `image_metadata`
- related `image_labels`
- related `dataset_clips`

Preserve the following:

- `raw_files`
- `video_metadata`
- `vlm-raw`
- `/nas/archive`
- staging
- MotherDuck

## Script

- [`cleanup_label_preprocess_prefixes.py`](/home/user/work_p/Datapipeline-Data-data_pipeline/scripts/cleanup_label_preprocess_prefixes.py)

## Pre-run checks

- Stop production writers first.
- Minimum targets: dispatch / label / process / yolo / sync related jobs and sensors
- Reason: DuckDB single-writer principle and prevention of recreation during cleanup

`--target-prefix` is a legacy option for deleting both sides (`labels`/`processed`) under the same prefix simultaneously.
For asymmetric requests, split them as shown below:

- `--labels-prefix`: deletion target under `vlm-labels/<prefix>`
- `--processed-prefix`: deletion target under `vlm-processed/<prefix>`
- Note: when deleting `labels`, `processed_clips`/`image_metadata` referencing those labels are deleted together for DB integrity.

## Dry-run

Example run inside container:

```bash
docker exec -i pipeline-dagster-1 python3 /scripts/cleanup_label_preprocess_prefixes.py \
  --labels-prefix source-b-202512/ \
  --labels-prefix source-a-rtsp-bucket/ \
  --processed-prefix source-a-rtsp-bucket/ \
  --scope label_preprocess \
  --db /data/pipeline.duckdb \
  --minio-endpoint http://pipeline-minio-1:9000 \
  --dry-run \
  --report-path /scripts/reports/production_label_preprocess_cleanup_dry_run.json
```

When running directly from the host, change the MinIO endpoint to `http://127.0.0.1:9000`.

## Apply

```bash
docker exec -i pipeline-dagster-1 python3 /scripts/cleanup_label_preprocess_prefixes.py \
  --labels-prefix source-b-202512/ \
  --labels-prefix source-a-rtsp-bucket/ \
  --processed-prefix source-a-rtsp-bucket/ \
  --scope label_preprocess \
  --db /data/pipeline.duckdb \
  --minio-endpoint http://pipeline-minio-1:9000 \
  --apply \
  --report-path /scripts/reports/production_label_preprocess_cleanup_apply.json
```

## Verification criteria

- Count related to target prefix in `labels`, `processed_clips`, `image_metadata`, `image_labels`, `dataset_clips` is 0
- `raw_files` count is identical before and after cleanup
- `video_metadata` count is also identical before and after cleanup
- Query result for target prefix in `vlm-labels`, `vlm-processed` is 0
- MinIO missing is acceptable but must be recorded separately in the report

## Current baseline

- `source-a-rtsp-bucket`
  - `raw_files`: 4215
  - `video_metadata`: 4215
  - `labels`: 1305
  - `processed_clips`: 1957
  - `image_metadata`: 9785
  - `image_labels`: 0
  - `dataset_clips`: 0
- `source-b-202512`
  - `raw_files`: 3497
  - `video_metadata`: 3497
  - `labels`: 106
  - `processed_clips`: 103
  - `image_metadata`: 0
  - `image_labels`: 0
  - `dataset_clips`: 0

## Notes

- Current observation shows more DB references remaining than actual MinIO objects.
- Therefore, the core of this task is cleaning up DB stale references rather than deleting actual MinIO objects.
