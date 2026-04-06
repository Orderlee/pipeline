# Production khon/adlib 라벨·전처리 정리 Runbook

production에서 아래 두 prefix의 라벨·전처리 흔적만 정리할 때 사용하는 runbook입니다.

- `khon-kaen-rtsp-bucket/`
- `adlib-hotel-202512/`

## 목적

- `labels`
- `processed_clips`
- 관련 `image_metadata`
- 관련 `image_labels`
- 관련 `dataset_clips`

위 범위만 삭제합니다.

아래는 유지합니다.

- `raw_files`
- `video_metadata`
- `vlm-raw`
- `/nas/archive`
- staging
- MotherDuck

## 스크립트

- [`cleanup_label_preprocess_prefixes.py`](/home/pia/work_p/Datapipeline-Data-data_pipeline/scripts/cleanup_label_preprocess_prefixes.py)

## 실행 전 확인

- production writer를 먼저 멈춥니다.
- 최소 대상: dispatch / label / process / yolo / sync 관련 job, sensor
- 이유: DuckDB single-writer 원칙과 cleanup 중 재생성 방지

`--target-prefix`는 양쪽(`labels`/`processed`)을 같은 prefix로 함께 지울 때 쓰는 레거시 옵션입니다.
비대칭 요청은 아래처럼 나눠서 실행합니다.

- `--labels-prefix`: `vlm-labels/<prefix>` 삭제 대상
- `--processed-prefix`: `vlm-processed/<prefix>` 삭제 대상
- 주의: `labels` 삭제 시 해당 label을 참조하는 `processed_clips`/`image_metadata`는 DB 무결성을 위해 함께 삭제됩니다.

## Dry-run

컨테이너 내부 실행 예시:

```bash
docker exec -i pipeline-dagster-1 python3 /scripts/cleanup_label_preprocess_prefixes.py \
  --labels-prefix adlib-hotel-202512/ \
  --labels-prefix khon-kaen-rtsp-bucket/ \
  --processed-prefix khon-kaen-rtsp-bucket/ \
  --scope label_preprocess \
  --db /data/pipeline.duckdb \
  --minio-endpoint http://pipeline-minio-1:9000 \
  --dry-run \
  --report-path /scripts/reports/production_label_preprocess_cleanup_dry_run.json
```

호스트에서 직접 실행할 때는 MinIO endpoint를 `http://127.0.0.1:9000`로 바꿉니다.

## Apply

```bash
docker exec -i pipeline-dagster-1 python3 /scripts/cleanup_label_preprocess_prefixes.py \
  --labels-prefix adlib-hotel-202512/ \
  --labels-prefix khon-kaen-rtsp-bucket/ \
  --processed-prefix khon-kaen-rtsp-bucket/ \
  --scope label_preprocess \
  --db /data/pipeline.duckdb \
  --minio-endpoint http://pipeline-minio-1:9000 \
  --apply \
  --report-path /scripts/reports/production_label_preprocess_cleanup_apply.json
```

## 검증 기준

- `labels`, `processed_clips`, `image_metadata`, `image_labels`, `dataset_clips`에서 target prefix 관련 count가 0
- `raw_files` count는 cleanup 전후 동일
- `video_metadata` count도 cleanup 전후 동일
- `vlm-labels`, `vlm-processed`의 target prefix 조회 결과가 0
- MinIO missing은 허용하되, report에 별도 기록

## 현재 베이스라인

- `khon-kaen-rtsp-bucket`
  - `raw_files`: 4215
  - `video_metadata`: 4215
  - `labels`: 1305
  - `processed_clips`: 1957
  - `image_metadata`: 9785
  - `image_labels`: 0
  - `dataset_clips`: 0
- `adlib-hotel-202512`
  - `raw_files`: 3497
  - `video_metadata`: 3497
  - `labels`: 106
  - `processed_clips`: 103
  - `image_metadata`: 0
  - `image_labels`: 0
  - `dataset_clips`: 0

## 메모

- 현재 관측상 DB reference가 MinIO 실오브젝트보다 많이 남아 있습니다.
- 따라서 이 작업은 MinIO 실삭제보다 DB stale reference 정리가 핵심입니다.
