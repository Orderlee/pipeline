# VLM DataOps Pipeline

VLM(Vision-Language Model) 학습 데이터를 구축하기 위한 데이터 파이프라인입니다.
NAS에 있는 이미지/비디오 미디어를 수집 → 중복 제거 → 라벨 매칭 → 전처리 → 학습 데이터셋 조립까지의 전 과정을 **Dagster** 기반으로 자동화합니다.

## Architecture

``` 
┌───────────────────────────────────────────────────────────┐
│  NAS (Network Attached Storage)                           │
│    /nas/incoming --- media files                          │
│    /nas/archive  --- archived originals                   │
└─────────┬─────────────────────────────────────────────────┘
          |
          v
┌───────────────────────────────────────────────────────────┐
│  Dagster (Orchestrator)                                   │
│                                                           │
│ Sensor --> INGEST --> DEDUP --> LABEL --> PROC --> BUILD  │
│  manifest   validate   dedup    match    preproc    build │
│  detect     upload     phash    labels   clips     dataset│
└──┬──────────┬──────────────────┬──────────────┬───────────┘
   |          |                  |              |
   v          v                  v              v
┌───────────────────────────────────────────────────────────┐
│  Storage                                                  │
│                                                           │
│  MinIO (S3)          DuckDB           PostgreSQL  Grafana │
│  |- vlm-raw          data catalog     NAS folder     ^    │
│  |- vlm-labels         |              tree           |    │
│  |- vlm-processed      v              '--------------'    │
│  '- vlm-dataset      MotherDuck (optional)                │
│                                                           │
│  GCS (Google Cloud Storage) --- external data source      │
└───────────────────────────────────────────────────────────┘
```

- **NAS**: 네트워크 스토리지. `/nas/incoming`으로 미디어가 수신되고, 처리 완료 후 `/nas/archive`로 이동
- **Dagster**: 파이프라인 오케스트레이터. Sensor가 manifest를 감지하면 5단계 파이프라인을 자동 실행
- **MinIO**: S3 호환 오브젝트 스토리지. 단계별로 4개 버킷(`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`)에 저장
- **DuckDB**: 데이터 카탈로그. 파일 메타데이터·라벨·데이터셋 정보를 관리 (선택적으로 MotherDuck 클라우드 동기화)
- **PostgreSQL + Grafana**: NAS 폴더 트리 데이터를 저장하고 대시보드로 시각화
- **GCS**: Google Cloud Storage에서 외부 데이터를 `/nas/incoming`으로 자동 다운로드 (스케줄 기반)

## Data Flow

파이프라인은 5단계로 구성되며, 각 단계는 Dagster Asset으로 정의되어 있습니다.

```
Data Sources
  |
  |--- NAS /incoming (direct upload)
  |--- GCS buckets  (gcs_download_schedule, every minute)
  |
  v
┌───────────────────────────────────────────────────────────┐
│ Phase 1 — Auto (Dagster Sensor trigger)                   │
│                                                           │
│ 1. INGEST (ingested_raw_files)                            │
│    parse manifest -> validate -> normalize -> upload      │
│    |- DuckDB: raw_files, image/video_metadata INSERT      │
│    |- MinIO:  upload to vlm-raw bucket                    │
│    '- NAS:    move originals to /nas/archive              │
├───────────────────────────────────────────────────────────┤
│ 2. DEDUP (dedup_results)                                  │
│    compute pHash -> detect similar media -> group         │
│    '- DuckDB: raw_files.dup_group_id UPDATE               │
└─────────────────────┬─────────────────────────────────────┘
                      |
                      v  read media from vlm-raw bucket
┌───────────────────────────────────────────────────────────┐
│ External Labeling (CVAT / Label Studio)                   │
│                                                           │
│  MinIO vlm-raw <-- labeling tool reads directly           │
│                       |                                   │
│                       v  annotation complete              │
│                   Label JSON Export                       │
│                       |                                   │
│                       v                                   │
│             Webhook / Dagster Sensor                      │
└─────────────────────┬─────────────────────────────────────┘
                      |
                      v
┌───────────────────────────────────────────────────────────┐
│ Phase 2 — Auto (Dagster Sensor trigger)                   │
│                                                           │
│ 3. LABEL (labeled_files)                                  │
│    match label JSON to raw_key                            │
│    |- DuckDB: labels INSERT                               │
│    '- MinIO:  upload to vlm-labels bucket                 │
├───────────────────────────────────────────────────────────┤
│ 4. PROCESS (processed_clips)                              │
│    raw media + labels -> generate processed clips         │
│    |- DuckDB: processed_clips INSERT                      │
│    '- MinIO:  upload to vlm-processed bucket              │
├───────────────────────────────────────────────────────────┤
│ 5. BUILD (built_dataset)                                  │
│    split train/val/test 80:10:10 -> assemble dataset      │
│    |- DuckDB: datasets, dataset_clips INSERT              │
│    '- MinIO:  upload to vlm-dataset bucket                │
└───────────────────────────────────────────────────────────┘
  |
  v  (optional)
MotherDuck sync: local DuckDB -> cloud
```

### 단계별 요약

| 단계 | Asset | 설명 | DuckDB 테이블 | MinIO 버킷 |
|------|-------|------|---------------|------------|
| 0. GCS | `gcs_download_to_incoming` | GCS 버킷에서 `/nas/incoming`으로 미디어 다운로드 | — | — |
| 1. INGEST | `ingested_raw_files` | manifest 파싱 → 파일 검증 → 정규화 → MinIO 업로드 → 메타데이터 추출 | `raw_files`, `image_metadata`, `video_metadata` | `vlm-raw` |
| 2. DEDUP | `dedup_results` | pHash 기반 유사 미디어 검출, 중복 그룹 ID 부여 | `raw_files` (dup_group_id 갱신) | — |
| — | 외부 라벨링 | CVAT / Label Studio에서 MinIO `vlm-raw` 미디어를 직접 참조하여 어노테이션 작업 | — | — |
| 3. LABEL | `labeled_files` | 라벨 JSON 파일을 raw_key 기준으로 매칭 | `labels` | `vlm-labels` |
| 4. PROCESS | `processed_clips` | 원본 미디어 + 라벨 → 전처리 결과물 생성 | `processed_clips` | `vlm-processed` |
| 5. BUILD | `built_dataset` | train/val/test 80:10:10 분할 → 학습 데이터셋 조립 | `datasets`, `dataset_clips` | `vlm-dataset` |
| — | `motherduck_sync` | 로컬 DuckDB 테이블을 MotherDuck 클라우드로 동기화 (선택) | — | — |

### 파이프라인 트리거 방식

- **자동**: `auto_bootstrap_manifest_sensor`가 `/nas/incoming` 감시 → manifest JSON 생성 → `incoming_manifest_sensor`가 `mvp_stage_job` 트리거
- **수동**: `scripts/bootstrap_manifest.sh`로 manifest 생성 후 sensor가 감지

## Database Schema

DuckDB에 7개 운영 테이블이 정의되어 있습니다. (`src/vlm_pipeline/sql/schema.sql`)

### 테이블 관계도

```
raw_files
  |
  |---- 1:1 ---- image_metadata
  |
  |---- 1:1 ---- video_metadata
  |
  |---- 1:N ---- labels
  |                |
  |                '---- 1:N ---- processed_clips
  |                                  |
  '---- 1:N -------------------------'
                                     |
                                     '---- M:N ---- datasets
                                        (via dataset_clips join table)
```

- `raw_files` → `image_metadata` / `video_metadata`: 1:1 관계. INGEST 시 미디어 타입에 따라 이미지 또는 비디오 메타데이터가 생성됩니다.
- `raw_files` → `labels`: 1:N 관계. 하나의 미디어 파일에 여러 라벨이 매칭될 수 있습니다.
- `raw_files` + `labels` → `processed_clips`: 원본과 라벨을 조합하여 전처리 결과물을 생성합니다.
- `processed_clips` ↔ `datasets`: M:N 관계. `dataset_clips` 연결 테이블을 통해 하나의 클립이 여러 데이터셋에, 하나의 데이터셋이 여러 클립을 포함할 수 있습니다.

### 테이블 상세

**`raw_files`** — NAS 파일 스캔 및 수집 상태 관리 (INGEST 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `asset_id` | VARCHAR (PK) | 고유 식별자 |
| `source_path` | VARCHAR | NAS 원본 경로 |
| `media_type` | VARCHAR | image / video |
| `file_size` | BIGINT | 파일 크기 (bytes) |
| `checksum` | VARCHAR (UNIQUE) | SHA-256 해시 |
| `phash` | VARCHAR | perceptual hash (DEDUP용) |
| `dup_group_id` | VARCHAR | 중복 그룹 ID |
| `raw_bucket` / `raw_key` | VARCHAR | MinIO 저장 위치 |
| `ingest_status` | VARCHAR | pending / ingested / failed |

**`image_metadata`** — 이미지 메타데이터 (INGEST 1-pass 추출)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `asset_id` | VARCHAR (PK, FK) | raw_files 참조 |
| `width` / `height` | INTEGER | 해상도 |
| `color_mode` | VARCHAR | RGB / RGBA 등 |
| `codec` | VARCHAR | 이미지 코덱 |

**`video_metadata`** — 비디오 메타데이터 (ffprobe 추출)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `asset_id` | VARCHAR (PK, FK) | raw_files 참조 |
| `width` / `height` | INTEGER | 해상도 |
| `duration_sec` | DOUBLE | 길이 (초) |
| `fps` | DOUBLE | 프레임 레이트 |
| `codec` | VARCHAR | 비디오 코덱 |
| `environment_type` | VARCHAR | indoor / outdoor (Places365) |
| `daynight_type` | VARCHAR | day / night (밝기 분석) |

**`labels`** — 라벨 데이터 (LABEL 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `label_id` | VARCHAR (PK) | 라벨 고유 ID |
| `asset_id` | VARCHAR (FK) | raw_files 참조 |
| `labels_key` | VARCHAR | MinIO 라벨 경로 |
| `label_format` | VARCHAR | 라벨 포맷 (JSON 등) |

**`processed_clips`** — 전처리 결과물 (PROCESS 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `clip_id` | VARCHAR (PK) | 클립 고유 ID |
| `source_asset_id` | VARCHAR (FK) | raw_files 참조 |
| `source_label_id` | VARCHAR (FK) | labels 참조 |
| `clip_key` | VARCHAR | MinIO 저장 경로 |
| `process_status` | VARCHAR | pending / completed / failed |

**`datasets`** — 데이터셋 정의 (BUILD 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `dataset_id` | VARCHAR (PK) | 데이터셋 고유 ID |
| `name` / `version` | VARCHAR | 이름, 버전 |
| `split_ratio` | JSON | `{"train":0.8,"val":0.1,"test":0.1}` |
| `build_status` | VARCHAR | pending / completed / failed |

**`dataset_clips`** — 데이터셋↔클립 연결 (M:N, BUILD 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `dataset_id` | VARCHAR (PK, FK) | datasets 참조 |
| `clip_id` | VARCHAR (PK, FK) | processed_clips 참조 |
| `split` | VARCHAR | train / val / test |

## Project Structure

```text
dataops/
├── src/
│   ├── vlm_pipeline/              # Dagster 기반 VLM 파이프라인 (핵심)
│   │   ├── definitions.py         #   Dagster Definitions 진입점
│   │   ├── defs/                   #   Asset 정의
│   │   │   ├── ingest/            #     INGEST (검증, MinIO 업로드, 메타 추출)
│   │   │   ├── dedup/             #     DEDUP (pHash 유사도 검출)
│   │   │   ├── label/             #     LABEL (라벨 매칭)
│   │   │   ├── process/           #     PROCESS (전처리)
│   │   │   ├── build/             #     BUILD (데이터셋 조립)
│   │   │   ├── gcp/               #     GCS 다운로드
│   │   │   └── sync/              #     MotherDuck 동기화
│   │   ├── lib/                    #   유틸리티 (phash, checksum, video_env 등)
│   │   ├── resources/              #   DuckDB, MinIO 리소스
│   │   └── sql/                    #   스키마 DDL, 마이그레이션
│   └── python/common/              # 공통 모듈 (설정, MinIO 클라이언트 등)
├── gcp/                            # GCS 다운로드 스크립트 (rclone 기반)
├── docker/                         # Docker Compose 인프라
│   ├── docker-compose.yaml
│   ├── app/                        #   Dagster 앱 (dagster_defs.py, workspace.yaml)
│   └── grafana/                    #   Grafana 대시보드 프로비저닝
├── scripts/                        # 유틸리티 스크립트
└── split_dataset/                  # 데이터셋 분할 도구 (YOLO 등)
```

## Infrastructure

Docker Compose(`docker/docker-compose.yaml`)로 아래 서비스를 실행합니다.

| 서비스 | 포트 | 역할 |
|--------|------|------|
| **dagster** | `3030` | Dagster dev 서버 — 파이프라인 실행/모니터링 UI |
| **app** | — | GPU 지원 메인 컨테이너 (docker exec로 스크립트 실행) |
| **minio** | `9000` (API) / `9001` (Console) | S3 호환 오브젝트 스토리지 |
| **postgres** | `5432` | NAS 폴더 트리 데이터 저장, Grafana 데이터소스 |
| **grafana** | `3000` | NAS 사용량 대시보드 |

### MinIO 버킷

| 버킷 | 용도 | 파이프라인 단계 |
|-------|------|----------------|
| `vlm-raw` | 원본 미디어 | INGEST |
| `vlm-labels` | 라벨 파일 | LABEL |
| `vlm-processed` | 전처리된 클립 | PROCESS |
| `vlm-dataset` | 최종 학습 데이터셋 | BUILD |

### NAS 볼륨 마운트

| 컨테이너 경로 | 용도 |
|---------------|------|
| `/nas/incoming` | 미디어 파일 수신 (읽기/쓰기) |
| `/nas/archive` | 처리 완료 원본 보관 (읽기/쓰기) |
| `/nas/datasets` | 데이터셋 저장소 |
| `/nas/datasets/projects` | 프로젝트별 데이터 (읽기 전용) |

## Getting Started

### 1. Requirements

- Python 3.10+
- Docker & Docker Compose
- NVIDIA GPU + CUDA 12.4 (비디오 환경 분류에 사용)
- NAS 마운트 (CIFS/NFS)

### 2. 환경 설정

`.env.example`을 복사하여 본인 환경에 맞게 수정합니다.

```bash
cp .env.example docker/.env
```

주요 환경변수:

| 변수 | 설명 |
|------|------|
| `DATAOPS_DUCKDB_PATH` | DuckDB 파일 경로 |
| `MINIO_ENDPOINT` | MinIO 접속 주소 |
| `INCOMING_DIR` | 미디어 수신 디렉터리 (컨테이너 내부) |
| `DATASETS_HOST_PATH` | NAS 데이터셋 호스트 경로 |
| `INCOMING_HOST_PATH` | NAS incoming 호스트 경로 |
| `DATAOPS_ASSET_PATH_PREFIX_FROM/TO` | 호스트↔컨테이너 경로 매핑 |
| `MOTHERDUCK_TOKEN` | MotherDuck 동기화 토큰 (선택) |

### 3. 인프라 실행

```bash
cd docker
docker compose up -d
```

### 4. 환경 검증

```bash
./scripts/verify_mvp.sh
```

Docker 상태, MinIO 연결, DuckDB 스키마, NAS 마운트를 확인합니다.

### 5. 파이프라인 실행

Dagster UI에 접속하여 파이프라인을 실행합니다.

```
http://(Workstation_IP):3030
```

- **자동 실행**: Sensor를 활성화하면 `/nas/incoming`에 파일이 들어올 때 자동으로 파이프라인이 실행됩니다.
- **수동 실행**: Dagster UI에서 `mvp_stage_job`을 Launchpad로 직접 실행할 수 있습니다.
- **manifest 수동 생성**: `./scripts/bootstrap_manifest.sh <디렉터리>` 로 manifest를 만들면 sensor가 감지하여 실행합니다.

## Dagster Jobs & Sensors

### Jobs

| Job | 설명 |
|-----|------|
| `mvp_stage_job` | 전체 파이프라인 (INGEST → DEDUP → LABEL → PROCESS → BUILD) |
| `ingest_job` | INGEST 단독 |
| `dedup_job` | DEDUP 단독 |
| `label_job` | LABEL 단독 |
| `process_build_job` | PROCESS + BUILD |
| `gcs_download_job` | GCS 버킷 → `/nas/incoming` 다운로드 |
| `motherduck_sync_job` | DuckDB → MotherDuck 동기화 |

### Sensors

| Sensor | 설명 |
|--------|------|
| `incoming_manifest_sensor` | `/nas/incoming/.manifests/pending/*.json` 감지 → `mvp_stage_job` 트리거 |
| `auto_bootstrap_manifest_sensor` | `/nas/incoming` 미디어 감시 → manifest 자동 생성 |
| `motherduck_*_sensor` (7개) | DuckDB 테이블별 row count 변경 감지 → `motherduck_sync_job` 트리거 |

### Schedules

| Schedule | Cron | 설명 |
|----------|------|------|
| `gcs_download_schedule` | `* * * * *` | 매분 GCS 버킷에서 새 미디어 다운로드 |

## Data Catalog Query

DuckDB를 사용하여 파이프라인 데이터를 조회할 수 있습니다.

```sql
-- 파일 수집 현황
SELECT ingest_status, COUNT(*) as cnt
FROM raw_files
GROUP BY ingest_status;

-- 미디어 타입별 파일 수 및 용량
SELECT media_type, COUNT(*) as cnt, SUM(file_size)/1024/1024 as size_mb
FROM raw_files
GROUP BY media_type
ORDER BY size_mb DESC;

-- 데이터셋 빌드 현황
SELECT d.name, d.build_status, COUNT(dc.clip_id) as clip_count, dc.split
FROM datasets d
JOIN dataset_clips dc ON d.dataset_id = dc.dataset_id
GROUP BY d.name, d.build_status, dc.split;
```
