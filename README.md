# VLM DataOps Pipeline

VLM(Vision-Language Model) 학습 데이터를 구축하기 위한 데이터 파이프라인입니다.
NAS에 있는 이미지/비디오 미디어를 수집 → inline 중복 제거 → 자동 라벨링/수동 검수 반영 → 전처리 데이터 생성 → 학습 데이터셋 조립까지의 전 과정을 **Dagster** 기반으로 자동화합니다.

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
│ Sensor --> INGEST+DEDUP --> AUTO_LABEL --> BUILD          │
│  manifest   validate       frames/labels   processed data │
│  detect     upload/phash   captions/clips  dataset        │
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
- **Dagster**: 파이프라인 오케스트레이터. Sensor가 manifest를 감지하면 `INGEST+DEDUP → AUTO LABELING → BUILD` 흐름을 자동 실행
- **MinIO**: S3 호환 오브젝트 스토리지. 단계별로 4개 버킷(`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`)에 저장
- **DuckDB**: 데이터 카탈로그. 파일 메타데이터·라벨·데이터셋 정보를 관리 (선택적으로 MotherDuck 클라우드 동기화)
- **PostgreSQL + Grafana**: NAS 폴더 트리 데이터를 저장하고 대시보드로 시각화
- **GCS**: Google Cloud Storage에서 외부 데이터를 `/nas/incoming`으로 자동 다운로드 (스케줄 기반)

## Data Flow

파이프라인은 4단계로 구성되며, 각 단계는 Dagster Asset으로 정의되어 있습니다.

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
│ 1. INGEST+DEDUP (ingested_raw_files)                      │
│    parse manifest -> validate -> normalize -> upload      │
│    |- DuckDB: raw_files, image/video_metadata INSERT      │
│    |- MinIO:  upload to vlm-raw bucket                    │
│    |- DuckDB: raw_files.phash / dup_group_id UPDATE       │
│    '- NAS:    move originals to /nas/archive              │
└─────────────────────┬─────────────────────────────────────┘
                      |
                      v  read media from vlm-raw bucket
┌───────────────────────────────────────────────────────────┐
│ External / Auto Labeling                                  │
│                                                           │
│  raw video -> external model inference                    │
│            |- timestamp                                   │
│            |- caption                                     │
│            '- object detection json                       │
│                     |                                     │
│                     v                                     │
│  labeled_files -> labels table / vlm-labels 등록          │
│                     |                                     │
│                     v                                     │
│  extracted_video_frames -> timestamp 기반 대표 frame 추출 │
│  manual review tool (optional) -> reviewed label JSON     │
└─────────────────────┬─────────────────────────────────────┘
                      |
                      v
┌───────────────────────────────────────────────────────────┐
│ Phase 2 — Auto (Dagster Sensor trigger)                   │
│                                                           │
│ 2. AUTO LABEL (labeled_files, extracted_video_frames)     │
│    register labels -> extract event frames               │
│    |- DuckDB: labels, image_metadata INSERT               │
│    '- MinIO:  upload to vlm-labels / vlm-processed        │
├───────────────────────────────────────────────────────────┤
│ 3. PROCESS (processed_data / processed_clips table)       │
│    raw media + labels -> generate processed data          │
│    |- DuckDB: processed_clips INSERT                      │
│    '- MinIO:  upload to vlm-processed bucket              │
├───────────────────────────────────────────────────────────┤
│ 4. BUILD (build_dataset)                                  │
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
| 1. INGEST+DEDUP | `raw_ingest` | manifest 파싱 → 파일 검증 → 정규화 → MinIO 업로드 → 메타데이터 추출 → pHash 기반 중복 그룹 반영 | `raw_files`, `image_metadata`, `video_metadata` | `vlm-raw` |
| 2a. TIMESTAMP | `clip_timestamp` | Gemini 호출 → 이벤트 구간 JSON 생성 → vlm-labels 업로드 | `video_metadata` | `vlm-labels` |
| 2b. CAPTIONING | `clip_captioning` | Gemini JSON 정규화 → 이벤트별 labels 테이블 upsert | `labels` | `vlm-labels` |
| 2c. PROCESS | `clip_to_frame` | label 기반 clip 절단 + ffprobe 메타 추출 + 적응형 프레임 추출 | `processed_clips`, `image_metadata` | `vlm-processed` |
| 2d. YOLO | `yolo_image_detection` | YOLO-World-L object detection (선택, `ENABLE_YOLO_DETECTION=true`) | `image_labels` | `vlm-labels` |
| 3. BUILD | `build_dataset` | train/val/test 80:10:10 분할 → 학습 데이터셋 조립 | `datasets`, `dataset_clips` | `vlm-dataset` |
| — | `motherduck_sync` | 로컬 DuckDB 테이블을 MotherDuck 클라우드로 동기화 (선택) | — | — |

### 파이프라인 트리거 방식

- **자동**: `auto_bootstrap_manifest_sensor`가 `/nas/incoming` 감시 → manifest JSON 생성 → `incoming_manifest_sensor`가 `mvp_stage_job` 트리거
- **수동**: `scripts/bootstrap_manifest.sh`로 manifest 생성 후 sensor가 감지

### Auto Labeling 상세

Auto Labeling은 3개 단계의 Dagster Asset으로 구성되며, `auto_labeling_sensor`가 backlog(Gemini 미처리, captioning 미완료, clip 미생성)을 감지하면 자동으로 `auto_labeling_job`을 트리거합니다.

```
raw_ingest (완료)
     │
     v  auto_labeling_sensor (backlog 감지)
┌────────────────────────────────────────────────────────────────┐
│ Step 1. clip_timestamp                                        │
│   Gemini 1회 호출 → 이벤트 JSON 생성                            │
│   ├─ 비디오 파일 확보 (archive_path 또는 MinIO vlm-raw fallback)  │
│   ├─ 대용량 비디오 자동 리사이즈 (ffmpeg preview, 아래 참조)        │
│   ├─ Gemini에 VIDEO_EVENT_PROMPT 전송                           │
│   │   → [{category, timestamp, ko_caption, en_caption}] 반환    │
│   ├─ vlm-labels/<raw_parent>/events/<video_stem>.json 업로드     │
│   └─ video_metadata.auto_label_status = 'generated' 갱신        │
│                                                                │
│ Step 2. clip_captioning                                        │
│   Gemini JSON 정규화 → labels 테이블 upsert                      │
│   ├─ vlm-labels에서 이벤트 JSON 다운로드 + JSON 정리               │
│   ├─ 이벤트별 label row 생성 (timestamp 구간 + caption 포함)       │
│   ├─ DuckDB labels 테이블에 upsert (stable label_id 사용)         │
│   └─ video_metadata.auto_label_status = 'completed' 갱신         │
│                                                                │
│ Step 3. clip_to_frame                                          │
│   label 기반 clip 절단 → 적응형 프레임 추출                         │
│   ├─ labels + raw_files 조인하여 processable 후보 조회             │
│   ├─ ffmpeg로 이벤트 구간별 clip 절단                              │
│   ├─ clip별 ffprobe로 메타데이터 추출 (width, height, fps 등)       │
│   ├─ 적응형 규칙으로 대표 프레임 JPEG 추출 (아래 규칙표 참조)         │
│   ├─ MinIO vlm-processed에 clip 영상 + 프레임 이미지 업로드         │
│   └─ DuckDB processed_clips + image_metadata INSERT              │
└────────────────────────────────────────────────────────────────┘
```

#### Gemini 설정

| 항목 | 값 |
|------|-----|
| 모델 | `gemini-2.5-flash` (Vertex AI) |
| 프로젝트 | `GEMINI_PROJECT` 환경변수 (기본: `gmail-361002`) |
| 리전 | `GEMINI_LOCATION` 환경변수 (기본: `us-central1`) |
| 인증 | `GOOGLE_APPLICATION_CREDENTIALS` 또는 `GEMINI_SERVICE_ACCOUNT_JSON` |

#### 대용량 비디오 자동 리사이즈

Gemini API에 전송 전 비디오 크기가 `GEMINI_SAFE_VIDEO_BYTES`(기본 450MB)를 초과하면 ffmpeg으로 preview를 자동 생성합니다. 3단계로 시도하며, 성공하면 해당 preview를 Gemini에 전송합니다.

| 시도 | 해상도 | FPS | 목표 크기 |
|------|--------|-----|----------|
| 1차 | 960px | 6fps | 120MB |
| 2차 | 640px | 4fps | 80MB |
| 3차 | 480px | 3fps | 48MB |

#### Gemini 이벤트 JSON 형식

`clip_timestamp`가 Gemini에서 받아오는 이벤트 JSON 예시입니다.

```json
[
  {
    "category": "smoke",
    "duration": 3.5,
    "timestamp": [12.0, 15.5],
    "ko_caption": "건물 좌측에서 연기가 발생하여 점차 확산됨",
    "en_caption": "Smoke emerges from the left side of the building and gradually spreads"
  }
]
```

지원 카테고리 예시: `fire`, `smoke`, `fall`, `intrusion`, `fight`, `normal_activity`, `vehicle_accident`, `loitering`, `vandalism`, `abandoned_object` 등

#### 적응형 프레임 추출 규칙

`clip_to_frame`은 비디오 길이와 FPS에 따라 추출 프레임 수를 자동 조절합니다.

| 비디오 길이 | 추출 프레임 수 | FPS 보정 |
|------------|--------------|----------|
| < 10초 | 3 | fps < 3 → max 3 |
| 10 ~ 30초 | 5 | fps 3~10 → max 6 |
| 30 ~ 120초 | 8 | |
| ≥ 120초 | 12 | |

- 전체 상한: `max_frames_per_video = 12`
- JPEG 품질: 90 (config으로 변경 가능)
- 프레임 위치: 비디오 구간의 10%~90% 범위 내 균등 배치 (2초 미만 영상은 20%~80%)

### YOLO Object Detection

`clip_to_frame`에서 추출된 이미지(`processed_clip_frame`)에 대해 YOLO-World-L 모델로 object detection을 수행합니다. `ENABLE_YOLO_DETECTION=true`로 활성화하며, `yolo_detection_sensor`가 backlog을 감지하면 자동으로 `yolo_detection_job`을 트리거합니다.

```
clip_to_frame (완료)
     │
     v  yolo_detection_sensor (backlog 감지)
┌────────────────────────────────────────────────────────────────┐
│ yolo_image_detection                                           │
│   ├─ processed_clip_frame 중 YOLO 미처리 이미지 조회              │
│   ├─ MinIO vlm-processed에서 이미지 다운로드                      │
│   ├─ 배치 단위 추론 (기본 batch_size=4)                           │
│   ├─ 결과 JSON: vlm-labels/<raw_parent>/detections/<stem>.json   │
│   └─ DuckDB image_labels INSERT                                │
└────────────────────────────────────────────────────────────────┘
```

| 설정 | 기본값 | 설명 |
|------|--------|------|
| `confidence_threshold` | 0.25 | 검출 신뢰도 임계값 |
| `iou_threshold` | 0.45 | NMS IoU 임계값 |
| `batch_size` | 4 | 배치 추론 크기 |
| YOLO 서버 | `docker/yolo/` | GPU 1 전용, 모델 상주 Docker 서비스 (포트 8001) |
| 모델 | `yolov8l-worldv2` | YOLO-World Large |

기본 검출 클래스: `person`, `car`, `truck`, `bus`, `motorcycle`, `bicycle`, `fire`, `smoke`, `flame`, `knife`, `gun`, `bag`, `backpack`, `suitcase`, `helmet`, `safety vest`, `hard hat`, `traffic cone`, `barricade`, `dog`, `cat`

## Database Schema

DuckDB에 9개 운영 테이블이 정의되어 있습니다. (`src/vlm_pipeline/sql/schema.sql`)

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
- `raw_files` → `labels`: 1:N 관계. 하나의 미디어 파일에 자동 라벨/수동 검수 라벨 등 여러 이벤트 라벨이 매칭될 수 있습니다.
- `raw_files` + `labels` → `processed_clips`: 원본과 라벨(timestamp, caption, detection 결과 등)을 조합하여 전처리 결과물을 생성합니다.
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

**`labels`** — 자동/수동 라벨 데이터 (AUTO LABEL 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `label_id` | VARCHAR (PK) | 라벨 고유 ID |
| `asset_id` | VARCHAR (FK) | raw_files 참조 |
| `labels_key` | VARCHAR | MinIO 라벨 경로 |
| `label_format` | VARCHAR | 라벨 포맷 (JSON 등) |
| `label_source` / `review_status` | VARCHAR | auto / manual_review / manual, reviewed 여부 |
| `timestamp_start_sec` / `timestamp_end_sec` | DOUBLE | 이벤트 구간 시작/종료 시각 |
| `caption_text` | TEXT | 자동 생성 설명 문장 |
| `object_count` | INTEGER | object detection 결과 개수 |

**`processed_clips`** — 전처리 결과물 (`processed_data` asset이 기록, PROCESS 단계)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `clip_id` | VARCHAR (PK) | 클립 고유 ID |
| `source_asset_id` | VARCHAR (FK) | raw_files 참조 |
| `source_label_id` | VARCHAR (FK) | labels 참조 |
| `clip_start_sec` / `clip_end_sec` | DOUBLE | 잘라낸 구간 시작/종료 시각 |
| `clip_key` | VARCHAR | MinIO 저장 경로 |
| `data_source` | VARCHAR | auto / manual_review / manual |
| `caption_text` | TEXT | clip과 연결된 설명 문장 |
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

**`image_labels`** — 이미지 단위 라벨 결과 (YOLO detection 등)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `image_label_id` | VARCHAR (PK) | 라벨 고유 ID |
| `image_id` | VARCHAR (FK) | image_metadata 참조 |
| `source_clip_id` | VARCHAR (FK) | processed_clips 참조 |
| `labels_key` | VARCHAR | MinIO 라벨 JSON 경로 |
| `label_format` | VARCHAR | `yolo_detection_json` 등 |
| `label_tool` | VARCHAR | `yolo-world` 등 |
| `label_source` | VARCHAR | auto / manual |
| `review_status` | VARCHAR | auto_generated / pending / reviewed |
| `object_count` | INTEGER | detection 결과 개수 |

## Project Structure

```text
dataops/
├── src/
│   ├── vlm_pipeline/              # Dagster 기반 VLM 파이프라인 (핵심)
│   │   ├── definitions.py         #   Dagster Definitions 진입점
│   │   ├── defs/                   #   Asset 정의
│   │   │   ├── ingest/            #     INGEST (검증, MinIO 업로드, 메타 추출)
│   │   │   ├── dedup/             #     DEDUP helper (inline ingest에서 재사용)
│   │   │   ├── label/             #     AUTO LABEL (자동/수동 라벨 등록)
│   │   │   ├── process/           #     AUTO LABEL + PROCESS (프레임 추출, processed_data)
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
| `vlm-labels` | 자동/수동 라벨 JSON | AUTO LABEL |
| `vlm-processed` | 추출 프레임 및 전처리 결과물 | AUTO LABEL / PROCESS |
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
| `mvp_stage_job` | 전체 파이프라인 (INGEST+DEDUP → AUTO LABELING → BUILD) |
| `ingest_job` | INGEST + inline DEDUP 단독 |
| `label_job` | 자동/수동 라벨 등록 단독 |
| `auto_labeling_job` | 프레임 추출 → 라벨 등록 → processed_data 생성 |
| `process_build_job` | processed_data → BUILD |
| `gcs_download_job` | GCS 버킷 → `/nas/incoming` 다운로드 |
| `video_frame_extract_job` | 비디오 프레임 추출 단독 |
| `motherduck_sync_job` | DuckDB → MotherDuck 동기화 |

### Sensors

| Sensor | 설명 |
|--------|------|
| `incoming_manifest_sensor` | `/nas/incoming/.manifests/pending/*.json` 감지 → `mvp_stage_job` 트리거 |
| `auto_bootstrap_manifest_sensor` | `/nas/incoming` 미디어 감시 → manifest 자동 생성 |
| `stuck_run_guard_sensor` | 오래 정체된 STARTED run 자동 cancel / requeue |
| `video_frame_extract_sensor` | 비디오 프레임 추출 backlog 감지 → `video_frame_extract_job` 트리거 |
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
