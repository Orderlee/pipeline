# VLM DataOps Pipeline

VLM(Vision-Language Model) 학습 데이터를 구축하기 위한 데이터 파이프라인입니다.
NAS에 있는 이미지/비디오 미디어를 수집하고, 중복을 정리한 뒤, Gemini(Vertex) 기반 이벤트 라벨링과 YOLO-World 검출을 수행하고, 최종적으로 학습 데이터셋을 조립합니다. 전체 파이프라인은 **Dagster + DuckDB + MinIO** 기반으로 운영됩니다.

현재 기준으로 이 저장소는 **production**과 **staging**을 분리해 운영합니다.

- **production**: `/nas/incoming` 기반 수집, `.dispatch/pending/*.json` 기반 자동 라벨링
- **staging**: `/nas/staging/incoming` 기반 검증, `piaspace-agent:8080` polling 기반 dispatch

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ NAS                                                                          │
│  Production: /nas/incoming, /nas/archive                                     │
│  Staging:    /nas/staging/incoming, /nas/staging/archive                     │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │
                                v
┌──────────────────────────────────────────────────────────────────────────────┐
│ Dagster                                                                      │
│                                                                              │
│ Production                                                                   │
│   incoming/manifest sensors -> ingest_job                                    │
│   .dispatch/pending/*.json -> dispatch_sensor -> dispatch_stage_job          │
│                                                                              │
│ Staging                                                                      │
│   piaspace-agent polling -> staging_agent_dispatch_sensor                    │
│   -> dispatch_stage_job / auto_labeling_routed_job / import jobs            │
└───────────────┬───────────────────────────────┬──────────────────────────────┘
                │                               │
                v                               v
┌──────────────────────────────┐    ┌─────────────────────────────────────────┐
│ MinIO                        │    │ DuckDB / MotherDuck                     │
│  vlm-raw                     │    │ raw_files                               │
│  vlm-labels                  │    │ video_metadata / image_metadata         │
│  vlm-processed               │    │ labels / processed_clips / image_labels │
│  vlm-dataset                 │    │ datasets / dataset_clips                │
└──────────────────────────────┘    │ staging_* tracking / spec tables        │
                                    └─────────────────────────────────────────┘
```

- **NAS**: 원본 미디어가 들어오는 파일 시스템입니다.
- **Dagster**: 수집, 라벨링, 전처리, 동기화를 오케스트레이션합니다.
- **MinIO**: 단계별 산출물을 저장하는 S3 호환 스토리지입니다.
- **DuckDB**: 메타데이터와 라벨링 결과의 source of truth입니다.
- **MotherDuck**: 운영 DuckDB의 선택적 클라우드 동기화 대상입니다.

## Production vs Staging

| 항목 | Production | Staging |
|------|------------|---------|
| Dagster UI | `http://<host>:3030` | `http://<host>:3031` |
| DuckDB | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| Incoming | `/nas/incoming` | `/nas/staging/incoming` |
| Archive | `/nas/archive` | `/nas/staging/archive` |
| MinIO endpoint | `http://172.168.47.36:9000` | `http://172.168.47.36:9002` |
| Dagster home | `/app/dagster_home` | `/app/dagster_home_staging` |
| Workspace | `/app/workspace_prod.yaml` | `/app/workspace_staging.yaml` |
| Main entrypoint | `src/vlm_pipeline/definitions.py` | `src/vlm_pipeline/definitions_staging.py` |
| Dispatch ingress | `.dispatch/pending/*.json` | `piaspace-agent` polling |

핵심 차이:

- **production**은 자동 라벨링을 `.dispatch/pending/*.json` 요청 파일로만 시작합니다.
- **staging**은 `staging_agent_dispatch_sensor`가 `piaspace-agent:8080`에서 pending 요청을 polling해서 시작합니다.
- staging의 파일 기반 `dispatch_sensor` 경로는 레거시/호환 목적이며 기본 ingress가 아닙니다.
- staging은 `manual_label_import_job`, `prelabeled_import_job`, spec 기반 라우팅 등 검증용 흐름을 더 포함합니다.

## Data Flow

### Production

```text
/nas/incoming
  ├─ auto_bootstrap / manifest sensor
  │    -> ingest_job (수집 전용)
  └─ .dispatch/pending/*.json
       -> dispatch_sensor
       -> dispatch_stage_job
       -> raw_ingest
       -> clip_timestamp
       -> clip_captioning
       -> clip_to_frame
       -> staging_yolo_image_detection
```

production 정책은 다음과 같습니다.

- `ingest_job` / `mvp_stage_job`는 **수집 전용**입니다.
- Gemini / clip / YOLO가 포함된 자동 라벨링은 **오직 `dispatch_stage_job`** 에서만 자동으로 실행됩니다.
- `incoming/gcp/**`는 GCS 수집 스케줄로 들어오고, 일반 incoming 폴더는 dispatch 요청이 있어야 자동 라벨링으로 이어집니다.

### Staging

```text
piaspace-agent:8080
  -> staging_agent_dispatch_sensor
  -> dispatch_stage_job
  -> raw_ingest
  -> spec_resolve_sensor
  -> clip_timestamp / clip_captioning / clip_to_frame
  -> bbox_labeling / activate_labeling_spec
```

staging의 주요 특징:

- 요청 ingress는 파일이 아니라 **API polling** 입니다.
- fully-empty 요청은 waiting으로 남기고, 실행 메타가 채워질 때까지 run을 만들지 않습니다.
- `필요없음` 계열 요청은 raw ingest를 수행하고, 같은 폴더 안에 라벨 결과가 있으면 자동 import까지 수행합니다.
- `prelabeled_import_job`으로 완료된 라벨 데이터를 별도 수동 적재할 수도 있습니다.

## 단계별 요약

| 단계 | Asset / Job | 설명 | 주요 저장 위치 |
|------|-------------|------|----------------|
| GCS 수집 | `gcs_download_to_incoming` | GCS 버킷에서 incoming으로 다운로드 | NAS |
| INGEST | `raw_ingest` | 파일 검증, checksum, MinIO 업로드, 메타데이터 추출, archive 이동 | `vlm-raw`, `raw_files`, `video_metadata`, `image_metadata` |
| TIMESTAMP | `clip_timestamp` | Gemini로 이벤트 구간 JSON 생성 | `vlm-labels`, `video_metadata` |
| CAPTIONING | `clip_captioning` | Gemini 이벤트 JSON을 labels row로 정규화 | `labels` |
| FRAME | `clip_to_frame` | clip 기반 프레임 추출 및 top-1 이미지 caption 저장 | `vlm-processed`, `processed_clips`, `image_metadata` |
| RAW FRAME | `raw_video_to_frame` | YOLO 전용 요청 시 raw video에서 직접 frame 추출 | `vlm-processed`, `image_metadata` |
| YOLO | `staging_yolo_image_detection`, `yolo_image_detection`, `bbox_labeling` | YOLO-World detection 및 bbox JSON 저장 | `vlm-labels`, `image_labels` |
| BUILD | `build_dataset` | 학습 데이터셋 조립 | `vlm-dataset`, `datasets`, `dataset_clips` |
| SYNC | `motherduck_sync` | DuckDB -> MotherDuck 동기화 | MotherDuck |

## Auto Labeling 상세

### 1. Gemini Timestamp / Caption

`clip_timestamp`는 Gemini(Vertex)를 사용해 비디오 이벤트를 분석합니다.

- 프롬프트 정의: `src/vlm_pipeline/lib/gemini_prompts.py`
- 호출 래퍼: `src/vlm_pipeline/lib/gemini.py`
- 429 `Resource exhausted`에는 공용 backoff/retry가 적용됩니다.
- 대형 비디오는 preview를 생성한 뒤 Gemini에 전달합니다.

반환된 이벤트 JSON은 `vlm-labels/.../events/*.json`에 저장되고, 이어서 `clip_captioning`이 이를 `labels` 테이블에 정규화합니다.

예시:

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

### 2. Frame Extraction

`clip_to_frame`와 `raw_video_to_frame`은 공통으로 `src/vlm_pipeline/lib/video_frames.py`의 정책을 사용합니다.

현재는 고정 `12 / 24` 상수가 아니라 아래 값을 보고 동적으로 결정합니다.

- `sampling_mode`: `clip_event` / `raw_video`
- `requested_outputs`
- `image_profile`: `current` / `dense`
- `duration_sec`, `fps`, `frame_count`
- staging spec의 `frame_extraction.max_frames_per_video` hard cap

기본 규칙:

- `duration < 3600s`이면 `1초` 간격
- `duration >= 3600s`이면 `10초` 간격
- interval로 뽑은 timestamp가 너무 많으면 균등 downsampling으로 상한을 맞춥니다.
- `sec < duration` 규칙으로 clip 끝 경계의 `empty_output`을 피합니다.

또한 `clip_to_frame`은 top-1 relevance frame에 대해 이미지 caption을 생성하고, 이를 `image_metadata.image_caption_text`와 `vlm-labels/.../image_captions/*.json`에 저장합니다.

### 3. YOLO-World Detection

YOLO 서버는 `docker/yolo/` 아래 별도 서비스로 동작합니다.

- 모델: `yolov8l-worldv2`
- 기본 디바이스: `cuda:1`
- health endpoint: `http://<host>:8001/health`

현재 YOLO 호출 방식의 특징:

- dispatch의 `classes`가 실제 YOLO 요청에 연결됩니다.
- 요청마다 `classes`를 전달하는 **request-scoped classes** 방식입니다.
- production/staging가 같은 YOLO 서버를 써도 lock으로 전역 class 충돌을 막습니다.
- 결과 JSON에는 `requested_classes`, `requested_classes_count`, `class_source`를 함께 저장합니다.

지원 클래스가 비어 있으면:

1. dispatch `classes`
2. 없으면 `categories -> derive_classes_from_categories()`
3. staging spec이면 `spec.classes`와 bbox config 교집합
4. 그래도 없으면 서버 기본 classes

## Database Schema

주요 테이블은 `src/vlm_pipeline/sql/schema.sql`에 정의되어 있습니다.

### 운영 핵심 테이블

| 테이블 | 설명 |
|--------|------|
| `raw_files` | 원본 미디어 메타, checksum, MinIO raw 위치 |
| `video_metadata` | ffprobe 기반 비디오 메타, Gemini 상태, 재인코딩 추적 |
| `image_metadata` | 이미지/프레임 메타, `image_caption_text`, `image_caption_score` |
| `labels` | 이벤트 단위 timestamp / caption 라벨 |
| `processed_clips` | clip 기반 전처리 산출물 |
| `image_labels` | YOLO detection 결과 |
| `datasets` | 데이터셋 정의 |
| `dataset_clips` | dataset ↔ clip 연결 |

### staging 전용 추적 / spec 테이블

| 테이블 | 설명 |
|--------|------|
| `staging_dispatch_requests` | staging dispatch 요청 추적 |
| `staging_pipeline_runs` | staging run 상태 추적 |
| `staging_model_configs` | staging 기본 설정 저장 |
| `labeling_specs` | spec 요청 정의 |
| `labeling_configs` | spec 설정 스냅샷 |
| `requester_config_map` | requester 별 config 연결 |

현재 스키마에서 중요한 점:

- `image_metadata`는 `caption_text` 대신 **`image_caption_text`** 를 canonical 컬럼으로 사용합니다.
- bbox JSON과 image caption JSON의 source of truth는 모두 **`vlm-labels`** 입니다.

## Project Structure

```text
.
├── src/
│   └── vlm_pipeline/
│       ├── definitions.py              # production entrypoint
│       ├── definitions_staging.py      # staging entrypoint
│       ├── definitions_profiles.py     # profile 기반 공통 조립
│       ├── definitions_common.py       # 공통 job/sensor 조립
│       ├── defs/
│       │   ├── dispatch/               # dispatch sensor / service / staging agent sensor
│       │   ├── ingest/                 # raw ingest, archive, manifest
│       │   ├── label/                  # Gemini label, manual/prelabeled import
│       │   ├── process/                # frame extraction, image captioning
│       │   ├── yolo/                   # YOLO assets / staging YOLO assets
│       │   ├── spec/                   # staging spec assets / sensors
│       │   ├── build/                  # dataset build
│       │   ├── gcp/                    # GCS download
│       │   └── sync/                   # MotherDuck sync
│       ├── lib/                        # prompts, frame planning, yolo client, env helpers
│       ├── resources/                  # DuckDB / MinIO / runtime settings
│       └── sql/                        # schema.sql, migration.sql
├── docker/
│   ├── docker-compose.yaml
│   ├── .env
│   ├── .env.staging
│   ├── app/
│   └── yolo/
├── scripts/
├── docs/
├── gcp/
└── tests/
```

## Infrastructure

Docker Compose(`docker/docker-compose.yaml`)로 주요 서비스를 실행합니다.

| 서비스 | 포트 | 설명 |
|--------|------|------|
| `dagster` | `3030` | production Dagster webserver |
| `dagster-staging` | `3031` | staging Dagster dev server |
| `app` | - | production code/runtime container |
| `dagster-daemon` | - | production sensor / schedule daemon |
| `dagster-code-server` | - | production code server |
| `yolo` | `8001` | YOLO-World inference server |
| `minio` | `9000` / `9001` | production MinIO API / Console |
| `postgres` | `5432` | Grafana datasource / metadata 보조 |
| `grafana` | `3000` | 대시보드 |

### MinIO 버킷

| 버킷 | 용도 |
|------|------|
| `vlm-raw` | 원본 미디어 |
| `vlm-labels` | 이벤트 JSON, bbox JSON, image caption JSON |
| `vlm-processed` | clip, frame 이미지 |
| `vlm-dataset` | 최종 데이터셋 |

### 운영 규칙

- `raw_key`는 `<source_unit>/<rel_path>` 규칙을 사용합니다.
- `vlm-labels`만 라벨 JSON의 source of truth로 사용합니다.
- 파일 단위 오류는 fail-forward로 처리하고, failure JSONL로 남깁니다.
- archive 이동 후 source 폴더가 비면 incoming 쪽 빈 부모 폴더도 정리합니다.
- DuckDB writer job은 `tags={"duckdb_writer": "true"}` 로 단일 writer를 강제합니다.

## Getting Started

### 1. Requirements

- Python 3.10+
- Docker / Docker Compose
- NVIDIA GPU + CUDA
- NAS mount

### 2. 환경 설정

production:

```bash
cp .env.example docker/.env
```

staging:

```bash
cp docker/.env docker/.env.staging
```

주요 환경변수:

| 변수 | 설명 |
|------|------|
| `DATAOPS_DUCKDB_PATH` | DuckDB 파일 경로 |
| `MINIO_ENDPOINT` | MinIO endpoint |
| `INCOMING_DIR` | incoming 경로 |
| `GOOGLE_APPLICATION_CREDENTIALS` | Vertex 인증 |
| `MOTHERDUCK_TOKEN` | MotherDuck 토큰 |
| `STAGING_AGENT_POLLING_ENABLED` | staging agent polling 활성화 |
| `STAGING_AGENT_BASE_URL` | staging agent base URL |

### 3. 인프라 실행

production:

```bash
cd docker
docker compose up -d
```

staging:

```bash
cd docker
docker compose --profile staging up -d dagster-staging
```

### 4. 환경 검증

```bash
python3 scripts/query_local_duckdb.py --sql "SELECT COUNT(*) FROM raw_files;"
curl -fsS http://127.0.0.1:3030/server_info
curl -fsS http://127.0.0.1:3031/server_info
curl -fsS http://127.0.0.1:8001/health
```

### 5. 테스트

```bash
pip install -e ".[dev]"
pytest tests/unit -q
pytest tests/integration -q
```

## Dagster Jobs & Sensors

### Production Jobs

| Job | 설명 |
|-----|------|
| `mvp_stage_job` | 수집 전용 호환 job |
| `ingest_job` | raw ingest 단독 |
| `dispatch_stage_job` | 운영 자동 라벨링의 유일한 진입점 |
| `gcs_download_job` | GCS -> incoming |
| `motherduck_sync_job` | DuckDB -> MotherDuck |
| `manual_label_import_job` | 수동 라벨 import, env로 선택 등록 |
| `yolo_standard_detection_job` | 표준 YOLO detection, env로 선택 등록 |

### Staging Jobs

| Job | 설명 |
|-----|------|
| `dispatch_stage_job` | staging dispatch run_mode 분기 |
| `auto_labeling_routed_job` | spec 기반 auto labeling |
| `yolo_detection_job` | staging YOLO detection |
| `manual_label_import_job` | incoming 수동 라벨 import |
| `prelabeled_import_job` | raw + 완료 라벨 세트 수동 적재 |
| `ingest_job` | 수집 전용 |

### Sensors / Schedules

| 이름 | 환경 | 설명 |
|------|------|------|
| `dispatch_sensor` | production | `.dispatch/pending/*.json` -> `dispatch_stage_job` |
| `staging_agent_dispatch_sensor` | staging | `piaspace-agent` polling -> `dispatch_stage_job` |
| `incoming_manifest_sensor` | both | pending manifest -> ingest |
| `auto_bootstrap_manifest_sensor` | both | incoming 스캔 후 manifest 생성 |
| `spec_resolve_sensor` | staging | spec 요청 -> routed job |
| `dispatch_run_*_sensor` | both | dispatch run status finalizer |
| `stuck_run_guard_sensor` | both | stuck / orphan run 정리 |
| `motherduck_*_sensor` | production | 핵심 4개 테이블 증분 sync |
| `gcs_download_schedule` | production | 매일 04:00 KST GCS 수집 |
| `motherduck_daily_schedule` | production | 매일 05:00 KST full sync |

MotherDuck sensor 기본 watched tables:

- `raw_files`
- `video_metadata`
- `labels`
- `processed_clips`

## Query Examples

```sql
-- INGEST 상태 집계
SELECT ingest_status, COUNT(*) AS cnt
FROM raw_files
GROUP BY ingest_status
ORDER BY ingest_status;

-- 자동 라벨링 완료된 비디오 수
SELECT auto_label_status, COUNT(*) AS cnt
FROM video_metadata
GROUP BY auto_label_status
ORDER BY auto_label_status;

-- bbox 결과가 있는 이미지 수
SELECT COUNT(*) AS labeled_images
FROM image_labels;

-- image caption이 저장된 frame 수
SELECT COUNT(*) AS captioned_frames
FROM image_metadata
WHERE image_caption_text IS NOT NULL;
```

## 운영 팁

- production에서 자동 라벨링을 시작하려면 `.dispatch/pending/*.json`을 사용합니다.
- staging은 `piaspace-agent` 연결 상태와 `STAGING_AGENT_POLLING_ENABLED=true` 여부를 먼저 확인합니다.
- `필요없음` 요청은 staging에서 raw ingest 후, 같은 폴더 안의 기존 라벨 결과를 best-effort로 자동 import 할 수 있습니다.
- `tmp_data_2` 같은 재테스트 전에는 DB / MinIO / `.dispatch` 상태를 정리한 뒤 다시 시작하는 것이 안전합니다.

## 참고 문서

- `AGENTS.md`
- `CLAUDE.md`
- `CLAUDE2.md`
- `docs/PRODUCTION_VS_STAGING.md`
- `docs/staging_agent_api_dispatch_plan.md`
- `docs/staging_agent_waiting_dispatch_plan.md`

---

이 README는 현재 `definitions.py`, `definitions_staging.py`, `definitions_profiles.py`, `runtime_settings.py` 기준의 운영 흐름을 요약합니다. 세부 스키마나 플레이북은 `CLAUDE2.md`와 `src/vlm_pipeline/sql/schema.sql`을 함께 참고하세요.
