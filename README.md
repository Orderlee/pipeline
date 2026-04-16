# VLM DataOps Pipeline

VLM(Vision-Language Model) 학습 데이터를 구축하기 위한 데이터 파이프라인입니다.
NAS에 있는 이미지/비디오 미디어를 수집하고, 중복을 정리한 뒤, Gemini(Vertex) 기반 이벤트 라벨링과 YOLO-World 검출을 수행하고, 최종적으로 학습 데이터셋을 조립합니다. 전체 파이프라인은 **Dagster + DuckDB + MinIO** 기반으로 운영됩니다.

현재 기준으로 이 저장소는 **branch-based runtime** 으로 운영합니다.

- **`main` = production**: Dagster `http://172.168.42.6:3030/`, MinIO Console `http://172.168.47.36:9001/`, runtime MinIO endpoint `http://172.168.47.36:9000`
- **`dev` = test**: Dagster `http://172.168.42.6:3031/`, MinIO Console `http://172.168.47.36:9003/`, runtime MinIO endpoint `http://172.168.47.36:9002`
- test는 기존 staging 데이터 plane(`/data/staging.duckdb`, `/home/pia/mou/staging/...`)를 재사용하지만, **코드 로직은 production과 동일**합니다.

문서 운영 기준은 역할을 분리합니다.

- `README.md`: 사람용 개요와 운영 흐름
- `AGENTS.md`: 에이전트용 짧은 맵
- `docs/`: 설계, 계획, 참고 문서의 기록 시스템

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ NAS                                                                          │
│  Production host bind: /home/pia/mou/incoming, /home/pia/mou/archive         │
│  Test host bind:       /home/pia/mou/staging/incoming, /home/pia/mou/staging/archive │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │
                                v
┌──────────────────────────────────────────────────────────────────────────────┐
│ Dagster                                                                      │
│                                                                              │
│ Production / Test (same logic)                                               │
│   incoming/manifest sensors -> ingest_job                                    │
│   piaspace-agent polling -> dispatch_agent_sensor                            │
│   .dispatch/pending/*.json -> dispatch_sensor (fallback)                     │
│   -> dispatch_stage_job                                                      │
└───────────────┬───────────────────────────────┬──────────────────────────────┘
                │                               │
                v                               v
┌──────────────────────────────┐    ┌─────────────────────────────────────────┐
│ MinIO                        │    │ DuckDB / MotherDuck                     │
│  vlm-raw                     │    │ raw_files                               │
│  vlm-labels                  │    │ video_metadata / image_metadata         │
│  vlm-processed               │    │ labels / processed_clips / image_labels │
│  vlm-dataset                 │    │ datasets / dataset_clips                │
└──────────────────────────────┘    │ dispatch_* tracking tables              │
                                    └─────────────────────────────────────────┘
```

- **NAS**: 원본 미디어가 들어오는 파일 시스템입니다.
- **Dagster**: 수집, 라벨링, 전처리, 동기화를 오케스트레이션합니다.
- **MinIO**: 단계별 산출물을 저장하는 S3 호환 스토리지입니다.
- **DuckDB**: 메타데이터와 라벨링 결과의 source of truth입니다.
- **MotherDuck**: 운영 DuckDB의 선택적 클라우드 동기화 대상입니다.

## Branch Runtime

| 항목 | Production (`main`) | Test (`dev`) |
|------|------------|---------|
| Dagster UI | `http://172.168.42.6:3030/` | `http://172.168.42.6:3031/` |
| DuckDB | `/data/pipeline.duckdb` | `/data/staging.duckdb` |
| Incoming host path | `/home/pia/mou/incoming` | `/home/pia/mou/staging/incoming` |
| Archive host path | `/home/pia/mou/archive` | `/home/pia/mou/staging/archive` |
| Runtime MinIO endpoint | `http://172.168.47.36:9000` | `http://172.168.47.36:9002` |
| MinIO Console | `http://172.168.47.36:9001/` | `http://172.168.47.36:9003/` |
| Dagster home (container) | `/app/dagster_home` | `/app/dagster_home` |
| Workspace | `/app/workspace.yaml` | `/app/workspace.yaml` |
| Main entrypoint | `src/vlm_pipeline/definitions.py` | `src/vlm_pipeline/definitions.py` |
| Dispatch ingress | `piaspace-agent:8080` polling + JSON fallback | `piaspace-agent:8081` polling + JSON fallback |

핵심 차이:

- prod/test 모두 `dispatch_agent_sensor`와 `dispatch_sensor`를 같은 방식으로 사용합니다.
- 차이는 branch, env 파일, host bind path, external endpoint뿐입니다.
- 애플리케이션이 쓰는 실제 MinIO endpoint는 `9000/9002`이고, `9001/9003`은 사람용 Console 주소입니다.

## Data Flow

### Production

```text
piaspace-agent:8080
  -> dispatch_agent_sensor
  -> dispatch_stage_job
  -> raw_ingest
  -> clip_timestamp
  -> clip_captioning
  -> clip_to_frame
  -> dispatch_yolo_image_detection

/nas/incoming/.dispatch/pending/*.json
  -> dispatch_sensor (fallback)
  -> dispatch_stage_job
```

production 정책은 다음과 같습니다.

- `ingest_job` / `mvp_stage_job`는 **수집 전용**입니다.
- Gemini / clip / YOLO가 포함된 자동 라벨링은 **오직 `dispatch_stage_job`** 에서만 자동으로 실행됩니다.
- `incoming/gcp/**`는 GCS 수집 스케줄로 들어오고, 일반 incoming 폴더는 dispatch 요청이 있어야 자동 라벨링으로 이어집니다.

### Test (`dev`)

```text
piaspace-agent:8081
  -> dispatch_agent_sensor
  -> dispatch_stage_job
  -> raw_ingest
  -> clip_timestamp
  -> clip_captioning
  -> clip_to_frame
  -> dispatch_yolo_image_detection
```

test의 주요 특징:

- branch는 `dev`이지만, 실행 로직은 production과 같습니다.
- 요청 ingress는 test agent API(`:8081`) polling으로 받아옵니다.
- host bind path와 DuckDB/MinIO endpoint만 test 자원을 바라봅니다.

## 단계별 요약

| 단계 | Asset / Job | 설명 | 주요 저장 위치 |
|------|-------------|------|----------------|
| GCS 수집 | `gcs_download_to_incoming` | GCS 버킷에서 incoming으로 다운로드 | NAS |
| INGEST | `raw_ingest` | 파일 검증, checksum, MinIO 업로드, 메타데이터 추출, archive 이동 | `vlm-raw`, `raw_files`, `video_metadata`, `image_metadata` |
| TIMESTAMP | `clip_timestamp` | Gemini로 이벤트 구간 JSON 생성 | `vlm-labels`, `video_metadata` |
| CAPTIONING | `clip_captioning` | Gemini 이벤트 JSON을 labels row로 정규화 | `labels` |
| FRAME | `clip_to_frame` | clip 기반 프레임 추출 및 top-1 이미지 caption 저장 | `vlm-processed`, `processed_clips`, `image_metadata` |
| RAW FRAME | `raw_video_to_frame` | YOLO 전용 요청 시 raw video에서 직접 frame 추출 | `vlm-processed`, `image_metadata` |
| YOLO | `dispatch_yolo_image_detection`, `yolo_image_detection` | YOLO-World detection 및 bbox JSON 저장 | `vlm-labels`, `image_labels` |
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
- legacy spec flow의 `frame_extraction.max_frames_per_video` hard cap

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
- production/test가 같은 YOLO 서버를 써도 lock으로 전역 class 충돌을 막습니다.
- 결과 JSON에는 `requested_classes`, `requested_classes_count`, `class_source`를 함께 저장합니다.

지원 클래스가 비어 있으면:

1. dispatch `classes`
2. 없으면 `categories -> derive_classes_from_categories()`
3. legacy spec flow면 `spec.classes`와 bbox config 교집합
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

### dispatch / spec 테이블

| 테이블 | 설명 |
|--------|------|
| `dispatch_requests` | dispatch 요청 추적 (YOLO 파라미터, labeling_method 포함) |
| `dispatch_pipeline_runs` | dispatch run 단계별 상태 추적 (step_name, step_status, 처리 통계) |
| `dispatch_model_configs` | output 타입별 모델 선택 + 기본 파라미터 (bbox/timestamp/captioning) |
| `labeling_specs` | spec 수신 → 라우팅/재시도/완료 추적 (categories, classes, labeling_method) |
| `labeling_configs` | config/parameters JSON 동기화 (버전 관리) |
| `requester_config_map` | requester/team → config 매핑 (personal → team → fallback 우선순위) |

현재 schema.sql 기준 핵심 테이블은 운영 테이블 + `dispatch_*` 추적 테이블 + legacy spec 테이블로 구성됩니다.

현재 스키마에서 중요한 점:

- `image_metadata`는 `caption_text` 대신 **`image_caption_text`** 를 canonical 컬럼으로 사용합니다.
- bbox JSON과 image caption JSON의 source of truth는 모두 **`vlm-labels`** 입니다.

## Project Structure

```text
.
├── src/
│   └── vlm_pipeline/
│       ├── definitions.py              # 단일 Definitions entrypoint (production canonical)
│       ├── definitions_production.py   # production job/sensor 조립
│       ├── defs/
│       │   ├── dispatch/               # dispatch sensor / service / production_agent_sensor
│       │   ├── ingest/                 # raw ingest, archive, manifest
│       │   ├── label/                  # assets + artifact_bbox/caption/classification, label_helpers, timestamp
│       │   ├── process/                # assets(라우팅) + helpers, captioning, frame_extract, raw_frames
│       │   ├── yolo/                   # YOLO assets
│       │   ├── build/                  # dataset build
│       │   ├── sam/                    # SAM3 segmentation
│       │   ├── gcp/                    # GCS download
│       │   └── sync/                   # MotherDuck sync
│       ├── lib/                        # prompts, frame planning, yolo client, key_builders, env helpers
│       ├── resources/                  # duckdb_base + duckdb_phash/migration/ingest_* / MinIO / runtime settings
│       └── sql/                        # schema.sql, migration.sql
├── docker/
│   ├── docker-compose.yaml
│   ├── .env
│   ├── .env.test
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
| `dagster` | `3030` 또는 `3031` | production/test Dagster webserver |
| `app` | - | production/test code/runtime container |
| `dagster-daemon` | - | production/test sensor / schedule daemon |
| `dagster-code-server` | - | production/test code server |
| `yolo` | `8001` | YOLO-World inference server |
| `minio` | 선택 실행 | 로컬 MinIO API / Console |
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
- DuckDB writer lane은 `duckdb_writer`(legacy dispatch), `duckdb_raw_writer`, `duckdb_label_writer`, `duckdb_yolo_writer`로 분리합니다.

## Getting Started

### 1. Requirements

- Python 3.10+
- Docker / Docker Compose
- NVIDIA GPU + CUDA
- NAS mount

### 2. 환경 설정

production (`main`):

```bash
cp .env.example docker/.env
```

test (`dev`):

```bash
cp docker/.env.test docker/.env.test.local
```

주요 환경변수:

| 변수 | 설명 |
|------|------|
| `DATAOPS_DUCKDB_PATH` | DuckDB 파일 경로 |
| `MINIO_ENDPOINT` | MinIO endpoint |
| `INCOMING_DIR` | incoming 경로 |
| `GOOGLE_APPLICATION_CREDENTIALS` | Vertex 인증 |
| `MOTHERDUCK_TOKEN` | MotherDuck 토큰 |
| `PROD_AGENT_POLLING_ENABLED` | production agent polling 활성화 (`true` 권장, production env 전용) |
| `PROD_AGENT_BASE_URL` | production agent base URL (기본 `http://host.docker.internal:8080`) |
| `STAGING_AGENT_POLLING_ENABLED` | staging agent polling 활성화 (`true` 권장, test env 전용) |
| `STAGING_AGENT_BASE_URL` | staging agent base URL (기본 `http://host.docker.internal:8081`) |
| `INGEST_UPLOAD_WORKERS` | raw ingest MinIO 업로드 worker 수 (`8` 권장) |
| `GEMINI_MAX_WORKERS` | clip timestamp Gemini 병렬 worker 수 (`5` 권장) |
| `GEMINI_CHUNK_MAX_WORKERS` | 긴 영상 chunk Gemini 병렬 worker 수 (`3` 권장) |

### 3. 인프라 실행

production (`main`):

```bash
cd docker
PIPELINE_ENV_FILE=.env COMPOSE_PROJECT_NAME=pipeline-prod docker compose up -d dagster-code-server dagster-daemon dagster
```

test (`dev`):

```bash
cd docker
PIPELINE_ENV_FILE=.env.test COMPOSE_PROJECT_NAME=pipeline-test docker compose up -d dagster-code-server dagster-daemon dagster
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

### Common Jobs

| Job | 설명 |
|-----|------|
| `dispatch_stage_job` | dispatch 트리거 JSON → ingest + clip_* + YOLO + SAM3 |
| `manual_label_import_job` | incoming 수동 라벨 import |
| `ingest_job` | 수집 전용 |

### Sensors / Schedules

| 이름 | 환경 | 설명 |
|------|------|------|
| `production_agent_dispatch_sensor` | prod/test | `piaspace-agent` polling (`/api/production/dispatch/*`) → `dispatch_stage_job` |
| `dispatch_sensor` | prod/test fallback | `.dispatch/pending/*.json` → `dispatch_stage_job` |
| `incoming_manifest_sensor` | both | pending manifest → ingest |
| `auto_bootstrap_manifest_sensor` | both | incoming 스캔 후 manifest 생성 |
| `dispatch_run_*_sensor` | both | dispatch run status finalizer |
| `stuck_run_guard_sensor` | both | stuck / orphan run 정리 |
| `motherduck_*_sensor` | prod/test | 핵심 4개 테이블 증분 sync |
| `gcs_download_schedule` | prod/test | 매일 04:00 KST GCS 수집 |
| `motherduck_daily_schedule` | prod/test | 매일 05:00 KST full sync |

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

- production에서 자동 라벨링은 `piaspace-agent:8080` polling이 기본 ingress입니다 (`PROD_AGENT_POLLING_ENABLED=true`). `.dispatch/pending/*.json`은 fallback으로 유지됩니다.
- test에서는 `piaspace-agent-staging:8081` 연결 상태와 `STAGING_AGENT_POLLING_ENABLED=true` 여부를 먼저 확인합니다.
- `필요없음` 요청은 test 데이터 plane에서 raw ingest 후, 같은 폴더 안의 기존 라벨 결과를 best-effort로 자동 import 할 수 있습니다.
- `tmp_data_2` 같은 재테스트 전에는 DB / MinIO / `.dispatch` 상태를 정리한 뒤 다시 시작하는 것이 안전합니다.

## 참고 문서

- `AGENTS.md`
- `docs/references/deployment-guide.md`
- `docs/runbook.md`
- `CLAUDE.md`
- `CLAUDE2.md`
- 역사 문서:
  `docs/PRODUCTION_VS_STAGING.md`, `docs/staging_agent_api_dispatch_plan.md`, `docs/staging_agent_waiting_dispatch_plan.md`

---

이 README는 현재 `definitions.py`, `definitions_production.py`, `runtime_settings.py`, `docker/.env.test` 기준의 운영 흐름을 요약합니다. 세부 스키마나 플레이북은 `CLAUDE2.md`와 `src/vlm_pipeline/sql/schema.sql`을 함께 참고하세요.
