# VLM DataOps Pipeline

VLM(Vision-Language Model) 학습 데이터를 구축하기 위한 데이터 파이프라인입니다.
NAS에 있는 이미지/비디오 미디어를 수집하고, 중복을 정리한 뒤, Gemini(Vertex) 기반 이벤트 라벨링과 SAM3 segmentation 검출을 수행하고, 최종적으로 학습 데이터셋을 조립합니다. 전체 파이프라인은 **Dagster + PostgreSQL(pgvector) + MinIO** 기반으로 운영됩니다.

🔎 **Vector DB (pgvector)**: `ENABLE_EMBEDDING=true`이면 프레임·캡션·비디오(frame-pool) 미디어를 PE-Core-L14-336로 1024-d 임베딩하여 PostgreSQL의 `image_embeddings`(pgvector)에 적재하고, entity_type별 HNSW 인덱스로 **텍스트→이미지(cross-modal)·이미지→이미지·캡션(keyword/semantic/hybrid)·비디오 유사검색**과 near-duplicate·label-suspect·active-learning 같은 데이터 품질 분석을 지원합니다. 상세는 [Vector Search & 임베딩 분석](#vector-search--임베딩-분석-pgvector) 참고.

> DuckDB는 2026-05-19 PG cutover 이후 primary store에서 제거되었습니다. 현재 메타데이터/라벨의 source of truth는 PostgreSQL이며, DuckDB는 `pg_duckdb` extension(analytics 쿼리 한정)으로만 남아 있습니다. MotherDuck sync는 production 기본 entrypoint(`definitions.py`)에서는 제외되었고, PG 소스를 지원하는 `src/python/local_duckdb_to_motherduck_sync.py` + profile 기반 entrypoint(`definitions_profiles.py`, staging 전용)에만 남아 있습니다 (구버전 사본은 `scripts/archive/`).

현재 기준으로 이 저장소는 **branch-based runtime** 으로 운영합니다.

- **`main` = production**: Dagster `http://10.0.0.10:3030/`, MinIO Console `http://10.0.0.51:9001/`, runtime MinIO endpoint `http://10.0.0.51:9000`, Postgres `docker-postgres-1:15433/vlm_pipeline`
- **`dev` = test(staging)**: Dagster `http://10.0.0.10:3031/`, MinIO Console `http://10.0.0.51:9003/`, runtime MinIO endpoint `http://10.0.0.51:9002`, Postgres `pipeline-test-postgres-1:15432/vlm_pipeline_staging`
- test는 staging 데이터 plane(Postgres `vlm_pipeline_staging`, `/home/user/mou/nas_200tb/staging/...`)을 재사용하지만, **코드 로직은 production과 동일**합니다.

문서 운영 기준은 역할을 분리합니다.

- `README.md`: 사람용 개요와 운영 흐름
- `AGENTS.md`: 에이전트용 짧은 맵
- `CLAUDE.md`: 로컬 상세 운영 요약
- `docs/`: 설계, 계획, 참고 문서의 기록 시스템

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│ NAS (NAS_200tb / 10.0.0.51, NFS)                                               │
│  Production host bind: /home/user/mou/nas_200tb            → 컨테이너 /nas/data      │
│  Test host bind:       /home/user/mou/nas_200tb/staging    → 컨테이너 /nas/data      │
│  (incoming = /nas/data/incoming, archive = /nas/data/archive)                      │
└───────────────────────────────┬────────────────────────────────────────────────────┘
                                │
                                v
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Dagster                                                                            │
│                                                                                    │
│ Production / Test (same logic)                                                     │
│   incoming/manifest sensors -> ingest_job                                          │
│   dispatch-agent polling -> production_agent_dispatch_sensor                       │
│   .dispatch/pending/*.json -> dispatch_sensor (fallback)                           │
│   -> dispatch_stage_job  (ingest + Gemini label + classification + SAM3)           │
│   LS 검수 확정 -> post_review_clip_job -> clip/frame -> build_dataset               │
└───────────────┬───────────────────────────────┬────────────────────────────────────┘
                │                               │
                v                               v
┌──────────────────────────────┐    ┌──────────────────────────────────────────────┐
│ MinIO (5 buckets)            │    │ PostgreSQL                                     │
│  vlm-raw                     │    │ raw_files                                      │
│  vlm-labels                  │    │ video_metadata / image_metadata                │
│  vlm-processed               │    │ labels / processed_clips / image_labels        │
│  vlm-dataset                 │    │ datasets / dataset_clips / classification_*    │
│  vlm-classification          │    │ dispatch_* / labeling_* / genai_*              │
└──────────────────────────────┘    │ image_embeddings (pgvector, ENABLE_EMBEDDING)  │
                                    └──────────────────────────────────────────────────┘
```

- **NAS**: 원본 미디어가 들어오는 파일 시스템입니다 (NAS_200tb, NFS).
- **Dagster**: 수집, 라벨링, 전처리, 검출, 데이터셋 빌드를 오케스트레이션합니다.
- **MinIO**: 단계별 산출물을 저장하는 S3 호환 스토리지입니다 (버킷 5개 고정).
- **PostgreSQL / pgvector**: 메타데이터와 라벨링 결과의 source of truth입니다 (prod: `docker-postgres-1/vlm_pipeline`, staging: `pipeline-test-postgres-1/vlm_pipeline_staging`). `ENABLE_EMBEDDING=true`이면 `image_embeddings` 테이블에 `frame` / `caption` / `video`(frame-pool) 임베딩(PE-Core-L14-336, 1024-d)을 저장하고 entity_type별 partial HNSW(`vector_cosine_ops`)로 유사검색합니다 — 즉 **벡터 DB 역할을 PostgreSQL이 겸합니다**(별도 벡터 스토어 없음).

## Branch Runtime

| 항목 | Production (`main`) | Test (`dev`) |
|------|------------|---------|
| Dagster UI | `http://10.0.0.10:3030/` | `http://10.0.0.10:3031/` |
| PostgreSQL | `docker-postgres-1:15433` / `vlm_pipeline` | `pipeline-test-postgres-1:15432` / `vlm_pipeline_staging` |
| NAS root (host, `NAS_DATA_ROOT`) | `/home/user/mou/nas_200tb` | `/home/user/mou/nas_200tb/staging` |
| Incoming / Archive (container) | `/nas/data/incoming`, `/nas/data/archive` | `/nas/data/incoming`, `/nas/data/archive` |
| Runtime MinIO endpoint | `http://10.0.0.51:9000` | `http://10.0.0.51:9002` |
| MinIO Console | `http://10.0.0.51:9001/` | `http://10.0.0.51:9003/` |
| Dagster home (container) | `/app/dagster_home` | `/app/dagster_home` |
| Compose project | `docker` | `pipeline-test` |
| env file | `docker/.env` | `docker/.env.test` |
| Main entrypoint | `src/vlm_pipeline/definitions.py` | `src/vlm_pipeline/definitions.py` |
| Dispatch ingress | `dispatch-agent:8080` polling + JSON fallback | `dispatch-agent:8081` polling + JSON fallback |

핵심 차이:

- prod/test 모두 `production_agent_dispatch_sensor`와 `dispatch_sensor`를 같은 방식으로 사용합니다.
- 차이는 branch, env 파일, host bind path, external endpoint, feature flag뿐입니다.
- 애플리케이션이 쓰는 실제 MinIO endpoint는 `9000/9002`이고, `9001/9003`은 사람용 Console 주소입니다.
- incoming/archive는 단일 부모(`NAS_DATA_ROOT`)를 `/nas/data`로 bind mount합니다. 이렇게 해야 `incoming → archive`의 `os.rename`이 같은 mount(동일 device)에서 일어나 folder fast-path를 탑니다 (별도 mount면 EXDEV로 per-file 이동 fallback).

## Data Flow

### Production

```text
dispatch-agent:8080
  -> production_agent_dispatch_sensor
  -> dispatch_stage_job
     -> raw_ingest
     -> clip_timestamp        (Gemini 이벤트 구간 JSON)
     -> clip_captioning       (JSON -> labels 테이블 upsert)
     -> classification_video  (Gemini 단일 비디오 분류)
     -> raw_video_to_frame    (raw 비디오 프레임 추출)
     -> dispatch_sam3_image_detection  (SAM3 segmentation/bbox)
     [-> dispatch_yolo_image_detection]  (ENABLE_YOLO_DETECTION=true일 때만)

/nas/data/incoming/.dispatch/pending/*.json
  -> dispatch_sensor (fallback)
  -> dispatch_stage_job

LS 검수 확정 (ls_webhook -> /sync-approve)
  -> post_review_clip_job
  -> clip_to_frame  (검수된 timestamp 기반 clip 분할 + 프레임 추출)
  -> build_dataset_on_finalize_sensor -> build_dataset / build_classification
```

production 정책은 다음과 같습니다.

- `ingest_job` / `mvp_stage_job`는 **수집 전용**입니다.
- Gemini / SAM3가 포함된 자동 라벨링은 **오직 `dispatch_stage_job`** 에서만 자동으로 실행됩니다.
- `dispatch_stage_job`은 Gemini 초벌(이벤트 JSON)까지만 수행하고, **`clip_to_frame`(clip 분할)는 LS 검수 확정 후 `post_review_clip_job`** 에서 수행합니다.
- `incoming/gcp/**`는 GCS 수집 스케줄로 들어오고, 일반 incoming 폴더는 dispatch 요청이 있어야 자동 라벨링으로 이어집니다.
- production 기본 feature flag: `ENABLE_SAM3_DETECTION=true`(SAM3가 primary bbox 엔진), `ENABLE_YOLO_DETECTION=false`, `ENABLE_EMBEDDING=true`, `ENABLE_MANUAL_LABEL_IMPORT=false`.

### Test (`dev`)

```text
dispatch-agent:8081
  -> production_agent_dispatch_sensor
  -> dispatch_stage_job
  -> (production과 동일)
```

test의 주요 특징:

- branch는 `dev`이지만, 실행 로직은 production과 같습니다.
- 요청 ingress는 test agent API(`:8081`) polling으로 받아옵니다 (`IS_STAGING=true` + `PROD_AGENT_BASE_URL` override).
- host bind path와 PostgreSQL DSN / MinIO endpoint만 test 자원을 바라봅니다.

## 단계별 요약

| 단계 | Asset / Job | 설명 | 주요 저장 위치 |
|------|-------------|------|----------------|
| GCS 수집 | `gcs_download_to_incoming` | GCS 버킷에서 incoming으로 다운로드 | NAS |
| INGEST | `raw_ingest` | 파일 검증, checksum, MinIO 업로드, 메타데이터 추출, archive 이동 | `vlm-raw`, `raw_files`, `video_metadata`, `image_metadata` |
| TIMESTAMP | `clip_timestamp` | Gemini로 이벤트 구간 JSON 생성 | `vlm-labels`, `video_metadata` |
| CLASSIFY(video) | `classification_video` | dispatch 전용 Gemini 단일 비디오 분류 | `vlm-labels`, `labels` |
| CAPTIONING | `clip_captioning` | Gemini 이벤트 JSON을 labels row로 정규화 | `labels` |
| FRAME | `clip_to_frame` | (LS 확정 후 `post_review_clip_job`) clip 분할 + 프레임 추출 + top-1 image caption | `vlm-processed`, `processed_clips`, `image_metadata`, `vlm-labels` |
| RAW FRAME | `raw_video_to_frame` | 검출 경로용 raw video 직접 frame 추출 | `vlm-processed`, `image_metadata` |
| SAM3 | `dispatch_sam3_image_detection`, `sam3_image_detection` | SAM3.1 text-prompt segmentation/bbox (**primary 검출 엔진**) | `vlm-labels`, `image_labels` |
| YOLO | `dispatch_yolo_image_detection`, `yolo_image_detection` | YOLO-World detection (`ENABLE_YOLO_DETECTION=true`일 때만, 기본 비활성) | `vlm-labels`, `image_labels` |
| BUILD | `build_dataset` | 학습 데이터셋 조립 | `vlm-dataset`, `datasets`, `dataset_clips` |
| CLASSIFY(build) | `build_classification` | 카테고리별 원본 복사 (video/image) | `vlm-classification`, `classification_datasets` |
| EMBED | `frame_embedding`, `caption_embedding`, `video_embedding` | PE-Core-L14-336 frame/caption 1024-d + video frame-pool(`/framepool`) 임베딩 → pgvector (`ENABLE_EMBEDDING=true`일 때만) | `image_embeddings` (pgvector) |
| BENCHMARK | `sam3_shadow_compare` | YOLO bbox vs SAM3 segmentation agreement 비교 | `vlm-labels/benchmarks/sam3_vs_yolo/` |

## Auto Labeling 상세

### 1. Gemini Timestamp / Caption / Classification

`clip_timestamp`는 Gemini(Vertex)를 사용해 비디오 이벤트를 분석합니다.

- 프롬프트 정의: `src/vlm_pipeline/lib/gemini_prompts.py`
- 호출 래퍼: `src/vlm_pipeline/lib/gemini.py`
- 429 `Resource exhausted`에는 공용 backoff/retry가 적용됩니다.
- 대형 비디오(450MB 초과)는 preview mp4를 생성한 뒤 Gemini에 전달합니다 (Vertex 524MB 제한 회피).
- 병렬도: `GEMINI_MAX_WORKERS`(기본 5), 긴 영상 chunk는 `GEMINI_CHUNK_MAX_WORKERS`(기본 3).

반환된 이벤트 JSON은 `vlm-labels/.../events/*.json`에 저장되고, 이어서 `clip_captioning`이 이를 `labels` 테이블에 정규화합니다. dispatch 경로에서는 `classification_video`가 비디오 단일 분류 결과를 `labels`에 1행으로 적재합니다.

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

> `labels`는 **per-event** 레코드입니다. 한 비디오가 N개의 이벤트를 가지면 N행, 0개면 0행입니다. 따라서 `labels` 행 수가 0이라고 라벨링 실패가 아니며, 완료 지표는 `video_metadata.timestamp_status='completed'` + `timestamp_label_key` 세팅 + `vlm-labels/.../events/*.json` 객체 존재입니다.

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

### 3. SAM3 / YOLO Detection

검출 단계는 별도 GPU 추론 컨테이너를 통해 동작합니다.

- **SAM3 (primary)**: `docker/sam3/` 서비스(port `8002`), 모델 `sam3.1_multiplex.pt`. dispatch 요청의 text prompt로 segmentation → mask bbox → `vlm-labels` COCO JSON + `image_labels` 적재. prod·staging이 단일 공유 컨테이너(`docker-sam3-1`)를 사용합니다 (staging은 `SAM3_API_URL=http://docker-sam3-1:8002`로 참조).
- **YOLO-World (optional)**: `docker/yolo/` 서비스(port `8001`), 모델 `yolov8l-worldv2.pt`. `ENABLE_YOLO_DETECTION=true`일 때만 asset/job/selection에 포함됩니다 (production 기본 비활성).
- bbox 결과 JSON의 source of truth는 모두 **`vlm-labels`** 입니다 (`vlm-processed`에 중복 저장 금지).

클래스 우선순위(검출 대상 비어 있을 때):

1. dispatch `classes`
2. 없으면 `categories -> derive_classes_from_categories()`
3. legacy spec flow면 `spec.classes`와 bbox config 교집합
4. 그래도 없으면 서버 기본 classes (`YOLO_DEFAULT_CLASSES`)

## Vector Search & 임베딩 분석 (pgvector)

`ENABLE_EMBEDDING=true`이면 `frame_embedding` / `caption_embedding` / `video_embedding` 자산이 PE-Core-L14-336 임베딩(1024-d)을 `image_embeddings`(pgvector)에 적재합니다. 검색은 cosine 거리 연산자 `<=>` + entity_type별 HNSW(`vector_cosine_ops`)로 수행하며, 캡션은 추가로 pg_trgm 키워드 검색과 결합한 **하이브리드 검색**을 지원합니다.

자주 쓰는 SQL은 [docker/analysis/vectordb_queries.sql](docker/analysis/vectordb_queries.sql)에 모아 두었습니다 (현황/통계 · 이미지 유사 · 메타데이터 필터+벡터 · 키워드(pg_trgm) · near-duplicate dedup · 캡션↔이미지 정합 · 인덱스 헬스).

### 임베딩 종류 (`entity_type`) & 인덱스

| entity_type | 내용 | partial HNSW |
|-------------|------|--------------|
| `frame` | 프레임 이미지 임베딩 | migration 008 |
| `caption` | 캡션 텍스트 임베딩 | migration 008 |
| `video` | 비디오 frame-pool 임베딩 (`model_name`에 `/framepool` suffix) | migration 009 |

- `entity_type`별 **partial HNSW**(`vector_cosine_ops`)로 분리 — 단일 통합 인덱스는 `WHERE entity_type='frame'` 같은 filtered cross-modal 쿼리에서 0건을 반환할 수 있기 때문. 다중 필터 조합은 `SET hnsw.iterative_scan=relaxed_order`로 보강.
- 캡션 키워드 검색용 **pg_trgm GIN 인덱스**(`labels.caption_text`, migration 010).
- recall/latency 튜닝(`hnsw.ef_search`)은 [docs/runbook/hnsw-tuning.md](docs/runbook/hnsw-tuning.md) 참고.

### 검색 모드

분석 컨테이너의 `fiftyone_pgvector` 헬퍼(노트북·Streamlit 대시보드 공용):

| 함수 | 검색 |
|------|------|
| `search_by_text(q, k, …filters)` / `count_by_text(q, threshold, …filters)` | 텍스트 → 프레임 이미지 top-k / 임계값 이상 전체 매칭 수 (cross-modal) |
| `search_by_image(image_id, k)` | 프레임 → 유사 프레임 |
| `search_by_uploaded_image(bytes, k)` | 업로드 이미지 → 유사 프레임 |
| `search_captions(q, mode=…)` | 캡션 검색 — `keyword`(pg_trgm) / `semantic`(pgvector) / `hybrid`(RRF 융합) |
| `search_videos_by_text(q, k)` / `search_similar_videos(asset_id, k)` | 비디오 텍스트 검색 / 유사 비디오 |

- **하이브리드**(`mode='hybrid'`)는 pg_trgm 키워드 결과와 pgvector semantic 결과를 **RRF(rrf_k=60)**로 융합합니다. `pg_trgm` 확장/인덱스가 없으면 keyword 절반이 빈 결과가 되어 semantic-only로 graceful 강등됩니다.
- `search_by_text` / `search_by_uploaded_image`는 메타데이터 facet 필터를 받습니다: `source`(image_key prefix), `image_role`, `daynight_type`, `environment_type`.

> **Cross-lingual (KO→EN)**: PE-Core 텍스트 인코더는 영어 중심이라, 텍스트→프레임(`search_by_text`)과 캡션 semantic/hybrid 검색의 한국어 쿼리는 `translate_query_ko_en`이 **컨테이너 내부에서** 영어로 번역한 뒤 임베딩합니다 (비디오 텍스트 검색·FiftyOne App 프롬프트에는 미적용) — ① 한글 없으면 무변경 → ② 도메인 사전 완전일치(`'화재'→'fire'`, 결정적·무호출) → ③ Vertex Gemini 일반 번역(캐시) → ④ Vertex 불가 시 사전 부분치환. `ENABLE_VERTEX_QUERY_TRANSLATION=1` + `GEMINI_*` creds가 있으면 ③이 활성, 없으면 ④로 graceful 동작합니다 (DB 임베딩은 건드리지 않고 쿼리 텍스트만 번역).

### 분석 surface (`analysis` profile)

`COMPOSE_PROFILES=analysis`로 `analysis` 컨테이너를 기동하면 임베딩 시각화/검색 도구가 뜹니다.

| 도구 | 포트 | 설명 |
|------|------|------|
| JupyterLab | `8888` | `fiftyone_pgvector` 헬퍼로 검색·클러스터·UMAP/PCA/MDS 시각화 (token=`JUPYTER_TOKEN`) |
| FiftyOne App | `5151` | `frames` / `captions` 데이터셋 projection 탐색 + SAM3 bbox/캡션 overlay |
| Streamlit | `8501` | `embedding_dashboard.py` — 검색(텍스트/이미지ID/이미지 업로드 + facet 필터, 캡션 keyword/semantic/hybrid), near-duplicate·class separability·label suspect·active-learning 큐 |

```bash
COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d analysis
```

대용량 `frames` 데이터셋의 빌드·재기동은 `docker/analysis/`의 전용 스크립트를 씁니다.

| 스크립트 | 설명 |
|----------|------|
| `fiftyone_full_build.py` | 대용량(188K+) `frames` 초기 빌드 — 병렬 미디어 다운로드(`FFB_WORKERS`)·청크 add(`FFB_CHUNK`)·UMAP sample-fit(`FFB_FIT`)·표본 상한(`FFB_LIMIT`)으로 OOM 방지 |
| `fiftyone_relaunch.py` | 이미 빌드된 데이터셋을 빌드 없이 앱만 재기동 + keep-alive |
| `fiftyone_umap_only.py` | 기존 데이터셋에 UMAP/PCA brain run만 재실행 |
| `label_qa_fiftyone.py` | 사람 GT 이미지를 격리 `pseudo_qa` 데이터셋으로 빌드 → FiftyOne `evaluate_detections`로 SAM3 pseudo-label FP/FN 육안 QA |

> ⚠️ FiftyOne App에서 이미지(presigned URL)가 브라우저에 뜨려면 `ANALYSIS_MINIO_ENDPOINT`를 host-reachable 주소(예: `http://10.0.0.10:9000`)로 설정해야 합니다. 내부 docker 명(`minio:9000`)은 브라우저에서 미도달합니다. 세부는 [docker/analysis/README.md](docker/analysis/README.md) 참고.

## Database Schema

주요 테이블/뷰는 `src/vlm_pipeline/sql/schema_postgres.sql`에 정의되고, 증분 변경은 `src/vlm_pipeline/sql/migrations/postgres/`(`001`~`016`)로 관리됩니다.

### 운영 핵심 테이블/뷰

| 테이블/뷰 | 설명 |
|-----------|------|
| `raw_files` | 원본 미디어 메타, checksum, MinIO raw 위치 |
| `video_metadata` | ffprobe 기반 비디오 메타, Gemini 상태, 재인코딩 추적 |
| `image_metadata` | 이미지/프레임 메타, `image_caption_text`, `image_caption_score` |
| `labels` | 이벤트 단위 timestamp / caption / classification 라벨 |
| `processed_clips` | clip 기반 전처리 산출물 |
| `image_labels` | SAM3 / YOLO detection 결과 |
| `image_label_annotations` | LS 확정 bbox의 박스 단위 projection (`box_index`, `category`, `bbox_x/y/w/h`, `score`; MinIO COCO JSON이 SoT, `image_labels` FK) |
| `v_finalized_labels` (VIEW) | finalized caption / timestamp / bbox 라벨 통합 조회 (`label_type` union; grain이 달라 테이블 대신 VIEW) |
| `datasets` / `dataset_clips` | 데이터셋 정의 및 dataset ↔ clip 연결 |
| `classification_datasets` | classification 빌드 산출물 추적 |
| `image_embeddings` | PE-Core 프레임/캡션 임베딩 (pgvector, `ENABLE_EMBEDDING`일 때) |
| `train_dataset_versions` | 동결 학습셋 버전 메타 및 lineage (`task`, `manifest_key`, `content_checksum`, count/split 통계) |
| `model_registry` | 모델 버전 레지스트리 및 promote 상태 (`model`, `version`, `train_dataset_version_id`, metrics/checkpoint/env lock) |
| `gpu_maintenance_lock` | GPU 서빙 정비락 상태 (`target`, `active`, `owner_run_id`, `heartbeat_at`, `ttl_seconds`) |
| `embedding_active_model` | AL 큐 / 텍스트→이미지 검색이 읽는 활성 임베딩 모델 포인터 (`scope`, `model_name`, promote 시 원자 갱신) |
| `dataset_catalog` | DVC 큐레이션 데이터셋 버전 카탈로그 (`task`, git/DVC pointer, commit message, ingestion status) |
| `dataset_catalog_aliases` | task별 `current` 등 가변 alias → `dataset_catalog` pin |
| `dataset_catalog_pin_events` | dataset catalog pin 변경 이력 감사 로그 (`previous_dataset_catalog_id`, `pinned_by`, `pin_reason`) |

### dispatch / spec / genai 테이블

| 테이블 | 설명 |
|--------|------|
| `dispatch_requests` | dispatch 요청 추적 (검출 파라미터, labeling_method 포함) |
| `dispatch_pipeline_runs` | dispatch run 단계별 상태 추적 (step_name, step_status, 처리 통계) |
| `staging_model_configs` | output 타입별 모델 선택 + 기본 파라미터 (bbox/timestamp/captioning) |
| `labeling_specs` | spec 수신 → 라우팅/재시도/완료 추적 (categories, classes, labeling_method) |
| `labeling_configs` | config/parameters JSON 동기화 (버전 관리) |
| `requester_config_map` | requester/team → config 매핑 (personal → team → fallback 우선순위) |
| `genai_batches` / `genai_jobs` | GenAI Studio 비디오 생성 batch/job lifecycle |

현재 스키마에서 중요한 점:

- `image_metadata`는 `caption_text` 대신 **`image_caption_text`** 를 canonical 컬럼으로 사용합니다.
- bbox JSON과 image caption JSON의 source of truth는 모두 **`vlm-labels`** 입니다.
- `image_embeddings`는 pgvector 확장이 필요하며(migration 006~009), 확장이 없는 환경에서는 해당 migration이 skip됩니다 (`ENABLE_EMBEDDING`로 gating). 캡션 키워드(하이브리드) 검색용 pg_trgm GIN 인덱스는 010이며, `pg_trgm` 미설치 시 skip됩니다.

## Project Structure

```text
.
├── src/
│   └── vlm_pipeline/
│       ├── definitions.py              # 단일 Definitions entrypoint (canonical)
│       ├── definitions_production.py   # job/sensor/asset/resource 조립
│       ├── defs/
│       │   ├── dispatch/               # dispatch sensor / service / production_agent_sensor / webhook_server
│       │   ├── ingest/                 # raw ingest, archive, manifest, health/stuck-guard sensors
│       │   ├── label/                  # clip_timestamp, classification_video, manual import, artifact_*
│       │   ├── process/                # clip_captioning, clip_to_frame, raw_video_to_frame
│       │   ├── sam/                     # SAM3 detection + shadow-compare benchmark
│       │   ├── yolo/                    # YOLO-World detection (flag-gated)
│       │   ├── embed/                  # PE-Core 프레임/캡션 임베딩 (pgvector, ENABLE_EMBEDDING)
│       │   ├── build/                  # build_dataset + build_classification
│       │   ├── train/                  # GPU maintenance guard sensor (MLOps finetune scaffolding)
│       │   ├── gcp/                    # GCS download
│       │   ├── genai/                  # GenAI Studio async job poll sensor
│       │   ├── ls/                     # Label Studio task 생성 sensor + presign 갱신 schedule
│       │   ├── spec/                   # spec config resolver (DB 의존)
│       │   └── shared/                 # 공용 helper
│       ├── lib/                        # prompts, frame planning, sam3/yolo/embedding client, key_builders, env helpers
│       ├── resources/                  # postgres_* (base/migration/ingest/labeling/process/detection/embedding/genai/train/maintenance) + minio + config + runtime_settings
│       └── sql/                        # schema_postgres.sql, migrations/postgres/ (001-016)
├── docker/
│   ├── docker-compose.yaml
│   ├── docker-compose.dev.yaml         # 로컬 dev overlay
│   ├── docker-compose.labelstudio.yaml # Label Studio overlay
│   ├── .env / .env.test
│   ├── app/        # Dagster code-server 이미지
│   ├── sam3/       # SAM3.1 segmentation 서버
│   ├── yolo/       # YOLO-World 추론 서버
│   ├── embedding/  # PE-Core 임베딩 서비스
│   ├── analysis/   # JupyterLab + FiftyOne
│   ├── genai/      # GenAI Studio (Kling/Veo/Higgsfield)
│   ├── pg-backup/  # pg_dump + restic 백업 sidecar
│   └── grafana/    # 대시보드 provisioning
├── scripts/        # compose-prod.sh / compose-staging.sh / 운영·검증 스크립트
├── docs/
├── gcp/
└── tests/
```

## Infrastructure

Docker Compose(`docker/docker-compose.yaml`)로 서비스를 실행합니다. 대부분의 부가 서비스는 **compose profile** 로 gating되며, production은 `.env`의 `COMPOSE_PROFILES=sam3,backup,genai,embedding,analysis`로 활성화합니다.

| 서비스 | 포트 | profile | 설명 |
|--------|------|---------|------|
| `dagster` | `3030` / `3031` | - | production/test Dagster webserver |
| `dagster-daemon` | - | - | sensor / schedule daemon |
| `dagster-code-server` | `4000`(내부) | - | gRPC code server |
| `app` | - | - | 빌드/런타임 베이스 컨테이너 |
| `postgres` | `${POSTGRES_PORT}` (prod `15433` / staging `15432`) | - | **primary 메타데이터 DB** (`vlm_pipeline` / `vlm_pipeline_staging`) |
| `minio` | `9000`(API) / `9001`(Console) | - | 로컬 MinIO (prod/staging 런타임은 외부 `10.0.0.51`) |
| `grafana` | `3000` | - | 운영 대시보드 |
| `dispatch-webhook` | `8090` | `webhook` | dispatch webhook 수신 서버 |
| `sam3` | `8002` | `sam3` | SAM3.1 segmentation 서버 (GPU 1, 단일 공유 컨테이너) |
| `embedding-service` | `8003` | `embedding` | PE-Core-L14-336 임베딩 서비스 (GPU 0) |
| `genai` | `8088` | `genai` | GenAI Studio (Kling/Veo/Higgsfield 등) |
| `pg-backup` | - | `backup` | pg_dump + restic 일일 백업 sidecar |
| `analysis` | `8888`/`5151`/`8501` | `analysis` | JupyterLab + FiftyOne + Streamlit |
| `fiftyone-mongo` | - | `analysis` | FiftyOne 메타데이터 MongoDB sidecar |
| `trainer` | - | `trainer` | 파인튜닝 학습 job (one-shot, 수동 기동, GPU) |
| `mlflow` | `5500`→5000 | `mlflow` | MLflow tracking 서버 (실험·모델 메트릭, 기본 비활성) |

### MinIO 버킷 (5개 고정)

| 버킷 | 용도 |
|------|------|
| `vlm-raw` | 원본 미디어 |
| `vlm-labels` | 이벤트 JSON, bbox(COCO) JSON, image caption JSON (라벨 source of truth) |
| `vlm-processed` | clip, frame 이미지 |
| `vlm-dataset` | 최종 데이터셋 |
| `vlm-classification` | 카테고리별 원본 복사 (`<folder_prefix>/{video,image}/<category>/<file>`, JSON/DB 미적재) |

### 운영 규칙

- `raw_key`는 `<source_unit>/<rel_path>` 규칙을 사용합니다 (`YYYY/MM` prefix 금지).
- `vlm-labels`만 라벨 JSON의 source of truth로 사용합니다.
- 파일 단위 오류는 fail-forward로 처리하고, `<manifest_dir>/failed/*.jsonl`로 남깁니다.
- archive 이동 후 source 폴더가 비면 incoming 쪽 빈 부모 폴더도 정리합니다.
- PostgreSQL 전환으로 단일-파일 write lock 제약은 사라졌지만, run-coordinator(`QueuedRunCoordinator`, `max_concurrent_runs: 4`)의 `duckdb_writer` tag concurrency=1 게이트는 보수적 write 직렬화 안전마진으로 아직 유지됩니다 (`docker/app/dagster.yaml`, `src/vlm_pipeline/definitions_common/jobs.py` — 태그 이름만 legacy).

## GenAI Studio (영상·이미지 생성 웹 UI)

`docker/genai/`는 외부 생성 모델(Kling·Veo·Higgsfield·Nanobanana·GPT-Image)을 batch로 호출하는 **FastAPI 웹 콘솔**입니다. `genai` compose profile로 기동하며, 생성 결과를 라벨링 파이프라인 입력으로 promote할 수 있습니다.

### 접속

- URL: `http://<HOST>:8088` (`GENAI_PORT`, profile `genai`)
- 인증: HTTP Basic (`GENAI_BASIC_AUTH_USER` / `GENAI_BASIC_AUTH_PASS`). `GENAI_AUTH_DISABLED=1`로 비활성(내부망 전용).
- 활성 엔진: `GENAI_ENGINES_ENABLED` (prod 기본 `kling,higgsfield,nanobanana,gpt_image`; staging 예시는 `veo` 포함).

```bash
COMPOSE_PROFILES=genai ./scripts/compose-prod.sh up -d genai
```

### 주요 화면

| 경로 | 설명 |
|------|------|
| `GET /` | 단건 batch 제출 폼 (엔진·프롬프트·레퍼런스 파일 업로드) |
| `GET /genai/bulk` | 대량 제출 |
| `GET /genai/batches` | batch 목록 |
| `GET /genai/batches/{id}` | batch 상세 (job별 상태·출력·비용) |
| `POST …/{id}/promote-to-labeling` | 생성 결과를 dispatch 라벨링으로 promote |
| `GET /genai/costs` | 엔진별 비용 집계 |
| `GET /healthz` | health (Dagster polling이 사용) |

### 출력 & 파이프라인 연동

- 생성물은 ingest 센서가 스캔하는 incoming이 아니라 **격리된 sibling** `GENAI_NAS_INCOMING`(기본 `/nas/data/genai_studio`)에 씁니다 (provenance: `source_type=genai_output`, `label_policy=none`).
- promote 시 `<INCOMING_DIR>/.dispatch/pending/<request_id>.json` + `<INCOMING_DIR>/genai_<batch_id>/<seq>.<ext>`를 작성해 dispatch 라벨링 경로로 넘깁니다.
- Dagster `genai_poll_sensor`가 `GENAI_INTERNAL_BASE`(기본 `http://genai:8088`)의 internal API를 `GENAI_INTERNAL_TOKEN`으로 polling하여 `genai_batches` / `genai_jobs` 테이블 lifecycle을 갱신합니다 (비동기 job 폴링).
- 가드레일: `GENAI_MAX_BYTES_PER_FILE`(기본 50MB), `GENAI_MAX_FILES_PER_BATCH`(기본 20), `GENAI_RATE_LIMIT_PER_MIN` / `GENAI_DAILY_BATCH_LIMIT` / `GENAI_DAILY_BYTES_LIMIT`.

## Getting Started

### 1. Requirements

- Python 3.10+
- Docker / Docker Compose
- NVIDIA GPU + CUDA (SAM3/YOLO/embedding/재인코딩)
- NAS mount (`NAS_DATA_ROOT`)
- PostgreSQL (compose `postgres` 서비스)

### 2. 환경 설정

production (`main`)은 `docker/.env`, test(`dev`)는 `docker/.env.test`를 사용합니다 (git 미추적, 호스트에서 직접 편집). 신규 셋업 시 예시 파일을 참고하세요.

```bash
cp .env.example docker/.env          # production
cp .env.dev.example docker/.env.test # test (staging clone에서)
```

주요 환경변수:

| 변수 | 설명 |
|------|------|
| `DATAOPS_POSTGRES_DSN` | **(필수)** PostgreSQL DSN — 예: `postgresql://airflow:****@docker-postgres-1:5432/vlm_pipeline`. 미설정 시 startup RuntimeError. |
| `DATAOPS_DB_BACKEND` | DB backend (현재 `postgres`) |
| `POSTGRES_PORT` | Postgres 호스트 노출 포트 (prod `15433`, staging `15432`) |
| `MINIO_ENDPOINT` | 런타임 MinIO endpoint (prod `http://10.0.0.51:9000`) |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO 자격 (기본값 `minioadmin`은 거부 — `ALLOW_INSECURE_DEFAULT_CREDS=1`로만 허용) |
| `NAS_DATA_ROOT` | incoming/archive 단일 bind mount 호스트 경로 (prod `/home/user/mou/nas_200tb`) |
| `INCOMING_DIR` / `ARCHIVE_DIR` | 컨테이너 내부 경로 (`/nas/data/incoming`, `/nas/data/archive`) |
| `COMPOSE_PROFILES` | 활성 compose profile (prod `sam3,backup,genai,embedding,analysis`) |
| `IS_STAGING` | staging 런타임 토글 (test env에서 `true`) |
| `ENABLE_SAM3_DETECTION` | SAM3 검출 asset 등록 (prod `true`) |
| `ENABLE_YOLO_DETECTION` | YOLO 검출 asset 등록 (기본 `false`) |
| `ENABLE_EMBEDDING` | 임베딩 asset/sensor 등록 (prod `true`) |
| `ENABLE_MANUAL_LABEL_IMPORT` | 수동 라벨 import asset 등록 (기본 `false`) |
| `SAM3_API_URL` | SAM3 서버 URL (기본 `http://sam3:8002`; staging은 공유 컨테이너 참조) |
| `EMBEDDING_API_URL` | 임베딩 서비스 URL (기본 `http://embedding-service:8003`) |
| `GENAI_INTERNAL_TOKEN` | `genai_poll_sensor`가 genai 컨테이너 호출 시 사용 |
| `GOOGLE_APPLICATION_CREDENTIALS` | Vertex 인증 |
| `PROD_AGENT_POLLING_ENABLED` / `PROD_AGENT_BASE_URL` | dispatch-agent dispatch polling (기본 `http://host.docker.internal:8080`; staging은 `:8081`로 override) |
| `INGEST_UPLOAD_WORKERS` | raw ingest MinIO 업로드 worker 수 (`8` 권장) |
| `GEMINI_MAX_WORKERS` / `GEMINI_CHUNK_MAX_WORKERS` | Gemini 병렬 worker 수 (`5` / `3` 권장) |
| `DATASET_REQUIRE_LS_FINALIZED` | `1`이면 LS 확정(`review_status='finalized'`)된 dispatch만 dataset 후보 (기본 `1`) |

### 3. 인프라 실행

반드시 wrapper 스크립트를 사용합니다 — 직접 `docker compose` 호출은 env 파일/프로젝트명 누락으로 staging을 건드리거나 DSN 미해결로 crashloop을 일으킵니다 (2026-05-19 실제 발생).

```bash
# production (main repo)
./scripts/compose-prod.sh up -d

# test/staging (staging clone repo)
./scripts/compose-staging.sh up -d
```

- `compose-prod.sh` → `docker compose -p docker --env-file .env ...`
- `compose-staging.sh` → `PIPELINE_ENV_FILE=.env.test docker compose -p pipeline-test --env-file .env.test ...`

### 4. 환경 검증

```bash
# Postgres row count
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "SELECT COUNT(*) FROM raw_files;"

# Dagster / 추론 서버 health
curl -fsS http://127.0.0.1:3030/server_info   # prod
curl -fsS http://127.0.0.1:3031/server_info   # staging
curl -fsS http://127.0.0.1:8002/health        # SAM3
curl -fsS http://127.0.0.1:8001/health        # YOLO (활성 시)
```

> `scripts/query_local_duckdb.py`는 DuckDB legacy 스크립트로, `ALLOW_LEGACY_DUCKDB_SCRIPT=1` 가드가 필요합니다. 운영 조회는 위의 `psql`을 사용하세요.

### 5. 테스트

```bash
pip install -e ".[dev]"
pytest tests/unit -q
pytest tests/integration -q
```

## Dagster Jobs & Sensors

### Jobs (항상 등록)

| Job | 설명 |
|-----|------|
| `mvp_stage_job` | 수집 전용 호환 job |
| `ingest_job` | raw ingest 단독 |
| `gcs_download_job` | GCS → incoming |
| `sourcea_download_job` | source-a 사이트 일일 수집 (06:00 KST 스케줄) |
| `dispatch_stage_job` | 운영 자동 라벨링의 유일한 진입점 (ingest + Gemini + classification + SAM3) |
| `auto_labeling_job` | dispatch 잔여/누락분 backlog 라벨링 (`clip_timestamp` + `clip_captioning` + `classification_video`) |
| `upload_label_job` | `from_archived=True` dispatch JSON → archive 파일 MinIO 업로드 |
| `post_review_clip_job` | LS 검수 확정 후 `clip_to_frame`(clip 분할 + 프레임 추출) |
| `sam3_shadow_compare_job` | YOLO vs SAM3 benchmark |
| `ls_presign_renew_job` | LS presigned URL 갱신 |

### Jobs (feature flag로 등록)

| Job | flag |
|-----|------|
| `manual_label_import_job` | `ENABLE_MANUAL_LABEL_IMPORT` |
| `yolo_standard_detection_job` | `ENABLE_YOLO_DETECTION` |
| `sam3_standard_detection_job` | `ENABLE_SAM3_DETECTION` |
| `frame_embedding_job` / `caption_embedding_job` / `video_embedding_job` | `ENABLE_EMBEDDING` |

### Sensors / Schedules

| 이름 | 기본 상태 | 설명 |
|------|----------|------|
| `production_agent_dispatch_sensor` | STOPPED (기본) | `dispatch-agent` polling → `dispatch_stage_job`. `PROD_AGENT_POLLING_ENABLED=true` + UI에서 ON 필요 |
| `dispatch_sensor` | STOPPED (기본) | `.dispatch/pending/*.json`(`from_archived=False`) → `dispatch_stage_job` / `ingest_job`. UI에서 ON 필요 |
| `archive_dispatch_sensor` | RUNNING | `.dispatch/pending/*.json`(`from_archived=True`) → `upload_label_job` |
| `incoming_manifest_sensor` | RUNNING | pending manifest → `ingest_job` |
| `auto_bootstrap_manifest_sensor` | RUNNING | incoming 스캔 후 manifest 생성 |
| `stuck_run_guard_sensor` | RUNNING | stuck / orphan run 정리 |
| `maintenance_guard_sensor` | RUNNING | GPU 정비락 stale 자동해제 (heartbeat TTL 초과 / owner run 종료 시) |
| `nas_health_sensor` | RUNNING | NAS 접근성 probe + Slack 알림 |
| `cross_table_consistency_sensor` | RUNNING | raw_files/video_metadata/labels 정합성 점검 |
| `dispatch_run_success/failure/canceled_sensor` | RUNNING | dispatch run status finalizer |
| `auto_labeling_sensor` | RUNNING | Gemini backlog 감지 → `auto_labeling_job` |
| `ls_task_create_sensor` | RUNNING | dispatch 완료 후 LS task 생성 |
| `build_dataset_on_finalize_sensor` | STOPPED | LS 확정 후 dataset build |
| `genai_poll_sensor` | RUNNING | 비동기 GenAI(Kling/Veo/Higgsfield) job polling |
| `frame_embedding_backlog_sensor` / `caption_embedding_backlog_sensor` | STOPPED (`ENABLE_EMBEDDING`) | 미임베딩 backlog → embedding job |
| `gcs_download_schedule` | - | 매일 04:00 KST GCS 수집 |
| `sourcea_download_schedule` | RUNNING | 매일 06:00 KST source-a 사이트 일일 수집 |
| `ls_presign_renew_schedule` | - | 매일 05:00 KST LS presigned URL 갱신 |

## Query Examples

```sql
-- INGEST 상태 집계
SELECT ingest_status, COUNT(*) AS cnt
FROM raw_files
GROUP BY ingest_status
ORDER BY ingest_status;

-- 자동 라벨링(timestamp) 완료된 비디오 수
SELECT timestamp_status, COUNT(*) AS cnt
FROM video_metadata
GROUP BY timestamp_status
ORDER BY timestamp_status;

-- bbox 결과가 있는 이미지 수 (SAM3/YOLO)
SELECT COUNT(*) AS labeled_images
FROM image_labels;

-- image caption이 저장된 frame 수
SELECT COUNT(*) AS captioned_frames
FROM image_metadata
WHERE image_caption_text IS NOT NULL;
```

위 쿼리는 `docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline` 로 실행합니다.

## 운영 팁

- production에서 자동 라벨링은 `dispatch-agent:8080` polling이 기본 ingress입니다 (`PROD_AGENT_POLLING_ENABLED=true`). `.dispatch/pending/*.json`은 fallback으로 유지됩니다.
- test에서는 `dispatch-agent-staging:8081` 연결 상태와 `PROD_AGENT_POLLING_ENABLED=true` (+ `PROD_AGENT_BASE_URL=http://host.docker.internal:8081`) 여부를 먼저 확인합니다.
- 라벨링 stage가 실제로 Gemini를 호출했는지 의심되면 Dagster run의 `clip_timestamp` step 실행 시간을 봅니다 (20 videos → 90~120s 정상, 0s면 skip).
- 깨끗한 staging 재테스트 전에는 staging Postgres(`vlm_pipeline_staging`) / MinIO(`:9003`) / `.dispatch` 상태를 정리한 뒤 다시 시작합니다 (`CLAUDE.md`의 "Staging 초기화" 절차 참고). staging incoming/archive 원본 폴더는 명시 요청 없이 삭제 금지.

## 참고 문서

- `AGENTS.md` — 에이전트용 진입점
- `CLAUDE.md` — 로컬 상세 운영 요약
- `docs/index.md` — 문서 전체 목차
- `docs/references/deployment-guide.md` — 배포 가이드
- `docs/runbook.md` / `docs/runbook/` — 운영 런북
- `LABEL_STORAGE_POLICY.md` — 라벨 저장 정책
- `docker/analysis/vectordb_queries.sql` — pgvector 자주 쓰는 SQL 모음
- `docs/runbook/hnsw-tuning.md` — pgvector HNSW recall/latency 튜닝
- `docker/analysis/README.md` — 임베딩 시각화/유사검색(analysis 컨테이너) 사용법

---

이 README는 현재 `definitions.py`, `definitions_production.py`, `docker/docker-compose.yaml`, `docker/.env(.test)` 기준의 운영 흐름을 요약합니다. 세부 스키마나 플레이북은 `CLAUDE.md`와 `src/vlm_pipeline/sql/schema_postgres.sql`(+ `migrations/postgres/`)을 함께 참고하세요.
