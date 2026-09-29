# VLM DataOps Pipeline

VLM(Vision-Language Model) 학습 데이터를 구축하기 위한 데이터 파이프라인입니다.
NAS에 있는 이미지/비디오 미디어를 수집하고, 중복을 정리한 뒤, Gemini(Vertex) 기반 이벤트 라벨링과 SAM3 segmentation 검출을 수행하고, 최종적으로 학습 데이터셋을 조립합니다. 전체 파이프라인은 **Dagster + PostgreSQL(pgvector) + MinIO** 기반으로 운영됩니다.

🔎 **Vector DB (pgvector)**: `ENABLE_EMBEDDING=true`이면 프레임·캡션·비디오(frame-pool) 미디어를 PE-Core-L14-336로 1024-d 임베딩하여 PostgreSQL의 `image_embeddings`(pgvector)에 적재하고, entity_type별 HNSW 인덱스로 **텍스트→이미지(cross-modal)·이미지→이미지·캡션(keyword/semantic/hybrid)·비디오 유사검색**과 near-duplicate·label-suspect·active-learning 같은 데이터 품질 분석을 지원합니다. 상세는 [Vector Search & 임베딩 분석](#vector-search--임베딩-분석-pgvector) 참고.

> DuckDB는 2026-05-19 PG cutover 이후 primary store에서 제거되었습니다. 현재 메타데이터/라벨의 source of truth는 PostgreSQL이며, DuckDB는 `pg_duckdb` extension(analytics 쿼리 한정)으로만 남아 있습니다. MotherDuck sync는 production 기본 entrypoint(`definitions.py`)에서는 제외되었고, PG 소스를 지원하는 `src/python/local_duckdb_to_motherduck_sync.py` + profile 기반 entrypoint(`definitions_profiles.py`, staging 전용)에만 남아 있습니다 (구버전 사본은 `scripts/archive/`).

현재 기준으로 이 저장소는 **branch-based runtime** 으로 운영합니다.

- **`main` = production**: Dagster `http://10.0.0.10:3030/`, MinIO Console `http://10.0.0.51:9001/`, runtime MinIO endpoint `http://10.0.0.51:9000`, Postgres `docker-postgres-1:15433/vlm_pipeline`
- **`dev` = test(staging)**: Dagster `http://10.0.0.10:3031/`, MinIO Console `http://10.0.0.51:9003/`, runtime MinIO endpoint `http://10.0.0.51:9002`, Postgres `pipeline-test-postgres-1:15432/vlm_pipeline_staging` — **상시 기동 아님** (필요 시 `./scripts/compose-staging.sh up -d`)
- test는 staging 데이터 plane(Postgres `vlm_pipeline_staging`, `/home/user/mou/nas_primary/staging/...`)을 재사용하지만, **코드 로직은 production과 동일**합니다.

문서 운영 기준은 역할을 분리합니다.

- `README.md`: 사람용 개요와 운영 흐름
- `AGENTS.md`: 에이전트용 짧은 맵
- `CLAUDE.md`: 로컬 상세 운영 요약
- `docs/`: 설계, 계획, 참고 문서의 기록 시스템

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│ NAS (NAS_primary / 10.0.0.51, NFS)                                               │
│  Production host bind: /home/user/mou/nas_primary            → 컨테이너 /nas/data      │
│  Test host bind:       /home/user/mou/nas_primary/staging    → 컨테이너 /nas/data      │
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

- **NAS**: 원본 미디어가 들어오는 파일 시스템입니다 (NAS_primary, NFS).
- **Dagster**: 수집, 라벨링, 전처리, 검출, 데이터셋 빌드를 오케스트레이션합니다.
- **MinIO**: 단계별 산출물을 저장하는 S3 호환 스토리지입니다 (버킷 5개 고정).
- **PostgreSQL / pgvector**: 메타데이터와 라벨링 결과의 source of truth입니다 (prod: `docker-postgres-1/vlm_pipeline`, staging: `pipeline-test-postgres-1/vlm_pipeline_staging`). `ENABLE_EMBEDDING=true`이면 `image_embeddings` 테이블에 `frame` / `caption` / `video`(frame-pool) 임베딩(PE-Core-L14-336, 1024-d)을 저장하고 entity_type별 partial HNSW(`vector_cosine_ops`)로 유사검색합니다 — 즉 **벡터 DB 역할을 PostgreSQL이 겸합니다**(별도 벡터 스토어 없음).

## Branch Runtime

| 항목 | Production (`main`) | Test (`dev`) |
|------|------------|---------|
| Dagster UI | `http://10.0.0.10:3030/` | `http://10.0.0.10:3031/` |
| PostgreSQL | `docker-postgres-1:15433` / `vlm_pipeline` | `pipeline-test-postgres-1:15432` / `vlm_pipeline_staging` |
| NAS root (host, `NAS_DATA_ROOT`) | `/home/user/mou/nas_primary` | `/home/user/mou/nas_primary/staging` |
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

`resolve_frame_sampling_policy()` 한 곳을 통과하며 **1fps 고정**입니다(2026-09-11). 예전의 장수 상한 기반 동적 샘플링
(`duration >= 3600s` → 10초 간격, 균등 downsampling)은 주석으로만 남아 있어 `requested_outputs` / `image_profile` /
spec 의 `max_frames_per_video` 는 더 이상 간격에 영향을 주지 않습니다.

- `clip_event`(검수 확정 후 clip 분할): 1초 간격 고정
- `raw_video`(검출용 프레임 추출): `RAW_VIDEO_FRAME_INTERVAL_SEC`(기본 1.0, 최소 1.0 — prod 5) 간격.
  상한은 `ceil(duration / interval)` 로 따라오므로 긴 영상은 간격을 올려 장수를 조절합니다.
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
| `prompt` | 뱅크 문장 텍스트 임베딩 (`entity_id`=content_hash — 뱅크 간 공유 문장은 벡터 1개) | migration 021 (`CONCURRENTLY`) |
| `al_frame` | 능동학습 후보 프레임 (`entity_id`=cohort/frame_key, 운영 파이프라인과 분리된 분석 레인) | migration 029 (pgvector 없는 CI 에선 optional skip) |

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

`COMPOSE_PROFILES=analysis`로 analysis 계열 서비스(JupyterLab · FiftyOne 좌석 1~5 + nginx 좌석 라우터 · Streamlit · analysis-sync · mongo)를 기동하면 임베딩 시각화/검색 도구가 뜹니다. ⚠️ 컨테이너는 **JupyterLab만 자동 기동**합니다 — FiftyOne App과 Streamlit은 `docker exec`로 수동 기동하는 백그라운드 프로세스라 restart/recreate 후 재기동이 필요합니다 ([docs/runbook/fiftyone-operations.md](docs/runbook/fiftyone-operations.md) §3 참고).

| 도구 | 포트 (컨테이너 / prod 호스트) | 설명 |
|------|------|------|
| JupyterLab | `8888` | `fiftyone_pgvector` 헬퍼로 검색·클러스터·UMAP/PCA/MDS 시각화 (token=`JUPYTER_TOKEN`) |
| FiftyOne App | `5151` / `5153`(nginx 좌석 라우터 → 좌석 1~5 — 좌석 1 직결 `5158`, 좌석 2~5 `5154`~`5157`) | `frames` / `captions` 데이터셋 projection 탐색 + SAM3 bbox/캡션 overlay |
| Streamlit | `8501` / `8503` | `embedding_dashboard.py` — 검색(텍스트/이미지ID/이미지 업로드 + facet 필터, 캡션 keyword/semantic/hybrid), near-duplicate·class separability·label suspect·active-learning 큐 |

```bash
COMPOSE_PROFILES=analysis ./scripts/compose-prod.sh up -d analysis
```

대용량 `frames` 데이터셋의 빌드·재기동은 `docker/analysis/`의 전용 스크립트를 씁니다.

| 스크립트 | 설명 |
|----------|------|
| `fiftyone_full_build.py` | 대용량(188K+) `frames` 초기 빌드 — keyset pagination·청크 add(`FFB_CHUNK`)·UMAP sample-fit(`FFB_FIT`)·IncrementalPCA·`MemAvailable` floor 가드로 OOM 방지, 중단 시 `FFB_RESUME=1`(기본)로 재개 |
| `fiftyone_relaunch.py` | 이미 빌드된 데이터셋(`FO_DATASET`)을 빌드 없이 앱만 재기동 + keep-alive |
| `fiftyone_umap_only.py` / `recompute_viz.py` | 기존 데이터셋에 UMAP/PCA brain run만 재실행/재계산 |
| `label_qa_fiftyone.py` | 사람 GT 이미지를 격리 `pseudo_qa` 데이터셋으로 빌드 → FiftyOne `evaluate_detections`로 SAM3 pseudo-label FP/FN 육안 QA |
| `backfill_caption_keyframes.py` | 캡션 샘플의 placeholder 썸네일을 presigned URL + ffmpeg keyframe 추출로 실제 프레임으로 교체 |
| `merge_frames_captions.py` / `enrich_frames_captions.py` | `frames` + `captions` 통합 `frames_captions` 데이터셋(~200K, `modality` 필드) 빌드 및 임베딩/정합도 필드 보강 |
| `reembed_captions_en.py` | 한국어 캡션을 영문 번역 후 재임베딩 — PE-Core 텍스트 타워의 한국어 변별력 한계 대응 (`caption_embedding_ko` 보존, A/B 비교) |

FiftyOne Embeddings 패널의 OSS 제한(대용량 시각화·색상 조합·좌표 export)은 커스텀 플러그인 `docker/analysis/plugins/user-embeddings/`로 우회합니다 (세부는 [docker/analysis/README.md](docker/analysis/README.md)).

> ⚠️ FiftyOne App에서 이미지(presigned URL)가 브라우저에 뜨려면 `ANALYSIS_MINIO_ENDPOINT`를 host-reachable 주소(예: `http://10.0.0.10:9000`)로 설정해야 합니다. 내부 docker 명(`minio:9000`)은 브라우저에서 미도달합니다. 세부는 [docker/analysis/README.md](docker/analysis/README.md) 참고.

## Database Schema

주요 테이블/뷰는 `src/vlm_pipeline/sql/schema_postgres.sql`에 정의되고, 증분 변경은 `src/vlm_pipeline/sql/migrations/postgres/`(`001`~`033`)로 관리됩니다.

### 운영 핵심 테이블/뷰

| 테이블/뷰 | 설명 |
|-----------|------|
| `raw_files` | 원본 미디어 메타, checksum, MinIO raw 위치 |
| `video_metadata` | ffprobe 기반 비디오 메타, Gemini 상태, 재인코딩 추적 + 씬 6축 분류 컬럼 (`camera_angle`/`subject_scale`/`occlusion_state`/`weather` 등, `angle_method`·`env_method` provenance — migration 017) |
| `image_metadata` | 이미지/프레임 메타, `image_caption_text`, `image_caption_score` |
| `labels` | 이벤트 단위 timestamp / caption / classification 라벨 |
| `processed_clips` | clip 기반 전처리 산출물 |
| `image_labels` | SAM3 / YOLO detection 결과 |
| `image_label_annotations` | LS 확정 bbox의 박스 단위 projection (`box_index`, `category`, `bbox_x/y/w/h`, `score`; MinIO COCO JSON이 SoT, `image_labels` FK) |
| `v_finalized_labels` (VIEW) | finalized caption / timestamp / bbox 라벨 통합 조회 (`label_type` union; grain이 달라 테이블 대신 VIEW) |
| `datasets` / `dataset_clips` | 데이터셋 정의 및 dataset ↔ clip 연결 |
| `classification_datasets` | classification 빌드 산출물 추적 |
| `image_embeddings` | PE-Core 임베딩 (pgvector) — `entity_type` = frame/caption/video/detection/**prompt**/**al_frame**(027·029 — 능동학습 후보 프레임, `entity_id`=cohort/frame_key)(021 신설, 뱅크 문장 벡터), entity_type 별 partial HNSW |
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

### 프롬프트 / 온톨로지 DB (migrations 018~023, 2026-08 신설)

라벨링 프롬프트와 제로샷 분류용 프롬프트 뱅크를 DB 로 정본화한 계층입니다.

| 테이블/뷰 | 설명 |
|--------|------|
| `generation_prompts` | 생성 모델에 실제로 보낸 프롬프트 원문의 정본(스키마). `UNIQUE(prompt_type, model_name, content_hash)` 로 dedup. `video_metadata.timestamp_generation_prompt_id` 가 참조 → 어떤 프롬프트로 라벨링됐는지 역추적. write 경로는 2026-08-21 timestamp 스테이지에 배선됨(upsert + `video_metadata` 역참조 UPDATE, 기록 실패는 경고만). ⚠️ 기록 헬퍼는 fail-soft(WARN 후 계속)라 **018 미적용 DB 에서는 행이 조용히 0개로 남는다**. prod 는 2026-09-09 에 적용됐다(`_pg_migrations` UTC 기준 018·020 = 02:35, 022·023 = 04:11). 늦어진 원인은 018 자체가 아니라 두 겹이었다 — ① 2026-07-29 이후 prod 배포가 2026-09-03 까지 연달아 실패해(NAS_primary 마운트가 죽어 `deploy-stack.sh` 의 `df -h` 에서 3회, unit test 에서 2회) 돌던 이미지에 018 파일이 아예 없었고, ② 09-03 배포로 파일이 들어온 뒤에는 **앞 번호의 `@ASSERT_AFTER` 가 드롭된 HNSW 인덱스에 걸려 러너가 그 지점에서 멈췄다**(018·020 은 009 의 `image_embeddings_hnsw_video`, 022·023 은 021 의 `image_embeddings_hnsw_prompt` 에서 — 러너는 이미 적용된 파일의 단언도 매 실행 재검증한다). 계보는 그 이후 run 부터 쌓이고 이전 라벨은 소급 불가. 적용 주체는 '배포 부팅'이 아니라 **첫 asset/센서 실행의 `ensure_runtime_schema()`** 다 — `.ensure_schema()` 를 부팅 경로에서 직접 부르는 코드는 없으므로 배포 직후 `_pg_migrations` 가 그대로여도 정상이다. |
| `prompt_banks` | 제로샷 분류용 프롬프트 뱅크 버전 원장. `UNIQUE(source, version_tag)`. `parent_bank_id` self-FK 로 델타 뱅크 lineage 추적 |
| `bank_sentences` | 뱅크 소속 문장 (bank_id CASCADE). `UNIQUE(bank_id, gidx)` — 같은 뱅크 안에 동일 문장이 반복 등장할 수 있어(뱅크는 집합이 아니라 순서열) content_hash 유니크가 아니다. 행 identity 는 gidx |
| `v_prompt_catalog` / `v_prompt_lineage` | 카탈로그 뷰 = `generation_prompts` ∪ `prompt_banks` UNION ALL 인벤토리. 계보 뷰 = `generation_prompts` ⋈ `video_metadata` ⋈ `labels` 로 human_edited 추적 — labels 조인은 `labels_key` 기준으로 grain 을 맞춘다(asset_id 조인은 classification 라벨을 오귀속시켰음). 두 뷰 모두 문장·임베딩 테이블은 조인하지 않는다 (migration 020) |
| `label_classes` / `label_class_aliases` | **라벨 클래스 정본의 read-side 투영** (migration 022). 코드 경로의 SoT 는 `src/vlm_pipeline/data/label_ontology.json` 이고, 이 두 테이블은 `image_label_annotations.category`·뱅크 `class_label` 을 canonical 클래스에 조인하기 위한 파생 카탈로그 — 라벨 의미를 결정하는 원장이 아니다. `label_class_aliases.canonical` → `label_classes.canonical` FK(ON UPDATE CASCADE). ⚠️ `smoking` 은 JSON 그대로 `smoke` 의 alias 이면서 별도 canonical — 소비자는 canonical 일치를 alias 보다 먼저 적용할 것 |
| `observed_categories` | **정본 밖 카테고리의 판단 유예 원장** (migration 023). 기계가 낸 값을 canonical 에 자동 편입하지 않고 원문 그대로(바깥 공백만 제거) 모아 사람이 승격·매핑·거절을 정한다. PK `(source, raw_value)`, `mapped_to` → `label_classes.canonical` FK(ON UPDATE CASCADE) — **022 없이는 성립하지 않는다**(러너가 파일명 정렬이라 022→023 순서는 보장됨). ⚠️ `observation_count` 는 원문 등장 횟수가 아니라 **관측 batch 수** — `gemini_event` 는 비디오 1건당, `dispatch_request` 는 run 1건당 1 증가한다. ⚠️ `source` CHECK 는 4종을 허용하지만 배선된 writer 는 `gemini_event`·`dispatch_request` **2종뿐** — `sam3_label`/`prompt_bank` 의 0행은 미배선이지 관측 부재가 아니다. ⚠️ 200자 초과·공백만인 값은 truncate 하지 않고 **WARN 후 제외**되므로 0행이 곧 '미상 값 없음'은 아니다 |
| (인덱스) `image_embeddings_hnsw_prompt` | 뱅크 문장 벡터용 partial HNSW — 문장 임베딩은 별도 테이블이 아니라 `image_embeddings(entity_type='prompt', entity_id=content_hash)` 에 흡수 (021, `CONCURRENTLY` 빌드) |

### 025~033 신설 객체 (라벨 컬럼·온톨로지 확장·능동학습·ComfyUI·합성 coverage)

위 표(018~023)에 이어지는 migration 025~033 의 객체입니다 — 026 은 위 `label_classes` 의 시드 확장이고 나머지는 새 계층입니다. 024 는 기존 테이블(`labels`/`video_metadata`) 조회 인덱스 3종뿐이라 표에서 뺐습니다.
prod `_pg_migrations` 에는 025~033 이 전부 기록돼 있습니다(2026-09-29 조회) — 파일 헤더의 "prod 미적용" 서술은 작성 시점 기준입니다.

| 테이블/뷰 | 설명 |
|--------|------|
| `labels.caption_text_en` (컬럼) | Gemini `en_caption` 원문만 담는 컬럼 (migration 025). `caption_text` 는 ko 우선 폴백이라 **한국어임이 보장되지 않고**, `caption_text_en` 은 폴백하지 않는다(언어를 섞지 않기 위해) — 영문 소비자(caption 임베딩 `defs/embed/helpers.py`)는 이 컬럼을 먼저 읽고 비어 있는 구 행만 `gemini_translate` 로 재번역한다(번역기 초기화가 실패하면 경고 로그만 남기고 ko 원문 그대로 임베딩한다 — 결과의 `fallback` 수로 드러난다). 구 행은 백필 불가(영문은 events JSON 에만 있었는데 025 작성 시점에 prod MinIO 버킷이 비어 있었다) — 새 `clip_captioning`/`clip_timestamp` run 부터 채워진다. ⚠️ LS 재동기화(`src/gemini/ls_sync_db.py`)가 labels 를 DELETE+INSERT 하므로 이 컬럼을 같이 옮기지 않으면 검수 한 번에 캡션이 통째로 NULL 이 된다 — 같은 파일이 DELETE 전에 캡션을 떠 두지만 **구간이 1프레임 이내로 그대로인 이벤트에만** 되붙인다. 사람이 경계를 옮긴 이벤트는 캡션이 그 구간을 더 이상 설명하지 않으므로 NULL 로 남는 것이 계약이다 |
| `label_classes` 시드 확장 (026) | `intrusion`/`no_harness` 를 canonical 로 승격 + alias 5개(`unauthorized_intrusion`, `safety_harness`, `normal` identity 등) → canonical 15개. 둘 다 `dispatch_category=false`(침입은 사건, 미착용은 부재라 SAM3 명사구 프롬프트 대상이 아님), `detect_phrases` 빈 배열. ⚠️ `safety_harness` alias 는 고객 폴더명이고 '착용'이 아니라 **'미착용 위반'** 을 뜻한다. ⚠️ **재구성 파일** — prod `_pg_migrations` 에는 적용 기록이 있는데 원본이 어느 브랜치에도 커밋된 적이 없어 prod 실측값으로 복원했다(헤더 `RECONSTRUCTED`: 데이터는 실측, SQL 문구는 추측). ⚠️ 코드 정본 `label_ontology.json` 은 여전히 13 클래스라 **DB 투영이 JSON 정본보다 앞서 있다** — 026 을 재적용해도 JSON drift 는 안 풀리고, 매핑 수정은 JSON 만 만지라는 규칙과 정면충돌 중이다 |
| `al_frames` | 능동학습 후보 프레임 통합 테이블 (027 + 030_al_frames_unit + 031). PK `(cohort, frame_key)`. **운영 파이프라인과 분리된 분석 레인** — `raw_files`/`image_metadata` 에 넣으면 dedup·dispatch·프레임추출이 집어가므로 별도 테이블이고 `asset_id` 는 FK 없는 소프트 참조(코호트가 정본보다 넓다). `label_source` 가 **자기학습 금지 게이트**: 학습·채점 GT 는 `human`(사람 확정)과 `derived`(사람 GT 구간을 프레임에 투영 — 원본은 사람, 투영 규칙만 기계)이고(`docker/analysis/` 의 `al_select`·`al_score_round`·`al_simulation`·`learner_value_probe` 4경로 `LABEL_SOURCES` 기본값이 `human,derived`), `model`(탐지기 알람·Gemini 캡션 파생)은 학습/eval 금지·후보 랭킹 전용이고 `unknown` 도 게이트 기본값에서 빠진다. CHECK 가 없어 027 주석에 없는 값도 들어간다 — prod 의 `unlabeled` 는 미라벨 풀의 관례값이라 **이 게이트를 풀 쿼리에 걸면 풀이 0건이 되어 선별이 죽는다**(`al_select.py` 가 GT 쿼리와 풀 쿼리를 나눠 둔 이유). `group_key` = 홀드아웃 단위이며 **코호트마다 다르다**(sitej certbody 는 연출 동시녹화라 session, sourcei 는 camera) — 이 컬럼을 안 보고 카메라로 나누면 조용히 누수된 수치가 나온다; 031 이 NOT NULL 로 올렸다. `unit`(frame/video/event, 030) 을 안 보고 세면 프레임과 이벤트를 섞어 센다(`t_sec IS NULL` 로 유추 금지). `eval_holdout` 은 `GENERATED ALWAYS ... STORED`(031) — `md5(cohort/group_key)` 하위 20% 를 **그룹 단위**로 봉인하고 `al_select.py` 가 풀에서 `NOT eval_holdout` 으로 빼서 AL 이 영구히 고를 수 없게 한다(프레임 단위 무작위 홀드아웃은 누수). ⚠️ **per-class eval 분모로 쓰지 말 것** — 그룹 크기 편차로 클래스별 봉인율이 20% 에서 크게 벗어난다(031 헤더 실측, 층화는 범위 밖). 임베딩은 `image_embeddings(entity_type='al_frame', entity_id=cohort/frame_key)` |
| `al_rounds` / `al_selections` | AL 라운드 원장 (028) — 어느 라운드에서 어떤 모델로 왜 이 프레임을 골랐는지 CSV 가 아니라 DB 에 남긴다. `al_selections` PK `(round_id, cohort, frame_key)`, `round_id` → `al_rounds` ON DELETE CASCADE; `al_frames` 로는 FK 없음(소프트 조인). `score` 의 의미가 strategy 마다 다르다(margin = top1−top2 확률차, 작을수록 헷갈림; coverage = 라벨셋과의 최대 코사인, 작을수록 안 덮임) — 부호를 섞지 말 것. `labeled_cls` 가 채워지는 곳이 루프가 닫히는 지점이고, NULL 은 "아직 라벨 안 됨"이지 부재 확정이 아니다(사람이 아무것도 아니라고 보면 `normal` 같은 실제 값이 들어간다). `ls_task_id`/`ls_project_id` 는 LS 앱 DB 가 다른 인스턴스라 FK 없는 integer |
| (인덱스) `image_embeddings_hnsw_al_frame` | al_frame 벡터용 partial HNSW (029). 원래 027 에 같이 있었는데 **CI 의 vanilla postgres(pgvector 없음)에서 `CREATE INDEX ON image_embeddings` 가 실패해 러너가 거기서 죽고 이후 마이그레이션이 전부 멈췄다** — 전제조건이 다른 DDL 을 한 파일에 섞지 말 것. `_OPTIONAL_MIGRATIONS` 에 등록돼 pgvector 없는 이미지에서는 적용·기록 없이 skip. 021 과 달리 CONCURRENTLY 아님(배치 적재라 라이브 쓰기 경로가 아니고, 실패 시 INVALID 인덱스를 `IF NOT EXISTS` 가 건너뛰는 함정 회피) |
| `genai_job_provenance` / `generation_gpu_leases` | 로컬 ComfyUI 이미지 엔진 편입 (030_comfy_local). `genai_batches.engine`·`raw_files.genai_engine` 의 CHECK 를 **DROP 후 재생성**해 `comfy_local` 을 허용값에 추가한다 — CHECK 는 ALTER 로 값을 못 늘리므로 003 이 `veo` 를 넣을 때와 같은 패턴이고, 기존 행은 보존된다. `genai_job_provenance` 는 `job_id` PK=FK(genai_jobs ON DELETE CASCADE)로 job 당 1행, workflow/model manifest/prompt/input 의 sha256 을 길이 64 CHECK 로 보관(재현·감사용 무결성 체크섬 — 033 의 md5 내용 주소와 역할이 다르다). `generation_gpu_leases` 는 PK `resource` 에 CHECK `('gpu0_comfy')` 라 **행이 최대 1개인 GPU0 락**, `state` active/released/expired + `WHERE state='active'` partial 인덱스, `owner_job_id` → genai_jobs ON DELETE SET NULL. 읽기 판정은 `lib/gpu_lease.py`, acquire/heartbeat/release 는 `docker/genai/db/pg.py` 전속. ⚠️ `_REQUIRED_MIGRATIONS` 에 들어 있다 — `ensure_runtime_schema()` 가 러너로 먼저 적용을 시도하므로 평소엔 드러나지 않지만, 필수 목록만 남고 이 파일이 이미지에서 빠지면 적용 기록이 없는 DB(staging·CI·새 DB)에서 `ensure_runtime_schema()` 를 부르는 센서·asset 이 전부 `PostgresSchemaBaselineError` 로 멈춘다 — 목록과 파일은 한 커밋으로 움직일 것 |
| `coverage_unit_facts` / `coverage_context_facts` / `camera_registry` / `asset_camera_map` / `generation_reference_pool` + `v_eligible_coverage_units` / `v_generation_reference_candidates` | 합성 coverage 의 **사실·reference 계층** (032). 라벨 정본의 투영이며 정본과 어긋나면 정본이 이긴다 — 투영 job 은 없어 배포 후 전부 0행으로 시작한다. `coverage_unit_facts` PK `(image_id, canonical_class, fact_source)`(같은 이미지·클래스가 bbox/event 두 경로로 들어오는 건 정상, 같은 경로 두 번은 중복), `review_status` CHECK 가 `reviewed`/`finalized` 만 허용해 auto_generated 는 **스키마상 진입 불가**(자기학습 금지를 WHERE 가 아니라 제약으로). image/asset FK 는 전부 ON DELETE CASCADE — CASCADE 가 아니면(RESTRICT 든 기본값 NO ACTION 이든) 인제스트 재적재(`postgres_ingest_raw.py` 의 image_metadata→video_metadata→raw_files DELETE)가 FK 위반으로 깨진다(`@ASSERT_AFTER` 가 불변식으로 감시). `label_id`/`ls_task_id` 는 FK 없음 — LS 재동기화가 labels 를 DELETE+INSERT 라 비영속 키이고 LS 는 다른 DB; 영속 자연키 `source_labels_key`+`source_event_index` 를 같이 적는다. `coverage_context_facts` 는 subject(image 또는 asset) 당 1행을 partial UNIQUE 2개로 강제하고(NULL 은 UNIQUE 에서 서로 다른 값) 축 값에 `deferred`/`unknown`/`indeterminate` 문자열을 CHECK 로 거부 — 관측 못 한 축은 NULL, `verified_axes` 가 어느 축까지 검증됐는지 말한다. `camera_registry`/`asset_camera_map` 은 만들되 채우는 경로가 없고 `source_unit_name` 은 `assignment_source` 허용값에서 뺐다(카메라 키가 아니다). `generation_reference_pool` 기본값은 전부 fail-closed(`draft`, `holdout_excluded=TRUE`, `requires_safe_region=TRUE` → safe region 없이 INSERT 거부). 두 뷰는 context 미검증을 **걸러내지 않고 컬럼으로 노출**한다 — 걸러내면 "비율 0" 과 "관측 불가" 가 같은 0 으로 보인다 |
| `synthetic_prompt_templates` / `synthetic_coverage_policies` / `synthetic_coverage_targets` / `coverage_snapshots` / `coverage_snapshot_cells` / `synthetic_generation_campaigns` / `synthetic_generation_tasks` / `generation_quality_reviews` / `generation_budget_events` + `v_synthetic_coverage_reservations` / `v_generation_budget_daily` | 합성 coverage 의 **정책·snapshot·campaign·task 제어평면** (033). ⚠️ 파일 서두는 "032 의 `generation_reference_pool` 을 FK 로 참조한다" 고 쓰지만 같은 파일 조정 5 에서 그 FK 를 끊었다 — 실제 DDL 에는 032 객체로 가는 FK 가 없고, `synthetic_generation_tasks.reference_id` 의 컬럼 COMMENT("ON DELETE SET NULL …")도 그 이전 문구가 남은 것이다. 승인 없는 생성을 코드가 아니라 스키마가 막는다 — policy `mode`(disabled/plan_only/approval_required/auto_dispatch)를 campaign 이 `policy_mode_at_plan` 으로 동결하고 plan_only/disabled 면 승인·dispatch 상태로 전이 불가(CHECK), `auto_dispatch` 는 `coverage_ready` 필수. `coverage_snapshot_cells.planned_count` 는 deficit·reference·share·budget·cap 다섯 상한을 CHECK 로 못 넘긴다 — planner 버그가 나도 INSERT 가 거부된다. "0" 과 "관측 불가" 를 `class_finalized_total`/`class_context_verified_total`/`class_context_missing_total` 세 카운터로 구분한다 — 뭉개면 관측 못 한 것을 부족분으로 읽고 생성을 지시한다. `dimensions_hash`·템플릿 `content_hash` 는 md5 생성 컬럼이다(`dimensions_hash` 는 jsonb 텍스트 출력이 키 순서를 정규화해 같은 셀이 두 해시를 못 가진다; sha256 을 못 쓴 건 text→bytea 변환 `convert_to` 가 IMMUTABLE 이 아니라서다 — 내용 주소이지 무결성 서명이 아니다). campaign 의 `idempotency_key` 도 생성 컬럼이지만 해시가 아니라 `policy_id`·`schedule_bucket`·`snapshot_input_hash` 를 구분자로 이어 붙인 문자열이라 애플리케이션이 다르게 조립할 수 없다. ⚠️ **FK 를 일부러 안 건 곳**: cells/tasks 의 `target_id`(snapshot 은 불변 감사 기록 — CASCADE 면 target 삭제가 과거 snapshot 을 지운다; cells 는 `dimensions_json` 동결 복사로, tasks 는 `dimensions_hash` 복사로 셀 정체성을 유지), tasks 의 `reference_id`/`genai_batch_id` — 한 DELETE 문이 같은 task 행에 참조 동작 두 개를 걸면(인제스트 재적재의 `DELETE FROM image_metadata` 가 pool 을 CASCADE 로 지우며 `reference_id` 에, 함께 지워지는 다른 이미지가 `output_image_id` 에 각각 SET NULL; `DELETE FROM genai_batches` 도 같은 모양) PG 가 두 cascade 순서를 보장하지 않아 FK 위반으로 실패할 수 있다(**손 재현은 통과할 수 있다** — 이미지를 하나씩 지우면 통과하고 한 문장이 같이 지울 때만 깨진다; 통합 테스트 `test_coverage_control_plane_migration_033.py` 만 신뢰). 삭제를 막는 FK(RESTRICT/NO ACTION) 0개와 task→pool·genai_batches FK 부재를 `@ASSERT_AFTER` 가 감시. share 합=1·dimension 존재 검증은 CHECK 로 못 걸어 `resources/postgres_coverage.py` 의 `activate_policy()` 가 지킨다(코드가 지키는 불변식). 예산 뷰는 KST 일 단위(`AT TIME ZONE` 이 STABLE 이라 생성 컬럼 불가) |

### 테이블 관계도 (ERD)

핵심 테이블만 표시합니다. 실선 = 명시 FK, 점선 = 코드 관례로만 조인되는 암묵 관계
(`image_embeddings` 는 polymorphic 이라 `entity_type`+`entity_id` 로 소프트 조인).

- ⚠️ `_pg_migrations` 는 **파일명만으로 중복을 판정합니다** (`checksum` 컬럼은 있으나 전 행 NULL).
  손으로 선적용한 뒤 파일 내용을 고쳐 커밋하면 러너는 이름이 같다는 이유로 영영 skip 하면서
  `@ASSERT_AFTER` 만 돌립니다. 마이그레이션 상태는 파일 목록이 아니라 `_pg_migrations` +
  `pg_catalog` 로 확인하세요 (실제로 러너 밖에서 손적용된 뒤 파일이 저장소에 없는 사례가 있습니다).
- ⚠️ **030 은 두 파일**(`030_al_frames_unit.sql`·`030_comfy_local.sql`)이 같은 번호를 씁니다. 러너 키는 번호가 아니라
  **파일명 전체**라 정렬상 `al_frames_unit` 이 먼저 돌고 둘 다 `_pg_migrations` 에 기록돼 있습니다. '정리'하려고 재번호하면
  새 이름은 미기록이라 prod 에서 **다시 실행**되고, `030_comfy_local.sql` 은 `_REQUIRED_MIGRATIONS` 에 이름으로 박혀 있어
  fresh DB 의 baseline 검사가 실패합니다 — 번호 중복은 그대로 둡니다.

```mermaid
erDiagram
    raw_files ||--o| video_metadata : "asset_id (1:0..1)"
    raw_files ||--o{ labels : "asset_id"
    raw_files ||--o{ processed_clips : "source_asset_id"
    raw_files ||--o{ image_metadata : "source_asset_id"
    labels ||--o{ processed_clips : "source_label_id"
    processed_clips ||--o{ image_metadata : "source_clip_id"
    processed_clips ||--o{ image_labels : "source_clip_id"
    image_metadata ||--o{ image_labels : "image_id"
    image_labels ||--o{ image_label_annotations : "image_label_id (CASCADE)"
    image_metadata ||--o{ image_label_annotations : "image_id"
    datasets ||--o{ dataset_clips : "dataset_id"
    processed_clips ||--o{ dataset_clips : "clip_id"
    labeling_specs |o..o{ raw_files : "spec_id (암묵)"

    datasets ||--o{ train_dataset_versions : "upstream_dataset_id"
    train_dataset_versions ||--o{ model_registry : "train_dataset_version_id"
    dataset_catalog ||--o{ train_dataset_versions : "dataset_catalog_id"
    dataset_catalog ||--o{ dataset_catalog_aliases : "dataset_catalog_id"

    labeling_specs ||--o{ generation_prompts : "spec_id (예약, 현재 NULL)"
    label_classes ||--o{ label_class_aliases : "canonical (ON UPDATE CASCADE)"
    label_classes |o..o{ image_label_annotations : "category (암묵, 022 투영)"
    label_classes |o..o{ bank_sentences : "class_label (암묵, 022 투영)"
    label_classes ||--o{ observed_categories : "mapped_to (ON UPDATE CASCADE, NULL 허용)"
    generation_prompts ||--o{ video_metadata : "timestamp_generation_prompt_id"
    prompt_banks ||--o{ bank_sentences : "bank_id (CASCADE)"
    prompt_banks ||--o{ prompt_banks : "parent_bank_id (델타 lineage)"

    image_metadata |o..o{ image_embeddings : "entity_type=frame (암묵)"
    labels |o..o{ image_embeddings : "entity_type=caption (암묵)"
    raw_files |o..o{ image_embeddings : "entity_type=video (암묵)"
    bank_sentences }o..o| image_embeddings : "entity_type=prompt, entity_id=content_hash (암묵)"
    embedding_active_model |o..o{ image_embeddings : "model_name 포인터 (암묵)"
    al_rounds ||--o{ al_selections : "round_id (ON DELETE CASCADE)"
    al_frames |o..o{ al_selections : "(cohort, frame_key) (암묵, FK 없음)"
    al_frames |o..o{ image_embeddings : "entity_type=al_frame, entity_id=cohort/frame_key (암묵)"
    raw_files |o..o{ al_frames : "asset_id (암묵, FK 없음 — 코호트가 더 넓다)"
    genai_batches ||--o{ genai_jobs : "batch_id (ON DELETE CASCADE)"
    genai_jobs ||--o| genai_job_provenance : "job_id (PK=FK, ON DELETE CASCADE)"
    genai_jobs |o--o| generation_gpu_leases : "owner_job_id (ON DELETE SET NULL, resource 단일 행)"
    image_metadata ||--o{ coverage_unit_facts : "image_id (ON DELETE CASCADE)"
    raw_files ||--o{ coverage_unit_facts : "asset_id (ON DELETE CASCADE)"
    label_classes ||--o{ coverage_unit_facts : "canonical_class (ON UPDATE CASCADE)"
    genai_jobs |o--o{ coverage_unit_facts : "genai_job_id (ON DELETE SET NULL)"
    image_metadata |o--o| coverage_context_facts : "image_id (ON DELETE CASCADE, subject_type=image 당 1행)"
    raw_files |o--o| coverage_context_facts : "asset_id (ON DELETE CASCADE, subject_type=asset 당 1행)"
    raw_files ||--o| asset_camera_map : "asset_id (PK=FK, ON DELETE CASCADE)"
    camera_registry ||--o{ asset_camera_map : "camera_id (ON UPDATE CASCADE)"
    image_metadata ||--o| generation_reference_pool : "image_id (UNIQUE, ON DELETE CASCADE)"
    raw_files ||--o{ generation_reference_pool : "asset_id (ON DELETE CASCADE)"
    coverage_context_facts |o--o{ generation_reference_pool : "context_fact_id (ON DELETE SET NULL)"
    synthetic_coverage_policies ||--o{ synthetic_coverage_targets : "policy_id (ON DELETE CASCADE)"
    synthetic_prompt_templates |o--o{ synthetic_coverage_targets : "prompt_template_id (ON DELETE SET NULL)"
    synthetic_coverage_policies ||--o{ coverage_snapshots : "policy_id (ON DELETE CASCADE)"
    coverage_snapshots ||--o{ coverage_snapshot_cells : "snapshot_id (ON DELETE CASCADE)"
    synthetic_coverage_targets |o..o{ coverage_snapshot_cells : "target_id (암묵, FK 없음 — snapshot 불변 보호)"
    synthetic_coverage_policies ||--o{ synthetic_generation_campaigns : "policy_id (ON DELETE CASCADE)"
    coverage_snapshots ||--o{ synthetic_generation_campaigns : "snapshot_id (ON DELETE CASCADE)"
    synthetic_generation_campaigns ||--o{ synthetic_generation_tasks : "campaign_id (ON DELETE CASCADE)"
    synthetic_prompt_templates |o--o{ synthetic_generation_tasks : "template_id (ON DELETE SET NULL)"
    genai_jobs |o--o{ synthetic_generation_tasks : "genai_job_id (ON DELETE SET NULL)"
    image_metadata |o--o{ synthetic_generation_tasks : "output_image_id (ON DELETE SET NULL)"
    generation_reference_pool |o..o{ synthetic_generation_tasks : "reference_id (암묵, FK 없음 — cascade 충돌 방지)"
    genai_batches |o..o{ synthetic_generation_tasks : "genai_batch_id (암묵, FK 없음)"
    synthetic_generation_tasks ||--o{ generation_quality_reviews : "task_id (ON DELETE CASCADE)"
    image_metadata |o--o{ generation_quality_reviews : "coverage_image_id (ON DELETE SET NULL)"
    synthetic_coverage_policies ||--o{ generation_budget_events : "policy_id (ON DELETE CASCADE)"
    synthetic_generation_campaigns |o--o{ generation_budget_events : "campaign_id (ON DELETE SET NULL)"
    synthetic_generation_tasks |o--o{ generation_budget_events : "task_id (ON DELETE SET NULL)"

    raw_files {
        TEXT asset_id PK
        TEXT checksum UK
        TEXT media_type "video|image"
        TEXT ingest_status
    }
    video_metadata {
        TEXT asset_id "PK, FK"
        TEXT timestamp_status
        TEXT camera_angle "017 씬축"
        UUID timestamp_generation_prompt_id FK "018"
    }
    labels {
        TEXT label_id PK
        TEXT asset_id FK
        INT event_index "UNIQUE(labels_key, event_index)"
        TEXT review_status
    }
    processed_clips {
        TEXT clip_id PK
        TEXT source_asset_id FK
        TEXT source_label_id FK
    }
    image_metadata {
        TEXT image_id PK
        TEXT source_asset_id FK
        TEXT source_clip_id FK
    }
    image_labels {
        TEXT image_label_id PK
        TEXT image_id FK
        TEXT label_tool
        TEXT review_status
    }
    image_label_annotations {
        TEXT annotation_id PK
        TEXT image_label_id FK "CASCADE, UNIQUE(image_label_id, box_index)"
        INT box_index
        TEXT category
    }
    image_embeddings {
        TEXT entity_type "frame|caption|video|detection|prompt|al_frame"
        TEXT entity_id "UNIQUE(entity_type, entity_id, model_name)"
        TEXT model_name
        VECTOR embedding "1024-d, entity_type 별 partial HNSW"
    }
    embedding_active_model {
        TEXT scope PK
        TEXT model_name "활성 포인터"
    }
    generation_prompts {
        UUID prompt_id PK
        TEXT prompt_type "CHECK 6종"
        TEXT model_name
        TEXT content_hash "UNIQUE(prompt_type, model_name, content_hash)"
        TEXT spec_id FK "예약, 현재 NULL"
    }
    prompt_banks {
        UUID bank_id PK
        TEXT source "UNIQUE(source, version_tag)"
        TEXT version_tag
        UUID parent_bank_id FK "self"
    }
    bank_sentences {
        UUID sentence_id PK
        UUID bank_id FK "CASCADE"
        INT gidx "UNIQUE(bank_id, gidx)"
        TEXT content_hash "암묵→embeddings(prompt)"
        TEXT class_label
    }
    model_registry {
        TEXT model_version_id PK
        TEXT status "candidate|promotable|promoted|archived|rolled_back"
        TEXT train_dataset_version_id FK
    }
    train_dataset_versions {
        TEXT train_dataset_version_id PK
        TEXT upstream_dataset_id FK
        UUID dataset_catalog_id FK "016"
    }
    dataset_catalog {
        UUID dataset_catalog_id PK
        TEXT task
        TEXT git_rev
    }
    dataset_catalog_aliases {
        TEXT task "PK 복합(task, alias)"
        TEXT alias
        UUID dataset_catalog_id FK
    }
    label_classes {
        TEXT canonical PK
        TEXT description "CHECK nonblank"
    }
    label_class_aliases {
        TEXT alias PK
        TEXT canonical FK "ON UPDATE CASCADE"
    }
    observed_categories {
        TEXT source PK "PK 복합(source, raw_value), CHECK 4종 중 2종만 배선"
        TEXT raw_value "바깥 공백만 제거, 1~200자"
        BIGINT observation_count "관측 batch 수 (원문 등장 횟수 아님)"
        TEXT source_units "TEXT[] — distinct sample 최대 32, 이후 count 만 증가"
        TEXT status "observed|candidate|promoted|rejected"
        TEXT mapped_to FK "label_classes.canonical, NULL 허용"
    }
    al_frames {
        TEXT cohort PK "PK 복합(cohort, frame_key)"
        TEXT frame_key "코호트 내 고유, NFC 정규화 필수"
        TEXT label_source "human|model|derived|unknown — 자기학습 금지 게이트, CHECK 없음(prod 에 unlabeled 도 실재)"
        TEXT group_key "NOT NULL(031) — 홀드아웃 단위, 코호트마다 다름(session 또는 camera)"
        TEXT unit "frame|video|event (030_al_frames_unit, CHECK 없음)"
        BOOLEAN eval_holdout "GENERATED STORED(031) — md5(cohort/group_key) 하위 20% 그룹 봉인"
        TEXT asset_id "raw_files 소프트 참조 (FK 없음 — 코호트가 더 넓다)"
    }
    al_rounds {
        TEXT round_id PK "pool__strategy__YYYYMMDDHHMM"
        TEXT pool_cohort "al_frames.cohort (암묵)"
        TEXT strategy "margin|coverage|rare|random"
        TEXT status "selected|sent|labeled|scored"
        INT ls_project_id "LS 는 다른 DB — FK 없음"
    }
    al_selections {
        TEXT round_id PK "PK 복합(round_id, cohort, frame_key), FK ON DELETE CASCADE"
        TEXT cohort "al_frames 소프트 참조 (FK 없음)"
        TEXT frame_key
        INT ls_task_id "LS 다른 DB — FK 없음"
        TEXT labeled_cls "NULL = 미라벨(부재 확정 아님), 루프가 닫히는 지점"
    }
    genai_batches {
        TEXT batch_id PK
        TEXT engine "CHECK 6종 — 030_comfy_local 이 DROP 후 comfy_local 추가"
        TEXT status
    }
    genai_jobs {
        TEXT job_id PK
        TEXT batch_id FK "genai_batches, ON DELETE CASCADE"
        TEXT status "pending|submitted|running|done|failed"
    }
    genai_job_provenance {
        TEXT job_id PK "FK genai_jobs, ON DELETE CASCADE — job 당 1행"
        TEXT workflow_sha256 "CHECK length=64 (재현용 서명)"
        TEXT model_manifest_sha256 "CHECK length=64"
        TEXT prompt_sha256 "CHECK length=64"
        BIGINT seed "CHECK >= 0"
        TEXT output_sha256 "NULL 허용"
    }
    generation_gpu_leases {
        TEXT resource PK "CHECK IN (gpu0_comfy) — 단일 행 GPU0 락"
        TEXT owner_job_id FK "genai_jobs, ON DELETE SET NULL"
        TEXT state "active|released|expired — partial idx WHERE active"
        TIMESTAMP expires_at "heartbeat/TTL"
    }
    coverage_unit_facts {
        TEXT image_id PK "PK 복합(image_id, canonical_class, fact_source), FK image_metadata CASCADE"
        TEXT canonical_class FK "label_classes, ON UPDATE CASCADE"
        TEXT fact_source "ls_bbox|ls_event|ls_image_event|manual_backfill"
        TEXT asset_id FK "raw_files CASCADE (source_asset_id 비정규화)"
        TEXT origin_kind "real|synthetic"
        TEXT review_status "reviewed|finalized — auto_generated 진입 불가"
        TEXT label_id "소프트 참조 (LS 재동기화마다 바뀌는 비영속 키)"
        TEXT genai_job_id FK "genai_jobs, ON DELETE SET NULL"
    }
    coverage_context_facts {
        TEXT context_fact_id PK
        TEXT subject_type "image|asset — subject 당 1행 (partial UNIQUE 2개)"
        TEXT image_id FK "image_metadata CASCADE, subject_type=image 일 때만"
        TEXT asset_id FK "raw_files CASCADE, subject_type=asset 일 때만"
        TEXT verification_status "unverified|inherited|verified|rejected"
        TEXT verified_axes "TEXT[] — 6축 부분집합, verified 면 비어선 안 됨"
        TEXT environment_type "6축 중 하나 — deferred/unknown 문자열은 CHECK 거부, 미관측=NULL"
    }
    camera_registry {
        TEXT camera_id PK
        TEXT holdout_group "al_frames.group_key 와 같은 역할"
        TEXT status "inactive|active|retired — 채우는 경로 없음"
    }
    asset_camera_map {
        TEXT asset_id PK "FK raw_files, ON DELETE CASCADE"
        TEXT camera_id FK "camera_registry, ON UPDATE CASCADE"
        TEXT assignment_source "operator|exif_device|stream_url|filename_rule — source_unit_name 금지"
    }
    generation_reference_pool {
        TEXT reference_id PK
        TEXT image_id UK "FK image_metadata CASCADE, UNIQUE(image_id)"
        TEXT asset_id FK "raw_files CASCADE"
        TEXT context_fact_id FK "coverage_context_facts, ON DELETE SET NULL"
        TEXT status "draft|approved|suspended|retired"
        BOOLEAN holdout_excluded "DEFAULT TRUE = 생성 금지 (fail-closed)"
        BOOLEAN requires_safe_region "TRUE 면 safe_region_json 없이 INSERT 불가"
    }
    synthetic_prompt_templates {
        TEXT template_id PK
        TEXT template_key "UNIQUE(template_key, version)"
        INT version
        TEXT content_hash "GENERATED md5 — 내용 주소, 서명 아님"
        TEXT status "draft|approved|retired"
    }
    synthetic_coverage_policies {
        TEXT policy_id PK
        TEXT policy_key "UNIQUE(policy_key, version)"
        INT version
        TEXT status "draft|active|paused|retired"
        TEXT mode "disabled|plan_only|approval_required|auto_dispatch"
        BOOLEAN coverage_ready "auto_dispatch 는 coverage_ready 필수 (CHECK)"
        TEXT holdout_scope "camera|site|session"
    }
    synthetic_coverage_targets {
        TEXT target_id PK
        TEXT policy_id FK "ON DELETE CASCADE"
        TEXT dimensions_hash "GENERATED md5(dimensions_json) — UNIQUE(policy_id, dimensions_hash)"
        TEXT prompt_template_id FK "ON DELETE SET NULL"
        TEXT status "active|disabled"
    }
    coverage_snapshots {
        TEXT snapshot_id PK
        TEXT policy_id FK "ON DELETE CASCADE"
        TEXT schedule_bucket "UNIQUE(policy_id, schedule_bucket, input_config_hash)"
        TEXT input_config_hash
        TEXT status "computing|complete|failed"
    }
    coverage_snapshot_cells {
        TEXT snapshot_id PK "PK 복합(snapshot_id, target_id), FK ON DELETE CASCADE"
        TEXT target_id "FK 없음 — snapshot 불변 보호, dimensions_json 동결 복사"
        TEXT dimensions_hash "GENERATED md5"
        INT planned_count "CHECK — deficit/reference/share/budget/cap 5상한 이하"
        INT class_finalized_total "0 vs 관측불가 구분 3카운터 중 하나"
        TEXT block_reason "planned_count > 0 이면 NULL"
    }
    synthetic_generation_campaigns {
        TEXT campaign_id PK
        TEXT policy_id FK "ON DELETE CASCADE"
        TEXT snapshot_id FK "ON DELETE CASCADE"
        TEXT idempotency_key UK "GENERATED policy|bucket|input_hash"
        TEXT status "planned|approved|dispatching|awaiting_review|closed|blocked|cancelled"
        TEXT policy_mode_at_plan "plan_only/disabled 면 승인·dispatch 전이 불가 (mode gate)"
    }
    synthetic_generation_tasks {
        TEXT task_id PK
        TEXT campaign_id FK "ON DELETE CASCADE, UNIQUE(campaign_id, reference_id, workflow_id, seed)"
        TEXT reference_id "generation_reference_pool 소프트 참조 (FK 없음 — cascade 충돌)"
        TEXT genai_batch_id "소프트 참조 (FK 없음 — 같은 이유)"
        TEXT genai_job_id FK "genai_jobs, ON DELETE SET NULL"
        TEXT output_image_id FK "image_metadata, ON DELETE SET NULL"
        TEXT template_id FK "synthetic_prompt_templates, ON DELETE SET NULL"
        TEXT state "planned|ready|deferred|dispatched|awaiting_review|accepted|rejected|failed|cancelled"
        TEXT deferred_reason "deferred 면 필수, 7종 CHECK"
    }
    generation_quality_reviews {
        TEXT review_id PK
        TEXT task_id FK "ON DELETE CASCADE"
        TEXT decision "accepted|rejected|rework — rejected 는 reason_codes 필수"
        TEXT coverage_image_id FK "image_metadata, ON DELETE SET NULL"
    }
    generation_budget_events {
        BIGSERIAL event_id PK
        TEXT policy_id FK "ON DELETE CASCADE"
        TEXT campaign_id FK "ON DELETE SET NULL"
        TEXT task_id FK "ON DELETE SET NULL"
        TEXT event_type "reserve|consume|release — 부호는 type, 금액은 >= 0"
    }
```

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
│       │   ├── viz/                    # FiftyOne 동기화 센서·job·03:00 스케줄
│       │   └── shared/                 # 공용 helper
│       ├── data/                       # label_ontology.json — 라벨 클래스 정본 (DB 022/026 은 투영)
│       ├── lib/                        # prompts, frame planning, sam3/yolo/embedding client, key_builders, env helpers
│       ├── resources/                  # postgres_* (base/migration/ingest/labeling/process/detection/embedding/genai/train/maintenance) + minio + config + runtime_settings
│       └── sql/                        # schema_postgres.sql, migrations/postgres/ (001-033)
├── docker/
│   ├── docker-compose.yaml
│   ├── docker-compose.dev.yaml         # 로컬 dev overlay
│   ├── docker-compose.labelstudio.yaml # Label Studio overlay
│   ├── docker-compose.labelstudio.local.yaml # LS 커스텀 포크 로컬 스택 (소스빌드)
│   ├── docker-compose.angle.yaml       # 카메라 앵글 추정 서비스 overlay (별도 compose project)
│   ├── labelstudio/# LS 커스텀 포크 overlay (assign-flow 등 커스터마이즈)
│   ├── angle/      # Depth Anything V2 카메라 앵글 추정 서버 (FastAPI, :8005)
│   ├── .env / .env.test
│   ├── app/        # Dagster code-server 이미지
│   ├── sam3/       # SAM3.1 segmentation 서버
│   ├── yolo/       # YOLO-World 추론 서버
│   ├── embedding/  # PE-Core 임베딩 서비스
│   ├── analysis/   # JupyterLab + FiftyOne
│   ├── genai/      # GenAI Studio (Kling/Veo/Higgsfield)
│   ├── comfyui/                    # ComfyUI 로컬 생성 엔진 (GPU 0, 승인 워크플로 템플릿)
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
| `embedding-service` | `8003` (prod 호스트 `8004`) | `embedding` | PE-Core-L14-336 임베딩 서비스 (GPU 0) + 선택 PLM 캡션 슬롯 `/caption` (GPU 1 — SAM3 에 단방향 양보: free VRAM < `PLM_MIN_FREE_GB` 면 503, idle 120s 반납) |
| `genai` | `8088` (prod 호스트 `8089`) | `genai` | GenAI Studio (Kling/Veo/Higgsfield 등) |
| `comfyui` | `8188`(내부 expose — GenAI 만 호출) | `comfyui` | ComfyUI 로컬 생성 엔진 (GPU 0, 승인 워크플로 2개만; unhealthy 면 배포 전체 실패) |
| `pg-backup` | - | `backup` | pg_dump + restic 일일 백업 sidecar |
| `analysis` | `8888` | `analysis` | JupyterLab (분석 배치 실행 자리) |
| `analysis-fiftyone` (좌석 1) | `5151` (prod 호스트 `5158`, 직결·우회로) | `analysis` | FiftyOne App 프로세스 1개 = 좌석 1개 (서버 상태가 프로세스 전역이라 사용자마다 프로세스를 나눔) |
| `analysis-fiftyone-2`~`-5` (좌석 2~5) | `5151` (prod 호스트 `5154`~`5157`) | `analysis` | 좌석 2~5, `mem_limit ${FIFTYONE_SEAT_MEM}` — 배포의 `up -d` 목록 밖(손으로 올림) |
| `analysis-fiftyone-proxy` | `5151`/`5443` (prod 호스트 `5153`/`5443`) | `analysis` | nginx 좌석 라우터 — **사용자 입구**. `?seat=N` > IP 지정석 > 쿠키 > 자동 배정 순 |
| `analysis-streamlit` | `8501` (prod 호스트 `8503`) | `analysis` | 임베딩 대시보드 |
| `analysis-sync` | `8010`(내부) | `analysis` | FiftyOne 증분 동기화 + 프로젝트 번들 업로드(`/upload/*`, `/__upload/ui`) + 좌석 배정(`/seat/*`) API |
| `fiftyone-mongo` | - | `analysis` | FiftyOne 메타데이터 MongoDB — `mongo:8.0` **마이너 고정**(메이저 태그가 8.2 로 흘러 FCV 거부 → 크래시루프 이력), `mem_limit`, wiredTiger 캐시 4GB |
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
- PostgreSQL 전환으로 단일-파일 write lock 제약이 사라지면서 `duckdb_writer` 계열 태그 게이트는 **폐기**됐습니다 (`build_asset_job(writer_tag=...)` 인자는 하위 호환 시그니처만 남은 no-op). 현재 run-coordinator(`QueuedRunCoordinator`)는 `max_concurrent_runs: 20`에 `gpu_trainer` limit 1(실사용)·`pg_writer` limit 1(설정만 있고 태그를 붙인 asset 없음 — 현재 no-op)입니다.

## GenAI Studio (영상·이미지 생성 웹 UI)

`docker/genai/`는 외부 생성 모델(Kling·Veo·Higgsfield·Nanobanana·GPT-Image)을 batch로 호출하는 **FastAPI 웹 콘솔**입니다. `genai` compose profile로 기동하며, 생성 결과를 라벨링 파이프라인 입력으로 promote할 수 있습니다.

### 접속

- URL: `http://<HOST>:8088` (`GENAI_PORT`, profile `genai`)
- 인증: HTTP Basic (`GENAI_BASIC_AUTH_USER` / `GENAI_BASIC_AUTH_PASS`). `GENAI_AUTH_DISABLED=1`로 비활성(내부망 전용).
- 활성 엔진: `GENAI_ENGINES_ENABLED` (compose 기본 `kling,higgsfield,nanobanana,gpt_image`; prod 는 `kling,veo,comfy_local`).

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

### ComfyUI 로컬 생성 (`comfy_local`)

- `docker/comfyui/`, 컨테이너 `docker-comfyui-1`, profile `comfyui`, **GPU 0**(embedding-service PE-Core·angle 분류기와 공유).
  GenAI 만 내부 `:8188` 로 호출하며 호스트 포트는 없습니다.
- 승인된 워크플로 2개(`flux2-klein-4b-edit-v1`, `sdxl-inpaint-cctv-v1`)만 실행 — 그래프는 항상 repo 템플릿이고 사용자 입력은
  승인된 scalar 만 재바인딩합니다(임의 그래프 실행 금지). 결과물은 기존 GenAI 격리 NAS → promote → dispatch 경로를 그대로 탑니다.
- VRAM 입장 임계 `COMFYUI_MIN_FREE_VRAM_GB`(기본 14.5) 미만이면 생성을 시작하지 않습니다. 배포는 comfyui 가 healthy 가 될 때까지
  최대 10분(5s × 120) 기다린 뒤 실패하면 **배포 전체를 중단**합니다 — GPU0 이 다른 작업에 잡혀 있으면 배포가 여기서 죽습니다.
- 켜기/끄기: `GENAI_ENGINES_ENABLED` 에 `comfy_local` + `COMPOSE_PROFILES` 에 `comfyui`. 끌 때는 엔진 목록에서 먼저 빼고 profile 을 뺍니다.

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
| `NAS_DATA_ROOT` | incoming/archive 단일 bind mount 호스트 경로 (prod `/home/user/mou/nas_primary`) |
| `INCOMING_DIR` / `ARCHIVE_DIR` | 컨테이너 내부 경로 (`/nas/data/incoming`, `/nas/data/archive`) |
| `COMPOSE_PROFILES` | 활성 compose profile (prod `sam3,backup,genai,embedding,analysis,comfyui`). `comfyui` 를 뺄 때는 `GENAI_ENGINES_ENABLED` 의 `comfy_local` 을 먼저 뺄 것 |
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
| `video_env_backfill_job` | Places365 환경 분류(indoor/outdoor·주야) `deferred` 백필 |
| `video_scene_backfill_job` | 씬 6축 분류 백필 — Gemini 5축(subject_scale/occlusion/environment/daynight/weather) + Depth Anything V2 `camera_angle` (`angle_method='deferred'` 큐 드레인) |

### Jobs (feature flag로 등록)

| Job | flag |
|-----|------|
| `manual_label_import_job` | `ENABLE_MANUAL_LABEL_IMPORT` |
| `yolo_standard_detection_job` | `ENABLE_YOLO_DETECTION` |
| `sam3_standard_detection_job` | `ENABLE_SAM3_DETECTION` |
| `frame_embedding_job` / `caption_embedding_job` / `video_embedding_job` | `ENABLE_EMBEDDING` |
| `fiftyone_sync_job` | `FIFTYONE_SYNC_API_URL` (analysis-sync HTTP 증분 동기화) |
| `synthetic_coverage_planner_job` | coverage 스냅샷 → 합성 생성 campaign/task 계획 (policy 승인 없는 생성을 코드가 막는 제어평면, migration 032/033) |

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
| `fiftyone_sync_sensor` | RUNNING | 5분 tick, PG 카운트 스냅샷 diff → `fiftyone_sync_job` |
| `synthetic_campaign_dispatch_sensor` | STOPPED | 5분 tick, 승인된 합성 task 를 tick 당 1건 GenAI internal endpoint 로 제출 (GPU lease·예산 검사) |
| `frame_embedding_backlog_sensor` / `caption_embedding_backlog_sensor` | STOPPED (`ENABLE_EMBEDDING`) | 미임베딩 backlog → embedding job |
| `gcs_download_schedule` | - | 매일 04:00 KST GCS 수집 |
| `sourcea_download_schedule` | RUNNING | 매일 06:00 KST source-a 사이트 일일 수집 |
| `ls_presign_renew_schedule` | STOPPED | 매일 05:00 KST LS presigned URL 갱신 (renew 중복재생성 버그픽스 배포 전까지 OFF 유지) |
| `video_env_backfill_schedule` | STOPPED | 평일 19:00 KST 환경 분류 백필. 백로그 소진 후 다시 OFF 권장 |
| `fiftyone_label_refresh_schedule` | RUNNING | 매일 03:00 KST FiftyOne 라벨 재적재(캐치업) |
| `synthetic_coverage_planner_schedule` | STOPPED | 매일 05:00 KST coverage planner (`synthetic_coverage_planner_job`) |
| `video_scene_backfill_schedule` | STOPPED | 평일 20:00 KST 씬 6축 백필 (env와 GPU 0 경합 회피용 1h 스태거). `camera_angle` 육안 GT 검증 게이트 통과 전까지 OFF 유지 |

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
