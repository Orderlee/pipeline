# 설계: Dagster 임베딩 도메인 + pgvector 벡터 검색

- **작성일**: 2026-06-15
- **상태**: 설계 확정 (구현 plan 작성 직전)
- **관련**: [data-platform-foundation-2026-05-27.md](../exec-plans/active/data-platform-foundation-2026-05-27.md) §C (이 설계가 "다음 스프린트로 이연"된 Dagster asset 화 부분). pgvector 설치 가능성은 2026-06-15 staging 에서 실증 완료 (`pipeline-test-postgres-1` 에 `datapipeline-pg-pgvector:15-v1.1.1` 이미지로 vector 0.8.2 + pg_duckdb 1.1.0 공존, HNSW 동작 확인)

---

## 1. 목적

CCTV/보안 영상 파이프라인의 **프레임**과 **검출 객체(bbox)** 에 대해 임베딩을 추출하여 Postgres(pgvector)에 저장하고, **유사도/중복 탐색·데이터셋 curation·클러스터·시각화·객체 유사 검색·텍스트→이미지 검색**을 가능하게 한다. Dagster 에 **라이브 sensor 자동 + 수동 트리거** 로직으로 통합한다.

## 2. 확정된 결정 (locked)

| # | 결정 | 값 |
|---|------|-----|
| 임베딩 소스 | 2종 | **프레임 단위** + **검출 bbox crop 단위** |
| 트리거 | 라이브 + 수동 | backlog **sensor 자동** + 동일 asset **수동 materialize**, 멱등 |
| 모델 | 전용·교체형 | **`facebook/PE-Core-L14-336`** (Perception Encoder Core L, CLIP 계열) |
| 차원 | 고정 | **1024** (`vector(1024)`), image size 336, Apache-2.0 |
| 구현 | 기본 + ready | **open_clip (`timm/PE-Core-L-14-336`)** 기본, **perception_models 전환 가능**하도록 백엔드 추상화 |
| 텍스트검색 | 단일 모델로 커버 | PE-Core 텍스트 타워로 query 임베딩 → 동일 1024-d 공간 검색 |
| 프레임 범위 | 전부 | 모든 `image_role`. 샘플링 knob 은 config 로 두되 **기본 off** |
| 벡터 인덱스 | HNSW cosine | 모델 1개 → 단일 인덱스. 추후 모델 추가 시 partial-by-model |
| prod 적용 | staging-first | **staging 검증 후** prod 진행 (prod pgvector 이미지 정식화는 P0 후반/이연) |

## 3. 스케일 (2026-06-15 실측)

- **PROD**: 프레임 105,076 (raw_video 95,291 + processed_clip 9,785) / 검출 crop ~46,635 → **임베딩 ~152K개**. 1024-d float4 ≈ 4KB/vec → 벡터 **~0.6GB + HNSW** → pgvector 로 충분히 가벼움. backfill 은 bounded 1회 job.
- **STAGING**: 프레임 2,416 / 검출 0. → 프레임 경로 즉시 검증 가능. **검출 경로는 staging 에 detection 0 → SAM3 선행 필요** (validation prerequisite).

## 4. 아키텍처

```
[image_metadata]──frame backlog sensor──▶ frame_embedding_asset ─┐
   (vlm-processed)                                               │ minio.download → POST /embed
[image_labels]──detection backlog sensor─▶ detection_embedding_asset┤ (PE-Core-L14-336, GPU 0)
   (COCO json/vlm-labels → bbox crop)                            │
                                                                 ▼
                                            image_embeddings (pgvector 1024-d) ◀── batch upsert
                                                                 ▲
                            text/image 검색 helper ── POST /embed_text ──┘ (query-time, 미저장)
```

- 신규 **`embedding-service` 컨테이너** (FastAPI, [lib/sam3.py](../../src/vlm_pipeline/lib/sam3.py)/SAM3 컨테이너 패턴 미러). **GPU 0** 배치 (GPU 1 은 SAM3 가 ~15GB/16GB 로 포화).
- 벡터 검색은 **native PG**(pgvector HNSW) — pg_duckdb 와 무관 (force_execution 이어도 vector 쿼리는 native PG fallback, 검증 완료).

## 5. 신규 컴포넌트 (5-layer import 계층 준수)

| 레이어 | 신규 파일 | 역할 | 미러 대상 |
|---|---|---|---|
| L1 lib | `src/vlm_pipeline/lib/embedding.py` | `EmbeddingClient` (env `EMBEDDING_API_URL`, lazy `requests.Session`, `wait_until_ready`, `embed(bytes)->list[float]`, `embed_text(str)->list[float]`, 모듈 싱글턴 `get_embedding_client()`) | [lib/sam3.py:18-105](../../src/vlm_pipeline/lib/sam3.py#L18) |
| L1 lib | (기존 `lib/key_builders.py` 재사용) | 프레임 키 빌드 | `build_raw_video_image_key` [:107](../../src/vlm_pipeline/lib/key_builders.py#L107) |
| L3 resource | `src/vlm_pipeline/resources/postgres_embedding.py` | `PostgresEmbeddingMixin`: `find_pending_frame_embeddings(model_name, limit, ...)`, `find_pending_detection_embeddings(...)`, `batch_insert_embeddings(rows)` (executemany + ON CONFLICT) | [postgres_detection.py](../../src/vlm_pipeline/resources/postgres_detection.py) |
| L3 wiring | `src/vlm_pipeline/resources/postgres.py` | 상속에 `PostgresEmbeddingMixin` 추가 | [postgres.py:30](../../src/vlm_pipeline/resources/postgres.py#L30) |
| L4 asset | `src/vlm_pipeline/defs/embed/assets.py` | `frame_embedding_asset`, `detection_embedding_asset` (`group_name="embed"`, `config_schema={limit, model_name, sampling}`) | [sam/detection_assets.py:98](../../src/vlm_pipeline/defs/sam/detection_assets.py#L98) |
| L4 sensor | `src/vlm_pipeline/defs/embed/sensor.py` | frame/detection backlog sensor (NOT EXISTS 커서) | [shared/detection_sensor_factory.py:24](../../src/vlm_pipeline/defs/shared/detection_sensor_factory.py#L24) |
| L4 helper | `src/vlm_pipeline/defs/embed/helpers.py` | bbox crop (COCO ann → PIL crop), batch 임베딩 루프, row 빌드 | — |
| L5 defs | `definitions_production.py` 등 | asset/sensor/resource 등록 + `EMBEDDING_API_URL` env | — |
| migration | `src/vlm_pipeline/sql/migrations/postgres/006_image_embeddings.sql` | `CREATE EXTENSION vector` + 테이블 + HNSW + `@ASSERT_AFTER` | runner [:144](../../src/vlm_pipeline/resources/postgres_migration.py#L144) |
| infra | `docker/embedding/` (Dockerfile, app.py, backends/) | PE-Core 서빙 (open_clip 기본, perception_models stub) | `docker/sam3/` |
| infra | `docker/postgres-pgvector/Dockerfile` | pgvector PG 이미지 (staging 검증 완료) | — |
| compose | `docker/docker-compose.yaml` | `embedding` 서비스 추가 (GPU 0), `EMBEDDING_API_URL` | sam3 service block |

### 임베딩 서비스 내부 (백엔드 교체 구조)
```
docker/embedding/
  app.py                     # FastAPI; EMBEDDING_BACKEND=open_clip|perception_models 로 선택
  backends/base.py           # ABC: load(); embed_image(bytes)->vec; embed_text(str)->vec; name; dim
  backends/open_clip_be.py   # 지금: open_clip.create_model_and_transforms('hf-hub:timm/PE-Core-L-14-336')
  backends/perception_be.py  # 나중: pe.CLIP.from_config("PE-Core-L14-336", pretrained=True) — stub
```
- HTTP 계약(`GET /health`{model_loaded,model_name,dim}, `POST /embed`(multipart image)→{vector}, `POST /embed_text`{text}→{vector})은 **백엔드 무관 동일** → Dagster 쪽 전환 시 무변경.
- perception_models 전환 = 이미지에 의존성 추가(별도 Dockerfile target) + `EMBEDDING_BACKEND` env 변경 뿐.

## 6. 스키마 (`006_image_embeddings.sql`)

```sql
BEGIN;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS image_embeddings (
  embedding_id  TEXT PRIMARY KEY,            -- deterministic: hash(entity_type|entity_id|model_name)
  entity_type   TEXT NOT NULL,               -- 'frame' | 'detection'
  entity_id     TEXT NOT NULL,               -- frame: image_id / detection: image_id#<ann_id>
  image_id      TEXT NOT NULL,               -- 소스 프레임 (image_metadata.image_id)
  model_name    TEXT NOT NULL,               -- 'facebook/PE-Core-L14-336'
  dim           INTEGER NOT NULL,            -- 1024
  embedding     vector(1024) NOT NULL,
  source_bucket TEXT,                        -- 임베딩한 이미지/크롭 출처 bucket
  source_key    TEXT,                        -- 〃 key
  bbox          JSONB,                        -- detection 만: {x,y,w,h,category,score}
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_id, model_name)
);

CREATE INDEX IF NOT EXISTS image_embeddings_entity_idx
  ON image_embeddings (entity_type, model_name);
CREATE INDEX IF NOT EXISTS image_embeddings_image_idx
  ON image_embeddings (image_id);
-- HNSW 는 초기 backfill 이후 생성 권장(대량 적재 후가 빌드 효율 좋음)
CREATE INDEX IF NOT EXISTS image_embeddings_hnsw
  ON image_embeddings USING hnsw (embedding vector_cosine_ops);

COMMIT;
-- @ASSERT_AFTER: SELECT 1 FROM pg_extension WHERE extname='vector'
-- @ASSERT_AFTER: SELECT 1 FROM information_schema.tables WHERE table_name='image_embeddings'
```
- 단일 DO/statement 원칙은 아니나 각 statement 독립 — runner 의 multi-statement 한계(005 사례) 회피 위해 `CREATE EXTENSION`·`CREATE TABLE`·`CREATE INDEX` 를 일반 statement 로 둠(DO 블록 미사용).
- `006` 은 runner 가 정렬 glob 으로 자동 적용. 부팅 필수(`_REQUIRED_MIGRATIONS`) 에는 **넣지 않음** (선택 기능 — pgvector 미적용 환경에서 부팅 실패 방지).

## 7. 데이터 흐름 & 멱등성

**프레임**: sensor 가 `image_metadata` 중 해당 `model_name` 의 `image_embeddings` 없는 row 백로그 감지 → `RunRequest(limit)` → asset 이 `find_pending_frame_embeddings(model_name, limit, sampling)` → 각 프레임 `minio.download(image_bucket, image_key)` → `EmbeddingClient.embed()` → row → `batch_insert_embeddings` (executemany, ON CONFLICT).

**검출**: sensor 가 `image_labels`(label_tool='sam3') 중 검출 임베딩 없는 것 감지 → asset 이 `download_json('vlm-labels', labels_key)` → COCO `annotations[]` 별 bbox 를 소스 이미지(`image_metadata.image_key`, vlm-processed)에서 crop → `embed()` → `entity_id=image_id#<ann_id>`, `bbox` JSONB 저장.

**수동**: 동일 asset 을 Dagster UI/job 으로 materialize. 백로그(NOT EXISTS) + ON CONFLICT 로 **재실행 안전**. sensor 기본 OFF (수동 ON — 기존 관례).

**검색** (query-time, 비-asset, 노트북/FiftyOne helper):
- 이미지 유사: `SELECT entity_id, image_id FROM image_embeddings WHERE entity_type='frame' ORDER BY embedding <=> $1 LIMIT k;`
- 텍스트→이미지: `embed_text(q)` → 동일 쿼리.

## 8. 에러 처리 (프로젝트 정책 준수)

- **per-file fail-forward**: 프레임/크롭 1건 실패 → 로그 후 계속, 해당 row 만 미삽입 (배치 전체 실패 아님).
- **임베딩 서비스**: `wait_until_ready` (asset 루프 진입 전) + `_retry_transient` (ConnectionError/Timeout/5xx 지수 백오프) 미러.
- **MinIO/NAS transient**: graceful skip, 다음 tick 재시도. **PG transient**: `connect()` 컨텍스트 자동 재시도.
- **빈/손상 이미지**: skip + 실패 로그. **GPU OOM**: 서비스측 batch size 제한 + GPU 0 배치.
- **pgvector 의존성**: 006 은 vector 확장 없는 이미지에서 fail-fast → **prod 는 파생 PG 이미지 배포가 006 보다 선행** (P0/이연).

## 9. GPU / 인프라

- 임베딩 서비스 **GPU 0** (Places365 + dagster torch 와 공유, RTX A4000 16GB). PE-Core-L fp16 ~2GB VRAM → 여유.
- compose `embedding` 서비스에 `CUDA_VISIBLE_DEVICES=0`. 모델 캐시 볼륨(`/data/models/pe-core`) 고정.
- prod PG 파생 이미지(`datapipeline-pg-pgvector`) 는 CI 가 빌드 안 함 → 레지스트리 push 또는 호스트 prebuild + `deploy-stack` build/pull step (P0 prod 단계에서).

## 10. 테스트

- **unit**: `postgres_embedding` (NOT EXISTS 백로그 쿼리, ON CONFLICT 멱등) — in-memory/mock PG fixture; `EmbeddingClient` (HTTP mock); bbox crop helper (COCO ann → crop 좌표); key 빌더; sensor 커서 로직.
- **integration**: pgvector 왕복 (insert vector → cosine 쿼리). CI PG 에 pgvector 필요 → **pgvector 테스트 이미지** 사용 또는 `skip if 'vector' not in pg_available_extensions`.
- **service contract**: `/health`, `/embed`, `/embed_text` (모델 mock 백엔드로 빠른 검증).
- **staging E2E**: 프레임 경로 2,416개로 backfill→검색 확인. 검출 경로는 SAM3 선행 후.

## 11. 단계 (phasing)

- **P0 — 인프라 (staging)**: 006 migration (staging, pgvector 이미 활성) + `embedding-service` 스캐폴드(open_clip 백엔드, /health·/embed·/embed_text) + `lib/embedding.py` + `postgres_embedding.py` + compose `embedding` 서비스.
- **P1 — 프레임 (staging 검증)**: `frame_embedding_asset` + frame backlog sensor + 수동 job. staging 2,416 프레임 backfill → 유사도/텍스트 검색 확인.
- **P2 — 검출 (staging 검증)**: SAM3 staging 선행 → `detection_embedding_asset` + detection sensor (bbox crop).
- **P3 — 검색 helper / FiftyOne**: text/image 검색 helper (+ 선택: FiftyOne similarity backend, UMAP).
- **P4 — prod 적용 (staging 검증 후)**: 파생 PG 이미지 prod 정식화(Dockerfile commit + deploy build/pull) → prod 006 → prod backfill (~152K).

## 12. Open items / 리스크

- **검출 entity_id 안정성**: COCO `annotation.id` 가 재라벨링 시 안정적인지 확인 필요. 불안정하면 `image_id#<category>#<bbox_index>` 등 결정적 키로 대체.
- **CI pgvector**: integration 테스트용 PG 이미지에 pgvector 필요 — 없으면 skip 가드.
- **샘플링 정책**: 기본 off(전부)지만 prod 152K 이상 증가 시 knob 활성 기준 문서화.
- **재임베딩**: 모델/백엔드 교체 시 `model_name` 다른 row 로 누적 → 구 row 정리 정책(보존 vs 삭제) 추후 결정.
- **HNSW 빌드 타이밍**: 대량 backfill 후 인덱스 생성 권장 (migration 에 인덱스 포함 시 빈 테이블에 생성됨 — 동작엔 무방).

## 13. 참고

- 모델: https://huggingface.co/facebook/PE-Core-L14-336 (dim 1024, Apache-2.0), open_clip 로딩: `open_clip.create_model_and_transforms('hf-hub:timm/PE-Core-L-14-336')`
- pgvector: HNSW `vector_cosine_ops`, 거리 연산자 `<=>`(cosine)/`<->`(L2)
