# PROD 임베딩 + FiftyOne(:5153) 롤아웃 런북 (유지보수 창)

> 라벨링이 한가한 창에서 실행. prod `docker-postgres-1` **재시작(이미지 swap)** 이 포함되므로
> in-flight 라벨링 run 이 잠깐 끊길 수 있음(Dagster transient 재시도로 대부분 복구).
> branch: `feature/embed-prod-captions` (PR → dev → main).

## 이미 완료 (무중단으로 적용됨)
- prod PG 에 **pgvector in-place 설치 + `image_embeddings` 테이블(006) + caption 컬럼(007)** 적용됨
  (`vector 0.8.2`, `pg_duckdb 1.1.0` 유지). 단, in-place 라 **prod PG 컨테이너 재생성 시 소실** → 아래 image swap 으로 영구화.

## 0. 사전 확인 (창 진입 전)
- 라벨링 backlog/active run 적은 시간대 선택.
- 확인할 prod 값: **prod MinIO S3 endpoint(브라우저 도달)**, MinIO 자격(`MINIO_ACCESS_KEY`/`SECRET` — 기본 minioadmin), prod 호스트에서 **빈 포트**(아래 기본값 확인).

## 1. 배포 (코드 → prod)
1. PR `feature/embed-prod-captions` → **dev** 머지 → staging 자동배포로 로드 확인(frame 경로 정상).
2. **dev → main** PR 머지 → prod 자동배포. 효과: dagster 이미지 재빌드(embed 코드 포함) + dagster 재기동(창의 다운타임) + `POSTGRES_IMAGE` 변경 시 prod PG 재생성.

## 2. prod `.env` (호스트, `/home/user/work_p/Datapipeline-Data-data_pipeline/docker/.env`)
```ini
# 임베딩/분석 서비스 생성 + 자산/센서 등록
COMPOSE_PROFILES=<기존>,embedding,analysis
ENABLE_EMBEDDING=true
# pgvector 영구화 (이 변경이 다음 up 에서 prod PG 재생성 → 창에서 수행)
POSTGRES_IMAGE=datapipeline-pg-pgvector:15-v1.1.1
POSTGRES_PRELOAD_LIBS=pg_duckdb
# 포트 (staging :8889/:5152/:8013 과 충돌 회피 — 호스트 공유)
EMBEDDING_PORT=8014
JUPYTER_PORT=8890
FIFTYONE_PORT=5153
# FiftyOne presign 이미지가 브라우저에서 열리도록 host-reachable prod MinIO S3
ANALYSIS_MINIO_ENDPOINT=http://<PROD_MINIO_HOST>:<PORT>
# stuck-run guard 에 임베딩 job 추가
STUCK_RUN_GUARD_TARGET_JOBS=<기존>,frame_embedding_job,caption_embedding_job
```

## 3. 컨테이너 빌드 + 기동 (prod = compose project `docker`)
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose --env-file .env build embedding-service analysis
docker compose --env-file .env up -d postgres        # POSTGRES_IMAGE swap → pgvector 영구 (재생성, 데이터 볼륨 보존)
docker compose --env-file .env up -d embedding-service analysis fiftyone-mongo
docker compose --env-file .env up -d dagster dagster-daemon dagster-code-server  # ENABLE_EMBEDDING 반영
```
- embedding-service `/health` 가 `model_loaded:true` 될 때까지 대기(최초 PE-Core ~1.5GB 다운로드).
- 006/007 은 pgvector-gated → dagster 부팅 시 `ensure_runtime_schema` 가 자동 적용·기록(이미 테이블 존재 → 멱등).

## 4. backfill (throttled — GPU0 경합 최소화)
prod 데이터: frame ~121K(raw_video_frame+processed_clip_frame), caption ~12.5K.
```bash
# 작은 batch 로 반복 (또는 sensor 수동 ON 해 자연 throttle). GPU0(Places365)와 경쟁하므로 한가할 때.
docker exec docker-dagster-1 sh -lc 'printf "ops:\n  frame_embedding:\n    config:\n      limit: 2000\n" > /tmp/c.yaml'
docker exec docker-dagster-1 dagster job execute -f /app/dagster_defs.py -j frame_embedding_job --config /tmp/c.yaml   # 반복
docker exec docker-dagster-1 sh -lc 'printf "ops:\n  caption_embedding:\n    config:\n      limit: 5000\n" > /tmp/cc.yaml'
docker exec docker-dagster-1 dagster job execute -f /app/dagster_defs.py -j caption_embedding_job --config /tmp/cc.yaml
```
적재 확인: `psql -d vlm_pipeline -c "SELECT entity_type,count(*) FROM image_embeddings GROUP BY 1;"`

## 5. FiftyOne dataset + App (:5153)
```bash
docker exec docker-analysis-1 python -c "
import fiftyone_pgvector as fp, fiftyone as fo
rows = fp.load_frame_embeddings()
ds = fp.build_fiftyone_dataset('prod_frames', rows)   # 로컬 media 다운로드 + UMAP + SAM3 검출/캡션 attach
fp.add_caption_clusters(ds, k=12)                       # caption_cluster 필드
print(len(ds), ds.get_field_schema().keys())
"
# 상시 App (keep-alive; wait() 는 헤드리스에서 즉시 리턴하므로 sleep 루프)
docker exec -d docker-analysis-1 sh -lc 'python -c "import fiftyone as fo,time; fo.launch_app(fo.load_dataset(\"prod_frames\"), port=5151); time.sleep(10**9)"'
```
- 접속: **JupyterLab `http://10.0.0.10:8890`**, **FiftyOne App `http://10.0.0.10:5153`**.
- 클러스터 그래프 x축 라벨값: **`detection_class`**(SAM3 클래스) + **`caption_cluster`**(캡션 의미 그룹) 둘 다 color-by/Distributions 가능.

## 6. 검증
- embedding-service healthy(python healthcheck), image_embeddings frame+caption rows, :5153 에서 썸네일+UMAP+검출 오버레이+클러스터 표시, cross-modal `fp.search_by_text("...")` 동작.

## 롤백
- 임베딩/분석만 제거: `.env` 에서 profiles/ENABLE_EMBEDDING 제거 + `compose up -d`(embedding/analysis/mongo 미생성), `image_embeddings` 는 두면 무해(다른 파이프라인 무관).
- PG image swap 롤백: `POSTGRES_IMAGE` 를 `pgduckdb/pgduckdb:15-v1.1.1` 로 되돌리고 `up -d postgres`(재생성). image_embeddings 데이터는 보존되나 vector 확장 미존재 시 쿼리 불가.
