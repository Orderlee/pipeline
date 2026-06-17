# 코디네이트 prod 재가동 런북 (2026-06-16)

> 목적: 복사 오케스트레이션이 만든 "재가동 가능 창"에서 **한 번의 재가동으로**
> ① 임베딩 파이프라인 정식화(우회 컨테이너 → compose) ② KPI/Grafana 포트 복구 를 동시에.
> dagster prod 라벨링/인제스트가 idle 일 때만 실행.

## 0. 사전 상태 (이 런북 작성 시점)
- 복사 오케스트레이터(`/home/user/batch_ingest/orchestrator.sh`, pgid **59247**)는 **SIGSTOP 정지** 상태(batch 7 dispatch 전). 재개는 `kill -CONT -59247`.
- 임베딩은 현재 **수동 `docker run`** 으로만 떠 있음: `prod-embed-analysis`(:5153 FiftyOne, :8503 Streamlit) + `prod-embed-mongo`. PG(vlm_pipeline)에 pgvector in-place + 008 적용, 임베딩 backfill(caption 11978 + frame 2740) 완료.
- KPI: `pipeline-postgres-1` 이 호스트 포트 **15434 미게시** → KPI 샘플러(cron, eng-a) 실패 → Grafana operational-kpi "No data".

## 1. GATE — 재가동해도 되는지 확인 (필수)
```bash
# (a) dispatch_stage_job 진행중 run 0개?  (69acd126 포함 전부 terminal)
curl -s http://10.0.0.10:3030/graphql -H 'Content-Type: application/json' \
  -d '{"query":"{runsOrError(filter:{statuses:[STARTED,STARTING,QUEUED]}){... on Runs{results{runId jobName}}}}"}'
# → results: []  여야 진행. 비어있지 않으면 대기.
# (b) 오케스트레이터가 정지 상태인지 (STAT=T)
ps -o pid,stat,cmd -p 59247,577849
```
**둘 다 OK 일 때만 다음 단계.**

## 2. 임베딩 정식화 (prod `docker` 프로젝트 — dagster 재생성 동반)

### 2-1. 수동 우회 컨테이너 제거 (5153/8503 포트 회수)
```bash
docker rm -f prod-embed-analysis prod-embed-mongo
```

### 2-2. prod `.env` 키 추가 (호스트 직접 편집 — git 미추적)
`/home/user/work_p/Datapipeline-Data-data_pipeline/docker/.env`:
```
COMPOSE_PROFILES=sam3,backup,genai,embedding,analysis   # embedding,analysis 추가
ENABLE_EMBEDDING=true
EMBEDDING_PORT=8004        # ⚠️ 호스트 8003 점유 중 → 8004
FIFTYONE_PORT=5153
STREAMLIT_PORT=8503
```
- `POSTGRES_IMAGE` **건드리지 말 것** (pgduckdb/pgduckdb:15-v1.1.1 유지) → `compose up -d postgres` 가 postgres 를 **재생성하지 않아** in-place pgvector(006/007/008) 보존. (재생성되면 vanilla 이미지라 pgvector 소실 → migration 실패하니 절대 변경 금지.)
- `EMBEDDING_API_URL` 미설정 OK (compose 기본 `http://embedding-service:8003` 내부통신).

### 2-3. 배포 (PR #271 → main)
- PR #271 머지 → `deploy-production.yml` 자동 배포. **또는** 머지 후 호스트에서 수동:
  `./scripts/compose-prod.sh up -d`
- 배포가 하는 일: rsync + git reset + `compose up -d`
  - dagster 3컨테이너 재생성 (idle 라 안전) + `ENABLE_EMBEDDING=true` → **라이브 임베딩 asset/sensor 등록**
  - migration runner 가 006/007/008 미기록분 idempotent 재적용 + `_pg_migrations` 정합
  - `embedding-service`(:8004), `analysis`(:5153 jupyter/fiftyone, :8503 streamlit), `fiftyone-mongo` 신규 기동
  - postgres 는 config 불변 → **재생성 안 됨** (in-place pgvector 유지)

### 2-4. 배포 후 — FiftyOne 데이터셋 + Streamlit 기동 (compose analysis 의 CMD 는 jupyter 뿐)
```bash
CTR=docker-analysis-1   # compose 가 만든 이름
docker exec -d $CTR sh -lc 'cd /workspace && DASHBOARD_REFRESH_SEC=60 streamlit run embedding_dashboard.py --server.port 8501 --server.address 0.0.0.0 --server.headless true > /tmp/streamlit.log 2>&1'
docker exec -d $CTR sh -lc 'cd /workspace && python fiftyone_prod_launch.py > /tmp/fiftyone.log 2>&1'
```
(embedding_dashboard.py 는 Dockerfile COPY 로 이미지에 포함됨 — 재빌드된 analysis 이미지 사용.)

### 2-5. 검증
```bash
curl -s http://10.0.0.10:8004/health                 # embedding-service model_loaded
curl -s -o /dev/null -w '%{http_code}\n' http://10.0.0.10:5153/   # FiftyOne
curl -s http://10.0.0.10:8503/_stcore/health          # Streamlit
# dagster: embedding sensor ON 확인 (UI :3030) → frame backlog(잔여 ~120K) 자동 처리 시작
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -tAc "SELECT entity_type,count(*) FROM image_embeddings GROUP BY 1;"
```

## 3. KPI / Grafana 복구 — `pipeline-postgres-1` 15434 포트 복원 (별도 `pipeline` 프로젝트)
prod 배포 오케스트레이션은 이 컨테이너를 **안 건드림**(수동 run). 같은 재가동 창에 별도 실행.

**방법 A (정석, 재생성):** `./restore_pipeline_postgres_port.sh` (동봉). 볼륨/네트워크/env 보존 + `-p 15434:5432`. LS 순단.
**방법 B (무중단 대안):** socat 포워더
```bash
docker run -d --name kpi-pg-portfwd --restart unless-stopped \
  --network pipeline_default -p 127.0.0.1:15434:15434 \
  alpine/socat tcp-listen:15434,fork,reuseaddr tcp-connect:pipeline-postgres-1:5432
```
검증: `docker exec pipeline-postgres-1 psql -U airflow -d nas_tree -tAc "SELECT max(synced_at) FROM kpi_sync_status;"` 가 현재시각 근처로 갱신(5분 내 cron).

## 4. 복사 오케스트레이터 재개
```bash
kill -CONT -59247          # batch 7→11 계속. wait_runs 가 재가동 다운을 ||n=0 로 흡수.
tail -f /home/user/batch_ingest/orchestrator.log
```

## 5. 롤백 포인트
- 임베딩 신규 컨테이너만 문제 시: `COMPOSE_PROFILES` 에서 embedding,analysis 제거 후 `compose up -d` (dagster 영향 최소).
- KPI 방법 A 실패 시: `docker rm -f pipeline-postgres-1 && docker rename pipeline-postgres-1-noport-bak pipeline-postgres-1 && docker start ...` (스크립트가 백업 컨테이너 남김) 또는 방법 B(socat)로 전환.
- 오케스트레이터는 정지/재개가 멱등(`already_dispatched`+`resume_copy`) — 꼬이면 `kill 59247` 후 `bash orchestrator.sh` 재실행.
