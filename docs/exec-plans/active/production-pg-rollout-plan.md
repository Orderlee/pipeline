# Production PostgreSQL 전환 롤아웃 계획

> 대상: 운영(`main`, `:3030`) 환경을 staging 에서 검증된 PG primary + `pg_duckdb`
> 모드로 전환할 때의 단계·리스크·롤백 절차.
> 전제: staging(`dev`, `:3031`) 에서 §[DBeaver 가이드](../references/dbeaver-pg-duckdb-test-guide.md)
> §3 (`DuckDB → PG ATTACH`) 검증이 통과한 상태.

작성: 2026-05-07 | 상태: **작성됨, 미실행** (실행 시 진행 상황을 본 문서 끝의 체크리스트에 기록)

---

## 0. TL;DR — 한 페이지 요약

| 단계 | 무엇을 | 다운타임 | 리스크 | 롤백 가능성 |
|------|--------|---------|--------|-----------|
| **1** code merge | dev → main PR | 0 (CI 자동배포 ~3-10분) | 코드만 들어가고 동작은 그대로(legacy DuckDB) | revert PR |
| **2** PG image 교체 | `.env` 에 `POSTGRES_IMAGE`+`POSTGRES_PRELOAD_LIBS` 추가 후 재기동 | postgres ~30s | DuckDB write lock 영향 없음 | `.env` 되돌리고 재기동 |
| **3** 스키마/DB 생성 | `vlm_pipeline` DB 생성 + Resource init 자동 부트스트랩 | 0 | 빈 PG 만 생기는 단계 | DB drop |
| **4** 데이터 백필 | `migrate_duckdb_to_postgres.py --apply --idempotent` | 0 (read-only on duckdb) | 백필 시간 = 행 수 비례 (~수분~수십분) | PG truncate, 재실행 |
| **5** dual write 검증 | `DATAOPS_DB_BACKEND=dual_pg_primary` 로 며칠 운영 | dagster ~30s 재시작 | DuckDB 가 mirror 로 안전망 | `.env` 되돌리고 재기동 |
| **6** PG single 모드 | `DATAOPS_DB_BACKEND=postgres` | dagster ~30s 재시작 | DuckDB write 0, mirror 없음 | `dual_pg_primary` 로 즉시 복귀 |
| **7** 정리 | 일정 기간 안정 운영 후 `pipeline.duckdb` 보존만 하고 잊기 | 0 | 없음 | — |

권장 페이스: 1~4 는 같은 날, 5 는 **최소 3~7일** 운영 검증, 6 은 그 후. 한꺼번에 안 함.

---

## 1. 현재 상태 실측 (2026-05-07 기준)

### 1.1 Production (`main` HEAD = `c820ec1`)

| 항목 | 값 |
|------|----|
| Postgres 이미지 | `postgres:15` (vanilla) |
| `shared_preload_libraries` | (빈 문자열) |
| `pg_duckdb` extension | ❌ 미설치 |
| Dagster `.env` 의 `DATAOPS_DB_BACKEND` | (미설정 → legacy `duckdb` 모드) |
| `DATAOPS_POSTGRES_DSN` | (미설정) |
| Active DB | `airflow`, `labeling`, `postgres` (vlm 데이터는 PG 에 없음) |
| 운영 데이터 위치 | `/data/pipeline.duckdb` (DuckDB 단일 파일) |

### 1.2 Staging (`dev` HEAD = `da389ef`, main 대비 14 commits ahead)

| 항목 | 값 |
|------|----|
| Postgres 이미지 | `pgduckdb/pgduckdb:15-v1.1.1` |
| `shared_preload_libraries` | `pg_duckdb` |
| `pg_duckdb` extension | ✅ v1.1.0 |
| `DATAOPS_DB_BACKEND` | `postgres` (single PG) |
| `DATAOPS_POSTGRES_DSN` | `postgresql://airflow:airflow@postgres:5432/vlm_pipeline_staging` |
| Active vlm DB | `vlm_pipeline_staging` (16 tables, 5396+ rows) |
| Sensor read | `open_sensor_read_connection()` → in-memory DuckDB + ATTACH PG (READ_ONLY) |

### 1.3 dev 가 main 보다 갖고 있는 commits (PR 단위)

- `#42 feature/db-migration-postgres` — Phase 0-7 (스키마, Resource, DualDB facade, MotherDuck PG source)
- `#43 chore/deploy-single-truth-source` — compose `../src` bind mount 제거
- `#44 feat/staging-pg-duckdb` — `POSTGRES_IMAGE` override 지원
- `#45 chore/deploy-stack-postgres-auto` — postgres 자동 재기동 + healthcheck wait
- `#46 feat/sensor-pg-attach-single-pg-mode` — sensor PG ATTACH facade
- `#47 chore/postgres-pg-duckdb-preload` — `POSTGRES_PRELOAD_LIBS` 지원
- `#48 chore/staging-ops-hygiene` — `.env.test` untrack + writer tag release
- `#49 fix/claude-action-native-install` — CI Claude action 우회

→ 단일 PR (`dev → main`) 으로 한 번에 머지하는 것을 권장.

---

## 2. Phase 1 — 코드 머지 (동작 변화 0)

**목표**: 코드만 운영 호스트에 도달. legacy DuckDB primary 그대로 유지.

### 단계

1. PR 생성: GitHub UI 에서 `dev → main`. 제목 예: `feat(db): PostgreSQL primary 모드 전체 코드 도입 (Phase 1)`
2. 본문에 본 문서 링크 + 단계 1 만 적용된다는 점 명시.
3. 머지 → `.github/workflows/deploy-production.yml` 자동 트리거 → ~3-10분 후 `:3030/server_info` 응답 확인.
4. 운영 host 에서:
   ```bash
   ssh 10.0.0.10
   docker exec docker-dagster-1 sh -c 'echo BACKEND=$DATAOPS_DB_BACKEND; echo DSN_SET=$([ -n "$DATAOPS_POSTGRES_DSN" ] && echo yes || echo no)'
   # 기대:  BACKEND= (빈값 → legacy duckdb)
   #        DSN_SET=no
   ```

### 검증

- Dagster UI(`:3030`) 에 sensor 들이 정상 tick (last evaluation 갱신, error 0)
- 일반 ingest/dispatch/label 동작 1회 이상 — DuckDB write/read 그대로
- 컨테이너 안 코드 path 가 `db_backend_mode()` → `"duckdb"` 반환하는지 (간접: error 없으면 OK)

### 롤백

- PR revert 후 다시 머지 → CI 가 이전 SHA 로 복귀.

### 리스크

- **거의 없음**. legacy 기본값(`duckdb`) 으로 fallback 하도록 [env_utils.py:104](../../src/vlm_pipeline/lib/env_utils.py#L104) 가 보호.
- 1회 sanity test 만 하고 다음 phase 로 진행 가능.

---

## 3. Phase 2 — Postgres 이미지 교체 (계획 다운타임 ~30s)

**목표**: 운영 postgres 컨테이너를 `pgduckdb/pgduckdb:15-v1.1.1` 로 swap, `pg_duckdb` 활성. **Dagster 에는 영향 없음** (Dagster 는 아직 PG 안 씀).

### 사전 준비

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# (a) PG 영구 볼륨 백업 (선택이지만 권장 — postgres_data 영역)
docker run --rm \
  -v docker_postgres_data:/from \
  -v /home/user/backups:/to \
  alpine sh -c "tar czf /to/prod-postgres-data-$(date +%Y%m%d_%H%M).tgz -C /from ."

# (b) 현재 PG 의 airflow / labeling DB 인벤토리 — swap 후 보존 확인용
docker exec docker-postgres-1 psql -U airflow -c "\l" > /tmp/prod-pg-databases-before.txt
docker exec docker-postgres-1 psql -U airflow -d airflow -c "\dt" > /tmp/prod-pg-airflow-tables-before.txt
```

### `docker/.env` 추가 (Phase 2 항목만)

```diff
+ # ===== PostgreSQL 이미지 / 확장 (Phase 2) =====
+ # pgduckdb 이미지로 swap 하면 postgres_data 볼륨 그대로 마운트되어 schema/데이터 손실 0.
+ # 단 pg_duckdb extension 사용을 위해 shared_preload_libraries 에 pg_duckdb 추가 필요.
+ POSTGRES_IMAGE=pgduckdb/pgduckdb:15-v1.1.1
+ POSTGRES_PRELOAD_LIBS=pg_duckdb
```

> ⚠️ `.env` 는 git 미추적이므로 **호스트에서 직접 편집**. `dev` 의 `.env.test`
> 와 다른 점은 [3.1 절](#31-staging-과-production-env-차이) 참고.

### 적용

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose up -d postgres        # postgres 만 recreate
docker compose ps postgres           # healthy 확인
```

`scripts/deploy/deploy-stack.sh` 의 `auto-restart postgres on compose change`
경로(PR #45)도 동일 로직을 수행하므로, 다음 deploy 가 알아서 처리하도록
미루는 것도 가능.

### 검증

```bash
# (a) 이미지 / shared_preload_libraries
docker inspect docker-postgres-1 --format '{{.Config.Image}}'
# 기대: pgduckdb/pgduckdb:15-v1.1.1
docker inspect docker-postgres-1 --format '{{.Config.Cmd}}'
# 기대: [postgres -c shared_preload_libraries=pg_duckdb]

# (b) extension 등록
docker exec docker-postgres-1 psql -U airflow -c "
  SHOW shared_preload_libraries;
  SELECT * FROM pg_available_extensions WHERE name='pg_duckdb';"

# (c) airflow DB 보존 — Phase 2 핵심 안전 점검
docker exec docker-postgres-1 psql -U airflow -c "\l"  > /tmp/prod-pg-databases-after.txt
diff /tmp/prod-pg-databases-before.txt /tmp/prod-pg-databases-after.txt
# 기대: 차이 없음 (airflow / labeling 그대로)
```

### 롤백

```bash
# .env 에서 POSTGRES_IMAGE / POSTGRES_PRELOAD_LIBS 두 줄 삭제
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose up -d postgres
```
이미지 다운그레이드되면서 `shared_preload_libraries` 도 빈 값으로 복귀.
postgres_data 볼륨은 그대로라 airflow/labeling 영향 없음.

### 리스크

- **이미지 swap 시 PG 자체 ~30s 재기동**. Dagster 는 PG 를 안 쓰는 단계라 영향 없지만, **Grafana 등 PG airflow DB 를 쓰는 외부 도구가 있으면 잠깐 끊김**.
- pgduckdb 이미지가 `postgres:15` 와 호환 안 되는 변경이 있으면 startup 실패 가능성 — staging 에서 같은 이미지로 검증 완료라 확률 낮음.

---

## 4. Phase 3 — `vlm_pipeline` DB 생성 + 스키마 부트스트랩

**목표**: PG 안에 빈 `vlm_pipeline` DB 와 16개 테이블 생성. Dagster 코드 path 는
아직 사용 안 함.

### 단계

```bash
ssh 10.0.0.10

# (a) DB 생성
docker exec docker-postgres-1 psql -U airflow -d postgres -c "
  CREATE DATABASE vlm_pipeline OWNER airflow;"

# (b) pg_duckdb extension 등록 (DB 단위)
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  CREATE EXTENSION IF NOT EXISTS pg_duckdb;
  SELECT extname, extversion FROM pg_extension WHERE extname='pg_duckdb';"

# (c) 스키마 부트스트랩 — Resource init 가 auto-create 하므로 이 단계는 사실 nop
#     명시 적용을 원하면:
docker exec docker-dagster-1 python3 -c "
from vlm_pipeline.resources.postgres_db import PostgresResource
import os
r = PostgresResource(dsn='postgresql://airflow:airflow@postgres:5432/vlm_pipeline')
r.ensure_runtime_schema()
"
```

### 검증

```bash
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "\dt"
# 기대: 16개 테이블 (raw_files, video_metadata, image_metadata, ...)

docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT 'raw_files' AS t, COUNT(*) FROM raw_files
  UNION ALL SELECT 'video_metadata', COUNT(*) FROM video_metadata
  ORDER BY t;"
# 기대: 모두 0 (빈 스키마)
```

### 롤백

```bash
docker exec docker-postgres-1 psql -U airflow -d postgres -c "
  DROP DATABASE IF EXISTS vlm_pipeline;"
```

### 리스크

- 거의 없음. 빈 DB 추가일 뿐.

---

## 5. Phase 4 — 데이터 백필 (DuckDB → PG, no downtime)

**목표**: `/data/pipeline.duckdb` 의 모든 row 를 `vlm_pipeline` 으로 idempotent
복사. 운영 Dagster 는 여전히 DuckDB 로 write 하므로 이 단계 동안 들어오는
신규 row 는 다음 backfill 또는 dual-write 시점에 따라잡는다.

### 단계

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# (a) dry-run — 복사 plan 만 출력 (write 0)
docker exec docker-dagster-1 python3 /src/python/scripts/migrate_duckdb_to_postgres.py \
  --source /data/pipeline.duckdb \
  --dsn "postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
  --dry-run

# (b) 실제 백필 — idempotent 모드 (재실행 안전)
docker exec docker-dagster-1 python3 /src/python/scripts/migrate_duckdb_to_postgres.py \
  --source /data/pipeline.duckdb \
  --dsn "postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
  --apply --idempotent

# (c) 검증 — 양쪽 row count 비교
docker exec docker-dagster-1 python3 /src/python/scripts/migrate_duckdb_to_postgres.py \
  --source /data/pipeline.duckdb \
  --dsn "postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
  --verify-only
```

> 📌 스크립트는 [scripts/migrate_duckdb_to_postgres.py](../../scripts/migrate_duckdb_to_postgres.py)
> 의 13개 테이블 spec 기반으로 PK 기준 INSERT … ON CONFLICT DO NOTHING (idempotent).
> 컨테이너 안에서 실행하면 DuckDB read lock 만 잠깐 잡으므로 write 하는 sensor
> 와 충돌 가능 — **트래픽 적은 시간(평일 새벽 또는 주말)** 권장.

### 검증

- `--verify-only` 출력에서 모든 테이블 source = target
- DBeaver PG 연결로 spot-check 한 두 행 비교

### 롤백

```bash
# vlm_pipeline DB 통째로 truncate
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  TRUNCATE raw_files, video_metadata, image_metadata, labels, processed_clips,
           datasets, dataset_clips, dispatch_requests, dispatch_pipeline_runs,
           image_labels, labeling_specs, labeling_configs, staging_model_configs,
           classification_datasets, requester_config_map
  CASCADE;"
# 또는 더 깔끔히 DB drop & recreate (Phase 3 재실행)
```

### 리스크

- **DuckDB read lock 충돌**: 백필 중 sensor 가 DuckDB write 시도 시 일시 대기. 백필 완료 후 자연 해소.
- **백필 도중 신규 row 누락**: Phase 5 로 넘어가는 시점에 한 번 더 `--apply --idempotent` 실행해 신규 row 따라잡기.

---

## 6. Phase 5 — Dual-write PG primary (안전망 켜고 PG 검증)

**목표**: Dagster 가 PG primary 로 쓰기 + DuckDB 는 mirror. 며칠 운영하면서
PG 단독 모드가 안정한지 관찰.

### `docker/.env` 추가

```diff
+ # ===== Phase 5 — DB backend (dual: PG primary, DuckDB mirror) =====
+ DATAOPS_DB_BACKEND=dual_pg_primary
+ DATAOPS_POSTGRES_DSN=postgresql://airflow:airflow@postgres:5432/vlm_pipeline
```

### 적용

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker

# Dagster 만 재시작 (postgres 는 그대로)
docker compose restart dagster dagster-daemon dagster-code-server

# 헬스체크
curl -fsS http://localhost:3030/server_info
```

### 검증 (며칠에 걸쳐)

- **(즉시)** sensor 들이 PG ATTACH 로 read 하는지: `docker logs docker-dagster-daemon-1 | grep -iE "ATTACH|postgres" | tail -20`
- **(즉시)** 신규 ingest 1회 후 PG 와 DuckDB 양쪽에 같은 row 가 있는지:
  ```bash
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c \
    "SELECT MAX(created_at) FROM raw_files;"
  docker exec docker-dagster-1 sh -c \
    'duckdb -readonly -csv /data/pipeline.duckdb "SELECT MAX(created_at) FROM raw_files;"'
  # 두 값이 ±1초 이내 일치
  ```
- **(매일)** row count drift 모니터:
  ```bash
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -t -c \
    "SELECT COUNT(*) FROM raw_files;"
  # vs DuckDB 같은 카운트
  ```
- **(상시)** Dagster UI 의 sensor / job 에러율 평소 수준 유지

### 권장 운영 기간

**최소 3~7 일**. 그 사이 한 번 이상 dispatch 전체 사이클(ingest → label → process → dataset)이 PG 에서 정상 동작하는 것을 확인.

### 롤백

```bash
# .env 의 두 줄 제거 후
docker compose restart dagster dagster-daemon dagster-code-server
# → legacy DuckDB primary 로 즉시 복귀 (DuckDB 데이터 그대로)
```
PG 에 mirror 된 데이터는 stale 해지지만 다음 phase 진입 시 idempotent 재백필.

### 리스크

- **Dual-write 비용**: 모든 INSERT 가 PG + DuckDB 양쪽으로 → write latency ~2배. 운영 트래픽이 sensor lag 임계치에 가까우면 일부 sensor 가 늦어질 수 있음. staging 에서 측정한 실제 lag 와 비교해 결정.
- **mirror divergence**: 한쪽 write 만 성공하는 케이스 — `DualDBResource` facade ([resources/data_db.py](../../src/vlm_pipeline/resources/data_db.py)) 가 양쪽 commit 보장하지만 부분 실패 시 PG 만 진실. 이 케이스 의심되면 idempotent 백필로 보정.

---

## 7. Phase 6 — PG single 모드 (DuckDB write 종료)

**목표**: DuckDB write 완전 종료. 모든 read/write 가 PG 단일 backend.

### `docker/.env` 수정

```diff
- DATAOPS_DB_BACKEND=dual_pg_primary
+ DATAOPS_DB_BACKEND=postgres
```
(`DATAOPS_POSTGRES_DSN` 은 그대로)

### 적용 + 검증

```bash
docker compose restart dagster dagster-daemon dagster-code-server

# (a) DuckDB 파일 mtime 이 멈춰야 함 — write 0 신호
ls -la /home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/pipeline.duckdb
# 5분 뒤 다시 보고 mtime 변경 없는지

# (b) duckdb_writer concurrency 제약 해소 — build_duckdb_writer_tags() == {}
#     동시에 dispatch run 2~3개 launch → 모두 RUNNING 상태로 가는지 확인
```

### 권장 운영 기간

**최소 1주**. 그 사이 한 번 이상 모든 주요 job (ingest, dispatch, label, process,
build_dataset, motherduck_sync) 이 PG 단독으로 통과한 기록 확보.

### 롤백

`dual_pg_primary` 로 즉시 복귀 가능. 그 사이 PG 에만 들어간 row 는 DuckDB 에 mirror 안 되어 있지만, 다시 `dual_pg_primary` 로 가면 신규 row 부터는 다시 mirror 됨. 과거 row 까지 복구하려면 PG → DuckDB 역방향 migrate 필요 (현재 스크립트 없음 — 필요 시 별도 작성).

### 리스크

- **Mirror 안전망 사라짐**. 이 시점에서 PG 데이터가 source of truth.
- **PG 백업 정책 필수**. 이 단계 진입 전에 일별 `pg_dump` 또는 volume snapshot
  cron 등록.

---

## 8. Phase 7 — 정리 (선택, ~1개월 후)

- `pipeline.duckdb` 는 지우지 말고 `pipeline.duckdb.frozen-YYYYMMDD` 로 rename 만. 만일을 대비.
- `.env` 에서 `DUCKDB_PATH` / `DATAOPS_DUCKDB_PATH` 는 **그대로 둠** — sensor_db 의 fallback 경로가 의존.
- MotherDuck sync sensor 는 이미 PG source 지원(`feat(sync) Phase 7` commit) → 별도 변경 불필요.

---

## 9. 운영 환경 특화 사항

### 9.1 staging 과 production `.env` 차이

| 항목 | staging (`.env.test`) | production (`.env`) | 이유 |
|------|---------------------|---------------------|------|
| `DATAOPS_DUCKDB_PATH` | `/data/staging.duckdb` | `/data/pipeline.duckdb` | 파일명 분리 |
| `DATAOPS_POSTGRES_DSN` 의 DB 이름 | `vlm_pipeline_staging` | **`vlm_pipeline`** | DB 분리 |
| `POSTGRES_PORT` | `15432` (host expose) | (unset, expose 안 함) | 운영은 외부 접근 차단. DBeaver 필요 시 Phase 5 후 임시로 `15432` 추가 가능 |
| MinIO 엔드포인트 | `:9002` (staging MinIO) | `:9000` (운영 MinIO) | 인스턴스 분리 |
| `POSTGRES_IMAGE` | `pgduckdb/pgduckdb:15-v1.1.1` | **동일** (Phase 2 에서 추가) | 동일 |
| `POSTGRES_PRELOAD_LIBS` | `pg_duckdb` | **동일** (Phase 2 에서 추가) | 동일 |
| `DATAOPS_DB_BACKEND` | `postgres` | **단계적**: unset → `dual_pg_primary` → `postgres` | 운영은 안전망 거쳐 진입 |

### 9.2 DBeaver 운영 PG 접속 (Phase 5 이후, 선택)

운영 DBeaver 분석이 필요하면 Phase 5 진입 시 `.env` 에 임시로:
```
POSTGRES_PORT=15433
```
(staging 이 15432 를 이미 쓰므로 운영은 15433 권장)
추가 후 `docker compose up -d postgres`. DBeaver 설정은 `docs/references/dbeaver-pg-duckdb-test-guide.md` 참고하되 SSH 터널의 LocalForward / Remote port 를 `15433` 으로 맞춤.

> ⚠️ 운영 PG 에 직접 쓰기 ABSOLUTE NO. read-only 분석만. ATTACH 에 `READ_ONLY` 빠뜨리지 말 것.

### 9.3 백업 / 복구 정책 (Phase 6 진입 전 필수)

```bash
# 일별 pg_dump (cron 등록)
0 3 * * *  docker exec docker-postgres-1 pg_dump -U airflow -d vlm_pipeline | \
           gzip > /home/user/backups/vlm_pipeline-$(date +\%Y\%m\%d).sql.gz

# 7일 이전 자동 삭제
0 4 * * *  find /home/user/backups -name 'vlm_pipeline-*.sql.gz' -mtime +7 -delete
```

복구:
```bash
gunzip -c vlm_pipeline-20260601.sql.gz | \
  docker exec -i docker-postgres-1 psql -U airflow -d vlm_pipeline
```

---

## 10. 실행 체크리스트 (실행 시 체크해 가기)

### Phase 1 — code merge
- [ ] PR `dev → main` 생성 + 본 문서 링크 첨부
- [ ] CI deploy 성공 (`:3030/server_info` OK)
- [ ] sensor last_evaluation 갱신 확인
- [ ] `docker exec ... echo BACKEND` → 빈값 (legacy 모드)

### Phase 2 — postgres 이미지 swap
- [ ] postgres_data 볼륨 백업 완료
- [ ] `airflow/labeling DB` snapshot 보관
- [ ] `.env` 에 `POSTGRES_IMAGE` + `POSTGRES_PRELOAD_LIBS` 추가
- [ ] `docker compose up -d postgres` 후 healthy
- [ ] `SHOW shared_preload_libraries` = `pg_duckdb`
- [ ] `airflow / labeling DB` 보존 확인 (diff before/after)

### Phase 3 — vlm_pipeline DB 생성
- [ ] `CREATE DATABASE vlm_pipeline`
- [ ] `CREATE EXTENSION pg_duckdb` (DB 단위)
- [ ] 스키마 부트스트랩 (16 tables)

### Phase 4 — 백필
- [ ] `--dry-run` 출력 확인
- [ ] `--apply --idempotent` 성공
- [ ] `--verify-only` 모든 테이블 source = target

### Phase 5 — dual_pg_primary
- [ ] `.env` 에 `DATAOPS_DB_BACKEND=dual_pg_primary` + `DATAOPS_POSTGRES_DSN`
- [ ] Dagster 재시작
- [ ] 신규 ingest 1건 → PG / DuckDB 양쪽 동일 row 확인
- [ ] N=3~7 일 운영 (drift 모니터, 에러율 평소 수준)

### Phase 6 — postgres single
- [ ] PG `pg_dump` cron 등록 + 1회 dump 검증
- [ ] `.env` 의 backend 를 `postgres` 로 변경
- [ ] Dagster 재시작
- [ ] DuckDB mtime 멈춤 확인 (5분 후)
- [ ] 동시 dispatch run 2~3개 RUNNING 확인 (concurrency lock 해소)

### Phase 7 — 정리 (선택)
- [ ] 1개월 안정 운영 후 `pipeline.duckdb` rename to `.frozen-YYYYMMDD`

---

## 11. 결정 일지 (실행 시 채워 가기)

| 일자 | Phase | 결정/관찰 | 작성자 |
|------|-------|----------|--------|
| 2026-05-07 | — | 본 plan 작성. staging 검증 통과 (raw_files=2698, video_metadata=288, image_metadata=2410). DBeaver DuckDB ATTACH READ_ONLY 정상 동작 확인 | claude |
| YYYY-MM-DD | Phase 1 | (PR# 등) | |
| YYYY-MM-DD | Phase 2 | (이미지 swap 시각, airflow DB 영향 여부) | |
| YYYY-MM-DD | Phase 3 | (vlm_pipeline DB 생성 시각) | |
| YYYY-MM-DD | Phase 4 | (백필 row count by table) | |
| YYYY-MM-DD | Phase 5 | (dual_pg_primary 진입 시각, 첫 dispatch 결과) | |
| YYYY-MM-DD | Phase 6 | (postgres single 진입 시각, concurrency 해소 확인) | |

---

## 12. 참고 문서

- [DBeaver staging Postgres + DuckDB extension 테스트 가이드](../references/dbeaver-pg-duckdb-test-guide.md) — 운영 PG 접속 시 동일 가이드 적용 (port 만 다름)
- [운영-테스트 환경 분리 및 자동 배포 계획](운영_테스트_환경_분리_자동배포_계획.md) — CI/CD 메커니즘 상세
- [DB Migration Topology](../references/db_migration_topology.md) — 마이그레이션 단계별 토폴로지 다이어그램
- [Production Runbook](../references/production-label-preprocess-cleanup-runbook.md) — 운영 트러블슈팅
- 코드:
  - [`scripts/migrate_duckdb_to_postgres.py`](../../scripts/migrate_duckdb_to_postgres.py) — 백필 스크립트
  - [`src/vlm_pipeline/lib/env_utils.py`](../../src/vlm_pipeline/lib/env_utils.py) — `db_backend_mode()` 정의
  - [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py) — sensor PG ATTACH facade
  - [`src/vlm_pipeline/resources/data_db.py`](../../src/vlm_pipeline/resources/data_db.py) — DualDB facade
