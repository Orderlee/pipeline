# DBeaver 로 staging Postgres + DuckDB extension 테스트 가이드

> 대상: Mac 에서 DBeaver 로 staging(`10.0.0.10`) Postgres / DuckDB 동작을 검증하려는 운영자.
> 전제: staging 은 이미 PG primary 모드(`DATAOPS_DB_BACKEND=postgres`) 로 가동 중이며 `pgduckdb/pgduckdb:15-v1.1.1` 이미지를 사용한다.

---

## 0. 두 종류의 extension 구분

이 가이드는 방향이 정반대인 **두 개**의 extension 을 모두 검증한다.

| 위치 | extension | 방향 | 코드에서 사용? | 어디서 검증 |
|------|-----------|------|--------------|------------|
| Postgres 안 | `pg_duckdb` v1.1.0 | PG → DuckDB 엔진 호출 (read_parquet, S3 등) | ❌ 운영 코드 미사용 (ad-hoc 분석용) | DBeaver **PostgreSQL** 연결 (§2) |
| DuckDB 안 | `postgres` (postgres_scanner) | DuckDB → PG ATTACH (READ_ONLY) | ✅ **모든 sensor 의 read path** | DBeaver **DuckDB** 연결 + SSH 터널 (§3) |

> 🔑 **§2 와 §3 는 비중이 다르다.**
> - §2 (`pg_duckdb`) = **인프라 스모크 테스트**. 깨져도 운영 파이프라인엔 영향 없음
>   (현 시점 `grep -rn "pg_duckdb\|duckdb.query\|postgres_scan" src/` 결과 0건).
>   장차 운영자가 DBeaver 등에서 ad-hoc 분석할 때 쓰려고 미리 설치해 둔 것.
> - §3 (`postgres_scanner`) = **운영 sensor read path 그 자체**.
>   [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py)
>   의 `open_sensor_read_connection()` 가 매 sensor tick 마다 in-memory DuckDB
>   를 띄우고 PG 를 ATTACH 한다. **깨지면 모든 sensor 가 PG read 불가 = 파이프라인 정지**.
>
> 시간이 빠듯하면 §3 만 해도 충분.

---

## 1. 사전 준비

### 1.1 staging 접속 정보

| 항목 | 값 |
|------|----|
| SSH 호스트 | `10.0.0.10` (port 22) |
| Postgres 호스트 포트 | `10.0.0.10:15432` (컨테이너 5432 와 매핑) |
| Database | `vlm_pipeline_staging` |
| User / Password | `airflow` / `airflow` |
| Postgres 이미지 | `pgduckdb/pgduckdb:15-v1.1.1` |

> ⚠️ Production Postgres 도 random 포트로 publish 되어 있을 수 있으나,
> **이 가이드는 staging 만 다룬다**. Production DB 는 절대 임의 SQL 로 건드리지 말 것.

### 1.2 SSH 키 등록 확인 (Mac)

```bash
# Mac 터미널
ssh <ssh_user>@10.0.0.10 'echo ok'
# "ok" 출력되어야 함. 비밀번호 없이 들어가지면 키 인증 OK.
```

### 1.3 (옵션) DuckDB CLI 설치 — §3 검증에서 DBeaver 외에 CLI 로도 확인할 때만

```bash
brew install duckdb
duckdb --version   # v1.1.x 권장 (서버 이미지의 DuckDB 와 호환)
```

### 1.4 (사전 점검) 컨테이너가 정말 PG primary 모드인지 확인

검증 전에 staging 컨테이너의 환경 변수를 한 번 확인한다.
`DATAOPS_POSTGRES_DSN` 이 비어 있으면 sensor 가 PG 가 아니라 **legacy DuckDB 파일**
(`/data/staging.duckdb`) 로 fallback 하므로, §3 결과가 의미 없어진다
([sensor_db.py:38-49](../../src/vlm_pipeline/lib/sensor_db.py#L38-L49) 참고).

```bash
ssh 10.0.0.10 \
  'docker exec pipeline-test-dagster-1 sh -c "
     echo BACKEND=\$DATAOPS_DB_BACKEND;
     echo DSN_SET=\$([ -n \"\$DATAOPS_POSTGRES_DSN\" ] && echo yes || echo no)
   "'
```

기대 출력:
```
BACKEND=postgres
DSN_SET=yes
```

`BACKEND=duckdb` 또는 `DSN_SET=no` 가 나오면 `.env.test` 가 컨테이너에 적용 안 된
상태이므로, staging 호스트에서 `cd .../data_pipeline_test/docker && docker compose up -d`
재기동 후 다시 확인.

---

## 2. PostgreSQL 연결 + `pg_duckdb` 검증

> 📌 이 섹션은 **인프라 스모크 테스트**다. 운영 코드는 `pg_duckdb` 를 호출하지
> 않으므로 (`grep -rn "pg_duckdb\|duckdb.query\|postgres_scan" src/` → 0 건),
> 이 섹션이 깨져도 파이프라인에 즉각적 영향은 없다. 단지 "DBeaver 에서 ad-hoc
> 분석할 때 동작하는가" 를 보장하는 검증이다.

### 2.1 DBeaver 연결 설정

**New Database Connection → PostgreSQL.**

#### Main 탭

| Field | 값 |
|-------|----|
| Host | `localhost`  *(SSH 터널 사용 시)* — 또는 `10.0.0.10` *(직접 접속)* |
| Port | `15432` |
| Database | `vlm_pipeline_staging` |
| Username | `airflow` |
| Password | `airflow` |

#### SSH 탭 (추천)

`Use SSH Tunnel` 체크 후:

| Field | 값 |
|-------|----|
| Host/IP | `10.0.0.10` |
| Port | `22` |
| User Name | (Mac 에서 평소 ssh 할 때 쓰는 계정) |
| Authentication Method | `Public Key` (`~/.ssh/id_ed25519` 또는 `id_rsa`) |

`Test tunnel configuration` → `Connected` 확인 후 `OK`.

> **직접 접속**(SSH 터널 없이)을 쓸 때는 Mac → `10.0.0.10:15432` 가 망 정책상
> 열려 있어야 한다. 안 되면 SSH 터널로 fallback.

### 2.2 연결 후 1차 sanity check

DBeaver SQL 편집기에서 차례로 실행:

```sql
-- (a) 16개 테이블 마이그레이션 확인
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY 1;

-- (b) 현재 row count snapshot (이 값을 §3 결과와 비교한다)
SELECT 'raw_files'              AS t, COUNT(*) FROM raw_files
UNION ALL SELECT 'video_metadata',         COUNT(*) FROM video_metadata
UNION ALL SELECT 'image_metadata',         COUNT(*) FROM image_metadata
UNION ALL SELECT 'dispatch_requests',      COUNT(*) FROM dispatch_requests
UNION ALL SELECT 'dispatch_pipeline_runs', COUNT(*) FROM dispatch_pipeline_runs
UNION ALL SELECT 'labels',                 COUNT(*) FROM labels
UNION ALL SELECT 'image_labels',           COUNT(*) FROM image_labels
UNION ALL SELECT 'processed_clips',        COUNT(*) FROM processed_clips
UNION ALL SELECT 'datasets',               COUNT(*) FROM datasets
UNION ALL SELECT 'dataset_clips',          COUNT(*) FROM dataset_clips
ORDER BY t;
```

### 2.3 `pg_duckdb` 활성 검증

```sql
-- (a) shared_preload_libraries 에 등록되었는가
SHOW shared_preload_libraries;
-- 기대값: 'pg_duckdb'

-- (b) extension 자체가 PG 에 설치되었는가
SELECT extname, extversion
FROM   pg_extension
WHERE  extname = 'pg_duckdb';
-- 기대값: pg_duckdb | 1.1.0

-- (c) DuckDB 엔진 호출 smoke (외부 데이터/리터럴 — duckdb.query 는 DuckDB 카탈로그만 봄)
LOAD 'pg_duckdb';
SELECT * FROM duckdb.query($$ SELECT 'pg_duckdb works' AS msg, 42 AS x $$);

-- (d) DuckDB 엔진으로 현재 DB 의 PG 테이블 스캔 — force_execution 패턴
--     ⚠️ duckdb.query($$ ... FROM raw_files ... $$) / postgres_scan('host=localhost ...')
--     둘 다 동작 안 함 (duckdb.query 는 PG 테이블이 카탈로그에 안 보이고,
--      postgres_scan 은 같은 DB 안에서 호출 시 libpq linking 충돌). 정답:
SET duckdb.force_execution = true;

SELECT COUNT(*) FROM raw_files;
-- 기대값: (a) 또는 §2.2 (b) 의 raw_files 카운트와 일치

-- (d-1) 진짜 DuckDB 엔진이 실행했는지 EXPLAIN 으로 증명
EXPLAIN SELECT COUNT(*) FROM raw_files;
-- 기대 plan 첫 줄:
--   Custom Scan (DuckDBScan)  (cost=0.00..0.00 rows=0 width=0)
--     DuckDB Execution Plan:
--     ┌───────────────────────────┐
--     │    UNGROUPED_AGGREGATE    │ ...

-- (d-2) 분석 쿼리 예시 (DuckDB 엔진 실행) — PG 호환 문법 필수.
--      DuckDB-native 함수 (예: quantile_cont(x, 0.5)) 는 PG 파서에서 거부되므로
--      PG 도 가진 표준 SQL 함수 (percentile_cont WITHIN GROUP, AVG, COUNT 등) 만 사용.
SELECT
  source_unit_name,
  COUNT(*)                  AS files,
  AVG(file_size)::bigint    AS avg_size_bytes
FROM   raw_files
GROUP  BY source_unit_name
ORDER  BY files DESC
LIMIT  5;

-- (e) DuckDB 엔진 끄기 (선택) — 이후 쿼리는 다시 PG 네이티브로
SET duckdb.force_execution = false;
```

> 💡 **`pg_duckdb` 의 사용 패턴 3가지 (헷갈리지 말 것)**
>
> | 패턴 | 용도 | 예시 |
> |------|------|------|
> | `SET duckdb.force_execution = true; SELECT ...` | **현재 DB 의 PG 테이블** 을 DuckDB 엔진으로 분석 | (d), (d-2) |
> | `SELECT * FROM duckdb.query($$ ... $$)` | **외부 데이터** (parquet/S3/Iceberg/리터럴) 를 DuckDB 자체 카탈로그로 read | (c), §2.4 |
> | `SELECT * FROM postgres_scan('host=...', 'schema', 'table')` | **다른 PG 인스턴스** (not the current one) 에 libpq 로 붙어 read | 멀티-DB 분석 시 |
>
> 운영 staging/prod 에서는 보통 첫 번째 패턴이면 충분.

### 2.4 (옵션) MinIO 객체 스토리지 — read_parquet / glob / COPY

`pg_duckdb` 의 진짜 강점은 **PG 트랜잭션 안에서 MinIO/S3 위 파일을 직접
read/write** 할 수 있다는 점이다. 사용 시나리오:

| 시나리오 | 누가 쓰나 | 어디 §에 |
|---------|---------|---------|
| **PG 테이블 → parquet 으로 export** (분석 archive, 외부 도구 전달) | 운영자 ad-hoc | (3) |
| **parquet 데이터를 PG 안에서 SQL 로 분석** (외부 데이터 lake read) | 분석 | (4) |
| **MinIO 객체 인벤토리/검색** (어떤 파일이 어디 있나 SQL 로) | 운영 점검 | (2) |
| **JSON 라벨 이벤트 직접 read** (`vlm-labels` 안 JSON 파일) | 라벨 분석 | (5) |

> 💡 staging MinIO 의 운영 데이터는 sensor 가 만들고 채운다. staging 이 비어있으면
> dispatch 한 사이클 돌린 뒤 본 절을 시도하는 게 의미 있음. 다만 (1)(2)(3) 은
> 빈 상태에서도 동작 검증 가능.

#### (1) S3 secret 등록 (1회, persist)

> ⚠️ **`CREATE SECRET ... (TYPE S3, ...)` 는 DuckDB CLI 문법이라 PG 안에서는 안
> 통한다.** pg_duckdb 는 별도 함수 `duckdb.create_simple_secret(...)` 제공.

```sql
-- staging MinIO 자격증명으로 simple S3 secret 등록
SELECT duckdb.create_simple_secret(
  type      := 'S3',
  key_id    := 'minioadmin',                 -- 실제 access key 로 교체
  secret    := 'minioadmin',                 -- 실제 secret 로 교체
  endpoint  := '10.0.0.51:9002',         -- staging MinIO API 포트 (운영은 :9000)
  url_style := 'path',                       -- MinIO 는 path-style 필수
  use_ssl   := 'false'                       -- 'true'/'false' 문자열로 (boolean 아님)
);
-- 기대값: simple_s3_secret  (등록된 secret 이름)
```

이 secret 은 PG postgres_data 볼륨에 persist 되므로 컨테이너 재기동해도 유지됨.
삭제는 `SELECT duckdb.drop_simple_secret('simple_s3_secret');`.

#### (2) MinIO 객체 인벤토리 (`glob`)

```sql
-- vlm-raw 의 모든 mp4 (recursive ** 로 모든 하위 경로)
SELECT * FROM duckdb.query($$
  SELECT * FROM glob('s3://vlm-raw/**/*.mp4') LIMIT 10
$$);

-- 모든 버킷의 파일 수 / 평균 크기 (DuckDB 의 list 합집합)
SELECT * FROM duckdb.query($$
  SELECT
    regexp_extract(file, 's3://([^/]+)/', 1) AS bucket,
    COUNT(*)                                  AS files
  FROM glob('s3://vlm-raw/**')
  GROUP BY 1
  ORDER BY files DESC
$$);

-- 특정 source_unit 의 mp4 만 (확장자 + 경로 패턴 동시 필터)
SELECT * FROM duckdb.query($$
  SELECT file FROM glob('s3://vlm-raw/source-e/**/*.mp4') LIMIT 5
$$);
```

#### (3) PG 테이블 → MinIO parquet export (가장 실용적)

```sql
-- (a) raw_files 의 핵심 컬럼만 parquet 으로 archive
COPY (
  SELECT asset_id, source_unit_name, ingest_status, file_size,
         media_type, raw_bucket, raw_key, created_at
  FROM   raw_files
  WHERE  created_at >= NOW() - INTERVAL '7 days'
)
TO 's3://vlm-bench-tmp/archive/raw_files_last7d.parquet'
(FORMAT 'parquet', COMPRESSION 'zstd');
-- 출력: COPY <N>  (export 된 row 수)

-- (b) JOIN 결과를 통째로 parquet — 외부 분석가 전달용
COPY (
  SELECT
    rf.asset_id, rf.source_unit_name, rf.media_type,
    vm.width, vm.height, vm.duration_sec, vm.fps, vm.codec,
    vm.frame_extract_status, vm.auto_label_status, vm.caption_status
  FROM   raw_files       AS rf
  LEFT JOIN video_metadata AS vm USING (asset_id)
  WHERE  rf.media_type = 'video'
)
TO 's3://vlm-bench-tmp/archive/video_inventory.parquet'
(FORMAT 'parquet');

-- (c) 일자별 파티션 — Hive-style partitioning
COPY (
  SELECT *, CAST(created_at AS DATE) AS dt FROM raw_files
)
TO 's3://vlm-bench-tmp/raw_files_partitioned/'
(FORMAT 'parquet', PARTITION_BY ('dt'), OVERWRITE_OR_IGNORE 1);
-- → s3://vlm-bench-tmp/raw_files_partitioned/dt=2026-04-23/data_0.parquet
--   ... 일자별 폴더 자동 생성. Spark/Athena/BigQuery 가 그대로 read.
```

#### (4) parquet 데이터 read (검증 + 분석)

```sql
-- (a) 위 (3)(a) 가 잘 export 됐는지 read-back 검증
SELECT * FROM duckdb.query($$
  SELECT
    COUNT(*)                          AS rows,
    COUNT(DISTINCT source_unit_name)  AS units,
    MIN(created_at)                   AS oldest,
    MAX(created_at)                   AS newest
  FROM read_parquet('s3://vlm-bench-tmp/archive/raw_files_last7d.parquet')
$$);

-- (b) 외부에서 받은 parquet 직접 분석 (가상 시나리오)
SELECT * FROM duckdb.query($$
  SELECT source_unit_name, COUNT(*) AS files
  FROM   read_parquet('s3://vlm-bench-tmp/archive/raw_files_last7d.parquet')
  GROUP  BY 1
  ORDER  BY files DESC
$$);

-- (c) parquet 메타데이터만 (실제 데이터 read 없이)
SELECT * FROM duckdb.query($$
  SELECT * FROM parquet_metadata('s3://vlm-bench-tmp/archive/raw_files_last7d.parquet')
$$);
-- column 수, row group 크기, compression 등 한눈에
```

#### (5) JSON 라벨 이벤트 직접 read (vlm-labels)

```sql
-- (a) vlm-labels 안의 모든 event JSON 한 번에 union
SELECT * FROM duckdb.query($$
  SELECT
    filename,
    event_index,
    timestamp_start_sec,
    timestamp_end_sec,
    caption_text
  FROM read_json_auto('s3://vlm-labels/**/*.json',
                      filename = true,
                      union_by_name = true)
  LIMIT 50
$$);

-- (b) 특정 source 의 라벨만 — recursive glob 의 강점
SELECT * FROM duckdb.query($$
  SELECT COUNT(*) AS labels,
         AVG(timestamp_end_sec - timestamp_start_sec) AS avg_duration_sec
  FROM read_json_auto('s3://vlm-labels/source-e/**/*.json',
                      union_by_name = true)
$$);
```

#### (6) 정리 (테스트 데이터 삭제)

```sql
-- 테스트로 만든 parquet 삭제 — DuckDB 자체에는 DELETE FROM s3 가 없으므로
-- MinIO mc 또는 boto3 로 별도 정리 필요. 예:
-- (호스트에서)  mc rm --recursive --force staging/vlm-bench-tmp/archive/

-- secret 만 정리:
-- SELECT duckdb.drop_simple_secret('simple_s3_secret');
```

#### 함정 / 주의

| 증상 | 원인 / 조치 |
|------|-----------|
| `IO Error: HTTP 403 ... AccessDenied` | secret 의 key_id/secret 불일치, 또는 endpoint 가 console 포트(`:9001`/`:9003`) 로 가리킴. **API 포트** (staging `:9002`, prod `:9000`) 사용 |
| `Catalog Error: Table function with name read_parquet does not exist` | secret 등록 전, 또는 LOAD 'pg_duckdb' 미실행 |
| `glob('s3://bucket/*')` 가 0행 반환 | `*` 는 한 단계만. recursive 는 `**` 또는 `**/*.<ext>` 사용 |
| `CREATE SECRET ... (TYPE S3, ...)` syntax error | DuckDB CLI 문법. PG 안에서는 `duckdb.create_simple_secret()` 함수 사용 |
| `COPY ... TO 's3://...' (FORMAT 'parquet')` 가 권한 부족 | secret 의 `key_id` 가 PUT 권한 없는 read-only 키. write 가능한 키로 재등록 |

> ✅ **위 (1)~(4) 시퀀스를 staging 에서 실측 검증 완료** — `simple_s3_secret`
> 등록 OK, `COPY ... TO 's3://vlm-bench-tmp/...' (FORMAT 'parquet')` 으로
> 100행 export 후 `read_parquet` 으로 재read 시 동일 row count 확인.

이 경로는 sensor 운영 path 와 무관하므로 (§3 와 별개) 검증 안 해도 파이프라인
영향은 없다. 다만 **PG primary 모드의 가장 큰 이점** 이 이 부분이라 (DuckDB
파일 1개에 갇혀 있던 데이터를 객체 스토리지로 자유롭게 export/import 가능),
한 번은 동작 확인해 두는 것을 권장.

---

## 3. DuckDB → PG ATTACH 검증 (sensor 운영 path 와 동일)

> 🔑 이 섹션은 **운영 sensor read path 그 자체** 를 재현한다.
> [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py)
> 의 `open_sensor_read_connection()` 코드:
>
> ```python
> con = duckdb.connect(":memory:")
> con.execute("LOAD postgres")
> con.execute(f"ATTACH '{escaped}' AS pg (TYPE postgres, READ_ONLY)")
> con.execute("USE pg.public")
> return con
> ```
>
> §3.3 의 SQL 시퀀스가 이 5줄을 바이트 단위로 재현한다. 따라서 §3.3 가 통과하면
> sensor read path 도 통과하는 것과 같다.

### 3.1 SSH 터널 — DBeaver 안에서 처리하기 (별도 터미널 불필요)

별도 터미널에 `ssh -L ...` 을 띄워두는 대신, **DuckDB connection 자체에 SSH
터널 설정을 박아두면** DBeaver 가 연결 열 때 자동으로 터널을 띄우고 닫을 때
자동으로 정리한다. 매번 SSH 명령을 다시 칠 필요 없음.

DBeaver 의 SSH 터널은 "이 connection 을 여는 동안만" 살아 있는 OS 레벨 포트
포워딩을 만든다. 그래서 DuckDB connection 이 열려 있는 동안에는 `localhost:15432`
로 접근하면 staging PG 에 정확히 도달한다 — `ATTACH 'host=localhost port=15432 ...'`
SQL 이 그대로 동작.

설정은 §3.2 의 `SSH 탭` 에서 한다. 별도 터미널 없이 연결만 열면 끝.

> 💡 그래도 OS 레벨 터널이 필요한 경우:
> - DBeaver 가 떠 있지 않을 때도 다른 도구(`psql`, `duckdb` CLI 등)에서 접속하고
>   싶다 → §3.1.1 참고
> - DBeaver 의 SSH 터널이 본인 환경에서 동작하지 않는다 (드물지만 가능) → §3.1.1

#### 3.1.1 (대안) `~/.ssh/config` + autossh — 항상 떠 있는 터널

별도 도구에서도 쓸 일이 잦으면 Mac 의 `~/.ssh/config` 에 한 번 적어두는 게 깔끔하다.

```ssh-config
# ~/.ssh/config
Host staging-pg
    HostName       10.0.0.10
    User           <ssh_user>
    LocalForward   15432 localhost:15432
    ServerAliveInterval 30
    ExitOnForwardFailure yes
```

평소엔:

```bash
# 한 번만 띄우면 끝 (background)
ssh -fN staging-pg

# 끊김 자동 복구가 필요하면 autossh
brew install autossh
autossh -fN staging-pg
```

이 경우 DBeaver 의 SSH 탭은 **비워두고** Path 만 `:memory:` 로 한다.

### 3.2 DBeaver DuckDB 연결 설정

**New Database Connection → DuckDB.**

처음 연결할 때 드라이버 자동 다운로드 다이얼로그가 뜨면 `Download` 클릭.

#### Main 탭

| Field | 값 |
|-------|----|
| Path | `:memory:` — RAM 에 임시 작업공간만 만든다는 뜻. DuckDB 자체에 저장할 게 없으므로 파일 만들 필요 없음. (영구 저장이 필요하면 `/tmp/scratch.duckdb` 같은 파일 경로) |

#### SSH 탭 (§3.1 본문 방식 — 권장)

`Use SSH Tunnel` 체크 후:

| Field | 값 |
|-------|----|
| Host/IP | `10.0.0.10` |
| Port | `22` |
| User Name | (Mac 에서 평소 ssh 할 때 쓰는 계정) |
| Authentication Method | `Public Key` (`~/.ssh/id_ed25519` 또는 `id_rsa`) |
| Local host | `localhost` |
| **Local port** | **`15432`** ← `ATTACH` SQL 의 `port=15432` 와 일치시킬 것 |
| Remote host | `localhost` *(staging 서버 입장에서 본 PG 호스트 — 컨테이너가 호스트에 publish 한 포트가 0.0.0.0:15432 이므로 `localhost` 로 충분)* |
| Remote port | `15432` |

> ⚠️ **Local port 를 0(auto) 로 두면 안 된다.** `ATTACH` SQL 안에서 정해진 포트를
> 써야 하므로, 명시적으로 `15432` 로 지정해야 한다. 다른 프로세스가 이미 15432 를
> 쓰고 있다면 `15433`, `15434` 등으로 바꾸고 §3.3 의 ATTACH SQL 도 같이 변경.

`Test tunnel configuration` → `Connected` 확인 후 `Test Connection` → `Finish`.

§3.1.1 의 `~/.ssh/config` 방식을 쓴다면 SSH 탭은 통째로 비워둔다.

### 3.3 ATTACH + sensor 코드와 동일한 SQL 실행

> ⚠️ **테이블 참조는 `pg.<table>` 형태로** 작성한다 (`pg.public.<table>` 또는
> `USE pg.public;` 후 unqualified 이름 모두 Mac DuckDB 1.4.x 이상에서는 동작
> 안 함). DuckDB 의 postgres extension 이 ATTACH 시 PG `public` 스키마를
> 자동 평탄화해 `pg.<table>` 로 노출하기 때문. 컨테이너 안 DuckDB 는 `USE pg.public`
> 이 동작하는 버전을 쓰므로 [`sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py)
> 코드는 정상 — DBeaver(Mac) 에서만 이 형태가 필요하다.

> 💡 DBeaver SQL 편집기에서 실행할 때는 statement 단위로 `Cmd+Enter`,
> 또는 전체 한 번에 돌리려면 `Cmd+Alt+Enter` (Execute SQL Script). 주석 줄에
> 커서가 있으면 `No statements to execute` 에러가 나니 SQL 이 있는 줄에
> 커서를 두고 실행.

```sql
-- (a) DuckDB 자체 동작 확인
SELECT version();
SELECT extension_name, installed, loaded
FROM   duckdb_extensions()
WHERE  extension_name = 'postgres';
-- 기대값: installed=true, loaded=false (Mac DuckDB 는 첫 실행 시 INSTALL 필요할 수 있음)
-- 결과가 비어있으면 (b) 의 INSTALL 로 바로 진행하면 됨.

-- (b) postgres extension 로드
INSTALL postgres;   -- 이미 설치돼 있으면 no-op
LOAD postgres;

-- (c) PG ATTACH (READ_ONLY) — sensor_db.open_sensor_read_connection() 와 동일
ATTACH 'host=localhost port=15432 dbname=vlm_pipeline_staging user=airflow password=airflow'
  AS pg (TYPE postgres, READ_ONLY);

-- (d) 16개 테이블 보이는가
SHOW TABLES FROM pg;

-- (e) row count — §2.2 (b) 결과와 정확히 일치해야 함
SELECT 'raw_files'              AS t, COUNT(*) FROM pg.raw_files
UNION ALL SELECT 'video_metadata',         COUNT(*) FROM pg.video_metadata
UNION ALL SELECT 'image_metadata',         COUNT(*) FROM pg.image_metadata
UNION ALL SELECT 'dispatch_requests',      COUNT(*) FROM pg.dispatch_requests
UNION ALL SELECT 'dispatch_pipeline_runs', COUNT(*) FROM pg.dispatch_pipeline_runs
UNION ALL SELECT 'labels',                 COUNT(*) FROM pg.labels
UNION ALL SELECT 'image_labels',           COUNT(*) FROM pg.image_labels
UNION ALL SELECT 'processed_clips',        COUNT(*) FROM pg.processed_clips
UNION ALL SELECT 'datasets',               COUNT(*) FROM pg.datasets
UNION ALL SELECT 'dataset_clips',          COUNT(*) FROM pg.dataset_clips
ORDER BY t;
```

### 3.4 DuckDB columnar 가속 확인 (PG GROUP BY 가속)

```sql
SELECT
  source_unit_name,
  COUNT(*)                                          AS files,
  COUNT(*) FILTER (WHERE ingest_status='completed') AS done
FROM   pg.raw_files
GROUP  BY 1
ORDER  BY files DESC
LIMIT  20;
```

같은 쿼리를 DBeaver PG 연결(§2)에서도 (`pg.` 접두사 없이) 돌려 결과가 동일한지 비교.

### 3.5 READ_ONLY 가드 검증 (반드시 에러나야 함)

```sql
INSERT INTO pg.raw_files (file_path) VALUES ('/tmp/should-fail');
-- 기대값: ERROR — read-only attached database 류
-- 에러가 안 나면 ATTACH 옵션이 잘못된 것이므로 즉시 보고할 것.
```

### 3.6 Live read 검증 (선택, end-to-end)

PG 가 운영 중에 새 row 가 들어오는지를 DuckDB ATTACH 가 즉시 보는지 검증.

1. 터미널 A — staging dispatch 트리거 (서버에서):
   ```bash
   ssh 10.0.0.10
   cd /home/user/work_p/Datapipeline-Data-data_pipeline_test
   python3 scripts/staging_test_dispatch.py --folder tmp_data_2 --round 1
   ```

2. 터미널 B — 같은 DBeaver DuckDB 편집기에서 §3.3 (e) 쿼리 5분 후 재실행.
   `raw_files`, `dispatch_requests` 카운트가 증가했으면 sensor PG write + DuckDB read 모두 정상.

---

## 4. (옵션) 컨테이너 내부 path 와의 일치 검증

Mac 의 DuckDB 버전과 컨테이너의 DuckDB 버전이 다를 경우 ATTACH 동작이 미묘하게
다를 수 있다. 컨테이너 안에서도 동일 결과가 나오는지 한 번은 확인:

```bash
ssh 10.0.0.10
docker exec -it pipeline-test-dagster-1 python3 - <<'PY'
from vlm_pipeline.lib.sensor_db import open_sensor_read_connection
con = open_sensor_read_connection()
print("tables   :", [r[0] for r in con.execute("SHOW TABLES").fetchall()])
print("raw_files:", con.execute("SELECT COUNT(*) FROM raw_files").fetchone()[0])
print("loaded   :", con.execute(
    "SELECT extension_name FROM duckdb_extensions() WHERE loaded").fetchall())
con.close()
PY
```

기대 결과: `raw_files` 카운트가 §2.2 (b) / §3.3 (e) 와 모두 일치.

---

## 5. 문제 해결

| 증상 | 원인 / 조치 |
|------|------------|
| DBeaver PG 연결 timeout | SSH 터널이 안 떠 있거나 망 정책. §1.2 ssh 직접 테스트 → §2.1 SSH 터널 다시 설정 |
| `LOAD 'pg_duckdb'` → `could not load library` | 컨테이너 부팅 시 `shared_preload_libraries` 미적용. `docker exec pipeline-test-postgres-1 psql ... -c "SHOW shared_preload_libraries"` 로 재확인. compose 의 postgres `command:` 값이 비어 있으면 `.env.test` 의 `POSTGRES_PRELOAD_LIBS=pg_duckdb` 누락 의심 |
| `(PGDuckDB/CreatePlan) ... libpq is incorrectly linked to backend functions` | `postgres_scan('host=localhost ...')` 를 같은 DB 안에서 호출함. `postgres_scan` 은 다른 PG 인스턴스 전용. 같은 DB 의 PG 테이블 분석은 `SET duckdb.force_execution = true; SELECT ...` 로 (§2.3 (d)) |
| `(PGDuckDB/CreatePlan) ... Catalog Error: Table with name raw_files does not exist! Did you mean "pg_views"?` | `SELECT * FROM duckdb.query($$ ... FROM raw_files $$)` 형태로 호출. `duckdb.query` 는 DuckDB 자체 카탈로그(parquet/S3 등)만 봄, PG 테이블은 못 봄. 같은 §2.3 (d) 의 `force_execution` 패턴으로 |
| `function quantile_cont(bigint, numeric) does not exist` (force_execution 모드) | pg_duckdb 는 SQL 을 PG 파서로 먼저 검증 후 DuckDB 로 실행. DuckDB-native 함수는 PG 파서가 모르므로 거부됨. PG 호환 표준 SQL (`percentile_cont WITHIN GROUP`, `AVG`, `COUNT` 등) 만 사용 |
| sensor 가 PG 가 아닌 legacy DuckDB 를 읽고 있음 (§3 결과는 맞는데 카운트가 §2 와 다름) | 컨테이너 안 `DATAOPS_POSTGRES_DSN` 미설정 → §1.4 사전 점검 다시. `.env.test` 변경 후 `docker compose up -d` 미반영일 수 있음 |
| DBeaver DuckDB → ATTACH 시 `connection refused` | DBeaver SSH 터널이 안 떠 있음. (a) §3.2 SSH 탭의 `Test tunnel configuration` 다시. (b) Local port 가 15432 가 아니라 auto(0) 로 설정됨 — 명시적으로 15432. (c) `~/.ssh/config` 방식이라면 `lsof -i :15432` 로 터널 listener 확인 |
| DBeaver SSH 터널은 Test 통과하는데 ATTACH 만 거부됨 | DBeaver 가 connection 을 아직 "open" 으로 보지 않아 터널이 안 띄워졌을 수 있음. SQL 편집기에서 일단 `SELECT 1;` 한 번 실행해 connection 을 확정시킨 뒤 ATTACH 재시도. 그래도 안 되면 §3.1.1 `~/.ssh/config` 방식으로 전환 |
| Local port 15432 이미 사용 중 | 다른 프로세스가 점유. `lsof -i :15432` 로 확인 후 종료하거나, §3.2 의 Local port 와 §3.3 (c) ATTACH SQL 의 `port=` 를 동일한 새 포트(예: 15433)로 변경 |
| ATTACH 됐는데 `SHOW TABLES` 비어 있음 | `SHOW TABLES FROM pg;` 로 attached DB 명시. DuckDB 는 default DB 가 in-memory 라 attach 한 `pg` 를 명시해야 함 |
| `Table with name raw_files does not exist! Did you mean "pg.raw_files"?` | Mac DuckDB 1.4.x 이상은 ATTACH 시 PG `public` 스키마를 평탄화해 `pg.<table>` 형태로 노출. **`pg.raw_files`** 로 쓸 것. `USE pg.public` 은 이 버전에서 안 먹음 |
| `No statements to execute` (DBeaver) | 커서가 주석(`--`) 줄에 있어서 발생. SQL 이 있는 줄로 커서 옮기거나, 전체 선택 후 `Cmd+Alt+Enter` (Execute SQL Script) |
| `duckdb_extensions()` 결과가 비어있음 | DuckDB 가 카탈로그를 아직 안 채운 상태. 그냥 §3.3 (b) 의 `INSTALL postgres; LOAD postgres;` 로 바로 진행하면 됨 |
| INSERT 가 성공해 버림 | ATTACH 옵션에서 `READ_ONLY` 누락. 즉시 ROLLBACK 후 `DETACH pg;` 하고 §3.3 (c) 다시 실행 |
| Mac duckdb 버전 ≠ 서버 버전 | §4 의 컨테이너 내부 path 와 결과가 다르면 Mac duckdb 를 서버와 같은 minor version 으로 맞출 것 (`brew install duckdb@1.1` 등) |

---

## 6. 체크리스트 (한 페이지 요약)

**사전 점검**
- [ ] §1.4 컨테이너 안 `BACKEND=postgres`, `DSN_SET=yes` 확인

**§2 PG 연결 + `pg_duckdb` (인프라 스모크)**
- [ ] §2.1 DBeaver PG 연결 (SSH 터널) 통과
- [ ] §2.2 (a) 16개 테이블 모두 보임
- [ ] §2.2 (b) row count snapshot 캡처
- [ ] §2.3 (a)(b) `shared_preload_libraries=pg_duckdb`, extension 1.1.0
- [ ] §2.3 (c)(d) `duckdb.query()` 동작, `postgres_scan` 카운트 일치

**§3 DuckDB → PG ATTACH (운영 sensor read path)**
- [ ] §3.2 DBeaver DuckDB 연결 SSH 터널 (Local port=15432) Test 통과
- [ ] §3.3 (b) postgres extension LOAD 성공
- [ ] §3.3 (b) postgres extension LOAD 성공
- [ ] §3.3 (c)(d) ATTACH 후 16개 테이블 보임
- [ ] §3.3 (e) row count §2.2 (b) 와 일치
- [ ] §3.5 INSERT 가 READ_ONLY 로 거부됨
- [ ] (옵션) §3.6 dispatch 트리거 후 카운트 증가 확인
- [ ] (옵션) §4 컨테이너 내부 결과와 일치

---

## 7. 운영(production) 환경에 적용할 때

> 본 가이드는 staging(`dev`, `:3031`)을 기준으로 작성됐다. 운영(`main`, `:3030`)에
> 같은 검증을 적용하려면 **사전 조건**과 **차이점**이 다르다. 본 절을 먼저 읽고
> 반드시 §[Production PG 전환 롤아웃 계획](../exec-plans/active/production-pg-rollout-plan.md)
> 의 단계 진행 상황을 확인한 뒤 진행할 것.

### 7.1 사전 조건 (이게 안 됐으면 운영 검증 의미 없음)

운영 환경 검증은 [Production PG 롤아웃](../exec-plans/active/production-pg-rollout-plan.md)
**Phase 2 (postgres 이미지 swap) 완료 이후** 부터 의미가 있다. 그 이전 단계에서는:

- Phase 1 (코드 머지만 됨): vanilla postgres 이미지 그대로 → §2 (`pg_duckdb`)
  검증 불가 (extension 자체가 없음)
- Phase 2 (이미지 swap 완료): §2 검증 가능. 단 `vlm_pipeline` DB 가 아직 비어
  있으니 §3 의 row count 는 0.
- Phase 3-4 (DB 생성 + 백필 완료): §2/§3 모두 의미 있음. 단 sensor 는 여전히
  DuckDB primary 라 §3 결과는 *과거 시점 스냅샷*.
- **Phase 5 (`dual_pg_primary`) 진입 후 부터 §3.6 live read 검증이 의미 있음**.
  이때부터는 sensor 가 PG ATTACH 로 read 하는 진짜 운영 path.

### 7.2 staging vs production 차이 한 페이지 표

| 항목 | staging (`:3031`) | **production (`:3030`)** | 비고 |
|------|------------------|---------------------------|------|
| Dagster UI | `http://10.0.0.10:3031` | `http://10.0.0.10:3030` | |
| Postgres 컨테이너 이름 | `pipeline-test-postgres-1` | `docker-postgres-1` | `docker exec` 시 사용 |
| Dagster 컨테이너 이름 | `pipeline-test-dagster-1` | `docker-dagster-1` | |
| Compose project | `pipeline-test` | `docker` | |
| DB 이름 | `vlm_pipeline_staging` | **`vlm_pipeline`** | 모든 DSN/연결의 dbname 변경 |
| DSN (컨테이너 내부) | `postgresql://airflow:airflow@postgres:5432/vlm_pipeline_staging` | `postgresql://airflow:airflow@postgres:5432/vlm_pipeline` | |
| 호스트 PG 포트 (publish) | `15432` | **기본 미공개** — 검증 시 `.env` 에 `POSTGRES_PORT=15433` 임시 추가 (§7.3) | staging 과 충돌 회피 |
| DuckDB 파일 (컨테이너) | `/data/staging.duckdb` | `/data/pipeline.duckdb` | §3.6 live read 비교 시 |
| DuckDB 파일 (호스트) | `Datapipeline-Data-data_pipeline_test/docker/data/staging.duckdb` | `Datapipeline-Data-data_pipeline/docker/data/pipeline.duckdb` | mtime 모니터 시 경로 |
| MinIO 엔드포인트 (API) | `10.0.0.51:9002` | **`10.0.0.51:9000`** | §2.4 secret 생성 시 |
| MinIO Console | `:9003` | `:9001` | 확인용 UI |
| MinIO 자격 증명 | `minioadmin/minioadmin` (default) | `.env` 의 `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` 확인 (보통 다름) | |
| `.env` 파일 | `docker/.env.test` (host: `..._test/docker/.env.test`) | `docker/.env` | git 미추적 |
| 자동 재배포 트리거 | `dev` push | `main` push | 호스트 src 수동 편집 금지 |

### 7.3 운영 PG 호스트 포트 임시 expose

Production `.env` 는 `POSTGRES_PORT` 가 기본 미설정이라 호스트 포트로 publish
안 한다 (보안). DBeaver 검증을 하려면 임시로 추가:

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# .env 편집 (호스트에서, vi 등으로)
# 다음 한 줄 추가 — staging 의 15432 와 충돌 회피하려고 15433 사용
# POSTGRES_PORT=15433

cd docker
docker compose up -d postgres   # postgres 만 recreate (다른 서비스 영향 없음)
docker port docker-postgres-1   # 5432/tcp -> 0.0.0.0:15433 확인
```

**검증 끝나면 반드시 되돌림** (보안):

```bash
# .env 의 POSTGRES_PORT=15433 줄 삭제 후
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose up -d postgres
docker port docker-postgres-1   # (출력 없음 → 호스트 포트 닫힘 확인)
```

### 7.4 SSH config 에 prod-pg 블록 추가 (staging-pg 와 분리)

Mac `~/.ssh/config` 에 staging 용(`staging-pg`)과 별개로:

```ssh-config
Host prod-pg
    HostName       10.0.0.10
    User           user
    LocalForward   15433 localhost:15433       # ← staging 의 15432 와 분리
    ServerAliveInterval 30
    ExitOnForwardFailure yes
```

> ⚠️ staging-pg 와 동시에 띄울 수 있도록 **포트 다른 값**(15433) 으로. 같은
> 포트(15432)로 둘 다 띄우면 두 번째가 bind 실패.

띄우기:

```bash
autossh -M 0 -fN prod-pg
lsof -i :15433       # ssh ... LISTEN 확인
nc -zv localhost 15433
```

### 7.5 DBeaver 연결 설정 변경 (운영용 connection 별도 추가)

**`prod-vlm-pipeline (PostgreSQL)`** 이라는 이름으로 별도 connection 만든다.
staging connection 은 그대로 두고:

| 탭 | Field | 값 |
|----|-------|-----|
| Main | Host | `localhost` (SSH 터널) 또는 `10.0.0.10` (직접) |
| Main | **Port** | **`15433`** |
| Main | **Database** | **`vlm_pipeline`** |
| Main | Username/Password | `airflow` / `airflow` (운영 PG 자격 — 다르면 `.env` 의 `POSTGRES_USER`/`POSTGRES_PASSWORD` 확인) |
| SSH (선택) | Host/User | `10.0.0.10` / `user` |
| SSH (선택) | Local port | `15433` (auto 금지) |

DuckDB connection 도 별도로 만든다 (`prod-duckdb-attach`). ATTACH SQL 의 DSN 만 바꿈:

```sql
ATTACH 'host=localhost port=15433 dbname=vlm_pipeline user=airflow password=airflow'
  AS pg (TYPE postgres, READ_ONLY);
```

### 7.6 §1.4 (사전 점검) 운영 버전

```bash
ssh 10.0.0.10
docker exec docker-dagster-1 sh -c '
  echo BACKEND=$DATAOPS_DB_BACKEND;
  echo DSN_SET=$([ -n "$DATAOPS_POSTGRES_DSN" ] && echo yes || echo no)
'
```

기대값은 **롤아웃 phase 에 따라 다르다**:

| Phase | BACKEND | DSN_SET |
|-------|---------|---------|
| 1 (코드 merge 완료) | (빈값) | no |
| 2-4 (이미지 swap, DB/백필 완료) | (빈값) | no |
| **5 (dual_pg_primary)** | `dual_pg_primary` | **yes** |
| **6 (postgres single)** | `postgres` | **yes** |

§3 의 `SHOW TABLES FROM pg` / row count 검증이 의미 있어지는 건 **Phase 5 부터**.

### 7.7 §2/§3 SQL 의 운영 환경 차이

대부분 SQL 은 그대로 동작하지만, **DSN string 안의 dbname**은 반드시 바꿔야 함.

#### §2.4 의 MinIO secret — 운영용

```sql
-- 운영 MinIO endpoint 와 자격증명 확인 후 (이전 staging secret 과 다른 이름)
SELECT duckdb.create_simple_secret(
  type      := 'S3',
  key_id    := '<운영 MINIO_ROOT_USER>',
  secret    := '<운영 MINIO_ROOT_PASSWORD>',
  endpoint  := '10.0.0.51:9000',         -- ← staging 9002 가 아님
  url_style := 'path',
  use_ssl   := 'false'
);
```

> ⚠️ secret 은 PG 단위로 persist 되므로, 운영 PG 안에서 등록한 secret 은
> staging PG 와 무관. 이름 충돌 걱정 없음. 단 **운영 PG 안에 **운영용 자격
> 증명이 평문으로 저장**된다는 점 유의 — 운영자만 PG superuser 권한 가지도록
> 관리.

#### §2.4 (3) PG → parquet export — 운영 환경 안전 가드

```sql
-- ❌ 운영 데이터 bucket 에 직접 export 금지
-- COPY (...) TO 's3://vlm-labels/...' (FORMAT 'parquet');     -- 라벨 source of truth 오염

-- ✅ 항상 분석 전용 bucket / 폴더 사용
COPY (SELECT ... FROM raw_files LIMIT 1000)
TO 's3://vlm-bench-tmp/adhoc/raw_files_snapshot.parquet'
(FORMAT 'parquet');
```

운영 5개 핵심 bucket(`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`,
`vlm-classification`)에는 ad-hoc 분석 파일을 **절대** 쓰지 말 것. 별도 bucket
(예: `vlm-bench-tmp` 또는 새로 만든 `vlm-analysis`)만 사용.

### 7.8 운영 환경 절대 금지 사항 (반복)

- **READ_ONLY 누락한 ATTACH 금지** — sensor 코드는 `READ_ONLY` 명시. ad-hoc 검증도 마찬가지. 한 번 write 가능 ATTACH 로 잘못 INSERT 하면 source of truth 오염
- **운영 데이터 bucket 에 ad-hoc 파일 쓰기 금지** (§7.7)
- **`POSTGRES_PORT=15433` 검증용 임시 publish 후 정리 안 함 → 보안 hole**. 항상 §7.3 마지막 단락대로 되돌리기
- **다른 사람 검증 중에 sensor / dispatch 트리거 금지** — live read 카운트가 흔들려서 검증 결과 신뢰도 깨짐
- **검증 끝난 secret 정리** — `SELECT duckdb.drop_simple_secret('simple_s3_secret');`. 사람이 바뀌어도 자격증명 평문이 PG 안에 영원히 남는 것 방지

### 7.9 운영 검증 체크리스트 (§6 운영 버전)

- [ ] §7.1 롤아웃 어느 phase 인지 확인 (Phase 5 미만이면 §3.6 live read 무의미)
- [ ] §7.3 `POSTGRES_PORT=15433` 임시 publish, `docker compose up -d postgres` 통과
- [ ] §7.4 `prod-pg` Host 블록 + autossh `-M 0 -fN` 통과, `nc -zv localhost 15433` succeeded
- [ ] §7.5 DBeaver 운영 connection (PG + DuckDB) 통과 — staging connection 과 분리
- [ ] §7.6 `BACKEND` / `DSN_SET` 이 phase 와 일치
- [ ] §2.2 (b) 운영 PG row count snapshot 캡처 (운영 데이터라 staging 보다 많을 것)
- [ ] §3.3 (e) DuckDB ATTACH 결과가 §2.2 와 동일
- [ ] §3.5 INSERT 거부 확인 (운영에서 매우 중요)
- [ ] **검증 종료 후** §7.3 의 `POSTGRES_PORT` 줄 제거 + `docker compose up -d postgres`
- [ ] **검증 종료 후** `pkill -f prod-pg` 로 autossh 정리
- [ ] **검증 종료 후** `SELECT duckdb.drop_simple_secret(...)` 로 secret 정리

---

## 참고

- 기능 명세: [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py) (`open_sensor_read_connection`)
- 환경 분리 정책: [`docs/exec-plans/운영_테스트_환경_분리_자동배포_계획.md`](../exec-plans/운영_테스트_환경_분리_자동배포_계획.md)
- DB 마이그레이션 토폴로지: [`db_migration_topology.md`](db_migration_topology.md)
- pg_duckdb 공식: <https://github.com/duckdb/pg_duckdb>
- DuckDB postgres_scanner 공식: <https://duckdb.org/docs/extensions/postgres>
