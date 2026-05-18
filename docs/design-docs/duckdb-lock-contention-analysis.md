# DuckDB Lock Contention 근본 원인 분석

> 날짜: 2026-04-03 | 분석 세션: debug-578762

## 1. 문제 현상

`ingest_job` 실행 중 DuckDB write lock 충돌이 반복 발생.
에러 메시지: `IO Error: Could not set lock on file "/data/pipeline.duckdb": Conflicting lock is held in /usr/bin/python3.10 (PID N)`

Lock holder PID는 **dagster-code-server의 gRPC 프로세스** 또는 **dagster-webserver**로 확인됨.

## 2. 근본 원인

### 센서가 매 tick마다 DuckDB write lock을 장기 점유

Dagster 센서는 `dagster-code-server`의 gRPC 프로세스(PID 56) 안에서 실행된다.
센서 실행 시 `db.ensure_runtime_schema()`를 호출하면 **`duckdb.connect(db_path)`**(기본 read_write 모드)로 연결이 열려 write lock을 획득한다.

### 런타임 로그 증거

| 센서 | 호출 | lock 보유 시간 |
|------|------|---------------|
| `production_agent_dispatch_sensor` | `ensure_runtime_schema()` | **~5.8초** |
| `production_agent_dispatch_sensor` | `ensure_dispatch_tracking_tables()` | **~1.2초** |
| `dispatch_sensor` | `ensure_runtime_schema()` | **~5.8초** |
| `dispatch_sensor` | `ensure_dispatch_tracking_tables()` | **~1.2초** |

`duckdb.connect()` 획득 자체에도 **~1.2~1.4초** 소요 (DuckDB WAL/내부 초기화).

### 시간 점유율

매 ~30~34초 cycle마다 **두 센서가 순차적으로 총 ~14초** write lock을 점유.
**전체 시간의 약 47%가 센서의 불필요한 DDL 실행에 소비**됨.

```
Timeline (30초 cycle):
[=====센서A schema 5.8s=====][==센서A dispatch 1.2s==][gap][=====센서B schema 5.8s=====][==센서B dispatch 1.2s==]
|<--------------------------- ~14초 lock 점유 ---------------------------->|<--- ~16초 가용 --->|
```

### 왜 `ensure_runtime_schema()`가 느린가

`ensure_runtime_schema()` 내부에서 1개의 write connection으로:
1. `schema.sql` DDL 전체 실행 (`CREATE TABLE IF NOT EXISTS` × 8 테이블)
2. `_ensure_image_metadata_columns()` — 컬럼 존재 체크 + 조건부 ALTER
3. `_ensure_video_metadata_frame_columns()` — 동일 패턴
4. `_ensure_labels_columns()` — 동일 패턴
5. `_ensure_processed_clips_columns()` — 동일 패턴
6. `_ensure_image_labels_table()` — 동일 패턴
7. `_ensure_staging_dispatch_columns()` — 동일 패턴
8. `_ensure_staging_model_configs()` — 동일 패턴
9. `_ensure_staging_pipeline_runs()` — 동일 패턴
10. `_ensure_raw_files_spec_columns()` — 동일 패턴
11. `_ensure_video_metadata_stage_columns()` — 동일 패턴
12. `_ensure_video_metadata_reencode_columns()` — 동일 패턴

각 `_ensure_*` 메서드는 `information_schema.columns` 조회 + 조건부 `ALTER TABLE ADD COLUMN`을 수행한다.
스키마가 이미 완전한 상태에서도 **12회의 메타데이터 조회**가 write lock 내에서 순차 실행되어 ~5.8초가 소요된다.

## 3. 영향 받는 센서 (Production)

| 센서 | interval | ensure_runtime_schema 호출 | write lock |
|------|----------|--------------------------|------------|
| `dispatch_sensor` | 30초 | ✅ + `ensure_dispatch_tracking_tables` | ✅ |
| `production_agent_dispatch_sensor` | 설정값 | ✅ + `ensure_dispatch_tracking_tables` | ✅ |
| `spec_resolve_sensor` | 60초 | ✅ + `db.connect()` (write) | ✅ |
| `ready_for_labeling_sensor` | 60초 | ✅ | ✅ |
| `incoming_manifest_sensor` | 180초 | ❌ | ❌ |
| `auto_bootstrap_manifest_sensor` | 180초 | ❌ | ❌ |
| `stuck_run_guard_sensor` | 120초 | ❌ | ❌ |

read_only 직접 연결 센서 (영향 없음):
- `process/sensor.py` — `duckdb.connect(read_only=True)`
- `sync/sensor.py` — `duckdb.connect(read_only=True)`
- `yolo/sensor.py` — `duckdb.connect(read_only=True)`
- `label/sensor.py` — `duckdb.connect(read_only=True)`

## 4. 수정 방향 제안

### 4.1 `ensure_runtime_schema()` 호출 최소화 (높은 우선순위)

**목표:** 센서에서 매 tick마다 `ensure_runtime_schema()`를 호출하지 않도록 한다.

**방안 A — 프로세스 레벨 1회 초기화 (권장)**
- `DuckDBResource`에 class-level flag `_schema_ensured: bool = False`를 추가
- `ensure_runtime_schema()` 첫 호출 시만 실행, 이후 skip
- 장점: 코드 변경 최소, 프로세스 시작 시 1회만 DDL 실행
- 단점: hot-reload 시 flag 리셋 필요 (Dagster code-server 재시작으로 충분)

**방안 B — 시작 시 1회만 호출**
- 센서 코드에서 `ensure_runtime_schema()` 호출 제거
- 대신 `DuckDBResource`의 `setup_for_execution()` 또는 별도 startup hook에서 1회 실행
- 장점: 센서 코드가 깔끔해짐
- 단점: Dagster lifecycle hook 의존

### 4.2 센서의 DuckDB 접근을 read_only로 전환 (중간 우선순위)

`spec_resolve_sensor`를 제외한 센서들이 `ensure_runtime_schema()` 이후 실제로 write하는 로직이 있는지 검토.
DDL 보장이 불필요한 센서는 `duckdb.connect(read_only=True)`로 전환 가능.

### 4.3 `ensure_runtime_schema()` 성능 최적화 (낮은 우선순위)

- 12개 `_ensure_*` 호출을 단일 쿼리(information_schema 일괄 조회)로 통합
- 누락 컬럼이 없으면 ALTER TABLE 건너뛰기
- DDL 자체가 이미 `IF NOT EXISTS`이므로 큰 비용은 아니지만, 메타데이터 조회 비용 절감 가능

## 5. 기대 효과

| 지표 | 현재 | 수정 후 |
|------|------|---------|
| 센서 cycle당 write lock 시간 | ~14초 | ~0초 (방안 A/B) |
| DuckDB 가용 시간 비율 | ~53% | ~100% |
| ingest_job lock conflict 확률 | 높음 (47% overlap) | 거의 0% |
