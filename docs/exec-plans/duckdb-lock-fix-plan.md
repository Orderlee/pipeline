# DuckDB Lock Contention 수정 실행 계획

> 근거 문서: [DuckDB Lock Contention 분석](../design-docs/duckdb-lock-contention-analysis.md)
> 날짜: 2026-04-03

## 목표

센서의 반복적 `ensure_runtime_schema()` 호출을 제거하여 DuckDB write lock 점유율을 ~47% → ~0%로 낮추고, `ingest_job` lock conflict를 근본적으로 해소한다.

## Phase 1: `ensure_runtime_schema()` 호출을 프로세스 레벨 1회로 제한

### 변경 대상

**`src/vlm_pipeline/resources/duckdb_migration.py`** — `ensure_runtime_schema()` 메서드

### 설계

```python
# DuckDBMigrationMixin 클래스에 추가
_runtime_schema_ensured: ClassVar[bool] = False

def ensure_runtime_schema(self) -> None:
    if DuckDBMigrationMixin._runtime_schema_ensured:
        return
    # 기존 DDL + _ensure_* 로직 실행
    ...
    DuckDBMigrationMixin._runtime_schema_ensured = True
```

- `ClassVar[bool]`로 선언하여 Pydantic/ConfigurableResource의 필드 검증을 우회
- 프로세스 내 모든 `DuckDBResource` 인스턴스가 flag를 공유
- 첫 호출 시만 DDL 실행, 이후 즉시 return
- code-server 재시작 시 자동 리셋 (프로세스 수준 변수)

### 영향 범위

- `ensure_runtime_schema()` 호출하는 모든 센서/에셋이 자동으로 혜택을 받음
- 호출 코드 변경 불필요 — 내부 로직만 변경

### 리스크

- 극히 낮음: 스키마가 프로세스 시작 후 동적으로 변경되는 경우는 없음
- `ensure_schema()`(전체 마이그레이션)는 flag와 무관하게 항상 실행됨

## Phase 2: `ensure_dispatch_tracking_tables()` 동일 패턴 적용

### 변경 대상

**`src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`**

### 설계

```python
_dispatch_tables_ensured: ClassVar[bool] = False

def ensure_dispatch_tracking_tables(self) -> None:
    if DuckDBIngestDispatchMixin._dispatch_tables_ensured:
        return
    # 기존 CREATE TABLE IF NOT EXISTS 실행
    ...
    DuckDBIngestDispatchMixin._dispatch_tables_ensured = True
```

## Phase 3: 센서 DuckDB 접근 모드 검토 (선택)

`spec_resolve_sensor`와 `ready_for_labeling_sensor`가 `ensure_runtime_schema()` 이후 실제로 write하는 로직:
- `db.update_spec_status()`, `db.update_spec_resolved_config()` → **write 필요**
- 이들은 `duckdb_writer` 태그가 없으므로 concurrency 제한 밖에 있음

Phase 1~2만으로도 lock 점유가 첫 호출 1회(~7초)로 줄어들어, Phase 3은 선택 사항.

## Phase 4: Instrumentation 제거

분석 완료 후 `duckdb_base.py`의 debug log instrumentation 제거.

## 기대 효과

| 지표 | 현재 | Phase 1+2 완료 후 |
|------|------|-------------------|
| 센서 cycle당 write lock | ~14초 | 0초 (첫 cycle만 ~7초) |
| 30초 내 DuckDB 가용률 | ~53% | ~100% |
| ingest_job lock conflict | 반복 발생 | 거의 0 |

## 구현 순서

1. Phase 1: `duckdb_migration.py` 수정
2. Phase 2: `duckdb_ingest_dispatch.py` 수정
3. Docker 이미지 rebuild + 서비스 재시작
4. instrumentation 로그로 검증 (held_sec가 0에 가까워지는지 확인)
5. Phase 4: instrumentation 제거 + 최종 deploy
