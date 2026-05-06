# PostgreSQL 인스턴스 토폴로지 — 옵션 비교

> 작성: VLM Data Pipeline DB 마이그레이션 (DuckDB → PostgreSQL) 사전 결정 문서
> 결정: **단계적 입장** — staging은 옵션 A, prod 승격 시 옵션 B로 교체

VLM 데이터 파이프라인의 메인 DB를 DuckDB → PostgreSQL로 옮길 때, 파이프라인 데이터를 어느 PG 인스턴스에 둘지에 대한 옵션을 정리한다. 현재 [docker/docker-compose.yaml:206](../../docker/docker-compose.yaml#L206)의 `postgres:15` 컨테이너는 Grafana(대시보드/사용자), LabelStudio(`POSTGRE_*` env 공유), `nas_folder_tree_to_postgres.py`(NAS 메타) 세 가지 용도로 이미 사용 중이다.

---

## 옵션 A — 기존 `postgres:15` 인스턴스에 신규 DB 추가

`vlm_pipeline` (또는 `vlm_pipeline_staging`) 데이터베이스만 동일 인스턴스에 추가.

### 장점
- **운영 객체 증가 없음**: 컨테이너 1개·디스크 볼륨 1개·헬스체크 1개·백업 절차 1개 그대로.
- **인프라 작업 거의 0**: 컨테이너는 떠있고 `psycopg2-binary>=2.9.9`도 이미 설치되어 있음 ([docker/app/requirements.txt:7](../../docker/app/requirements.txt#L7), [src/python/requirements.txt:6](../../src/python/requirements.txt#L6)).
- **staging↔prod 동일 패턴**: 두 환경 다 같은 compose 구조라 토폴로지 차이가 발생하지 않음.
- **빠른 검증 시작**: PR이 머지되면 즉시 cutover 시도 가능.

### 단점
- **자원 경합 위험**: 파이프라인은 ingest 피크 때 BIGINT 카운트·INSERT 폭주가 발생. Grafana/LS와 `shared_buffers`·connection pool을 공유 → 한쪽 쿼리 폭주가 다른 쪽 응답 지연을 유발. 특히 LS는 사용자 인터랙티브 응답이라 체감 영향 큼.
- **백업/복구 묶임**: `pg_dump` 한 번에 4개 DB가 같이 떠짐. 파이프라인 DB만 PITR(point-in-time recovery)이 불편.
- **튜닝 타협**: 파이프라인은 OLTP-write heavy(WAL 자주), Grafana는 read-heavy(query cache). `wal_level`·`max_wal_size`·`effective_cache_size`를 한쪽에 맞추면 다른 쪽이 손해.
- **장애 폭발반경 큼**: 파이프라인 마이그레이션 중 PG 재시작 필요 시 Grafana/LS도 같이 다운.
- **버전 업그레이드 묶임**: PG 16/17 가고 싶을 때 4개 DB 모두 같이 가야 함.

---

## 옵션 B — 파이프라인 전용 `postgres-pipeline` 컨테이너 신규

compose에 `postgres-pipeline` 별도 서비스(별도 볼륨 `postgres_pipeline_data`, 별도 포트, 별도 credential)를 추가.

### 장점
- **자원·튜닝 독립**: 파이프라인 워크로드(write-heavy, BIGINT, JSONB 후보)만 보고 `shared_buffers`·`max_connections`·`wal_buffers` 튜닝 가능. Grafana는 영향 없음.
- **백업·PITR 분리**: 파이프라인만 별도 cron `pg_basebackup` + WAL archive 가능. 분석/모니터링 DB와 보존 정책을 따로 갈 수 있음.
- **장애 격리**: 파이프라인 컨테이너 재시작/마이그레이션이 Grafana/LS에 영향 0.
- **버전 업그레이드 독립**: 파이프라인 PG는 보수적, 모니터링은 빠르게 따로 갈 수 있음.
- **prod에서 의미가 큼**: ingest 피크 시 LS UI 끊김 같은 사용자 체감 사고를 차단.
- **`duckdb_writer` 태그 해제 후(Phase 8) 동시 write 폭발 시에도 모니터링 DB와 격리**.

### 단점
- **운영 객체 +1**: 컨테이너·볼륨·헬스체크·백업 cron이 하나 더 생김.
- **디스크 사용량 +1 인스턴스**: 파이프라인 데이터는 결국 같은 호스트 SSD를 쓰지만 파일은 분리.
- **포트 충돌 주의**: 새 포트 하나 잡아야 함 (LS 충돌과 비슷한 패턴이 한 번 더).
- **CI 통합 시 컨테이너 1개 더**: e2e에서 `postgres-pipeline` sidecar 추가, 테스트 시간 +5초 미만.

---

## 단순 기준

- **staging만** 보면 옵션 A가 합리적: 트래픽 적음, 격리 가치 낮음, 빠른 검증 가치 큼.
- **prod까지** 보면 **옵션 B가 안전마진이 큼**: 특히 Phase 8에서 `duckdb_writer` 태그를 풀어 동시 write가 폭발할 때, Grafana 체감 지연으로 번지지 않게 막아줌.

---

## 채택 결정 (단계적 입장)

| 환경 | 인스턴스 | DB 이름 | 이유 |
|---|---|---|---|
| **Staging (dev/3031)** | 옵션 A — 기존 `postgres:15` 공유 | `vlm_pipeline_staging` | 인프라 작업 0, 빠른 검증 시작. staging 트래픽 수준에선 자원 경합 무시 가능. |
| **Prod (main/3030)** | 옵션 B — 신규 `postgres-pipeline` 컨테이너 | `vlm_pipeline` | ingest 피크의 사용자 체감 사고 차단, 백업·튜닝·버전 업그레이드 독립. |

스테이징에서 검증된 스키마/리소스 코드는 prod 컨테이너에 그대로 적용. 토폴로지 차이는 **DSN 한 줄(`DATAOPS_POSTGRES_DSN`)**과 **compose 서비스 정의**에만 한정된다.

---

## 운영 영향 요약

- **백업**: staging은 기존 인스턴스 백업 정책 상속, prod는 별도 `pg_basebackup` cron 신설.
- **모니터링**: prod는 `postgres-pipeline` 전용 metric을 Grafana에 추가 (별도 datasource).
- **시크릿**: prod는 `POSTGRES_PIPELINE_USER`/`POSTGRES_PIPELINE_PASSWORD`를 별도 관리.
- **네트워크**: 두 환경 모두 `pipeline-network` 동일 도커 네트워크 사용 → 컨테이너 간 통신만으로 충분 (호스트 포트 publish는 디버깅 용도에만).
