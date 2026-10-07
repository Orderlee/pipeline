---
description: PostgreSQL migration·DDL·raw SQL 변경을 설계·검증할 때의 안전 절차; DuckDB cutover 전용 규칙은 폐기됨
---

# PostgreSQL migration review

2026-05-19 cutover 이후 write path는 PostgreSQL이다. DuckDB→PostgreSQL 전환 전용 역할 상승·backfill 절차는 **폐기**했다. 새 migration과 SQL 검토에는 이 문서만 사용한다.

1. `src/vlm_pipeline/sql/migrations/postgres/`의 최근 파일 헤더와 `PostgresResource` 호출부를 읽는다. 파일명은 적용 이력이므로 변경하지 않는다.
2. 새 migration은 forward-only로 작성한다. 기존 데이터 보존이 기본이며 `DROP`/`TRUNCATE`는 쓰지 않는다.
3. 센서 조회는 `lib/sensor_db.py`의 read-only 연결을 사용한다. 애플리케이션 쓰기는 `PostgresResource`를 사용한다.
4. SQL·제약·인덱스 변경은 대상 테이블의 호출부와 migration assertion을 함께 검토하고, 별도 테스트 PG에 integration test를 실행한다.

## 제약

- staging DB는 `pipeline-test-postgres-1`의 `vlm_pipeline_staging`이다.
- 운영 DSN으로 integration test를 실행하지 않는다.
- SQL 제안에 비밀값을 포함하지 않으며, 외부 에이전트는 read-only 제안만 한다.
