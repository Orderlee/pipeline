# Staging reset — 수동 범위 확정 절차

기존 `.agent/skill/staging_reset/scripts/reset_staging.sh`(project 미지정 `docker compose` → prod `docker` 프로젝트를 건드림)는 DuckDB와 `dagster-staging`을 가정하므로 **폐기**됐다. 현재 staging은 별도 `_test` clone, `pipeline-test-postgres-1`, DB `vlm_pipeline_staging`이다.

1. staging `_test` clone에서만 `scripts/compose-staging.sh ps`로 대상과 실행 중 run을 확인한다.
2. 필요한 범위를 명시한다: 테스트 DB, MinIO 객체, NAS archive/manifest, Dagster run history. MinIO/NAS 삭제는 되돌릴 수 없으므로 대상 prefix와 보존 대상을 사용자와 확정한다.
3. DB만 초기화하는 integration test는 `DATAOPS_TEST_POSTGRES_DSN`을 사용한다. 운영 DSN·운영 compose wrapper는 사용하지 않는다.
4. Dagster run/센서 상태 초기화 대상은 **`/home/user/work_p/Datapipeline-Data-data_pipeline_test/docker/app/dagster_home/storage/`** 뿐이다(staging dagster 정지 후). 초기화하면 센서 토글도 기본값으로 돌아가므로 `dispatch_sensor`·`production_agent_dispatch_sensor`(기본 STOPPED)를 UI(:3031)에서 다시 켠다.

## 금지

- 운영 checkout에서 staging compose를 실행하지 않는다.
- 운영 checkout의 `docker/app/dagster_home/storage/`(같은 상대경로)는 prod Dagster SQLite storage다 — 삭제 금지.
- `.agent/skill/staging_reset/scripts/reset_staging.sh`와 DuckDB 파일 삭제를 실행하지 않는다.
- 확인되지 않은 버킷·NAS 경로·run history를 삭제하지 않는다.
