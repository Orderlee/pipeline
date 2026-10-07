# 폐기됨

2026-05-19 PostgreSQL cutover로 staging write path에서 DuckDB를 제거했다. 기존 스크립트는 `docker/data/staging.duckdb`와 WAL만 삭제한다.

대체: `staging_reset`의 현재 PostgreSQL staging 절차를 사용한다.
