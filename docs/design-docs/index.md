# Design Docs Index

- [DuckDB Lock Contention 근본 원인 분석](duckdb-lock-contention-analysis.md) — 센서의 반복적 ensure_runtime_schema() 호출이 write lock을 장기 점유하는 문제 분석 (2026-04-03)
- [Ingest 파이프라인 성능 벤치마크](ingest-performance-bench-20260424.md) — staging 파일럿 10GB에 8h 41m 소요한 원인을 단계별(NAS/checksum/phash/MinIO/DuckDB) isolation 벤치로 분석, threading 우선 도입 권장 (2026-04-24)
