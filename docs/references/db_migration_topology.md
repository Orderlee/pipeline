# PostgreSQL Instance Topology — Option Comparison

> Authored: VLM Data Pipeline DB migration (DuckDB → PostgreSQL) pre-decision document
> Decision: **Staged adoption** — staging uses Option A, replaced with Option B upon prod promotion

This document outlines the options for which PostgreSQL instance to place the pipeline data in when migrating the VLM data pipeline's main DB from DuckDB → PostgreSQL. The `postgres:15` container currently at [docker/docker-compose.yaml:206](../../docker/docker-compose.yaml#L206) is already used for three purposes: Grafana (dashboards/users), LabelStudio (`POSTGRE_*` env shared), and `nas_folder_tree_to_postgres.py` (NAS metadata).

---

## Option A — Add a new DB to the existing `postgres:15` instance

Add only the `vlm_pipeline` (or `vlm_pipeline_staging`) database to the same instance.

### Pros
- **No increase in operational objects**: 1 container · 1 disk volume · 1 health check · 1 backup procedure — unchanged.
- **Near-zero infrastructure work**: Container is already running and `psycopg2-binary>=2.9.9` is already installed ([docker/app/requirements.txt:7](../../docker/app/requirements.txt#L7), [src/python/requirements.txt:6](../../src/python/requirements.txt#L6)).
- **Consistent staging↔prod pattern**: Both environments share the same compose structure so no topology divergence arises.
- **Quick validation start**: Cutover can be attempted immediately once the PR is merged.

### Cons
- **Resource contention risk**: The pipeline generates BIGINT count · INSERT bursts at ingest peaks. Sharing `shared_buffers` · connection pool with Grafana/LS → a query surge on one side causes response latency on the other. LS is particularly impactful as it serves interactive users.
- **Coupled backup/recovery**: A single `pg_dump` pulls all 4 DBs together. PITR (point-in-time recovery) for the pipeline DB alone is inconvenient.
- **Tuning compromises**: The pipeline is OLTP-write heavy (frequent WAL), Grafana is read-heavy (query cache). Tuning `wal_level` · `max_wal_size` · `effective_cache_size` for one side penalizes the other.
- **Large failure blast radius**: If a PG restart is needed during pipeline migration, Grafana/LS go down together.
- **Coupled version upgrades**: Moving to PG 16/17 requires all 4 DBs to upgrade together.

---

## Option B — New dedicated `postgres-pipeline` container for the pipeline

Add a `postgres-pipeline` separate service to compose (separate volume `postgres_pipeline_data`, separate port, separate credentials).

### Pros
- **Independent resources and tuning**: Can tune `shared_buffers` · `max_connections` · `wal_buffers` focused solely on the pipeline workload (write-heavy, BIGINT, JSONB candidate). No impact on Grafana.
- **Separate backup/PITR**: Can set up a dedicated cron `pg_basebackup` + WAL archive for the pipeline only. Retention policies can diverge between analytics/monitoring DB and pipeline DB.
- **Failure isolation**: Pipeline container restart/migration has zero impact on Grafana/LS.
- **Independent version upgrades**: Pipeline PG can be kept conservative while monitoring upgrades faster independently.
- **More meaningful in prod**: Blocks user-facing incidents such as LS UI interruptions during ingest peaks.
- **Isolated from monitoring DB even when concurrent writes explode after `duckdb_writer` tag is removed (Phase 8)**.

### Cons
- **+1 operational object**: One more container · volume · health check · backup cron.
- **+1 instance disk usage**: Pipeline data ultimately uses the same host SSD but files are separate.
- **Port collision caution**: Need to allocate one new port (same pattern as the LS port collision once more).
- **+1 container for CI integration**: `postgres-pipeline` sidecar added to e2e, test time increase < 5 seconds.

---

## Simple decision criteria

- **Staging only**: Option A is reasonable — low traffic, isolation value is low, fast validation value is high.
- **Through to prod**: **Option B has a larger safety margin** — especially when the `duckdb_writer` tag is removed in Phase 8 and concurrent writes explode, it prevents that from cascading into Grafana response latency.

---

## Adoption decision (staged)

| Environment | Instance | DB name | Reason |
|---|---|---|---|
| **Staging (dev/3031)** | Option A — share existing `postgres:15` | `vlm_pipeline_staging` | Zero infrastructure work, quick validation start. Resource contention is negligible at staging traffic levels. |
| **Prod (main/3030)** | Option B — new `postgres-pipeline` container | `vlm_pipeline` | Blocks user-facing incidents at ingest peaks; independent backup, tuning, and version upgrade. |

The schema/resource code validated on staging is applied as-is to the prod container. The topology difference is confined to **one DSN line (`DATAOPS_POSTGRES_DSN`)** and the **compose service definition**.

---

## Operational impact summary

- **Backup**: staging inherits the existing instance backup policy; prod adds a dedicated `pg_basebackup` cron.
- **Monitoring**: prod adds a dedicated `postgres-pipeline` metric to Grafana (separate datasource).
- **Secrets**: prod manages `POSTGRES_PIPELINE_USER`/`POSTGRES_PIPELINE_PASSWORD` separately.
- **Network**: Both environments use the same `pipeline-network` Docker network → inter-container communication is sufficient (host port publishing for debugging only).
