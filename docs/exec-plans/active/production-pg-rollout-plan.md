# Production PostgreSQL Transition Rollout Plan

> Scope: Steps, risks, and rollback procedures for transitioning the production
> (`main`, `:3030`) environment to the PG primary + `pg_duckdb` mode validated
> in staging.
> Prerequisite: §[DBeaver guide](../references/dbeaver-pg-duckdb-test-guide.md)
> §3 (`DuckDB → PG ATTACH`) validation passed in staging (`dev`, `:3031`).

Written: 2026-05-07 | Status: **Drafted, not yet executed** (record progress in the checklist at the end of this document when executing)

---

## 0. TL;DR — One-Page Summary

| Phase | What | Downtime | Risk | Rollback |
|-------|------|----------|------|----------|
| **1** code merge | dev → main PR | 0 (CI automatic deployment ~3-10 min) | Only code lands; behavior unchanged (legacy DuckDB) | revert PR |
| **2** PG image swap | Add `POSTGRES_IMAGE`+`POSTGRES_PRELOAD_LIBS` to `.env`, restart | postgres ~30s | No impact on DuckDB write lock | Remove lines from `.env`, restart |
| **3** schema/DB creation | Create `vlm_pipeline` DB + Resource init auto-bootstrap | 0 | Only an empty PG is created | DB drop |
| **4** data backfill | `migrate_duckdb_to_postgres.py --apply --idempotent` | 0 (read-only on duckdb) | Backfill time proportional to row count (~minutes to tens of minutes) | PG truncate, re-run |
| **5** dual write validation | Run for a few days with `DATAOPS_DB_BACKEND=dual_pg_primary` | dagster ~30s restart | DuckDB as mirror safety net | Remove lines from `.env`, restart |
| **6** PG single mode | `DATAOPS_DB_BACKEND=postgres` | dagster ~30s restart | DuckDB writes = 0, no mirror | Immediate revert to `dual_pg_primary` |
| **7** cleanup | After stable operation for a period, just preserve `pipeline.duckdb` and leave it | 0 | None | — |

Recommended pace: phases 1–4 on the same day, phase 5 **minimum 3–7 days** of operational validation, phase 6 after that. Do not rush through all at once.

---

## 1. Current State Snapshot (as of 2026-05-07)

### 1.1 Production (`main` HEAD = `c820ec1`)

| Item | Value |
|------|-------|
| Postgres image | `postgres:15` (vanilla) |
| `shared_preload_libraries` | (empty string) |
| `pg_duckdb` extension | ❌ not installed |
| `DATAOPS_DB_BACKEND` in Dagster `.env` | (not set → legacy `duckdb` mode) |
| `DATAOPS_POSTGRES_DSN` | (not set) |
| Active DBs | `airflow`, `labeling`, `postgres` (vlm data not in PG) |
| Operational data location | `/data/pipeline.duckdb` (DuckDB single file) |

### 1.2 Staging (`dev` HEAD = `da389ef`, 14 commits ahead of main)

| Item | Value |
|------|-------|
| Postgres image | `pgduckdb/pgduckdb:15-v1.1.1` |
| `shared_preload_libraries` | `pg_duckdb` |
| `pg_duckdb` extension | ✅ v1.1.0 |
| `DATAOPS_DB_BACKEND` | `postgres` (single PG) |
| `DATAOPS_POSTGRES_DSN` | `postgresql://airflow:airflow@postgres:5432/vlm_pipeline_staging` |
| Active vlm DB | `vlm_pipeline_staging` (16 tables, 5396+ rows) |
| Sensor read | `open_sensor_read_connection()` → in-memory DuckDB + ATTACH PG (READ_ONLY) |

### 1.3 Commits in dev that main does not have (by PR)

- `#42 feature/db-migration-postgres` — Phase 0-7 (schema, Resource, DualDB facade, MotherDuck PG source)
- `#43 chore/deploy-single-truth-source` — remove compose `../src` bind mount
- `#44 feat/staging-pg-duckdb` — `POSTGRES_IMAGE` override support
- `#45 chore/deploy-stack-postgres-auto` — postgres auto-restart + healthcheck wait
- `#46 feat/sensor-pg-attach-single-pg-mode` — sensor PG ATTACH facade
- `#47 chore/postgres-pg-duckdb-preload` — `POSTGRES_PRELOAD_LIBS` support
- `#48 chore/staging-ops-hygiene` — `.env.test` untrack + writer tag release
- `#49 fix/claude-action-native-install` — CI Claude action workaround

→ Recommended: merge all at once with a single PR (`dev → main`).

---

## 2. Phase 1 — Code Merge (zero behavior change)

**Goal**: Land code on the production host. Keep legacy DuckDB primary as-is.

### Steps

1. Create PR: `dev → main` from GitHub UI. Example title: `feat(db): introduce full PostgreSQL primary mode code (Phase 1)`
2. Note in the PR body that only Phase 1 is being applied, with a link to this document.
3. Merge → `.github/workflows/deploy-production.yml` auto-triggered → verify `:3030/server_info` responds after ~3-10 minutes.
4. On the production host:
   ```bash
   ssh 10.0.0.10
   docker exec docker-dagster-1 sh -c 'echo BACKEND=$DATAOPS_DB_BACKEND; echo DSN_SET=$([ -n "$DATAOPS_POSTGRES_DSN" ] && echo yes || echo no)'
   # Expected:  BACKEND= (empty → legacy duckdb)
   #            DSN_SET=no
   ```

### Validation

- Sensors on Dagster UI (`:3030`) are ticking normally (last evaluation updated, error = 0)
- At least one normal ingest/dispatch/label run — DuckDB write/read unchanged
- Code path inside container returns `db_backend_mode()` → `"duckdb"` (indirect: OK if no errors)

### Rollback

- Revert PR and re-merge → CI reverts to previous SHA.

### Risk

- **Almost none**. [env_utils.py:104](../../src/vlm_pipeline/lib/env_utils.py#L104) protects by falling back to the legacy default (`duckdb`).
- One sanity test is sufficient before proceeding to the next phase.

---

## 3. Phase 2 — Postgres Image Swap (planned downtime ~30s)

**Goal**: Swap the production postgres container to `pgduckdb/pgduckdb:15-v1.1.1` and activate `pg_duckdb`. **No impact on Dagster** (Dagster does not use PG at this stage).

### Pre-requisites

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# (a) Backup PG persistent volume (optional but recommended — postgres_data volume)
docker run --rm \
  -v docker_postgres_data:/from \
  -v /home/user/backups:/to \
  alpine sh -c "tar czf /to/prod-postgres-data-$(date +%Y%m%d_%H%M).tgz -C /from ."

# (b) Inventory current PG airflow / labeling DBs — for post-swap preservation check
docker exec docker-postgres-1 psql -U airflow -c "\l" > /tmp/prod-pg-databases-before.txt
docker exec docker-postgres-1 psql -U airflow -d airflow -c "\dt" > /tmp/prod-pg-airflow-tables-before.txt
```

### `docker/.env` additions (Phase 2 items only)

```diff
+ # ===== PostgreSQL image / extensions (Phase 2) =====
+ # Swapping to the pgduckdb image mounts the postgres_data volume as-is — zero schema/data loss.
+ # However, pg_duckdb must be added to shared_preload_libraries to use the pg_duckdb extension.
+ POSTGRES_IMAGE=pgduckdb/pgduckdb:15-v1.1.1
+ POSTGRES_PRELOAD_LIBS=pg_duckdb
```

> ⚠️ `.env` is not git-tracked, so **edit directly on the host**. Differences from
> `dev`'s `.env.test` are documented in [section 3.1](#31-staging-vs-production-env-differences).

### Apply

```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose up -d postgres        # recreate postgres only
docker compose ps postgres           # confirm healthy
```

The `auto-restart postgres on compose change` path in `scripts/deploy/deploy-stack.sh`
(PR #45) performs the same logic, so it is also valid to defer this and let the next
deploy handle it automatically.

### Validation

```bash
# (a) Image / shared_preload_libraries
docker inspect docker-postgres-1 --format '{{.Config.Image}}'
# Expected: pgduckdb/pgduckdb:15-v1.1.1
docker inspect docker-postgres-1 --format '{{.Config.Cmd}}'
# Expected: [postgres -c shared_preload_libraries=pg_duckdb]

# (b) Extension registration
docker exec docker-postgres-1 psql -U airflow -c "
  SHOW shared_preload_libraries;
  SELECT * FROM pg_available_extensions WHERE name='pg_duckdb';"

# (c) Airflow DB preservation — core safety check for Phase 2
docker exec docker-postgres-1 psql -U airflow -c "\l"  > /tmp/prod-pg-databases-after.txt
diff /tmp/prod-pg-databases-before.txt /tmp/prod-pg-databases-after.txt
# Expected: no diff (airflow / labeling unchanged)
```

### Rollback

```bash
# Remove the POSTGRES_IMAGE / POSTGRES_PRELOAD_LIBS lines from .env
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose up -d postgres
```
The image will be downgraded and `shared_preload_libraries` will revert to empty.
The postgres_data volume is preserved, so airflow/labeling are unaffected.

### Risk

- **~30s PG restart during image swap**. Dagster is not using PG at this stage so there is no impact, but **external tools that use the PG airflow DB (e.g., Grafana) will experience a brief outage**.
- If the pgduckdb image has an incompatible change relative to `postgres:15`, startup may fail — low probability since the same image has been validated in staging.

---

## 4. Phase 3 — `vlm_pipeline` DB Creation + Schema Bootstrap

**Goal**: Create an empty `vlm_pipeline` DB with 16 tables inside PG. The Dagster
code path does not use it yet.

### Steps

```bash
ssh 10.0.0.10

# (a) Create DB
docker exec docker-postgres-1 psql -U airflow -d postgres -c "
  CREATE DATABASE vlm_pipeline OWNER airflow;"

# (b) Register pg_duckdb extension (per-database)
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  CREATE EXTENSION IF NOT EXISTS pg_duckdb;
  SELECT extname, extversion FROM pg_extension WHERE extname='pg_duckdb';"

# (c) Schema bootstrap — Resource init auto-creates, so this step is essentially a nop.
#     For an explicit apply:
docker exec docker-dagster-1 python3 -c "
from vlm_pipeline.resources.postgres_db import PostgresResource
import os
r = PostgresResource(dsn='postgresql://airflow:airflow@postgres:5432/vlm_pipeline')
r.ensure_runtime_schema()
"
```

### Validation

```bash
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "\dt"
# Expected: 16 tables (raw_files, video_metadata, image_metadata, ...)

docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  SELECT 'raw_files' AS t, COUNT(*) FROM raw_files
  UNION ALL SELECT 'video_metadata', COUNT(*) FROM video_metadata
  ORDER BY t;"
# Expected: all 0 (empty schema)
```

### Rollback

```bash
docker exec docker-postgres-1 psql -U airflow -d postgres -c "
  DROP DATABASE IF EXISTS vlm_pipeline;"
```

### Risk

- Almost none. This is simply adding an empty DB.

---

## 5. Phase 4 — Data Backfill (DuckDB → PG, no downtime)

**Goal**: Idempotently copy all rows from `/data/pipeline.duckdb` into
`vlm_pipeline`. Production Dagster is still writing to DuckDB, so rows that
arrive during this phase will be caught up at the next backfill or when dual-write
begins.

### Steps

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# (a) dry-run — print copy plan only (zero writes)
docker exec docker-dagster-1 python3 /src/python/scripts/migrate_duckdb_to_postgres.py \
  --source /data/pipeline.duckdb \
  --dsn "postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
  --dry-run

# (b) actual backfill — idempotent mode (safe to re-run)
docker exec docker-dagster-1 python3 /src/python/scripts/migrate_duckdb_to_postgres.py \
  --source /data/pipeline.duckdb \
  --dsn "postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
  --apply --idempotent

# (c) verification — compare row counts on both sides
docker exec docker-dagster-1 python3 /src/python/scripts/migrate_duckdb_to_postgres.py \
  --source /data/pipeline.duckdb \
  --dsn "postgresql://airflow:airflow@postgres:5432/vlm_pipeline" \
  --verify-only
```

> 📌 The script is based on the 13-table spec in
> [scripts/migrate_duckdb_to_postgres.py](../../scripts/migrate_duckdb_to_postgres.py)
> and performs `INSERT … ON CONFLICT DO NOTHING` (idempotent) by PK.
> Running inside the container briefly acquires a DuckDB read lock, which may
> conflict with sensors that are writing — run during **low-traffic hours
> (weekday late night or weekend)**.

### Validation

- `--verify-only` output shows source = target for all tables
- Spot-check a row or two via DBeaver PG connection

### Rollback

```bash
# Truncate the entire vlm_pipeline DB
docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c "
  TRUNCATE raw_files, video_metadata, image_metadata, labels, processed_clips,
           datasets, dataset_clips, dispatch_requests, dispatch_pipeline_runs,
           image_labels, labeling_specs, labeling_configs, staging_model_configs,
           classification_datasets, requester_config_map
  CASCADE;"
# Or more cleanly: DB drop & recreate (re-run Phase 3)
```

### Risk

- **DuckDB read lock contention**: if a sensor attempts a DuckDB write during the backfill, it will wait briefly; resolves naturally after the backfill completes.
- **New rows missed during backfill**: run `--apply --idempotent` once more just before entering Phase 5 to catch up any new rows.

---

## 6. Phase 5 — Dual-write PG Primary (validate PG with safety net enabled)

**Goal**: Dagster writes to PG as primary, with DuckDB as mirror. Operate for a
few days to observe whether PG-only mode is stable.

### `docker/.env` additions

```diff
+ # ===== Phase 5 — DB backend (dual: PG primary, DuckDB mirror) =====
+ DATAOPS_DB_BACKEND=dual_pg_primary
+ DATAOPS_POSTGRES_DSN=postgresql://airflow:airflow@postgres:5432/vlm_pipeline
```

### Apply

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker

# Restart only Dagster (postgres unchanged)
docker compose restart dagster dagster-daemon dagster-code-server

# Health check
curl -fsS http://localhost:3030/server_info
```

### Validation (over several days)

- **(Immediately)** Verify sensors read via PG ATTACH: `docker logs docker-dagster-daemon-1 | grep -iE "ATTACH|postgres" | tail -20`
- **(Immediately)** After one new ingest, verify both PG and DuckDB have the same row:
  ```bash
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -c \
    "SELECT MAX(created_at) FROM raw_files;"
  docker exec docker-dagster-1 sh -c \
    'duckdb -readonly -csv /data/pipeline.duckdb "SELECT MAX(created_at) FROM raw_files;"'
  # The two values should match within ±1 second
  ```
- **(Daily)** Monitor row count drift:
  ```bash
  docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline -t -c \
    "SELECT COUNT(*) FROM raw_files;"
  # vs same count from DuckDB
  ```
- **(Continuously)** Sensor / job error rate on Dagster UI stays at normal levels

### Recommended Operating Period

**Minimum 3–7 days**. During this period, confirm at least one full dispatch cycle
(ingest → label → process → dataset) completes successfully from PG.

### Rollback

```bash
# Remove the two lines from .env, then:
docker compose restart dagster dagster-daemon dagster-code-server
# → immediately reverts to legacy DuckDB primary (DuckDB data intact)
```
Data mirrored to PG becomes stale, but can be caught up with an idempotent re-backfill
when re-entering this phase.

### Risk

- **Dual-write overhead**: every INSERT goes to both PG and DuckDB → write latency roughly doubles. If operational traffic is close to the sensor lag threshold, some sensors may slow down. Compare with actual lag measured in staging before deciding.
- **Mirror divergence**: cases where only one side of a write succeeds — the `DualDBResource` facade ([resources/data_db.py](../../src/vlm_pipeline/resources/data_db.py)) guarantees both commits, but PG is the source of truth on partial failure. Correct with an idempotent backfill if suspected.

---

## 7. Phase 6 — PG Single Mode (end DuckDB writes)

**Goal**: Completely stop DuckDB writes. All reads and writes go to PG as the
single backend.

### `docker/.env` change

```diff
- DATAOPS_DB_BACKEND=dual_pg_primary
+ DATAOPS_DB_BACKEND=postgres
```
(`DATAOPS_POSTGRES_DSN` remains as-is)

### Apply + Validation

```bash
docker compose restart dagster dagster-daemon dagster-code-server

# (a) DuckDB file mtime should stop changing — signal of zero writes
ls -la /home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/pipeline.duckdb
# Check again 5 minutes later and confirm mtime has not changed

# (b) duckdb_writer concurrency constraint lifted — build_duckdb_writer_tags() == {}
#     Launch 2–3 simultaneous dispatch runs → confirm all go to RUNNING state
```

### Recommended Operating Period

**Minimum 1 week**. During this period, confirm that every major job (ingest,
dispatch, label, process, build_dataset, motherduck_sync) has completed at least
once on PG alone.

### Rollback

Immediate revert to `dual_pg_primary` is possible. Rows that were inserted into
PG only during this period will not be mirrored to DuckDB, but starting fresh
`dual_pg_primary` will again mirror new rows. To recover historical rows as well,
a reverse PG → DuckDB migration is needed (no script currently exists — write
separately if needed).

### Risk

- **Mirror safety net is gone**. At this point PG data is the source of truth.
- **PG backup policy required**. Register a daily `pg_dump` or volume snapshot cron before entering this phase.

---

## 8. Phase 7 — Cleanup (optional, ~1 month later)

- Do not delete `pipeline.duckdb`; only rename it to `pipeline.duckdb.frozen-YYYYMMDD`. Keep it as a last resort.
- Leave `DUCKDB_PATH` / `DATAOPS_DUCKDB_PATH` in `.env` **as-is** — the sensor_db fallback path depends on them.
- The MotherDuck sync sensor already supports PG source (`feat(sync) Phase 7` commit) → no separate change needed.

---

## 9. Production-Specific Details

### 9.1 Staging vs Production `.env` Differences

| Item | staging (`.env.test`) | production (`.env`) | Reason |
|------|-----------------------|---------------------|--------|
| `DATAOPS_DUCKDB_PATH` | `/data/staging.duckdb` | `/data/pipeline.duckdb` | Separate filenames |
| DB name in `DATAOPS_POSTGRES_DSN` | `vlm_pipeline_staging` | **`vlm_pipeline`** | Separate DBs |
| `POSTGRES_PORT` | `15432` (host expose) | (unset, not exposed) | Production blocks external access. Can temporarily add `15432` if DBeaver is needed after Phase 5 |
| MinIO endpoint | `:9002` (staging MinIO) | `:9000` (production MinIO) | Separate instances |
| `POSTGRES_IMAGE` | `pgduckdb/pgduckdb:15-v1.1.1` | **same** (added in Phase 2) | Same |
| `POSTGRES_PRELOAD_LIBS` | `pg_duckdb` | **same** (added in Phase 2) | Same |
| `DATAOPS_DB_BACKEND` | `postgres` | **phased**: unset → `dual_pg_primary` → `postgres` | Production enters via safety net |

### 9.2 DBeaver Production PG Access (Phase 5 onward, optional)

If production DBeaver analysis is needed, temporarily add to `.env` when entering Phase 5:
```
POSTGRES_PORT=15433
```
(staging already uses 15432, so 15433 is recommended for production)
Then run `docker compose up -d postgres`. For DBeaver configuration, refer to
`docs/references/dbeaver-pg-duckdb-test-guide.md` but set the SSH tunnel
LocalForward / Remote port to `15433`.

> ⚠️ Direct writes to production PG are ABSOLUTELY PROHIBITED. Read-only analysis only. Do not omit `READ_ONLY` from the ATTACH statement.

### 9.3 Backup / Recovery Policy (required before entering Phase 6)

```bash
# Daily pg_dump (register as cron)
0 3 * * *  docker exec docker-postgres-1 pg_dump -U airflow -d vlm_pipeline | \
           gzip > /home/user/backups/vlm_pipeline-$(date +\%Y\%m\%d).sql.gz

# Auto-delete dumps older than 7 days
0 4 * * *  find /home/user/backups -name 'vlm_pipeline-*.sql.gz' -mtime +7 -delete
```

Recovery:
```bash
gunzip -c vlm_pipeline-20260601.sql.gz | \
  docker exec -i docker-postgres-1 psql -U airflow -d vlm_pipeline
```

---

## 10. Execution Checklist (check off as you go)

### Phase 1 — code merge
- [ ] Create PR `dev → main` + attach link to this document
- [ ] CI deploy succeeded (`:3030/server_info` OK)
- [ ] Confirm sensor last_evaluation updated
- [ ] `docker exec ... echo BACKEND` → empty value (legacy mode)

### Phase 2 — postgres image swap
- [ ] postgres_data volume backup complete
- [ ] `airflow/labeling DB` snapshot saved
- [ ] Added `POSTGRES_IMAGE` + `POSTGRES_PRELOAD_LIBS` to `.env`
- [ ] `docker compose up -d postgres` — healthy
- [ ] `SHOW shared_preload_libraries` = `pg_duckdb`
- [ ] `airflow / labeling DB` preserved (before/after diff shows no change)

### Phase 3 — vlm_pipeline DB creation
- [ ] `CREATE DATABASE vlm_pipeline`
- [ ] `CREATE EXTENSION pg_duckdb` (per database)
- [ ] Schema bootstrap (16 tables)

### Phase 4 — backfill
- [ ] Review `--dry-run` output
- [ ] `--apply --idempotent` succeeded
- [ ] `--verify-only` shows source = target for all tables

### Phase 5 — dual_pg_primary
- [ ] Added `DATAOPS_DB_BACKEND=dual_pg_primary` + `DATAOPS_POSTGRES_DSN` to `.env`
- [ ] Dagster restarted
- [ ] 1 new ingest → same row confirmed in both PG and DuckDB
- [ ] N=3–7 days of operation (drift monitor, error rate at normal levels)

### Phase 6 — postgres single
- [ ] PG `pg_dump` cron registered + 1 dump verified
- [ ] Backend in `.env` changed to `postgres`
- [ ] Dagster restarted
- [ ] Confirmed DuckDB mtime stopped (5 minutes later)
- [ ] Confirmed 2–3 concurrent dispatch runs in RUNNING state (concurrency lock resolved)

### Phase 7 — cleanup (optional)
- [ ] After 1 month of stable operation, rename `pipeline.duckdb` to `.frozen-YYYYMMDD`

---

## 11. Decision Log (fill in as you execute)

| Date | Phase | Decision / Observation | Author |
|------|-------|------------------------|--------|
| 2026-05-07 | — | Plan written. Staging validation passed (raw_files=2698, video_metadata=288, image_metadata=2410). DBeaver DuckDB ATTACH READ_ONLY confirmed working | claude |
| YYYY-MM-DD | Phase 1 | (PR# etc.) | |
| YYYY-MM-DD | Phase 2 | (image swap time, airflow DB impact) | |
| YYYY-MM-DD | Phase 3 | (vlm_pipeline DB creation time) | |
| YYYY-MM-DD | Phase 4 | (backfill row count by table) | |
| YYYY-MM-DD | Phase 5 | (dual_pg_primary entry time, first dispatch result) | |
| YYYY-MM-DD | Phase 6 | (postgres single entry time, concurrency resolution confirmed) | |

---

## 12. Reference Documents

- [DBeaver staging Postgres + DuckDB extension test guide](../references/dbeaver-pg-duckdb-test-guide.md) — apply the same guide for production PG access (only the port differs)
- [Production-Test Environment Separation and Automatic Deployment Plan](운영_테스트_환경_분리_자동배포_계획.md) — CI/CD mechanism details
- [DB Migration Topology](../references/db_migration_topology.md) — topology diagrams by migration phase
- [Production Runbook](../references/production-label-preprocess-cleanup-runbook.md) — production troubleshooting
- Code:
  - [`scripts/migrate_duckdb_to_postgres.py`](../../scripts/migrate_duckdb_to_postgres.py) — backfill script
  - [`src/vlm_pipeline/lib/env_utils.py`](../../src/vlm_pipeline/lib/env_utils.py) — `db_backend_mode()` definition
  - [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py) — sensor PG ATTACH facade
  - [`src/vlm_pipeline/resources/data_db.py`](../../src/vlm_pipeline/resources/data_db.py) — DualDB facade
