# DBeaver — Staging & Production Postgres + DuckDB Extension Test Guide

> Audience: Operators who want to validate the staging or production (`10.0.0.10`) Postgres / DuckDB behavior using DBeaver on Mac.
> Prerequisite: The target environment is already running in PG primary mode (`DATAOPS_DB_BACKEND=postgres`) and uses the `pgduckdb/pgduckdb:15-v1.1.1` image. Production reached this state on 2026-05-13 (Phase 6 of the rollout plan).
> §1-§6 use staging as the worked example; §7 covers the deltas required for production.

---

## 0. Distinguishing the two extensions

This guide validates **two** extensions whose directions are opposite to each other.

| Location | Extension | Direction | Used in prod code? | Where to validate |
|------|-----------|------|--------------|------------|
| Inside Postgres | `pg_duckdb` v1.1.0 | PG → DuckDB engine call (read_parquet, S3, etc.) | ❌ Not used in production code (for ad-hoc analysis only) | DBeaver **PostgreSQL** connection (§2) |
| Inside DuckDB | `postgres` (postgres_scanner) | DuckDB → PG ATTACH (READ_ONLY) | ✅ **Read path of every sensor** | DBeaver **DuckDB** connection + SSH tunnel (§3) |

> 🔑 **§2 and §3 are not equally important.**
> - §2 (`pg_duckdb`) = **infrastructure smoke test**. Even if broken, it has no impact on the operational pipeline
>   (current `grep -rn "pg_duckdb\|duckdb.query\|postgres_scan" src/` yields 0 results).
>   It was pre-installed for future use by operators doing ad-hoc analysis in DBeaver etc.
> - §3 (`postgres_scanner`) = **the operational sensor read path itself**.
>   `open_sensor_read_connection()` in [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py)
>   spins up an in-memory DuckDB and ATTACHes PG on every sensor tick. **If broken, all sensors cannot read PG = pipeline halted**.
>
> If time is tight, §3 alone is sufficient.

---

## 1. Prerequisites

### 1.1 Staging connection details

| Item | Value |
|------|----|
| SSH host | `10.0.0.10` (port 22) |
| Postgres host port | `10.0.0.10:15432` (mapped to container 5432) |
| Database | `vlm_pipeline_staging` |
| User / Password | `airflow` / `airflow` |
| Postgres image | `pgduckdb/pgduckdb:15-v1.1.1` |

> ⚠️ Production Postgres is **not** published by default (security). For production validation, follow §7
> which temporarily exposes a different host port (`15433`) to avoid collision with staging's `15432`.
> Always treat production with extra care — see §7.8 for absolute prohibitions.

### 1.2 Verify SSH key registration (Mac)

```bash
# Mac terminal
ssh <ssh_user>@10.0.0.10 'echo ok'
# Must print "ok". If it connects without a password, key auth is OK.
```

### 1.3 (Optional) Install DuckDB CLI — only if you also want CLI-based verification in §3

```bash
brew install duckdb
duckdb --version   # v1.1.x recommended (compatible with server image's DuckDB)
```

### 1.4 (Pre-check) Confirm the container is really running in PG primary mode

Before validating, check the staging container's environment variables once.
If `DATAOPS_POSTGRES_DSN` is empty, sensors will fallback to the **legacy DuckDB file**
(`/data/staging.duckdb`) instead of PG, making §3 results meaningless
(see [sensor_db.py:38-49](../../src/vlm_pipeline/lib/sensor_db.py#L38-L49)).

```bash
ssh 10.0.0.10 \
  'docker exec pipeline-test-dagster-1 sh -c "
     echo BACKEND=\$DATAOPS_DB_BACKEND;
     echo DSN_SET=\$([ -n \"\$DATAOPS_POSTGRES_DSN\" ] && echo yes || echo no)
   "'
```

Expected output:
```
BACKEND=postgres
DSN_SET=yes
```

If you see `BACKEND=duckdb` or `DSN_SET=no`, `.env.test` has not been applied to the container.
Restart staging from the host with `cd .../data_pipeline_test/docker && docker compose up -d`
and re-check.

---

## 2. PostgreSQL connection + `pg_duckdb` validation

> 📌 This section is an **infrastructure smoke test**. Production code does not call `pg_duckdb`
> (`grep -rn "pg_duckdb\|duckdb.query\|postgres_scan" src/` → 0 results),
> so a failure here has no immediate pipeline impact. This validation only ensures
> "does it work when an operator runs ad-hoc analysis in DBeaver?"

### 2.1 DBeaver connection setup

**New Database Connection → PostgreSQL.**

#### Main tab

| Field | Value |
|-------|----|
| Host | `localhost` *(when using SSH tunnel)* — or `10.0.0.10` *(direct connection)* |
| Port | `15432` |
| Database | `vlm_pipeline_staging` |
| Username | `airflow` |
| Password | `airflow` |

#### SSH tab (recommended)

Check `Use SSH Tunnel` and fill in:

| Field | Value |
|-------|----|
| Host/IP | `10.0.0.10` |
| Port | `22` |
| User Name | (account used for your usual Mac SSH) |
| Authentication Method | `Public Key` (`~/.ssh/id_ed25519` or `id_rsa`) |

`Test tunnel configuration` → confirm `Connected` → `OK`.

> For **direct connection** (without SSH tunnel), `Mac → 10.0.0.10:15432` must be open
> under network policy. If not, fall back to SSH tunnel.

### 2.2 Initial sanity check after connection

Run the following in the DBeaver SQL editor in order:

```sql
-- (a) Confirm 16 table migration
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY 1;

-- (b) Current row count snapshot (compare this with §3 results)
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

### 2.3 `pg_duckdb` activation validation

```sql
-- (a) Is it registered in shared_preload_libraries?
SHOW shared_preload_libraries;
-- Expected: 'pg_duckdb'

-- (b) Is the extension installed in PG?
SELECT extname, extversion
FROM   pg_extension
WHERE  extname = 'pg_duckdb';
-- Expected: pg_duckdb | 1.1.0

-- (c) DuckDB engine call smoke (external data/literal — duckdb.query only sees the DuckDB catalog)
LOAD 'pg_duckdb';
SELECT * FROM duckdb.query($$ SELECT 'pg_duckdb works' AS msg, 42 AS x $$);

-- (d) Scan PG table in current DB with the DuckDB engine — force_execution pattern
--     ⚠️ duckdb.query($$ ... FROM raw_files ... $$) / postgres_scan('host=localhost ...')
--     neither works (duckdb.query does not see PG tables in its catalog,
--      postgres_scan causes libpq linking collision when called within the same DB). Correct approach:
SET duckdb.force_execution = true;

SELECT COUNT(*) FROM raw_files;
-- Expected: matches raw_files count in (a) or §2.2 (b)

-- (d-1) Prove via EXPLAIN that the DuckDB engine actually ran it
EXPLAIN SELECT COUNT(*) FROM raw_files;
-- Expected first line of plan:
--   Custom Scan (DuckDBScan)  (cost=0.00..0.00 rows=0 width=0)
--     DuckDB Execution Plan:
--     ┌───────────────────────────┐
--     │    UNGROUPED_AGGREGATE    │ ...

-- (d-2) Sample analytics query (DuckDB engine execution) — PG-compatible syntax required.
--      DuckDB-native functions (e.g. quantile_cont(x, 0.5)) are rejected by the PG parser,
--      so only use standard SQL functions also available in PG (percentile_cont WITHIN GROUP, AVG, COUNT, etc.).
SELECT
  source_unit_name,
  COUNT(*)                  AS files,
  AVG(file_size)::bigint    AS avg_size_bytes
FROM   raw_files
GROUP  BY source_unit_name
ORDER  BY files DESC
LIMIT  5;

-- (e) Turn off DuckDB engine (optional) — subsequent queries revert to PG native
SET duckdb.force_execution = false;
```

> 💡 **Three usage patterns for `pg_duckdb` (do not confuse them)**
>
> | Pattern | Purpose | Example |
> |------|------|------|
> | `SET duckdb.force_execution = true; SELECT ...` | Analyze **PG tables in the current DB** with the DuckDB engine | (d), (d-2) |
> | `SELECT * FROM duckdb.query($$ ... $$)` | Read **external data** (parquet/S3/Iceberg/literals) via DuckDB's own catalog | (c), §2.4 |
> | `SELECT * FROM postgres_scan('host=...', 'schema', 'table')` | Connect via libpq to **another PG instance** (not the current one) and read | For multi-DB analysis |
>
> The first pattern is usually sufficient for operational staging/prod use.

### 2.4 (Optional) MinIO object storage — read_parquet / glob / COPY

The real strength of `pg_duckdb` is being able to **directly read/write files on MinIO/S3 inside a PG transaction**. Usage scenarios:

| Scenario | Who uses it | Section |
|---------|---------|---------|
| **PG table → parquet export** (analysis archive, handoff to external tools) | Operator ad-hoc | (3) |
| **Analyze parquet data with SQL inside PG** (external data lake read) | Analysis | (4) |
| **MinIO object inventory/search** (SQL query for which files are where) | Operational checks | (2) |
| **Directly read JSON label events** (JSON files in `vlm-labels`) | Label analysis | (5) |

> 💡 Operational data in staging MinIO is populated by sensors. If staging is empty,
> it is more meaningful to run a dispatch cycle first before attempting this section.
> However (1)(2)(3) can be verified even on an empty state.

#### (1) Register S3 secret (one-time, persistent)

> ⚠️ **`CREATE SECRET ... (TYPE S3, ...)` is DuckDB CLI syntax and does not work inside PG.** pg_duckdb provides a separate function `duckdb.create_simple_secret(...)`.

```sql
-- Register simple S3 secret with staging MinIO credentials
SELECT duckdb.create_simple_secret(
  type      := 'S3',
  key_id    := 'minioadmin',                 -- replace with actual access key
  secret    := 'minioadmin',                 -- replace with actual secret
  endpoint  := '10.0.0.36:9002',         -- staging MinIO API port (prod is :9000)
  url_style := 'path',                       -- MinIO requires path-style
  use_ssl   := 'false'                       -- use string 'true'/'false' (not boolean)
);
-- Expected: simple_s3_secret  (name of registered secret)
```

This secret persists in the PG postgres_data volume and survives container restarts.
To delete: `SELECT duckdb.drop_simple_secret('simple_s3_secret');`.

#### (2) MinIO object inventory (`glob`)

```sql
-- All mp4 files in vlm-raw (recursive ** for all subdirectory paths)
SELECT * FROM duckdb.query($$
  SELECT * FROM glob('s3://vlm-raw/**/*.mp4') LIMIT 10
$$);

-- File count / average size across all buckets (DuckDB list union)
SELECT * FROM duckdb.query($$
  SELECT
    regexp_extract(file, 's3://([^/]+)/', 1) AS bucket,
    COUNT(*)                                  AS files
  FROM glob('s3://vlm-raw/**')
  GROUP BY 1
  ORDER BY files DESC
$$);

-- Only mp4 files for a specific source_unit (extension + path pattern filter simultaneously)
SELECT * FROM duckdb.query($$
  SELECT file FROM glob('s3://vlm-raw/abb_banwoldang/**/*.mp4') LIMIT 5
$$);
```

#### (3) PG table → MinIO parquet export (most practical)

```sql
-- (a) Archive key columns from raw_files as parquet
COPY (
  SELECT asset_id, source_unit_name, ingest_status, file_size,
         media_type, raw_bucket, raw_key, created_at
  FROM   raw_files
  WHERE  created_at >= NOW() - INTERVAL '7 days'
)
TO 's3://vlm-bench-tmp/archive/raw_files_last7d.parquet'
(FORMAT 'parquet', COMPRESSION 'zstd');
-- Output: COPY <N>  (number of rows exported)

-- (b) Export JOIN result as parquet — for handoff to external analysts
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

-- (c) Partition by date — Hive-style partitioning
COPY (
  SELECT *, CAST(created_at AS DATE) AS dt FROM raw_files
)
TO 's3://vlm-bench-tmp/raw_files_partitioned/'
(FORMAT 'parquet', PARTITION_BY ('dt'), OVERWRITE_OR_IGNORE 1);
-- → s3://vlm-bench-tmp/raw_files_partitioned/dt=2026-04-23/data_0.parquet
--   ... per-date folders created automatically. Spark/Athena/BigQuery can read them directly.
```

#### (4) Read parquet data (validation + analysis)

```sql
-- (a) Read-back validation of the (3)(a) export
SELECT * FROM duckdb.query($$
  SELECT
    COUNT(*)                          AS rows,
    COUNT(DISTINCT source_unit_name)  AS units,
    MIN(created_at)                   AS oldest,
    MAX(created_at)                   AS newest
  FROM read_parquet('s3://vlm-bench-tmp/archive/raw_files_last7d.parquet')
$$);

-- (b) Direct analysis of externally received parquet (hypothetical scenario)
SELECT * FROM duckdb.query($$
  SELECT source_unit_name, COUNT(*) AS files
  FROM   read_parquet('s3://vlm-bench-tmp/archive/raw_files_last7d.parquet')
  GROUP  BY 1
  ORDER  BY files DESC
$$);

-- (c) Parquet metadata only (without reading actual data)
SELECT * FROM duckdb.query($$
  SELECT * FROM parquet_metadata('s3://vlm-bench-tmp/archive/raw_files_last7d.parquet')
$$);
-- Column count, row group size, compression, etc. at a glance
```

#### (5) Directly read JSON label events (vlm-labels)

```sql
-- (a) Union all event JSONs in vlm-labels at once
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

-- (b) Labels for a specific source only — advantage of recursive glob
SELECT * FROM duckdb.query($$
  SELECT COUNT(*) AS labels,
         AVG(timestamp_end_sec - timestamp_start_sec) AS avg_duration_sec
  FROM read_json_auto('s3://vlm-labels/abb_banwoldang/**/*.json',
                      union_by_name = true)
$$);
```

#### (6) Cleanup (delete test data)

```sql
-- DuckDB itself has no DELETE FROM s3, so clean up test parquet files via
-- MinIO mc or boto3 separately. Example:
-- (from host)  mc rm --recursive --force staging/vlm-bench-tmp/archive/

-- Delete only the secret:
-- SELECT duckdb.drop_simple_secret('simple_s3_secret');
```

#### Caveats / notes

| Symptom | Cause / action |
|------|-----------|
| `IO Error: HTTP 403 ... AccessDenied` | key_id/secret mismatch in secret, or endpoint points to console port (`:9001`/`:9003`). Use **API port** (staging `:9002`, prod `:9000`) |
| `Catalog Error: Table function with name read_parquet does not exist` | Secret not yet registered, or `LOAD 'pg_duckdb'` not run |
| `glob('s3://bucket/*')` returns 0 rows | `*` is single-level only. Use `**` or `**/*.<ext>` for recursive |
| `CREATE SECRET ... (TYPE S3, ...)` syntax error | DuckDB CLI syntax. Inside PG, use `duckdb.create_simple_secret()` function |
| `COPY ... TO 's3://...' (FORMAT 'parquet')` fails with insufficient permission | `key_id` in secret is a read-only key without PUT permission. Re-register with a write-capable key |

> ✅ **Sequences (1)~(4) above were verified on staging** — `simple_s3_secret`
> registration OK, `COPY ... TO 's3://vlm-bench-tmp/...' (FORMAT 'parquet')` exported
> 100 rows, then `read_parquet` confirmed identical row count.

This path is independent of the sensor operational path (separate from §3), so skipping validation has no pipeline impact. However, since **this is the biggest advantage of PG primary mode** (data that was locked in a single DuckDB file can now be freely exported/imported to/from object storage), it is recommended to verify at least once.

---

## 3. DuckDB → PG ATTACH validation (identical to the sensor operational path)

> 🔑 This section reproduces **the operational sensor read path itself**.
> The `open_sensor_read_connection()` code in [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py):
>
> ```python
> con = duckdb.connect(":memory:")
> con.execute("LOAD postgres")
> con.execute(f"ATTACH '{escaped}' AS pg (TYPE postgres, READ_ONLY)")
> con.execute("USE pg.public")
> return con
> ```
>
> The SQL sequence in §3.3 reproduces these 5 lines byte-for-byte. Therefore, if §3.3 passes,
> the sensor read path also passes equivalently.

### 3.1 SSH tunnel — handling it inside DBeaver (no separate terminal needed)

Instead of keeping an `ssh -L ...` open in a separate terminal, **embedding the SSH tunnel configuration in the DuckDB connection itself** causes DBeaver to automatically set up the tunnel when the connection opens and tear it down when it closes. No need to re-enter SSH commands each time.

DBeaver's SSH tunnel creates an OS-level port forwarding that lives only "while this connection is open". So while the DuckDB connection is open, accessing `localhost:15432` reaches the staging PG exactly — the SQL `ATTACH 'host=localhost port=15432 ...'` works as-is.

Configure in the `SSH tab` in §3.2. Just open the connection — no separate terminal needed.

> 💡 Cases where you still need an OS-level tunnel:
> - You want to connect from other tools (`psql`, `duckdb` CLI, etc.) even when DBeaver is not running → see §3.1.1
> - DBeaver's SSH tunnel does not work in your environment (rare but possible) → §3.1.1

#### 3.1.1 (Alternative) `~/.ssh/config` + autossh — always-on tunnel

If you frequently use other tools, it is cleaner to write it once in Mac's `~/.ssh/config`.

```ssh-config
# ~/.ssh/config
Host staging-pg
    HostName       10.0.0.10
    User           <ssh_user>
    LocalForward   15432 localhost:15432
    ServerAliveInterval 30
    ExitOnForwardFailure yes
```

Normally:

```bash
# Start once and done (background)
ssh -fN staging-pg

# For auto-reconnect on disconnect, use autossh
brew install autossh
autossh -fN staging-pg
```

In this case, leave the DBeaver SSH tab **empty** and set Path to `:memory:`.

### 3.2 DBeaver DuckDB connection setup

**New Database Connection → DuckDB.**

If a driver auto-download dialog appears on first connection, click `Download`.

#### Main tab

| Field | Value |
|-------|----|
| Path | `:memory:` — means only a temporary workspace is created in RAM. No need to create a file since there is nothing to permanently store in DuckDB. (Use a file path like `/tmp/scratch.duckdb` if persistent storage is needed) |

> ⚠️ **Because Path is `:memory:`, the `LOAD postgres` + `ATTACH ...` sequence in §3.3 must be re-run
> every time you open this DuckDB connection** (and after every DBeaver restart). The in-memory
> catalog — including the `pg` attachment — is wiped when the connection closes. `INSTALL postgres`
> is persistent (cached on disk by DuckDB), so re-running it is a fast no-op; `LOAD` and `ATTACH`
> are the ones that must actually re-execute. The `DETACH DATABASE IF EXISTS pg;` prefix in §3.3
> makes the whole block idempotent so you can simply re-run the same SQL on every connection open.

#### `:memory:` vs file path — what actually persists

| State | `:memory:` | File path (e.g. `/tmp/scratch.duckdb`) |
|---|---|---|
| DuckDB extension binary (after `INSTALL postgres`) | persistent (DuckDB's extension repo on disk, shared by all connections) | persistent (same repo) |
| `LOAD postgres` | per-connection only — re-LOAD every time | per-connection only — re-LOAD every time |
| `ATTACH ... AS pg` | wiped on connection close | **persisted inside the .duckdb file** — survives connection restart |
| DuckDB-side `CREATE VIEW`, `CREATE TABLE`, scratch query results | wiped on close | persisted in the file |
| pg_duckdb `simple_s3_secret` | (lives inside PG, not DuckDB — see §2.4) | same — DuckDB path is unrelated |

When to switch from `:memory:` to a file path:

- You want to save analysis VIEWs/intermediate tables on the DuckDB side (e.g. a curated
  `last7d_inventory` materialization for repeated ad-hoc queries)
- You want the `pg` ATTACH to survive DBeaver restarts so you don't have to re-paste the DSN
- You're using non-DBeaver tools (`duckdb` CLI, scripts) that benefit from a stable workspace

Caveats with file paths:

- The `.duckdb` file is a local file on the Mac — not shared between operators, not backed up by
  the pipeline infra. Treat it as scratch.
- The DSN inside an ATTACH is stored in the file as plaintext, so the file inherits the
  sensitivity of the `airflow:airflow` credential. Keep it in `~/Library/Caches/...` or
  `/tmp/...` and clean up periodically.
- Concurrent opens of the same file from two DuckDB processes can conflict on the file lock.
  For ad-hoc analysis from DBeaver alone, this is rarely an issue.
- For production analysis, `:memory:` is still the recommended default — its read-only nature
  reduces the risk of accidentally writing a stale snapshot to disk.

#### SSH tab (§3.1 main approach — recommended)

Check `Use SSH Tunnel` and fill in:

| Field | Value |
|-------|----|
| Host/IP | `10.0.0.10` |
| Port | `22` |
| User Name | (account used for your usual Mac SSH) |
| Authentication Method | `Public Key` (`~/.ssh/id_ed25519` or `id_rsa`) |
| Local host | `localhost` |
| **Local port** | **`15432`** ← must match `port=15432` in the `ATTACH` SQL |
| Remote host | `localhost` *(PG host as seen from the staging server — since the container publishes to 0.0.0.0:15432 on the host, `localhost` is sufficient)* |
| Remote port | `15432` |

> ⚠️ **Do not leave Local port as 0 (auto).** Since the `ATTACH` SQL uses a fixed port,
> explicitly set it to `15432`. If another process is already using 15432, change to `15433`,
> `15434`, etc. and update the `port=` in the §3.3 ATTACH SQL accordingly.

`Test tunnel configuration` → confirm `Connected` → `Test Connection` → `Finish`.

If using the `~/.ssh/config` method from §3.1.1, leave the SSH tab entirely empty.

### 3.3 ATTACH + same SQL as sensor code

> ⚠️ **Reference tables as `pg.<table>`** (`pg.public.<table>` or unqualified names
> after `USE pg.public;` do not work on Mac DuckDB 1.4.x or later).
> The DuckDB postgres extension auto-flattens the PG `public` schema on ATTACH
> and exposes it as `pg.<table>`. The container's DuckDB uses a version where `USE pg.public`
> works, so [`sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py) code is correct —
> this form is only required in DBeaver (Mac).

> 💡 In the DBeaver SQL editor, run statement by statement with `Cmd+Enter`,
> or run all at once with `Cmd+Alt+Enter` (Execute SQL Script). If the cursor is on a
> comment line, you will get `No statements to execute` — move the cursor to a SQL line before running.

> 🔁 **Re-run the entire block (a)→(c) every time you open this DuckDB connection** —
> because Path is `:memory:`, the `LOAD postgres` + `ATTACH ...` state is wiped on close. The
> `DETACH DATABASE IF EXISTS pg;` line in (c) makes the block safely idempotent.

```sql
-- (a) Verify DuckDB itself works
SELECT version();
SELECT extension_name, installed, loaded
FROM   duckdb_extensions()
WHERE  extension_name = 'postgres';
-- Expected: installed=true, loaded=false (Mac DuckDB may need INSTALL on first run)
-- If result is empty, proceed directly to INSTALL in (b).

-- (b) Load postgres extension
INSTALL postgres;   -- no-op if already installed
LOAD postgres;

-- (c) PG ATTACH (READ_ONLY) — same as sensor_db.open_sensor_read_connection()
--     DETACH first to make this block safely re-runnable in the same connection.
--     Without this, re-running the block raises:
--       Binder Error: Failed to attach database: database with name "pg" already exists
DETACH DATABASE IF EXISTS pg;

ATTACH 'host=localhost port=15432 dbname=vlm_pipeline_staging user=airflow password=airflow'
  AS pg (TYPE postgres, READ_ONLY);

-- (d) Are the user tables visible?
--     The pgduckdb image auto-creates two internal helper tables (`extensions`, `tables`)
--     in every database where the extension is registered. Ignore them — they are not
--     part of the operational schema.
--     Staging expected: 16 user tables + 1 migration meta (`_pg_migrations`) + 2 internals = 19
--     Production expected: 17 user tables + 1 migration meta + 2 internals = 20
SHOW TABLES FROM pg;

-- (e) Row count — must exactly match §2.2 (b) results
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

### 3.4 DuckDB columnar acceleration verification (PG GROUP BY acceleration)

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

Run the same query in the DBeaver PG connection (§2) without the `pg.` prefix and compare results.

### 3.5 READ_ONLY guard verification (must error)

```sql
INSERT INTO pg.raw_files (file_path) VALUES ('/tmp/should-fail');
-- Expected: ERROR — something like "read-only attached database"
-- If no error occurs, the ATTACH options are wrong — report immediately.
```

### 3.6 Live read verification (optional, end-to-end)

Verify that DuckDB ATTACH immediately sees new rows entering PG while it is running.

1. Terminal A — trigger staging dispatch (on the server):
   ```bash
   ssh 10.0.0.10
   cd /home/user/work_p/Datapipeline-Data-data_pipeline_test
   python3 scripts/staging_test_dispatch.py --folder tmp_data_2 --round 1
   ```

2. Terminal B — re-run the §3.3 (e) query in the same DBeaver DuckDB editor 5 minutes later.
   If `raw_files` and `dispatch_requests` counts increased, sensor PG write + DuckDB read are both working.

---

## 4. (Optional) Verifying match with container-internal path

If the Mac DuckDB version differs from the container's DuckDB version, ATTACH behavior may differ subtly. Verify at least once that the container produces the same result:

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

Expected result: `raw_files` count matches both §2.2 (b) and §3.3 (e).

---

## 5. Troubleshooting

| Symptom | Cause / action |
|------|------------|
| DBeaver PG connection timeout | SSH tunnel not running or network policy. Test `ssh` directly per §1.2 → redo SSH tunnel in §2.1 |
| `LOAD 'pg_duckdb'` → `could not load library` | `shared_preload_libraries` not applied at container startup. Re-check with `docker exec pipeline-test-postgres-1 psql ... -c "SHOW shared_preload_libraries"`. If compose's postgres `command:` is empty, check for missing `POSTGRES_PRELOAD_LIBS=pg_duckdb` in `.env.test` |
| `(PGDuckDB/CreatePlan) ... libpq is incorrectly linked to backend functions` | Called `postgres_scan('host=localhost ...')` within the same DB. `postgres_scan` is exclusively for other PG instances. For analyzing PG tables in the same DB, use `SET duckdb.force_execution = true; SELECT ...` (§2.3 (d)) |
| `(PGDuckDB/CreatePlan) ... Catalog Error: Table with name raw_files does not exist! Did you mean "pg_views"?` | Called with `SELECT * FROM duckdb.query($$ ... FROM raw_files $$)`. `duckdb.query` only sees DuckDB's own catalog (parquet/S3, etc.), not PG tables. Use the same `force_execution` pattern in §2.3 (d) |
| `function quantile_cont(bigint, numeric) does not exist` (in force_execution mode) | pg_duckdb first validates SQL through the PG parser before executing in DuckDB. DuckDB-native functions are unknown to the PG parser and rejected. Use only PG-compatible standard SQL (`percentile_cont WITHIN GROUP`, `AVG`, `COUNT`, etc.) |
| Sensor is reading legacy DuckDB instead of PG (§3 result is correct but count differs from §2) | `DATAOPS_POSTGRES_DSN` not set inside container → re-check §1.4. May be a case where `.env.test` changes were not applied after `docker compose up -d` |
| DBeaver DuckDB → `connection refused` on ATTACH | DBeaver SSH tunnel not running. (a) Redo `Test tunnel configuration` in SSH tab of §3.2. (b) Local port is set to auto(0) instead of 15432 — explicitly set to 15432. (c) If using `~/.ssh/config` method, check tunnel listener with `lsof -i :15432` |
| `IO Error: ... connection to server at "localhost" ... port NNNN failed: Connection refused` (from DuckDB ATTACH only) | **DBeaver's SSH tunnel does not always wrap the libpq TCP calls that the DuckDB postgres extension issues** — the PG connection in the same DBeaver shows ESTABLISHED to the remote host but ATTACH still fails because there is no OS-level listener on `localhost:NNNN`. Verified workaround: launch an OS-level tunnel from §3.1.1 (`ssh -fN staging-pg` or `autossh -M 0 -fN staging-pg`), confirm with `lsof -i :NNNN` that an `ssh ... LISTEN` line appears, then retry the ATTACH. Alternatively, change the ATTACH DSN to `host=10.0.0.10` (direct), which bypasses the tunnel entirely — only works if the network policy allows direct access to the host port. |
| DBeaver SSH tunnel passes Test but only ATTACH is refused | Same root cause as the row above. DBeaver's SSH tunnel handles JDBC drivers but not in-process DuckDB libpq calls. Fall back to the OS-level tunnel in §3.1.1 |
| `Binder Error: Failed to attach database: database with name "pg" already exists` | A previous ATTACH in the same DuckDB session has not been detached. Prepend the ATTACH block with `DETACH DATABASE IF EXISTS pg;` (Mac DuckDB 1.4.x supports this). Closing and reopening the `:memory:` connection also clears it |
| `SHOW TABLES FROM pg` shows 18-20 tables instead of the documented 16 | Normal — pgduckdb's pg_duckdb extension auto-creates two internal helper tables (`extensions`, `tables`) in every database where it is loaded. They are not part of the operational schema and should be ignored when validating row counts |
| Local port 15432 already in use | Another process is occupying it. Confirm with `lsof -i :15432`, terminate the process, or change Local port in §3.2 and `port=` in §3.3 (c) ATTACH SQL to the same new port (e.g. 15433) |
| ATTACH succeeded but `SHOW TABLES` is empty | Use `SHOW TABLES FROM pg;` to explicitly specify the attached DB. DuckDB's default DB is in-memory, so you must specify the attached `pg` |
| `Table with name raw_files does not exist! Did you mean "pg.raw_files"?` | Mac DuckDB 1.4.x and later flatten the PG `public` schema on ATTACH and expose it as `pg.<table>`. Use **`pg.raw_files`**. `USE pg.public` does not work in this version |
| `No statements to execute` (DBeaver) | Cursor is on a comment (`--`) line. Move cursor to a SQL line or select all and use `Cmd+Alt+Enter` (Execute SQL Script) |
| `duckdb_extensions()` result is empty | DuckDB hasn't populated the catalog yet. Just proceed directly to `INSTALL postgres; LOAD postgres;` in §3.3 (b) |
| INSERT succeeds | `READ_ONLY` missing from ATTACH options. Immediately ROLLBACK, then `DETACH pg;` and re-run §3.3 (c) |
| Mac duckdb version ≠ server version | If results differ from §4 container-internal results, match Mac duckdb to the same minor version as the server (`brew install duckdb@1.1`, etc.) |

---

## 6. Checklist (one-page summary)

**Pre-checks**
- [ ] §1.4 Confirmed `BACKEND=postgres`, `DSN_SET=yes` inside container

**§2 PG connection + `pg_duckdb` (infrastructure smoke)**
- [ ] §2.1 DBeaver PG connection (SSH tunnel) passed
- [ ] §2.2 (a) All 16 tables visible
- [ ] §2.2 (b) Row count snapshot captured
- [ ] §2.3 (a)(b) `shared_preload_libraries=pg_duckdb`, extension 1.1.0
- [ ] §2.3 (c)(d) `duckdb.query()` works, `postgres_scan` count matches

**§3 DuckDB → PG ATTACH (operational sensor read path)**
- [ ] §3.2 DBeaver DuckDB connection SSH tunnel (Local port=15432) Test passed
- [ ] §3.3 (b) postgres extension LOAD succeeded
- [ ] §3.3 (b) postgres extension LOAD succeeded
- [ ] §3.3 (c)(d) ATTACH succeeded and 16 tables visible
- [ ] §3.3 (e) Row count matches §2.2 (b)
- [ ] §3.5 INSERT rejected as READ_ONLY
- [ ] (Optional) §3.6 Count increased after dispatch trigger
- [ ] (Optional) §4 Results match container-internal

---

## 7. When applying to the production environment

> This guide is written for staging (`dev`, `:3031`). When applying the same validation
> to production (`main`, `:3030`), the **prerequisites** and **differences** are different.
> Read this section first and confirm the progress of
> [Production PG Transition Rollout Plan](../exec-plans/active/production-pg-rollout-plan.md)
> before proceeding.

### 7.1 Prerequisites (production validation is meaningless without these)

**Current state (2026-05-13): production is at Phase 6 (`postgres` single mode).** §2 and §3 are
both fully meaningful — sensors read PG via ATTACH on every tick (the same code path that §3.3
reproduces). The §3.6 live read check is also now meaningful, but new ingest only writes to PG (not
DuckDB), so the `pipeline.duckdb` mtime should stay frozen as new rows appear in PG.

Historical phase compatibility (kept for reference if a future operator rewinds the rollout):

- Phase 1 (code merge only): vanilla postgres image unchanged → §2 (`pg_duckdb`)
  validation impossible (extension does not exist)
- Phase 2 (image swap complete): §2 validation possible. However `vlm_pipeline` DB is still
  empty so §3 row count will be 0.
- Phase 3-4 (DB creation + backfill complete): §2/§3 both meaningful. However sensors still
  use DuckDB primary so §3 results are *a past-point snapshot*.
- Phase 5 (`dual_pg_primary`): §3.6 live read validation starts to be meaningful — sensors read
  via PG ATTACH and write to both backends.
- **Phase 6 (`postgres`)** ← *current*: only PG receives writes; DuckDB file is frozen at the
  Phase 5/6 cutover point and is kept on disk only as a passive backup.

### 7.2 Staging vs production differences — one-page table

| Item | Staging (`:3031`) | **Production (`:3030`)** | Notes |
|------|------------------|---------------------------|------|
| Dagster UI | `http://10.0.0.10:3031` | `http://10.0.0.10:3030` | |
| Postgres container name | `pipeline-test-postgres-1` | `docker-postgres-1` | Used in `docker exec` |
| Dagster container name | `pipeline-test-dagster-1` | `docker-dagster-1` | |
| Compose project | `pipeline-test` | `docker` | |
| DB name | `vlm_pipeline_staging` | **`vlm_pipeline`** | Change dbname in all DSNs/connections |
| DSN (inside container) | `postgresql://airflow:airflow@postgres:5432/vlm_pipeline_staging` | `postgresql://airflow:airflow@postgres:5432/vlm_pipeline` | |
| Host PG port (published) | `15432` | **Not published by default** — temporarily add `POSTGRES_PORT=15433` to `.env` for validation (§7.3) | Avoids collision with staging |
| DuckDB file (container) | `/data/staging.duckdb` | `/data/pipeline.duckdb` | When comparing during §3.6 live read |
| DuckDB file (host) | `Datapipeline-Data-data_pipeline_test/docker/data/staging.duckdb` | `Datapipeline-Data-data_pipeline/docker/data/pipeline.duckdb` | When monitoring mtime |
| MinIO endpoint (API) | `10.0.0.36:9002` | **`10.0.0.36:9000`** | When creating §2.4 secret |
| MinIO Console | `:9003` | `:9001` | Confirmation UI |
| MinIO credentials | `minioadmin/minioadmin` (default) | Check `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` in `.env` (usually different) | |
| `.env` file | `docker/.env.test` (host: `..._test/docker/.env.test`) | `docker/.env` | Not tracked by git |
| Auto-redeploy trigger | `dev` push | `main` push | Manual host src edits prohibited |

### 7.3 Temporarily expose production PG host port

Production `.env` does not set `POSTGRES_PORT` by default, so it is not published to the host port (security). Add it temporarily for DBeaver validation:

```bash
ssh 10.0.0.10
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# Edit .env (from host, using vi etc.)
# Add the following line — using 15433 to avoid collision with staging's 15432
# POSTGRES_PORT=15433

cd docker
docker compose up -d postgres   # recreate only postgres (no impact on other services)
docker port docker-postgres-1   # verify 5432/tcp -> 0.0.0.0:15433
```

**Always revert after validation** (security):

```bash
# Delete POSTGRES_PORT=15433 line from .env, then
cd /home/user/work_p/Datapipeline-Data-data_pipeline/docker
docker compose up -d postgres
docker port docker-postgres-1   # (no output → host port closed, confirmed)
```

### 7.4 Add prod-pg block to SSH config (separate from staging-pg)

In Mac `~/.ssh/config`, separate from the staging entry (`staging-pg`):

```ssh-config
Host prod-pg
    HostName       10.0.0.10
    User           user
    LocalForward   15433 localhost:15433       # ← separate from staging's 15432
    ServerAliveInterval 30
    ExitOnForwardFailure yes
```

> ⚠️ Use a **different port** (15433) so it can be raised simultaneously with staging-pg.
> If both use the same port (15432), the second one will fail to bind.

Raise:

```bash
autossh -M 0 -fN prod-pg
lsof -i :15433       # verify ssh ... LISTEN
nc -zv localhost 15433
```

### 7.5 DBeaver connection setup changes (add separate production connections)

Create two separate connections, leaving the staging connections as-is. The SSH tunnel must be
configured on **each connection independently** — DBeaver does not share a tunnel across the PG
and DuckDB connections automatically. (The `Share this tunnel with other connections` checkbox
helps the JDBC drivers but not the in-process DuckDB libpq calls — see the row about
`Connection refused on localhost` in §5.)

#### `prod-vlm-pipeline` (PostgreSQL)

| Tab | Field | Value |
|----|-------|-----|
| Main | Host | `localhost` (when using SSH tunnel) or `10.0.0.10` (direct, if network policy allows) |
| Main | **Port** | **`15433`** |
| Main | **Database** | **`vlm_pipeline`** |
| Main | Username / Password | `airflow` / `airflow` (if different, check `POSTGRES_USER`/`POSTGRES_PASSWORD` in `.env`) |
| SSH | Use SSH Tunnel | ✅ |
| SSH | Host/IP, Port | `10.0.0.10`, `22` |
| SSH | User Name, Auth | `user`, Password (same as staging) |
| SSH → Advanced settings → Port Forwarding | Local host, **Local port** | `localhost`, **`15433`** (do not leave as auto/0) |
| SSH → Advanced settings → Port Forwarding | Remote host, **Remote port** | `localhost`, **`15433`** |
| SSH → Advanced settings | Share this tunnel | ✅ (lets the PG connection reuse the tunnel across re-opens) |

#### `prod-duckdb-attach` (DuckDB)

| Tab | Field | Value |
|----|-------|-----|
| Main | Path | `:memory:` |
| SSH | (same SSH tab as `prod-vlm-pipeline`) | — |

The ATTACH SQL is the only DSN-dependent part — `host` matches the connection's effective endpoint:

```sql
DETACH DATABASE IF EXISTS pg;   -- safe re-run (Mac DuckDB 1.4.x supports IF EXISTS)

ATTACH 'host=localhost port=15433 dbname=vlm_pipeline user=airflow password=airflow'
  AS pg (TYPE postgres, READ_ONLY);
```

> 💡 If the ATTACH still hits `Connection refused on localhost` even after both SSH tabs are
> configured, the safest workaround is to launch an OS-level tunnel from §7.4 (`autossh -M 0 -fN
> prod-pg`) which produces a real `ssh ... LISTEN` line on `localhost:15433`. The DuckDB postgres
> extension makes its own libpq call out-of-band of DBeaver's JVM-side SSH wrapper, and on some
> environments only the OS-level listener is reachable.

### 7.6 §1.4 (Pre-check) Production version

```bash
ssh 10.0.0.10
docker exec docker-dagster-1 sh -c '
  echo BACKEND=$DATAOPS_DB_BACKEND;
  echo DSN_SET=$([ -n "$DATAOPS_POSTGRES_DSN" ] && echo yes || echo no)
'
```

Expected value **depends on rollout phase**:

| Phase | BACKEND | DSN_SET |
|-------|---------|---------|
| 1 (code merge complete) | (empty) | no |
| 2-4 (image swap, DB/backfill complete) | (empty) | no |
| 5 (dual_pg_primary) | `dual_pg_primary` | yes |
| **6 (postgres single)** ← *current production state* | **`postgres`** | **yes** |

`SHOW TABLES FROM pg` / row count validation in §3 becomes meaningful only **from Phase 5**.

#### Reference row counts captured at the Phase 6 cutover (2026-05-13)

Use these as an upper-bound sanity check — actual production counts only grow from here. Any
serious divergence (e.g. PG missing rows that DuckDB still has) would indicate an incomplete
backfill, not a production issue.

| Table | Rows at Phase 6 cutover |
|---|---:|
| `raw_files` | 23,592 |
| `video_metadata` | 23,576 |
| `image_metadata` | 10,073 |
| `processed_clips` | 2,060 |
| `labels` | 1,608 |
| `image_labels` | 288 |
| `dispatch_pipeline_runs` | 15 |
| `datasets` | 5 |
| `dispatch_requests` | 4 |
| `staging_model_configs` | 3 |

### 7.7 Production environment differences in §2/§3 SQL

Most SQL works as-is, but the **dbname inside the DSN string** must be changed.

#### §2.4 MinIO secret — production version

```sql
-- After confirming production MinIO endpoint and credentials (different name from previous staging secret)
SELECT duckdb.create_simple_secret(
  type      := 'S3',
  key_id    := '<production MINIO_ROOT_USER>',
  secret    := '<production MINIO_ROOT_PASSWORD>',
  endpoint  := '10.0.0.36:9000',         -- ← not staging 9002
  url_style := 'path',
  use_ssl   := 'false'
);
```

> ⚠️ Secrets persist per PG, so secrets registered inside production PG are
> unrelated to staging PG. No name collision concern. However, note that **production credentials are stored in plaintext inside production PG** — ensure only operators with PG superuser access can manage this.

#### §2.4 (3) PG → parquet export — production environment safety guard

```sql
-- ❌ Direct export to production data buckets is prohibited
-- COPY (...) TO 's3://vlm-labels/...' (FORMAT 'parquet');     -- contaminates label source of truth

-- ✅ Always use analysis-only buckets / folders
COPY (SELECT ... FROM raw_files LIMIT 1000)
TO 's3://vlm-bench-tmp/adhoc/raw_files_snapshot.parquet'
(FORMAT 'parquet');
```

Do **not** write ad-hoc analysis files to the 5 core production buckets (`vlm-raw`, `vlm-labels`, `vlm-processed`, `vlm-dataset`, `vlm-classification`). Use only separate buckets
(e.g. `vlm-bench-tmp` or a newly created `vlm-analysis`).

### 7.8 Production environment absolute prohibitions (repeated)

- **No ATTACH without READ_ONLY** — sensor code explicitly specifies `READ_ONLY`. Ad-hoc validation must do the same. A mistaken INSERT via a write-capable ATTACH contaminates the source of truth
- **No ad-hoc file writes to production data buckets** (§7.7)
- **Leaving `POSTGRES_PORT=15433` temporary publish unrevertted after validation → security hole**. Always revert per the last paragraph of §7.3
- **No triggering sensor / dispatch while someone else is validating** — live read counts fluctuate, undermining validation reliability
- **Clean up secrets after validation** — `SELECT duckdb.drop_simple_secret('simple_s3_secret');`. Prevent credentials from remaining in plaintext inside PG indefinitely as personnel changes

### 7.9 Production validation checklist (§6 production version)

- [ ] §7.1 Confirm which rollout phase (§3.6 live read is meaningless before Phase 5)
- [ ] §7.3 `POSTGRES_PORT=15433` temporary publish, `docker compose up -d postgres` passed
- [ ] §7.4 `prod-pg` Host block + autossh `-M 0 -fN` passed, `nc -zv localhost 15433` succeeded
- [ ] §7.5 DBeaver production connection (PG + DuckDB) passed — separate from staging connection
- [ ] §7.6 `BACKEND` / `DSN_SET` match the phase
- [ ] §2.2 (b) Production PG row count snapshot captured (should be higher than staging as production data)
- [ ] §3.3 (e) DuckDB ATTACH result matches §2.2
- [ ] §3.5 INSERT rejected (very important in production)
- [ ] **After validation** remove `POSTGRES_PORT` line per §7.3 + `docker compose up -d postgres`
- [ ] **After validation** clean up autossh with `pkill -f prod-pg`
- [ ] **After validation** clean up secret with `SELECT duckdb.drop_simple_secret(...)`

---

## Reference

- Feature specification: [`src/vlm_pipeline/lib/sensor_db.py`](../../src/vlm_pipeline/lib/sensor_db.py) (`open_sensor_read_connection`)
- Environment separation policy: [`docs/exec-plans/운영_테스트_환경_분리_자동배포_계획.md`](../exec-plans/운영_테스트_환경_분리_자동배포_계획.md)
- DB migration topology: [`db_migration_topology.md`](db_migration_topology.md)
- pg_duckdb official: <https://github.com/duckdb/pg_duckdb>
- DuckDB postgres_scanner official: <https://duckdb.org/docs/extensions/postgres>
