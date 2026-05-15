# DuckDB Lock Contention Root Cause Analysis

> Date: 2026-04-03 | Analysis session: debug-578762

## 1. Problem Description

Repeated DuckDB write lock conflicts occurred during `ingest_job` execution.
Error message: `IO Error: Could not set lock on file "/data/pipeline.duckdb": Conflicting lock is held in /usr/bin/python3.10 (PID N)`

The lock holder PID was identified as the **dagster-code-server gRPC process** or **dagster-webserver**.

## 2. Root Cause

### Sensors Hold the DuckDB Write Lock for Extended Periods on Every Tick

Dagster sensors run inside the gRPC process (PID 56) of `dagster-code-server`.
When a sensor calls `db.ensure_runtime_schema()`, it opens a connection via **`duckdb.connect(db_path)`** (default read-write mode), acquiring the write lock.

### Runtime Log Evidence

| Sensor | Call | Lock hold duration |
|------|------|---------------|
| `production_agent_dispatch_sensor` | `ensure_runtime_schema()` | **~5.8 sec** |
| `production_agent_dispatch_sensor` | `ensure_dispatch_tracking_tables()` | **~1.2 sec** |
| `dispatch_sensor` | `ensure_runtime_schema()` | **~5.8 sec** |
| `dispatch_sensor` | `ensure_dispatch_tracking_tables()` | **~1.2 sec** |

Acquiring `duckdb.connect()` itself takes **~1.2–1.4 seconds** (DuckDB WAL/internal initialization).

### Lock Time Utilization

In every ~30–34 second cycle, **the two sensors sequentially hold the write lock for a total of ~14 seconds**.
**Approximately 47% of all time is spent on unnecessary DDL execution by sensors**.

```
Timeline (30-second cycle):
[=====Sensor A schema 5.8s=====][==Sensor A dispatch 1.2s==][gap][=====Sensor B schema 5.8s=====][==Sensor B dispatch 1.2s==]
|<--------------------------- ~14 sec lock held ---------------------------->|<--- ~16 sec available --->|
```

### Why `ensure_runtime_schema()` Is Slow

Inside `ensure_runtime_schema()`, using a single write connection:
1. Full `schema.sql` DDL execution (`CREATE TABLE IF NOT EXISTS` × 8 tables)
2. `_ensure_image_metadata_columns()` — column existence check + conditional ALTER
3. `_ensure_video_metadata_frame_columns()` — same pattern
4. `_ensure_labels_columns()` — same pattern
5. `_ensure_processed_clips_columns()` — same pattern
6. `_ensure_image_labels_table()` — same pattern
7. `_ensure_staging_dispatch_columns()` — same pattern
8. `_ensure_staging_model_configs()` — same pattern
9. `_ensure_staging_pipeline_runs()` — same pattern
10. `_ensure_raw_files_spec_columns()` — same pattern
11. `_ensure_video_metadata_stage_columns()` — same pattern
12. `_ensure_video_metadata_reencode_columns()` — same pattern

Each `_ensure_*` method performs an `information_schema.columns` lookup + conditional `ALTER TABLE ADD COLUMN`.
Even when the schema is already complete, **12 sequential metadata lookups execute inside the write lock**, taking ~5.8 seconds.

## 3. Affected Sensors (Production)

| Sensor | Interval | ensure_runtime_schema call | Write lock |
|------|----------|--------------------------|------------|
| `dispatch_sensor` | 30 sec | ✅ + `ensure_dispatch_tracking_tables` | ✅ |
| `production_agent_dispatch_sensor` | configured | ✅ + `ensure_dispatch_tracking_tables` | ✅ |
| `spec_resolve_sensor` | 60 sec | ✅ + `db.connect()` (write) | ✅ |
| `ready_for_labeling_sensor` | 60 sec | ✅ | ✅ |
| `incoming_manifest_sensor` | 180 sec | ❌ | ❌ |
| `auto_bootstrap_manifest_sensor` | 180 sec | ❌ | ❌ |
| `stuck_run_guard_sensor` | 120 sec | ❌ | ❌ |

Sensors with direct read-only connections (not affected):
- `process/sensor.py` — `duckdb.connect(read_only=True)`
- `sync/sensor.py` — `duckdb.connect(read_only=True)`
- `yolo/sensor.py` — `duckdb.connect(read_only=True)`
- `label/sensor.py` — `duckdb.connect(read_only=True)`

## 4. Proposed Fixes

### 4.1 Minimize `ensure_runtime_schema()` Calls (High Priority)

**Goal:** Prevent sensors from calling `ensure_runtime_schema()` on every tick.

**Option A — Process-level one-time initialization (recommended)**
- Add a class-level flag `_schema_ensured: bool = False` to `DuckDBResource`
- Execute `ensure_runtime_schema()` only on the first call; skip thereafter
- Advantage: minimal code change, DDL runs only once at process startup
- Disadvantage: flag resets on hot-reload (a dagster code-server restart is sufficient)

**Option B — Call only once at startup**
- Remove `ensure_runtime_schema()` calls from sensor code
- Instead, run it once in `DuckDBResource`'s `setup_for_execution()` or a dedicated startup hook
- Advantage: cleaner sensor code
- Disadvantage: depends on Dagster lifecycle hooks

### 4.2 Switch Sensor DuckDB Access to read_only (Medium Priority)

Review whether sensors other than `spec_resolve_sensor` actually write anything after `ensure_runtime_schema()`.
Sensors that do not need DDL guarantees can be switched to `duckdb.connect(read_only=True)`.

### 4.3 Optimize `ensure_runtime_schema()` Performance (Low Priority)

- Consolidate 12 `_ensure_*` calls into a single query (batch `information_schema` lookup)
- Skip `ALTER TABLE` when no columns are missing
- The DDL itself already uses `IF NOT EXISTS`, so the cost is not huge, but metadata lookup overhead can be reduced

## 5. Expected Impact

| Metric | Current | After Fix |
|------|------|---------|
| Write lock time per sensor cycle | ~14 sec | ~0 sec (Option A/B) |
| DuckDB available time ratio | ~53% | ~100% |
| ingest_job lock conflict probability | High (47% overlap) | Near 0% |
