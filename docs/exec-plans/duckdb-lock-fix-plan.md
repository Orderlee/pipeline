# DuckDB Lock Contention Fix — Execution Plan

> Reference document: [DuckDB Lock Contention Analysis](../design-docs/duckdb-lock-contention-analysis.md)
> Date: 2026-04-03

## Goal

Eliminate repeated `ensure_runtime_schema()` calls from sensors to reduce DuckDB write lock occupancy from ~47% → ~0% and fundamentally resolve `ingest_job` lock conflicts.

## Phase 1: Limit `ensure_runtime_schema()` to One Call per Process

### Target

**`src/vlm_pipeline/resources/duckdb_migration.py`** — `ensure_runtime_schema()` method

### Design

```python
# Add to DuckDBMigrationMixin class
_runtime_schema_ensured: ClassVar[bool] = False

def ensure_runtime_schema(self) -> None:
    if DuckDBMigrationMixin._runtime_schema_ensured:
        return
    # Execute existing DDL + _ensure_* logic
    ...
    DuckDBMigrationMixin._runtime_schema_ensured = True
```

- Declared as `ClassVar[bool]` to bypass Pydantic/ConfigurableResource field validation
- All `DuckDBResource` instances within the process share the flag
- DDL executes only on the first call; subsequent calls return immediately
- Automatically resets when the code-server restarts (process-level variable)

### Affected Scope

- All sensors/assets that call `ensure_runtime_schema()` benefit automatically
- No changes to call sites — only the internal logic changes

### Risk

- Extremely low: there are no cases where the schema changes dynamically after process startup
- `ensure_schema()` (full migration) always runs regardless of the flag

## Phase 2: Apply Same Pattern to `ensure_dispatch_tracking_tables()`

### Target

**`src/vlm_pipeline/resources/duckdb_ingest_dispatch.py`**

### Design

```python
_dispatch_tables_ensured: ClassVar[bool] = False

def ensure_dispatch_tracking_tables(self) -> None:
    if DuckDBIngestDispatchMixin._dispatch_tables_ensured:
        return
    # Execute existing CREATE TABLE IF NOT EXISTS
    ...
    DuckDBIngestDispatchMixin._dispatch_tables_ensured = True
```

## Phase 3: Review Sensor DuckDB Access Mode (optional)

The actual write logic performed by `spec_resolve_sensor` and
`ready_for_labeling_sensor` after `ensure_runtime_schema()`:
- `db.update_spec_status()`, `db.update_spec_resolved_config()` → **write required**
- These do not have the `duckdb_writer` tag and are therefore outside concurrency limits

Phases 1–2 alone reduce lock occupancy to a single call (~7 seconds) on the first
invocation, making Phase 3 optional.

## Phase 4: Remove Instrumentation

Remove the debug log instrumentation from `duckdb_base.py` after analysis is complete.

## Expected Outcomes

| Metric | Current | After Phase 1+2 |
|--------|---------|-----------------|
| Write lock per sensor cycle | ~14 seconds | 0 seconds (only ~7 seconds on first cycle) |
| DuckDB availability within 30s | ~53% | ~100% |
| ingest_job lock conflicts | recurring | nearly 0 |

## Implementation Order

1. Phase 1: modify `duckdb_migration.py`
2. Phase 2: modify `duckdb_ingest_dispatch.py`
3. Docker image rebuild + service restart
4. Verify using instrumentation logs (confirm `held_sec` approaches 0)
5. Phase 4: remove instrumentation + final deploy
