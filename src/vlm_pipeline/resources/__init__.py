"""Dagster resources — DuckDB, MinIO, runtime settings.

DuckDB resource uses mixin composition:
    duckdb.py           DuckDBResource (combining class)
    duckdb_base.py      DuckDBBaseMixin (connect, lock, schema DDL)
    duckdb_phash.py     DuckDBPhashMixin (perceptual hash prefix candidates)
    duckdb_migration.py DuckDBMigrationMixin (schema ensure/migration)
    duckdb_dedup.py     DuckDBDedupMixin (deduplication queries)
    duckdb_ingest.py    DuckDBIngestMixin (combining ingest sub-mixins)
    duckdb_ingest_dispatch.py  Dispatch tracking tables/queries
    duckdb_ingest_raw.py       Raw files CRUD
    duckdb_ingest_metadata.py  Image/video metadata CRUD
    duckdb_labeling.py  DuckDBLabelingMixin (label/caption queries)
    duckdb_spec.py      DuckDBSpecMixin (labeling spec queries)
"""
