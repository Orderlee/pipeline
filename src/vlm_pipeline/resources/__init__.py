"""Dagster resources — PostgreSQL, MinIO, runtime settings.

PostgresResource uses mixin composition (file-based DuckDB was removed
2026-05-19; analytical queries now use the ``pg_duckdb`` extension):

    postgres.py                 PostgresResource (combining class)
    postgres_base.py            PostgresBaseMixin (connect / pool / introspection)
    postgres_migration.py       PostgresMigrationMixin (schema ensure / migration)
    postgres_phash.py           PostgresPhashMixin (perceptual hash prefix lookup)
    postgres_dedup.py           PostgresDedupMixin (deduplication queries)
    postgres_ingest.py          PostgresIngestMixin (dispatch + raw + metadata)
    postgres_ingest_dispatch.py Dispatch tracking CRUD
    postgres_ingest_raw.py      Raw files CRUD
    postgres_ingest_metadata.py Image/video metadata CRUD
    postgres_labeling.py        PostgresLabelingMixin (label / caption queries)
    postgres_spec.py            PostgresSpecMixin (labeling spec queries)
    postgres_genai.py           PostgresGenAIMixin (genai batch / job CRUD)
"""
