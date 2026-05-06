"""PG INGEST 도메인 — raw_files CRUD, metadata INSERT 등."""

from __future__ import annotations

from vlm_pipeline.resources.postgres_ingest_dispatch import PostgresIngestDispatchMixin
from vlm_pipeline.resources.postgres_ingest_metadata import PostgresIngestMetadataMixin
from vlm_pipeline.resources.postgres_ingest_raw import PostgresIngestRawMixin


class PostgresIngestMixin(PostgresIngestDispatchMixin, PostgresIngestRawMixin, PostgresIngestMetadataMixin):
    """INGEST 관련 PostgreSQL 메서드 mixin."""
