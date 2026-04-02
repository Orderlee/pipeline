"""DuckDB INGEST 도메인 — raw_files CRUD, metadata INSERT 등."""

from __future__ import annotations

from vlm_pipeline.resources.duckdb_ingest_dispatch import DuckDBIngestDispatchMixin
from vlm_pipeline.resources.duckdb_ingest_metadata import DuckDBIngestMetadataMixin
from vlm_pipeline.resources.duckdb_ingest_raw import DuckDBIngestRawMixin


class DuckDBIngestMixin(DuckDBIngestDispatchMixin, DuckDBIngestRawMixin, DuckDBIngestMetadataMixin):
    """INGEST 관련 DuckDB 메서드 mixin."""
