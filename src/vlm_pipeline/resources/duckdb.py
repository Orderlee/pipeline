"""DuckDBResource — Dagster ConfigurableResource, mixin 합성 클래스.

기존 import 경로 호환: `from vlm_pipeline.resources.duckdb import DuckDBResource`
"""

from dagster import ConfigurableResource

from .duckdb_base import DuckDBBaseMixin
from .duckdb_dedup import DuckDBDedupMixin
from .duckdb_ingest import DuckDBIngestMixin
from .duckdb_labeling import DuckDBLabelingMixin
from .duckdb_spec import DuckDBSpecMixin


class DuckDBResource(
    DuckDBBaseMixin,
    DuckDBIngestMixin,
    DuckDBDedupMixin,
    DuckDBLabelingMixin,
    DuckDBSpecMixin,
    ConfigurableResource,
):
    """DuckDB 통합 리소스 — 섹션별 CRUD 메서드 제공."""

    db_path: str
