"""PostgresResource — Dagster ConfigurableResource, mixin 합성 클래스.

Mixin 구조:
    PostgresBaseMixin (connect / pool / introspection + migration runner)
    PostgresIngestMixin   (Dispatch + Raw + Metadata via inheritance)
    PostgresDedupMixin
    PostgresLabelingMixin
    PostgresSpecMixin
    PostgresGenAIMixin    (genai_batches / genai_jobs CRUD + status rollup)

Import:
    from vlm_pipeline.resources.postgres import PostgresResource
"""

from dagster import ConfigurableResource

from .postgres_base import PostgresBaseMixin
from .postgres_dedup import PostgresDedupMixin
from .postgres_genai import PostgresGenAIMixin
from .postgres_ingest import PostgresIngestMixin
from .postgres_labeling import PostgresLabelingMixin
from .postgres_spec import PostgresSpecMixin


class PostgresResource(
    PostgresBaseMixin,
    PostgresIngestMixin,
    PostgresDedupMixin,
    PostgresLabelingMixin,
    PostgresSpecMixin,
    PostgresGenAIMixin,
    ConfigurableResource,
):
    """PostgreSQL 통합 리소스 — 섹션별 CRUD 메서드 mixin 합성.

    Fields (all from PostgresBaseMixin):
        dsn      : ``postgresql://user:pass@host:port/dbname``
        pool_min : ThreadedConnectionPool 최소 슬롯 (default 2)
        pool_max : ThreadedConnectionPool 최대 슬롯 (default 10)
    """

    # 필드는 PostgresBaseMixin 에 선언되어 있고 그대로 상속된다.
    pass
