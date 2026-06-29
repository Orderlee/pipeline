"""PostgresResource — Dagster ConfigurableResource, mixin 합성 클래스.

Mixin 구조:
    PostgresBaseMixin (connect / pool / introspection + migration runner)
    PostgresIngestMixin   (Dispatch + Raw + Metadata via inheritance)
    PostgresDedupMixin    (phash / dup_group — inherits ProcessMixin + BuildMixin)
    PostgresProcessMixin  (labels insert, processable, processed_clips CRUD)
    PostgresBuildMixin    (dataset-build + classification-build queries)
    PostgresLabelingMixin  (auto-label, clip image extract; inherits PostgresDetectionMixin)
    PostgresDetectionMixin (image_labels CRUD + detection 대상 이미지 조회; inherited via Labeling — not listed explicitly to keep C3 MRO valid)
    PostgresSpecMixin
    PostgresGenAIMixin    (genai_batches / genai_jobs CRUD + status rollup)

Import:
    from vlm_pipeline.resources.postgres import PostgresResource
"""

from dagster import ConfigurableResource

from .postgres_base import PostgresBaseMixin
from .postgres_build import PostgresBuildMixin
from .postgres_dedup import PostgresDedupMixin
from .postgres_embedding import PostgresEmbeddingMixin
from .postgres_genai import PostgresGenAIMixin
from .postgres_ingest import PostgresIngestMixin
from .postgres_labeling import PostgresLabelingMixin
from .postgres_maintenance import PostgresMaintenanceMixin
from .postgres_process import PostgresProcessMixin
from .postgres_spec import PostgresSpecMixin
from .postgres_train import PostgresTrainMixin


class PostgresResource(
    PostgresBaseMixin,
    PostgresMaintenanceMixin,
    PostgresIngestMixin,
    PostgresDedupMixin,
    PostgresProcessMixin,
    PostgresBuildMixin,
    PostgresLabelingMixin,
    PostgresSpecMixin,
    PostgresGenAIMixin,
    PostgresEmbeddingMixin,
    PostgresTrainMixin,
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
