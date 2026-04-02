"""공통 Dagster 리소스 빌더."""

from __future__ import annotations

from dagster import EnvVar

from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


def build_common_resources() -> dict[str, object]:
    return {
        "db": DuckDBResource(db_path=EnvVar("DATAOPS_DUCKDB_PATH")),
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
        ),
    }
