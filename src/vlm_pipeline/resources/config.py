"""PipelineConfig — Pydantic BaseSettings, 환경변수 자동 바인딩 (docker-compose.yml에서 주입)."""

from typing import Optional

from pydantic_settings import BaseSettings


class PipelineConfig(BaseSettings):
    """MVP 설정 — 환경변수 자동 바인딩 (docker-compose.yml에서 주입)."""

    # DuckDB
    dataops_duckdb_path: str = "/data/pipeline.duckdb"  # 컨테이너 기준

    # MinIO (Docker 내부 네트워크)
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"

    # NAS 마운트 (컨테이너 내부 경로)
    incoming_dir: str = "/nas/incoming"
    archive_dir: str = "/nas/archive"
    manifest_dir: str = "/nas/incoming/.manifests"
    scratch_dir: Optional[str] = None

    # Canonical Path 정책
    dataops_asset_path_prefix_from: str = ""
    dataops_asset_path_prefix_to: str = ""

    model_config = {
        "env_prefix": "",
        "case_sensitive": False,
    }
