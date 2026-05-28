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

    # NAS 마운트 (컨테이너 내부 경로). incoming/archive 는 같은 단일 bind mount(/nas/data)
    # 하위에 둬야 폴더 fast-path(os.rename) 가 활성 — 별도 mount 면 cross-device(EXDEV) 로
    # per-file 이동 fallback. 실제 값은 docker-compose / .env 의 env 로 주입됨.
    incoming_dir: str = "/nas/data/incoming"
    archive_dir: str = "/nas/data/archive"
    manifest_dir: str = "/nas/data/incoming/.manifests"
    scratch_dir: Optional[str] = None

    # Canonical Path 정책
    dataops_asset_path_prefix_from: str = ""
    dataops_asset_path_prefix_to: str = ""

    model_config = {
        "env_prefix": "",
        "case_sensitive": False,
    }
