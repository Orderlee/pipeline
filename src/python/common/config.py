import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class MinioSettings(BaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


class Settings(BaseSettings):
    nas_root: str
    duckdb_path: str
    minio: MinioSettings

    class Config:
        env_prefix = "DATAOPS_"
        env_nested_delimiter = "__"


def _normalize_endpoint(endpoint: str) -> str:
    endpoint = (endpoint or "").strip()
    if not endpoint:
        return endpoint
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return f"http://{endpoint}"


def _apply_compat_env_overrides() -> None:
    """Bridge legacy env names into DATAOPS_* names."""
    mapping = {
        "MINIO_ENDPOINT": "DATAOPS_MINIO__ENDPOINT",
        "MINIO_ACCESS_KEY": "DATAOPS_MINIO__ACCESS_KEY",
        "MINIO_SECRET_KEY": "DATAOPS_MINIO__SECRET_KEY",
        "DUCKDB_PATH": "DATAOPS_DUCKDB_PATH",
        "NAS_ROOT": "DATAOPS_NAS_ROOT",
    }

    for src, dst in mapping.items():
        if os.getenv(dst):
            continue
        val = os.getenv(src)
        if not val:
            continue
        if src == "MINIO_ENDPOINT":
            val = _normalize_endpoint(val)
        os.environ[dst] = val


def _apply_env_overrides_on_data(data: dict) -> dict:
    """Apply env overrides directly onto YAML data before Settings construction."""
    _apply_compat_env_overrides()

    minio = dict(data.get("minio") or {})

    endpoint = os.getenv("DATAOPS_MINIO__ENDPOINT") or os.getenv("MINIO_ENDPOINT")
    if endpoint:
        minio["endpoint"] = _normalize_endpoint(endpoint)

    access_key = os.getenv("DATAOPS_MINIO__ACCESS_KEY") or os.getenv("MINIO_ACCESS_KEY")
    if access_key:
        minio["access_key"] = access_key

    secret_key = os.getenv("DATAOPS_MINIO__SECRET_KEY") or os.getenv("MINIO_SECRET_KEY")
    if secret_key:
        minio["secret_key"] = secret_key

    bucket = os.getenv("DATAOPS_MINIO__BUCKET")
    if bucket:
        minio["bucket"] = bucket

    if minio:
        data["minio"] = minio

    duckdb_path = os.getenv("DATAOPS_DUCKDB_PATH") or os.getenv("DUCKDB_PATH")
    if duckdb_path:
        data["duckdb_path"] = duckdb_path

    nas_root = os.getenv("DATAOPS_NAS_ROOT") or os.getenv("NAS_ROOT")
    if nas_root:
        data["nas_root"] = nas_root

    return data


def _load_yaml_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache()
def get_settings(config_path: Optional[str] = None) -> Settings:
    """Load settings from YAML (with environment overrides)."""
    base_dir = Path(__file__).resolve().parent.parent  # src/python
    repo_root = base_dir.parent.parent  # project root

    env_config = os.getenv("DATAOPS_CONFIG_PATH")
    raw_cfg_path = config_path or env_config or str(repo_root / "configs" / "global.yaml")
    cfg_path = Path(raw_cfg_path)
    if not cfg_path.is_absolute():
        cfg_path = (repo_root / cfg_path).resolve()

    data = _load_yaml_config(cfg_path)
    data = _apply_env_overrides_on_data(data)

    duckdb_path = data.get("duckdb_path")
    if duckdb_path:
        if str(duckdb_path).startswith("./"):
            data["duckdb_path"] = str((repo_root / str(duckdb_path)[2:]).resolve())
        else:
            data["duckdb_path"] = str(duckdb_path)

    return Settings(**data)


__all__ = [
    "Settings",
    "MinioSettings",
    "get_settings",
]
