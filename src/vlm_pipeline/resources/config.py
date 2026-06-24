"""PipelineConfig — Pydantic BaseSettings, 환경변수 자동 바인딩 (docker-compose.yml에서 주입)."""

import os
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings

_KNOWN_DEFAULTS = {"minioadmin", "admin", ""}


def _insecure_creds_allowed() -> bool:
    """ALLOW_INSECURE_DEFAULT_CREDS env 가 truthy ('1', 'true', 'yes') 면 fail-fast 우회.

    운영 정책상 default 자격 회전 전까지의 임시 escape hatch. 보안 가치 일시 손상 인지.
    회전 완료 후 env 제거 권장.
    """
    return os.environ.get("ALLOW_INSECURE_DEFAULT_CREDS", "").strip().lower() in {"1", "true", "yes"}


class PipelineConfig(BaseSettings):
    """MVP 설정 — 환경변수 자동 바인딩 (docker-compose.yml에서 주입)."""

    # MinIO (Docker 내부 네트워크)
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str
    minio_secret_key: str

    @field_validator("minio_access_key", "minio_secret_key")
    @classmethod
    def reject_default_minio_credentials(cls, v: str, info) -> str:
        if v in _KNOWN_DEFAULTS:
            if _insecure_creds_allowed():
                # escape hatch — 자격 회전 전 임시. ALLOW_INSECURE_DEFAULT_CREDS=1 명시 시만.
                return v
            raise ValueError(
                f"{info.field_name} is set to a well-known default value '{v}'. "
                "Set MINIO_ACCESS_KEY / MINIO_SECRET_KEY to non-default credentials before starting. "
                "임시 우회: ALLOW_INSECURE_DEFAULT_CREDS=1 env set (자격 회전 전 한정)."
            )
        return v

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
