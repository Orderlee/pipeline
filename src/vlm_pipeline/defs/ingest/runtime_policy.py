"""운영/스테이징 ingest 정책 헬퍼."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from vlm_pipeline.lib.env_utils import IS_STAGING
from vlm_pipeline.resources.runtime_settings import load_ingest_feature_settings

from .archive import (
    manifest_allows_auto_bootstrap_without_dispatch,
    should_archive_manifest,
)


@dataclass(frozen=True)
class IngestRuntimePolicy:
    is_staging: bool
    defer_video_env_classification: bool
    premove_archive_enabled: bool


def resolve_ingest_runtime_policy(*, transfer_tool: str | None) -> IngestRuntimePolicy:
    normalized_transfer = str(transfer_tool or "").strip().lower()
    settings = load_ingest_feature_settings()
    return IngestRuntimePolicy(
        is_staging=IS_STAGING,
        defer_video_env_classification=(
            not IS_STAGING and settings.defer_video_env_classification
        ),
        premove_archive_enabled=(
            IS_STAGING
            or normalized_transfer != "dispatch_sensor"
            or settings.premove_archive_enabled
        ),
    )


def auto_bootstrap_unit_allowed(
    unit_path: str | Path,
    *,
    incoming_dir: Path,
    config,
) -> bool:
    resolved_path = Path(unit_path).resolve()
    if IS_STAGING:
        gcp_root = (incoming_dir / "gcp").resolve()
        try:
            resolved_path.relative_to(gcp_root)
            return True
        except Exception:
            return False
    return manifest_allows_auto_bootstrap_without_dispatch(
        {"source_unit_path": str(resolved_path)},
        config=config,
    )


def pending_manifest_allowed(payload: dict, *, config) -> bool:
    if not IS_STAGING:
        return True
    return should_archive_manifest(payload, config=config)
