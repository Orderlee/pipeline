"""운영 기준 ingest 정책 헬퍼."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from vlm_pipeline.lib.runtime_profile import RuntimeProfile, resolve_runtime_profile
from vlm_pipeline.resources.runtime_settings import load_ingest_feature_settings

from .archive import (
    manifest_allows_auto_bootstrap_without_dispatch,
    should_archive_manifest,
)


@dataclass(frozen=True)
class IngestRuntimePolicy:
    runtime_profile: RuntimeProfile
    is_test: bool
    defer_video_env_classification: bool
    premove_archive_enabled: bool


def resolve_ingest_runtime_policy(
    *,
    transfer_tool: str | None,  # noqa: ARG001 - 정책 확장 대비 인터페이스 유지
    runtime_profile: RuntimeProfile | None = None,
) -> IngestRuntimePolicy:
    settings = load_ingest_feature_settings()
    profile = runtime_profile or resolve_runtime_profile()
    return IngestRuntimePolicy(
        runtime_profile=profile,
        is_test=profile.is_test,
        defer_video_env_classification=settings.defer_video_env_classification,
        premove_archive_enabled=settings.premove_archive_enabled,
    )


def auto_bootstrap_unit_allowed(
    unit_path: str | Path,
    *,
    incoming_dir: Path,
    config,
    runtime_profile: RuntimeProfile | None = None,
) -> bool:
    resolved_path = Path(unit_path).resolve()
    return manifest_allows_auto_bootstrap_without_dispatch(
        {"source_unit_path": str(resolved_path)},
        config=config,
        runtime_profile=runtime_profile or resolve_runtime_profile(),
    )


def pending_manifest_allowed(
    payload: dict,
    *,
    config,
    runtime_profile: RuntimeProfile | None = None,
) -> bool:
    return True


def auto_bootstrap_manifest_archive_requested(
    manifest: dict,
    *,
    config,
    runtime_profile: RuntimeProfile | None = None,
) -> bool:
    profile = runtime_profile or resolve_runtime_profile()
    return manifest_allows_auto_bootstrap_without_dispatch(
        manifest,
        config=config,
        runtime_profile=profile,
    )


def archive_only_artifact_import_allowed(
    *,
    runtime_profile: RuntimeProfile,
    archive_only: bool,
    folder_name: str | None,
    dispatch_mode: str = "standard",
) -> bool:
    return (
        runtime_profile is not None
        and archive_only
        and str(dispatch_mode or "").strip().lower() != "ingest_only"
        and bool(str(folder_name or "").strip())
    )
