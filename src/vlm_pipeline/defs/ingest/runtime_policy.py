"""운영/스테이징 ingest 정책 헬퍼."""

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
    is_staging: bool
    defer_video_env_classification: bool
    premove_archive_enabled: bool


def resolve_ingest_runtime_policy(
    *,
    transfer_tool: str | None,  # noqa: ARG001 - 정책 확장 대비 인터페이스 유지
    runtime_profile: RuntimeProfile | None = None,
) -> IngestRuntimePolicy:
    settings = load_ingest_feature_settings()
    profile = runtime_profile or resolve_runtime_profile()
    is_staging = profile.is_staging
    return IngestRuntimePolicy(
        runtime_profile=profile,
        is_staging=is_staging,
        defer_video_env_classification=(
            not is_staging and settings.defer_video_env_classification
        ),
        premove_archive_enabled=settings.premove_archive_enabled,
    )


def auto_bootstrap_unit_allowed(
    unit_path: str | Path,  # noqa: ARG001 - 정책 확장 대비 인터페이스 유지
    *,
    incoming_dir: Path,  # noqa: ARG001
    config,  # noqa: ARG001
    runtime_profile: RuntimeProfile | None = None,  # noqa: ARG001
) -> bool:
    # 모든 incoming 하위 폴더를 auto_bootstrap 대상으로 허용.
    # dispatch JSON 과의 중복은 sensor_bootstrap.py:148 의 excluded_top_level_names 에서 처리.
    return True


def pending_manifest_allowed(
    payload: dict,
    *,
    config,
    runtime_profile: RuntimeProfile | None = None,
) -> bool:
    profile = runtime_profile or resolve_runtime_profile()
    if not profile.is_staging:
        return True
    return should_archive_manifest(
        payload,
        config=config,
        runtime_profile=profile,
    )


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
) -> bool:
    return (
        runtime_profile is not None
        and archive_only
        and bool(str(folder_name or "").strip())
    )
