"""archive 이동 여부 판단 정책 — gcp / dispatch / staging 게이트."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from vlm_pipeline.lib.runtime_profile import RuntimeProfile, resolve_runtime_profile

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig

TRUTHY_STRINGS: frozenset[str] = frozenset({"1", "true", "t", "yes", "y", "on"})
FALSY_STRINGS: frozenset[str] = frozenset({"0", "false", "f", "no", "n", "off", ""})


def manifest_source_under_gcp(manifest: dict, config: "PipelineConfig | None" = None) -> bool:
    """source_unit_path가 <incoming_dir>/gcp 하위인지 (GCS 다운로드 표준 경로)."""
    raw = str(manifest.get("source_unit_path", "")).strip()
    if not raw:
        return False
    if config is None:
        from vlm_pipeline.resources.config import PipelineConfig

        config = PipelineConfig()
    incoming = Path(str(config.incoming_dir).strip()).resolve()
    gcp_root = (incoming / "gcp").resolve()
    try:
        Path(raw).resolve().relative_to(gcp_root)
        return True
    except Exception:
        return False


def manifest_allows_auto_bootstrap_without_dispatch(
    manifest: dict,  # noqa: ARG001
    config: "PipelineConfig | None" = None,  # noqa: ARG001
    runtime_profile: RuntimeProfile | None = None,  # noqa: ARG001
) -> bool:
    """dispatch 트리거 JSON 없이 auto_bootstrap·ingest 허용: 전면 허용 (previously gcp-only)."""
    return True


def _staging_transfer_allows_archive(
    manifest: dict,  # noqa: ARG001
    transfer_tool: str,
    *,
    config: "PipelineConfig | None",  # noqa: ARG001
) -> bool:
    """staging에서 archive+ingest 허용 transfer: dispatch·retry·auto_bootstrap 전면."""
    return transfer_tool in {"dispatch_sensor", "ingest_retry_manifest", "auto_bootstrap_sensor"}


def should_archive_manifest(
    manifest: dict,
    *,
    config: "PipelineConfig | None" = None,
    runtime_profile: RuntimeProfile | None = None,
) -> bool:
    """manifest 정책에 따라 archive 이동 여부를 결정한다.

    staging: dispatch·ingest_retry 또는 **incoming/gcp** auto_bootstrap만 (트리거 JSON 없이 gcp만).

    production: dispatch 트리거 JSON이 없는 auto_bootstrap은 **incoming/gcp/** 경로만 허용.
    `incoming/tmp_data_2` 같은 직접 드롭 폴더는 dispatch JSON 없이 archive 이동하지 않는다.
    legacy manifest 호환을 위해 archive_requested가 없으면 transfer_tool 기준으로 fallback 판단한다.
    """
    profile = runtime_profile or resolve_runtime_profile()
    is_staging = profile.is_staging
    transfer_tool = str(manifest.get("transfer_tool", "")).strip().lower()

    def _production_auto_bootstrap_gate() -> bool:
        if transfer_tool != "auto_bootstrap_sensor":
            return True
        return manifest_allows_auto_bootstrap_without_dispatch(manifest, config)

    archive_requested = manifest.get("archive_requested")
    if archive_requested is not None:
        if isinstance(archive_requested, str):
            normalized = archive_requested.strip().lower()
            if normalized in TRUTHY_STRINGS:
                if is_staging and not _staging_transfer_allows_archive(manifest, transfer_tool, config=config):
                    return False
                if not is_staging and not _production_auto_bootstrap_gate():
                    return False
                return True
            if normalized in FALSY_STRINGS:
                return False
        if not bool(archive_requested):
            return False
        if is_staging and not _staging_transfer_allows_archive(manifest, transfer_tool, config=config):
            return False
        if not is_staging and not _production_auto_bootstrap_gate():
            return False
        return True

    if not is_staging:
        if not _production_auto_bootstrap_gate():
            return False
        return True

    return _staging_transfer_allows_archive(manifest, transfer_tool, config=config)
