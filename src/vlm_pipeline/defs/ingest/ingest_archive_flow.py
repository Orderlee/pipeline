"""raw_ingest — archive 준비 + completed manifest compaction."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .archive import should_archive_manifest
from .compaction import compact_completed_manifest_group, is_gcp_compaction_candidate
from .ingest_state import RawIngestState

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig

    from .runtime_policy import IngestRuntimePolicy


def _set_archive_requested(
    context,
    *,
    config: "PipelineConfig",
    state: RawIngestState,
    policy: "IngestRuntimePolicy",
) -> None:
    if state.manifest is None:
        return

    state.archive_requested = should_archive_manifest(
        state.manifest,
        config=config,
        runtime_profile=policy.runtime_profile,
    )
    context.log.info(f"archive_prepare:done archive_requested={state.archive_requested}")


def _maybe_compact_completed_gcp_manifests(
    context,
    *,
    config: "PipelineConfig",
    state: RawIngestState,
    archive_done_marker_path: Path | None,
) -> dict | None:
    if state.manifest is None or archive_done_marker_path is None:
        return None
    if not is_gcp_compaction_candidate(state.manifest):
        return None

    try:
        report = compact_completed_manifest_group(
            manifest_dir=Path(config.manifest_dir),
            archive_dir=Path(config.archive_dir),
            source_unit_path=str(state.manifest.get("source_unit_path", "")).strip(),
            stable_signature=str(state.manifest.get("stable_signature", "")).strip(),
            archive_done_marker_path=archive_done_marker_path,
            apply=True,
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"completed manifest compaction 실패(ingest는 유지): {exc}")
        return None

    if report.get("status") == "compacted":
        context.log.info(
            "completed manifest compaction 완료: "
            f"source_unit={report.get('source_unit_name', '')} "
            f"deleted={report.get('deleted_manifest_count', 0)} "
            f"summary={report.get('summary_path', '')}"
        )
    elif report.get("status") == "skipped":
        context.log.info(
            "completed manifest compaction 스킵: "
            f"reason={report.get('reason', '')} "
            f"source_unit={report.get('source_unit_name', '')}"
        )
    return report
