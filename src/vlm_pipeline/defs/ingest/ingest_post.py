"""raw_ingest — post-ingest: NAS health, dispatch status 관리, local artifact import.

파이프라인 종료 후 상태 확정 (success / failure) + staging 환경 archive-only
artifact 재import + summary/output metadata 조립을 담당.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from vlm_pipeline.defs.label.artifact_import_support import (
    import_local_label_artifacts,
    resolve_local_artifact_source_dirs,
)

from .archive import TRUTHY_STRINGS
from .ingest_state import RawIngestState
from .runtime_policy import archive_only_artifact_import_allowed

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig
    from vlm_pipeline.resources.duckdb import DuckDBResource
    from vlm_pipeline.resources.minio import MinIOResource

NAS_HEALTH_TIMEOUT_SEC: int = 5


def _check_nas_health(context, incoming_dir: str) -> bool:
    """NAS incoming 경로에 stat을 시도하여 응답 가능 여부를 확인한다.

    타임아웃 시 False를 반환하여 caller가 early return할 수 있게 한다.
    """
    target = Path(incoming_dir)

    def _probe() -> bool:
        target.stat()
        return True

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_probe)
            return future.result(timeout=NAS_HEALTH_TIMEOUT_SEC)
    except FuturesTimeoutError:
        context.log.error(
            f"nas_health_check TIMEOUT: {incoming_dir} 에 {NAS_HEALTH_TIMEOUT_SEC}s 내 응답 없음"
        )
        return False
    except OSError as exc:
        context.log.error(f"nas_health_check FAILED: {incoming_dir} — {exc}")
        return False


def _mark_dispatch_archive_step_started(
    db: "DuckDBResource",
    request_id: str | None,
) -> None:
    if not request_id:
        return
    now = datetime.now()
    db.update_dispatch_request_status(request_id, "running", processed_at=now)
    db.update_dispatch_pipeline_step(
        request_id,
        "archive_move",
        "running",
        error_message=None,
        started_at=now,
    )


def _run_staging_archive_only_artifact_import(
    context,
    db: "DuckDBResource",
    minio: "MinIOResource",
    *,
    config: "PipelineConfig",
    state: RawIngestState,
    runtime_profile,
) -> dict[str, object] | None:
    if not archive_only_artifact_import_allowed(
        runtime_profile=runtime_profile,
        archive_only=state.archive_only,
        folder_name=state.folder_name,
    ):
        return None

    source_unit_dirs = resolve_local_artifact_source_dirs(
        config=config,
        source_unit_name=state.folder_name,
        archive_unit_dir_hint=state.archive_unit_dir_hint,
    )
    if not source_unit_dirs:
        context.log.info(
            "archive-only local label import 스킵: "
            f"folder={state.folder_name} source_unit_dir 없음"
        )
        return None

    try:
        summary = import_local_label_artifacts(
            context,
            db,
            minio,
            config=config,
            source_unit_name=state.folder_name,
            source_unit_dirs=source_unit_dirs,
            scan_global_dirs=False,
            scan_local_dirs=True,
            failure_log_prefix="archive_only_artifact_import",
            update_timestamp_status=True,
        )
        context.log.info(
            "archive-only local label import 완료: "
            f"folder={state.folder_name} summary={summary}"
        )
        return summary
    except Exception as exc:  # noqa: BLE001
        context.log.warning(
            "archive-only local label import 실패(원본 적재는 유지): "
            f"folder={state.folder_name} error={exc}"
        )
        return None


def _finalize_dispatch_success(db: "DuckDBResource", state: RawIngestState) -> None:
    if not state.request_id:
        return
    completed_at = datetime.now()
    db.update_dispatch_pipeline_step(
        state.request_id,
        "archive_move",
        "completed",
        error_message=None,
        completed_at=completed_at,
    )
    if state.archive_only:
        db.update_dispatch_request_status(
            state.request_id,
            "completed",
            archive_path=(
                str(state.archive_unit_dir_hint) if state.archive_unit_dir_hint else None
            ),
            completed_at=completed_at,
            processed_at=completed_at,
        )


def _mark_failed_records(
    context,
    db: "DuckDBResource",
    *,
    state: RawIngestState,
    failure_message: str,
) -> None:
    if not state.records:
        return
    failed_updates = []
    for rec in state.records:
        record = rec.get("record") or {}
        asset_id = str(record.get("asset_id") or "").strip()
        if not asset_id:
            continue
        failed_updates.append(
            {
                "asset_id": asset_id,
                "status": "failed",
                "error_message": failure_message,
            }
        )
    if not failed_updates:
        return
    try:
        db.batch_update_status(failed_updates)
    except Exception as update_exc:  # noqa: BLE001
        context.log.warning(f"raw_ingest 실패 후 raw_files 상태 정리 실패: {update_exc}")


def _finalize_dispatch_failure(
    context,
    db: "DuckDBResource",
    *,
    state: RawIngestState,
    failure_message: str,
) -> None:
    if not state.request_id:
        return
    completed_at = datetime.now()
    try:
        db.update_dispatch_pipeline_step(
            state.request_id,
            "archive_move",
            "failed",
            error_message=failure_message,
            completed_at=completed_at,
        )
        db.update_dispatch_request_status(
            state.request_id,
            "failed",
            error_message=failure_message,
            archive_path=(
                str(state.archive_unit_dir_hint) if state.archive_unit_dir_hint else None
            ),
            completed_at=completed_at,
            processed_at=completed_at,
        )
    except Exception as dispatch_exc:  # noqa: BLE001
        context.log.warning(
            f"dispatch 실패 상태 기록 실패: request_id={state.request_id}: {dispatch_exc}"
        )


def _dispatch_request_context(context) -> tuple[str | None, str | None, bool]:
    tags = getattr(context.run, "tags", {}) or {}
    request_id = (
        str(tags.get("dispatch_request_id") or tags.get("dagster/run_key") or "").strip() or None
    )
    folder_name = (
        str(tags.get("folder_name_original") or tags.get("folder_name") or "").strip() or None
    )
    archive_only = str(tags.get("dispatch_archive_only") or "").strip().lower() in TRUTHY_STRINGS
    return request_id, folder_name, archive_only


def _build_raw_ingest_state(context) -> RawIngestState:
    request_id, folder_name, archive_only = _dispatch_request_context(context)
    return RawIngestState(
        manifest_path=context.run.tags.get("manifest_path"),
        request_id=request_id,
        folder_name=folder_name,
        archive_only=archive_only,
    )


def _build_ingest_result_metadata(
    state: RawIngestState,
    *,
    dedup_summary: dict,
    retry_manifest_path: Path | None,
    archive_done_marker_path: Path | None,
    local_artifact_summary: dict | None,
) -> dict:
    """summary와 output_metadata에 공통으로 들어가는 필드를 한 번만 구성한다."""
    uploaded_count = len(state.uploaded)
    archived_count = len(state.archived)
    meta = {
        "uploaded_count": uploaded_count,
        "archived_count": archived_count,
        "archive_missing_count": max(0, uploaded_count - archived_count),
        "archive_requested": state.archive_requested,
        "ingest_rejection_count": len(state.ingest_rejections),
        "retry_candidate_count": len(state.retry_candidates),
        "retry_manifest_created": bool(retry_manifest_path),
        "archive_done_marker_present": bool(archive_done_marker_path),
        "dedup_computed": dedup_summary["computed"],
        "dedup_similar_found": dedup_summary["similar_found"],
        "dedup_failed": dedup_summary["failed"],
    }
    if local_artifact_summary:
        meta.update(
            {
                "local_event_labels_inserted": int(local_artifact_summary.get("event_labels_inserted", 0)),
                "local_bbox_inserted": int(local_artifact_summary.get("bbox_inserted", 0)),
                "local_image_captions_inserted": int(local_artifact_summary.get("image_captions_inserted", 0)),
                "local_artifact_failure_count": int(local_artifact_summary.get("failure_count", 0)),
            }
        )
        if local_artifact_summary.get("failure_log_path"):
            meta["local_artifact_failure_log_path"] = str(local_artifact_summary["failure_log_path"])
    return meta
