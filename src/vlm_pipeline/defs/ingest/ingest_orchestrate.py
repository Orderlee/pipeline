"""raw_ingest — step 함수들 + 전체 파이프라인 오케스트레이션.

파이프라인 단계:
1. manifest 로드 → archive 준비 → hydration (`_step_load_and_hydrate`)
2. DB 등록 + MinIO 업로드 + archive 이동 (`_step_register_and_upload`)
3. artifact import, retry manifest, inline dedup, done marker (`_step_post_ingest`)
4. summary/output metadata 조립 + dedup hard-gate + dispatch 상태 확정 (`_run_raw_ingest_pipeline`)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .archive import (
    archive_uploaded_assets,
    complete_uploaded_assets_without_archive,
)
from .duplicate import collect_duplicate_asset_file_map
from .hydration import (
    STALE_MANIFEST_ALL_MISSING_REASON,
    manifest_hydration_failure_reason,
)
from .ingest_archive_flow import (
    _maybe_compact_completed_gcp_manifests,
    _prepare_archive_inputs,
)
from .ingest_manifest_flow import (
    _hydrate_manifest,
    _load_manifest_or_summary,
    _move_manifest_to_failed,
    _move_manifest_to_processed,
    _raise_if_manifest_hydration_failed,
)
from .ingest_post import (
    _build_ingest_result_metadata,
    _check_nas_health,
    _finalize_dispatch_failure,
    _finalize_dispatch_success,
    _mark_dispatch_archive_step_started,
    _mark_failed_records,
    _run_staging_archive_only_artifact_import,
)
from .ingest_state import RawIngestState
from .inline_dedup import run_inline_dedup
from .manifest import build_retry_manifest, maybe_write_archive_done_marker, write_ingest_failure_logs
from .ops import ingest_summary, normalize_and_archive, register_incoming
from .runtime_policy import resolve_ingest_runtime_policy

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig
    from vlm_pipeline.resources.duckdb import DuckDBResource
    from vlm_pipeline.resources.minio import MinIOResource


def _step_load_and_hydrate(context, db, *, config, state):
    """manifest 로드 → archive 준비 → hydration 단계."""
    state.stage = "nas_health_check"
    if not _check_nas_health(context, config.incoming_dir):
        return {
            "processed": 0,
            "failed": 0,
            "skipped": True,
            "skip_reason": "nas_unreachable",
        }

    state.stage = "ensure_schema"
    context.log.info("ensure_schema:start")
    db.ensure_runtime_schema()
    context.log.info("ensure_schema:done")

    state.stage = "manifest_load"
    state.manifest, summary = _load_manifest_or_summary(
        context, db, manifest_path=state.manifest_path, request_id=state.request_id,
    )
    if summary is not None:
        return summary

    assert state.manifest is not None
    state.transfer_tool = str(state.manifest.get("transfer_tool", "")).strip().lower()
    runtime_policy = resolve_ingest_runtime_policy(transfer_tool=state.transfer_tool)

    state.stage = "archive_prepare"
    _prepare_archive_inputs(context, config=config, state=state, policy=runtime_policy)

    state.stage = "manifest_hydrate"
    _hydrate_manifest(context, state, db=db)
    _raise_if_manifest_hydration_failed(context, state)
    return runtime_policy


def _step_register_and_upload(context, db, minio, *, config, state, runtime_policy):
    """DB 등록 → (옵션) MinIO 업로드 → archive 확정.

    manifest.upload_enabled=False 면 MinIO 업로드를 건너뛰고 archive 만 이동한 뒤
    raw_files.ingest_status='archived' 로 마크한다. 실제 MinIO 업로드 + 라벨링은
    dispatch/piaagent API 수신 시점에 별도 manifest 로 수행된다.
    """
    state.stage = "register"
    context.log.info("register:start")
    state.records = register_incoming(
        context, db, state.manifest, ingest_rejections=state.ingest_rejections,
    )
    context.log.info(f"register:done records={len(state.records)}")

    target_archive_dir = config.archive_dir

    # upload_enabled 플래그 (manifest 기본 True, auto_bootstrap 생성 manifest 는 False).
    upload_enabled = bool(state.manifest.get("upload_enabled", True))

    state.stage = "upload"
    context.log.info(
        f"upload_prepare:start defer_video_env_classification={runtime_policy.defer_video_env_classification} "
        f"upload_enabled={upload_enabled}"
    )
    state.uploaded = normalize_and_archive(
        context, db, minio, state.records, target_archive_dir,
        ingest_rejections=state.ingest_rejections,
        retry_candidates=state.retry_candidates,
        defer_video_env_classification=runtime_policy.defer_video_env_classification,
        upload_enabled=upload_enabled,
    )
    context.log.info(
        f"upload_execute:done uploaded={len(state.uploaded)} "
        f"retry_candidates={len(state.retry_candidates)} upload_enabled={upload_enabled}"
    )

    state.stage = "archive_finalize"
    completion_status = "completed" if upload_enabled else "archived"
    completion_bucket = "vlm-raw" if upload_enabled else None
    context.log.info(
        f"archive_finalize:start archive_requested={state.archive_requested} "
        f"prepared={state.archive_prepared_for_upload} uploaded={len(state.uploaded)} "
        f"completion_status={completion_status}"
    )
    if state.archive_requested:
        try:
            state.archived, state.archive_unit_dir_hint = archive_uploaded_assets(
                context=context, db=db, manifest=state.manifest,
                uploaded=state.uploaded, archive_dir=target_archive_dir,
                ingest_rejections=state.ingest_rejections,
                completion_status=completion_status,
                raw_bucket=completion_bucket,
            )
        except OSError as exc:
            if "archive_move_timeout" in str(exc):
                context.log.error(
                    f"archive_finalize TIMEOUT — 업로드 완료분을 {completion_status}로 전환하고 후속 진행: {exc}"
                )
                state.archived = complete_uploaded_assets_without_archive(
                    context=context, db=db, manifest=state.manifest, uploaded=state.uploaded,
                )
                state.archive_unit_dir_hint = None
            else:
                raise
    else:
        state.archived = complete_uploaded_assets_without_archive(
            context=context, db=db, manifest=state.manifest, uploaded=state.uploaded,
        )
        state.archive_unit_dir_hint = None
    context.log.info(
        f"archive_finalize:done archived={len(state.archived)} "
        f"uploaded={len(state.uploaded)} archive_requested={state.archive_requested} "
        f"completion_status={completion_status}"
    )
    return target_archive_dir


def _step_post_ingest(context, db, minio, *, config, state, runtime_policy, target_archive_dir):
    """라벨 artifact import, retry manifest, inline dedup, done marker."""
    state.stage = "label_artifact_import"
    local_artifact_summary = _run_staging_archive_only_artifact_import(
        context, db, minio, config=config, state=state,
        runtime_profile=runtime_policy.runtime_profile,
    )

    state.stage = "retry_manifest"
    retry_manifest_path = build_retry_manifest(
        context=context, config=config, manifest=state.manifest, retry_candidates=state.retry_candidates,
    )
    failure_log_path = write_ingest_failure_logs(
        context=context, config=config, manifest=state.manifest, ingest_rejections=state.ingest_rejections,
    )

    state.stage = "inline_dedup"
    duplicate_targets = collect_duplicate_asset_file_map(state.records)
    if duplicate_targets:
        duplicate_files_count = sum(
            1 for rec in state.records
            if str((rec.get("record") or {}).get("error_message") or "").startswith("duplicate_of:")
        )
        updated_assets = db.mark_duplicate_skipped_assets(duplicate_targets)
        context.log.warning(
            f"중복 스킵 이력 반영(자산별): duplicate_assets={len(duplicate_targets)}, "
            f"duplicate_files={duplicate_files_count}, updated_assets={updated_assets}"
        )

    dedup_summary = run_inline_dedup(context, db, minio, state.uploaded)

    state.stage = "done_marker"
    archive_done_marker_path = maybe_write_archive_done_marker(
        context=context, db=db, manifest=state.manifest,
        manifest_dir=config.manifest_dir, manifest_path=state.manifest_path,
        archive_dir=target_archive_dir, archive_unit_dir_hint=state.archive_unit_dir_hint,
    )

    return dedup_summary, retry_manifest_path, failure_log_path, archive_done_marker_path, local_artifact_summary


def _run_raw_ingest_pipeline(
    context,
    db: "DuckDBResource",
    minio: "MinIOResource",
    *,
    config: "PipelineConfig",
    state: RawIngestState,
) -> dict:
    _mark_dispatch_archive_step_started(db, state.request_id)

    try:
        # 1) manifest 로드 + hydration
        result = _step_load_and_hydrate(context, db, config=config, state=state)
        if isinstance(result, dict):
            return result
        runtime_policy = result

        # 2) DB 등록 + MinIO 업로드 + archive
        target_archive_dir = _step_register_and_upload(
            context, db, minio, config=config, state=state, runtime_policy=runtime_policy,
        )

        # 3) 후처리 (artifact import, retry, dedup, done marker)
        dedup_summary, retry_manifest_path, failure_log_path, archive_done_marker_path, local_artifact_summary = (
            _step_post_ingest(
                context, db, minio, config=config, state=state,
                runtime_policy=runtime_policy, target_archive_dir=target_archive_dir,
            )
        )

        # 4) summary + output metadata 조립
        common_meta = _build_ingest_result_metadata(
            state, dedup_summary=dedup_summary,
            retry_manifest_path=retry_manifest_path,
            archive_done_marker_path=archive_done_marker_path,
            local_artifact_summary=local_artifact_summary,
        )
        summary = ingest_summary(context, state.archived, state.records)
        summary.update(common_meta)

        output_metadata = dict(common_meta)
        if retry_manifest_path is not None:
            output_metadata["retry_manifest_path"] = str(retry_manifest_path)
        if failure_log_path is not None:
            output_metadata["failure_log_path"] = str(failure_log_path)
        if archive_done_marker_path is not None:
            output_metadata["archive_done_marker_path"] = str(archive_done_marker_path)
        context.add_output_metadata(output_metadata)

        # 5) dedup hard-gate
        if dedup_summary["gated_failed"] > 0:
            raise RuntimeError(
                f"inline DEDUP failed for current manifest assets: "
                f"failed={dedup_summary['gated_failed']}, "
                f"computed={dedup_summary['computed']}, "
                f"similar_found={dedup_summary['similar_found']}"
            )

        # 6) 완료 처리 + compaction
        _move_manifest_to_processed(context, config, state.manifest_path)
        _finalize_dispatch_success(db, state)
        compaction_report = _maybe_compact_completed_gcp_manifests(
            context, config=config, state=state, archive_done_marker_path=archive_done_marker_path,
        )
        if compaction_report:
            context.add_output_metadata(
                {
                    "manifest_compaction_status": str(compaction_report.get("status", "")),
                    "manifest_compaction_deleted_count": int(compaction_report.get("deleted_manifest_count", 0)),
                    "manifest_compaction_summary_written": bool(compaction_report.get("summary_written", False)),
                    "manifest_compaction_summary_path": str(compaction_report.get("summary_path", "")),
                }
            )

        return summary

    except Exception as exc:
        failure_message = f"{state.stage}_failed:{exc}"
        context.log.error(
            f"raw_ingest 실패: stage={state.stage}, request_id={state.request_id or ''}, error={exc}"
        )
        _mark_failed_records(context, db, state=state, failure_message=failure_message)
        _finalize_dispatch_failure(context, db, state=state, failure_message=failure_message)
        if manifest_hydration_failure_reason(state.manifest) == STALE_MANIFEST_ALL_MISSING_REASON:
            _move_manifest_to_failed(context, config, state.manifest_path)
        raise
