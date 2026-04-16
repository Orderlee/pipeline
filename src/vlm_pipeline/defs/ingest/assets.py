"""INGEST @asset — NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw + DuckDB raw_files.

Layer 4: Dagster @asset, 같은 도메인의 ops.py만 import.
"""

import json
import shutil
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from dagster import AssetKey, asset

from vlm_pipeline.defs.label.artifact_import_support import import_local_label_artifacts, resolve_local_artifact_source_dirs
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .archive import (
    TRUTHY_STRINGS,
    archive_uploaded_assets,
    complete_uploaded_assets_without_archive,
    should_archive_manifest,
)
from .compaction import compact_completed_manifest_group, is_gcp_compaction_candidate
from .duplicate import collect_duplicate_asset_file_map

from .hydration import (
    STALE_MANIFEST_ALL_MISSING_REASON,
    hydrate_manifest_files,
    manifest_hydration_failure_reason,
    raise_if_manifest_hydration_failed as check_manifest_hydration_failure,
)
from .inline_dedup import run_inline_dedup
from .manifest import (
    build_retry_manifest,
    maybe_write_archive_done_marker,
    write_ingest_failure_logs,
)
from .ops import ingest_summary, normalize_and_archive, register_incoming
from .runtime_policy import (
    IngestRuntimePolicy,
    archive_only_artifact_import_allowed,
    resolve_ingest_runtime_policy,
)


@dataclass
class RawIngestState:
    manifest_path: str | None
    request_id: str | None
    folder_name: str | None
    archive_only: bool
    stage: str = "entered"
    manifest: dict | None = None
    transfer_tool: str = ""
    archive_requested: bool = False
    archive_unit_dir_hint: Path | None = None
    archive_prepared_for_upload: bool = False
    ingest_rejections: list[dict] = field(default_factory=list)
    retry_candidates: list[dict] = field(default_factory=list)
    records: list[dict] = field(default_factory=list)
    uploaded: list[dict] = field(default_factory=list)
    archived: list[dict] = field(default_factory=list)


def _persist_manifest(manifest_path: str | None, manifest: dict | None) -> None:
    if not manifest_path or manifest is None:
        return
    Path(manifest_path).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_manifest_or_summary(
    context,
    db: DuckDBResource,
    *,
    manifest_path: str | None,
    request_id: str | None,
) -> tuple[dict | None, dict | None]:
    if manifest_path and Path(manifest_path).exists():
        return json.loads(Path(manifest_path).read_text(encoding="utf-8")), None

    message = "manifest_path가 없거나 파일이 존재하지 않습니다."
    if request_id:
        raise FileNotFoundError(message)

    context.log.warning(f"{message} 상태 요약만 반환합니다.")
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM raw_files").fetchone()[0]
        completed = conn.execute(
            "SELECT COUNT(*) FROM raw_files WHERE ingest_status = 'completed'"
        ).fetchone()[0]
    return None, {"total": int(total), "success": int(completed), "failed": 0, "skipped": 0}


def _mark_dispatch_archive_step_started(
    db: DuckDBResource,
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


def _prepare_archive_inputs(
    context,
    *,
    config: PipelineConfig,
    state: RawIngestState,
    policy: IngestRuntimePolicy,
) -> None:
    if state.manifest is None:
        return

    state.archive_requested = should_archive_manifest(
        state.manifest,
        config=config,
        runtime_profile=policy.runtime_profile,
    )
    state.archive_prepared_for_upload = False
    context.log.info("archive_prepare:start")
    if state.archive_requested and policy.premove_archive_enabled:
        context.log.info(
            "archive_prepare:premove_disabled "
            "pre-move path is disabled; archive_finalize에서 uploaded 파일만 이동"
        )

    context.log.info(
        "archive_prepare:done "
        f"archive_requested={state.archive_requested} "
        f"prepared={state.archive_prepared_for_upload} "
        f"premove_enabled={policy.premove_archive_enabled} "
        "premove_applied=False"
    )


def _hydrate_manifest(context, state: RawIngestState) -> None:
    if state.manifest is None:
        return
    context.log.info("manifest_hydrate:start")
    state.manifest = hydrate_manifest_files(context, state.manifest)
    _persist_manifest(state.manifest_path, state.manifest)
    context.log.info(
        "manifest_hydrate:done "
        f"file_count={int(state.manifest.get('file_count') or len(state.manifest.get('files', [])) or 0)}"
    )


def _raise_if_manifest_hydration_failed(context, state: RawIngestState) -> None:
    check_manifest_hydration_failure(context, state.manifest, state.ingest_rejections)


def _move_manifest(
    context,
    config: PipelineConfig,
    manifest_path: str | None,
    *,
    target_subdir: str,
    unique_name: bool = False,
) -> None:
    """manifest 파일을 target_subdir 하위로 이동한다."""
    if not manifest_path:
        return
    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        return
    target_dir = Path(config.manifest_dir) / target_subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / manifest_file.name
    if unique_name:
        suffix = 2
        while destination.exists():
            destination = target_dir / f"{manifest_file.stem}__{suffix}{manifest_file.suffix}"
            suffix += 1
    try:
        shutil.move(str(manifest_file), str(destination))
        context.log.info(f"manifest 이동 완료: {manifest_file.name} -> {target_subdir}/{destination.name}")
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"manifest 이동 실패: {exc}")


def _move_manifest_to_processed(context, config: PipelineConfig, manifest_path: str | None) -> None:
    _move_manifest(context, config, manifest_path, target_subdir="processed")


def _move_manifest_to_failed(context, config: PipelineConfig, manifest_path: str | None) -> None:
    _move_manifest(context, config, manifest_path, target_subdir="failed", unique_name=True)


def _maybe_compact_completed_gcp_manifests(
    context,
    *,
    config: PipelineConfig,
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


def _run_staging_archive_only_artifact_import(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    config: PipelineConfig,
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


def _finalize_dispatch_success(db: DuckDBResource, state: RawIngestState) -> None:
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
    db: DuckDBResource,
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
    db: DuckDBResource,
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
    _hydrate_manifest(context, state)
    _raise_if_manifest_hydration_failed(context, state)
    return runtime_policy


def _step_register_and_upload(context, db, minio, *, config, state, runtime_policy):
    """DB 등록 → MinIO 업로드 → archive 확정."""
    state.stage = "register"
    context.log.info("register:start")
    state.records = register_incoming(
        context, db, state.manifest, ingest_rejections=state.ingest_rejections,
    )
    context.log.info(f"register:done records={len(state.records)}")

    target_archive_dir = config.archive_dir

    state.stage = "upload"
    context.log.info(
        f"upload_prepare:start defer_video_env_classification={runtime_policy.defer_video_env_classification}"
    )
    state.uploaded = normalize_and_archive(
        context, db, minio, state.records, target_archive_dir,
        ingest_rejections=state.ingest_rejections,
        retry_candidates=state.retry_candidates,
        defer_video_env_classification=runtime_policy.defer_video_env_classification,
    )
    context.log.info(
        f"upload_execute:done uploaded={len(state.uploaded)} retry_candidates={len(state.retry_candidates)}"
    )

    state.stage = "archive_finalize"
    context.log.info(
        f"archive_finalize:start archive_requested={state.archive_requested} "
        f"prepared={state.archive_prepared_for_upload} uploaded={len(state.uploaded)}"
    )
    if state.archive_requested:
        try:
            state.archived, state.archive_unit_dir_hint = archive_uploaded_assets(
                context=context, db=db, manifest=state.manifest,
                uploaded=state.uploaded, archive_dir=target_archive_dir,
                ingest_rejections=state.ingest_rejections,
            )
        except OSError as exc:
            if "archive_move_timeout" in str(exc):
                context.log.error(
                    f"archive_finalize TIMEOUT — 업로드 완료분을 completed로 전환하고 후속 진행: {exc}"
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
        f"uploaded={len(state.uploaded)} archive_requested={state.archive_requested}"
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
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    config: PipelineConfig,
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


@asset(
    name="raw_ingest",
    description="NAS 미디어 검증·정규화 → MinIO(vlm-raw) 업로드 + 메타데이터 등록 + pHash 중복 검출",
    group_name="ingest",
    deps=[AssetKey(["pipeline", "incoming_nas"])],
)
def raw_ingest(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST asset — manifest 기반 미디어 파일 수집.

    sensor에서 run_config tags로 manifest_path를 전달받음.
    manifest가 없으면 DuckDB 상태 요약만 반환.
    """
    config = PipelineConfig()
    state = _build_raw_ingest_state(context)

    context.log.info(
        "raw_ingest entered: "
        f"run_id={context.run.run_id}, request_id={state.request_id or ''}, "
        f"folder={state.folder_name or ''}, manifest_path={state.manifest_path or ''}"
    )
    return _run_raw_ingest_pipeline(
        context,
        db,
        minio,
        config=config,
        state=state,
    )
