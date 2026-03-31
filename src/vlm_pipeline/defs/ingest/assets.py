"""INGEST @asset — NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw + DuckDB raw_files.

Layer 4: Dagster @asset, 같은 도메인의 ops.py만 import.
"""

import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from dagster import AssetKey, asset

from vlm_pipeline.defs.label.artifact_import_support import import_local_label_artifacts, resolve_local_artifact_source_dirs
from vlm_pipeline.lib.phash import compute_phash
from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .archive import (
    archive_uploaded_assets,
    complete_uploaded_assets_without_archive,
    should_archive_manifest,
)
from .duplicate import collect_duplicate_asset_file_map
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
    state.manifest = _hydrate_manifest_files(context, state.manifest)
    _persist_manifest(state.manifest_path, state.manifest)
    context.log.info(
        "manifest_hydrate:done "
        f"file_count={int(state.manifest.get('file_count') or len(state.manifest.get('files', [])) or 0)}"
    )


def _move_manifest_to_processed(context, config: PipelineConfig, manifest_path: str | None) -> None:
    if not manifest_path:
        return
    manifest_file = Path(manifest_path)
    processed_dir = Path(config.manifest_dir) / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(manifest_file), str(processed_dir / manifest_file.name))
    except Exception as exc:
        context.log.warning(f"manifest 이동 실패: {exc}")


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


def _hydrate_manifest_files(context, manifest: dict) -> dict:
    """files가 비어 있으면 source_unit_path를 기준으로 실제 파일 목록을 지연 생성한다."""
    if manifest.get("files"):
        return manifest

    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    source_unit_path = Path(source_unit_path_raw) if source_unit_path_raw else None
    if source_unit_path is None or not source_unit_path.exists():
        return manifest

    allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
    files: list[dict] = []

    def _scan_recursive(base: Path, rel_prefix: str = "") -> None:
        try:
            for entry in os.scandir(base):
                if entry.is_dir(follow_symlinks=False):
                    sub_rel = f"{rel_prefix}{entry.name}/" if rel_prefix else f"{entry.name}/"
                    _scan_recursive(Path(entry.path), sub_rel)
                elif entry.is_file(follow_symlinks=False):
                    if Path(entry.name).suffix.lower() in allowed_exts:
                        rel = f"{rel_prefix}{entry.name}" if rel_prefix else entry.name
                        files.append({"path": entry.path, "size": 0, "rel_path": rel})
        except OSError:
            return

    _scan_recursive(source_unit_path)
    manifest["files"] = files
    manifest["file_count"] = len(files)
    manifest["source_unit_total_file_count"] = len(files)
    context.log.info(
        "manifest 파일 인덱싱 완료: "
        f"source_unit_path={source_unit_path}, file_count={len(files)}"
    )
    return manifest


def _dispatch_request_context(context) -> tuple[str | None, str | None, bool]:
    tags = getattr(context.run, "tags", {}) or {}
    request_id = (
        str(tags.get("dispatch_request_id") or tags.get("dagster/run_key") or "").strip() or None
    )
    folder_name = (
        str(tags.get("folder_name_original") or tags.get("folder_name") or "").strip() or None
    )
    archive_only = str(tags.get("dispatch_archive_only") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    return request_id, folder_name, archive_only


def _resolve_dedup_image_bytes(target: dict, minio: MinIOResource) -> tuple[bytes, str]:
    """inline DEDUP 입력 소스 우선순위: archive_path -> source_path -> MinIO."""
    for path_key in ("archive_path", "source_path"):
        local_path = target.get(path_key)
        if local_path and Path(local_path).is_file():
            return Path(local_path).read_bytes(), path_key

    raw_bucket = target.get("raw_bucket")
    raw_key = target.get("raw_key")
    if raw_bucket and raw_key:
        return minio.download(raw_bucket, raw_key), "minio"

    raise FileNotFoundError(
        f"No source available: archive_path={target.get('archive_path')}, "
        f"source_path={target.get('source_path')}, "
        f"raw_bucket={target.get('raw_bucket')}, raw_key={target.get('raw_key')}"
    )


def _load_inline_dedup_targets(
    db: DuckDBResource,
    *,
    prioritized_asset_ids: list[str],
    limit: int,
) -> list[dict]:
    """현재 manifest 자산을 우선 처리하고, 남는 슬롯은 기존 backlog로 채운다."""
    normalized_limit = max(1, int(limit))
    prioritized_targets: list[dict] = []
    prioritized_set = {str(asset_id).strip() for asset_id in prioritized_asset_ids if str(asset_id).strip()}

    with db.connect() as conn:
        columns = ["asset_id", "raw_bucket", "raw_key", "archive_path", "source_path"]

        if prioritized_set:
            placeholders = ", ".join("?" * len(prioritized_set))
            rows = conn.execute(
                f"""
                SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
                FROM raw_files
                WHERE asset_id IN ({placeholders})
                  AND media_type = 'image'
                  AND ingest_status = 'completed'
                  AND phash IS NULL
                ORDER BY created_at
                """,
                list(prioritized_set),
            ).fetchall()
            prioritized_targets = [dict(zip(columns, row)) for row in rows]

        remaining_limit = max(0, normalized_limit - len(prioritized_targets))
        if remaining_limit <= 0:
            return prioritized_targets

        params: list[object] = []
        exclude_sql = ""
        if prioritized_set:
            placeholders = ", ".join("?" * len(prioritized_set))
            exclude_sql = f"AND asset_id NOT IN ({placeholders})"
            params.extend(list(prioritized_set))

        rows = conn.execute(
            f"""
            SELECT asset_id, raw_bucket, raw_key, archive_path, source_path
            FROM raw_files
            WHERE media_type = 'image'
              AND ingest_status = 'completed'
              AND phash IS NULL
              {exclude_sql}
            ORDER BY created_at
            LIMIT ?
            """,
            [*params, remaining_limit],
        ).fetchall()
        backlog_targets = [dict(zip(columns, row)) for row in rows]

    return prioritized_targets + backlog_targets


def _mark_inline_dedup_failure(db: DuckDBResource, asset_id: str, error_message: str) -> None:
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE raw_files
            SET error_message = ?, updated_at = ?
            WHERE asset_id = ?
            """,
            [f"phash_failed:{error_message}", datetime.now(), asset_id],
        )


def _run_inline_dedup(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    uploaded: list[dict],
) -> dict:
    """INGEST 내부 hard-gate DEDUP.

    현재 manifest에서 성공적으로 업로드된 이미지 자산을 우선 처리하고,
    남는 슬롯이 있으면 기존 phash backlog도 함께 정리한다.
    """
    prioritized_asset_ids = [
        str(item["asset_id"])
        for item in uploaded
        if str(item.get("media_type") or "").strip().lower() == "image"
    ]
    if not prioritized_asset_ids:
        return {"computed": 0, "similar_found": 0, "failed": 0, "gated_failed": 0}

    limit = max(len(prioritized_asset_ids), 200)
    threshold = 5
    targets = _load_inline_dedup_targets(
        db,
        prioritized_asset_ids=prioritized_asset_ids,
        limit=limit,
    )
    if not targets:
        return {"computed": 0, "similar_found": 0, "failed": 0, "gated_failed": 0}

    prioritized_set = set(prioritized_asset_ids)
    computed = 0
    similar_found = 0
    failed = 0
    gated_failed = 0

    for target in targets:
        asset_id = str(target["asset_id"])
        try:
            image_bytes, source_label = _resolve_dedup_image_bytes(target, minio)
            context.log.debug(f"inline DEDUP source: {asset_id} via {source_label}")

            phash_hex = compute_phash(image_bytes)
            db.update_phash(asset_id, phash_hex)
            db.clear_error_message(asset_id)
            computed += 1

            candidates = db.find_similar_phash(
                phash_hex=phash_hex,
                threshold=threshold,
                exclude_asset_id=asset_id,
            )
            if candidates:
                best = candidates[0]
                other_asset_id = str(best["asset_id"])
                dist = int(best["distance"])
                group_id = f"dup_{min(asset_id, other_asset_id)}_{max(asset_id, other_asset_id)}"
                db.update_dup_group(asset_id, group_id)
                db.update_dup_group(other_asset_id, group_id)
                similar_found += 1
                context.log.warning(
                    f"inline DEDUP 유사 이미지 발견: {asset_id} ↔ {other_asset_id} (distance={dist})"
                )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            if asset_id in prioritized_set:
                gated_failed += 1
            _mark_inline_dedup_failure(db, asset_id, str(exc))
            context.log.error(f"inline DEDUP 실패: {asset_id}: {exc}")

    return {
        "computed": computed,
        "similar_found": similar_found,
        "failed": failed,
        "gated_failed": gated_failed,
    }


def _build_raw_ingest_state(context) -> RawIngestState:
    request_id, folder_name, archive_only = _dispatch_request_context(context)
    return RawIngestState(
        manifest_path=context.run.tags.get("manifest_path"),
        request_id=request_id,
        folder_name=folder_name,
        archive_only=archive_only,
    )


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
        state.stage = "ensure_schema"
        context.log.info("ensure_schema:start")
        db.ensure_runtime_schema()
        context.log.info("ensure_schema:done")

        state.stage = "manifest_load"
        state.manifest, summary = _load_manifest_or_summary(
            context,
            db,
            manifest_path=state.manifest_path,
            request_id=state.request_id,
        )
        if summary is not None:
            return summary

        assert state.manifest is not None
        state.transfer_tool = str(state.manifest.get("transfer_tool", "")).strip().lower()
        runtime_policy = resolve_ingest_runtime_policy(transfer_tool=state.transfer_tool)

        state.stage = "archive_prepare"
        _prepare_archive_inputs(
            context,
            config=config,
            state=state,
            policy=runtime_policy,
        )

        state.stage = "manifest_hydrate"
        _hydrate_manifest(context, state)

        state.stage = "register"
        context.log.info("register:start")
        state.records = register_incoming(
            context,
            db,
            state.manifest,
            ingest_rejections=state.ingest_rejections,
        )
        context.log.info(f"register:done records={len(state.records)}")

        target_archive_dir = config.archive_dir

        state.stage = "upload"
        context.log.info(
            "upload_prepare:start "
            f"defer_video_env_classification={runtime_policy.defer_video_env_classification}"
        )
        state.uploaded = normalize_and_archive(
            context,
            db,
            minio,
            state.records,
            target_archive_dir,
            ingest_rejections=state.ingest_rejections,
            retry_candidates=state.retry_candidates,
            defer_video_env_classification=runtime_policy.defer_video_env_classification,
        )
        context.log.info(
            f"upload_execute:done uploaded={len(state.uploaded)} "
            f"retry_candidates={len(state.retry_candidates)}"
        )

        state.stage = "archive_finalize"
        context.log.info(
            "archive_finalize:start "
            f"archive_requested={state.archive_requested} "
            f"prepared={state.archive_prepared_for_upload} "
            f"uploaded={len(state.uploaded)}"
        )
        if state.archive_requested:
            state.archived, state.archive_unit_dir_hint = archive_uploaded_assets(
                context=context,
                db=db,
                manifest=state.manifest,
                uploaded=state.uploaded,
                archive_dir=target_archive_dir,
                ingest_rejections=state.ingest_rejections,
            )
        else:
            state.archived = complete_uploaded_assets_without_archive(
                context=context,
                db=db,
                manifest=state.manifest,
                uploaded=state.uploaded,
            )
            state.archive_unit_dir_hint = None
        context.log.info(
            "archive_finalize:done "
            f"archived={len(state.archived)} "
            f"uploaded={len(state.uploaded)} "
            f"archive_requested={state.archive_requested}"
        )

        state.stage = "label_artifact_import"
        local_artifact_summary = _run_staging_archive_only_artifact_import(
            context,
            db,
            minio,
            config=config,
            state=state,
            runtime_profile=runtime_policy.runtime_profile,
        )

        state.stage = "retry_manifest"
        retry_manifest_path = build_retry_manifest(
            context=context,
            config=config,
            manifest=state.manifest,
            retry_candidates=state.retry_candidates,
        )
        failure_log_path = write_ingest_failure_logs(
            context=context,
            config=config,
            manifest=state.manifest,
            ingest_rejections=state.ingest_rejections,
        )

        state.stage = "inline_dedup"
        duplicate_targets = collect_duplicate_asset_file_map(state.records)
        if duplicate_targets:
            duplicate_files_count = sum(
                1
                for rec in state.records
                if str((rec.get("record") or {}).get("error_message") or "").startswith("duplicate_of:")
            )
            updated_assets = db.mark_duplicate_skipped_assets(duplicate_targets)
            context.log.warning(
                "중복 스킵 이력 반영(자산별): "
                f"duplicate_assets={len(duplicate_targets)}, "
                f"duplicate_files={duplicate_files_count}, "
                f"updated_assets={updated_assets}"
            )

        dedup_summary = _run_inline_dedup(context, db, minio, state.uploaded)

        state.stage = "done_marker"
        archive_done_marker_path = maybe_write_archive_done_marker(
            context=context,
            db=db,
            manifest=state.manifest,
            manifest_dir=config.manifest_dir,
            manifest_path=state.manifest_path,
            archive_dir=target_archive_dir,
            archive_unit_dir_hint=state.archive_unit_dir_hint,
        )

        summary = ingest_summary(context, state.archived, state.records)
        uploaded_count = len(state.uploaded)
        archived_count = len(state.archived)
        archive_missing_count = max(0, uploaded_count - archived_count)
        summary.update(
            {
                "uploaded_count": uploaded_count,
                "archived_count": archived_count,
                "archive_missing_count": archive_missing_count,
                "archive_requested": state.archive_requested,
                "ingest_rejection_count": len(state.ingest_rejections),
                "retry_candidate_count": len(state.retry_candidates),
                "retry_manifest_created": bool(retry_manifest_path),
                "archive_done_marker_present": bool(archive_done_marker_path),
                "dedup_computed": dedup_summary["computed"],
                "dedup_similar_found": dedup_summary["similar_found"],
                "dedup_failed": dedup_summary["failed"],
            }
        )
        if local_artifact_summary:
            summary.update(
                {
                    "local_event_labels_inserted": int(local_artifact_summary.get("event_labels_inserted", 0)),
                    "local_bbox_inserted": int(local_artifact_summary.get("bbox_inserted", 0)),
                    "local_image_captions_inserted": int(local_artifact_summary.get("image_captions_inserted", 0)),
                    "local_artifact_failure_count": int(local_artifact_summary.get("failure_count", 0)),
                }
            )
            if local_artifact_summary.get("failure_log_path"):
                summary["local_artifact_failure_log_path"] = str(local_artifact_summary["failure_log_path"])
        output_metadata = {
            "uploaded_count": uploaded_count,
            "archived_count": archived_count,
            "archive_missing_count": archive_missing_count,
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
            output_metadata.update(
                {
                    "local_event_labels_inserted": int(local_artifact_summary.get("event_labels_inserted", 0)),
                    "local_bbox_inserted": int(local_artifact_summary.get("bbox_inserted", 0)),
                    "local_image_captions_inserted": int(local_artifact_summary.get("image_captions_inserted", 0)),
                    "local_artifact_failure_count": int(local_artifact_summary.get("failure_count", 0)),
                }
            )
            if local_artifact_summary.get("failure_log_path"):
                output_metadata["local_artifact_failure_log_path"] = str(local_artifact_summary["failure_log_path"])
        if retry_manifest_path is not None:
            output_metadata["retry_manifest_path"] = str(retry_manifest_path)
        if failure_log_path is not None:
            output_metadata["failure_log_path"] = str(failure_log_path)
        if archive_done_marker_path is not None:
            output_metadata["archive_done_marker_path"] = str(archive_done_marker_path)
        context.add_output_metadata(output_metadata)

        if dedup_summary["gated_failed"] > 0:
            raise RuntimeError(
                "inline DEDUP failed for current manifest assets: "
                f"failed={dedup_summary['gated_failed']}, "
                f"computed={dedup_summary['computed']}, "
                f"similar_found={dedup_summary['similar_found']}"
            )

        _move_manifest_to_processed(context, config, state.manifest_path)
        _finalize_dispatch_success(db, state)

        return summary

    except Exception as exc:
        failure_message = f"{state.stage}_failed:{exc}"
        context.log.error(
            f"raw_ingest 실패: stage={state.stage}, request_id={state.request_id or ''}, error={exc}"
        )
        _mark_failed_records(
            context,
            db,
            state=state,
            failure_message=failure_message,
        )
        _finalize_dispatch_failure(
            context,
            db,
            state=state,
            failure_message=failure_message,
        )

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
