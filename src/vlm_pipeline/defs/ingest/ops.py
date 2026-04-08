"""INGEST @op 정의 — register, normalize, upload, summary.

Layer 3: Dagster @op, lib/ + resources/ import.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from vlm_pipeline.lib.env_utils import storage_raw_key_prefix_from_source_unit
from vlm_pipeline.lib.file_loader import load_image_once
from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component
from vlm_pipeline.lib.validator import detect_media_type, validate_incoming
from vlm_pipeline.lib.video_loader import load_video_once
from vlm_pipeline.lib.video_reencode import (
    STANDARD_PRESET_NAME,
    needs_reencode,
    reencode_to_tmp,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

# ── 업로드 / 재인코딩 / 메타 추출 워커 상수 ──────────────────────
DEFAULT_UPLOAD_WORKERS: int = 4
MAX_UPLOAD_WORKERS: int = 16
DEFAULT_REENCODE_WORKERS: int = 3
DEFAULT_REENCODE_THREADS: int = 4
DEFAULT_META_WORKERS: int = 4
MAX_META_WORKERS: int = 8

# ── MinIO content-type 기본값 ────────────────────────────────────
DEFAULT_VIDEO_CONTENT_TYPE: str = "video/mp4"
DEFAULT_IMAGE_CODEC: str = "jpeg"
DEFAULT_RAW_BUCKET: str = "vlm-raw"

TRANSIENT_ERROR_MARKERS = (
    "could not set lock on file",
    "conflicting lock is held",
    "duckdb lock",
    "ffprobe_timeout",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "connection reset",
)


def _is_transient_error(exc: Exception) -> bool:
    message = str(exc or "").strip().lower()
    return any(marker in message for marker in TRANSIENT_ERROR_MARKERS)


from .duplicate import error_code_from_message as _derive_error_code


def _append_ingest_rejection(
    ingest_rejections: list[dict] | None,
    *,
    source_path: str,
    rel_path: str | None,
    media_type: str,
    stage: str,
    error_message: str,
    retryable: bool,
    error_code: str | None = None,
) -> None:
    if ingest_rejections is None:
        return
    normalized_message = str(error_message or "").strip()
    ingest_rejections.append(
        {
            "source_path": str(source_path or ""),
            "rel_path": str(rel_path or ""),
            "media_type": str(media_type or "unknown"),
            "stage": stage,
            "error_code": error_code or _derive_error_code(normalized_message),
            "error_message": normalized_message,
            "retryable": bool(retryable),
        }
    )


def _is_retryable_failed_record(stale_record: dict) -> bool:
    """failed 레코드 중 자동 재처리 정리 가능한 경우만 선별."""
    error_message = str(stale_record.get("error_message", "") or "").strip().lower()
    if not error_message:
        return False

    non_retryable_prefixes = (
        "duplicate_of:",
        "archive_move_failed",
        "archive_source_missing",
        "duplicate_skipped_in_manifest:",
    )
    if any(error_message.startswith(prefix) for prefix in non_retryable_prefixes):
        return False

    return any(marker in error_message for marker in TRANSIENT_ERROR_MARKERS)


def register_incoming(
    context,
    db: DuckDBResource,
    manifest: dict,
    ingest_rejections: list[dict] | None = None,
) -> list[dict]:
    """검증 → 정규화 → raw_files 배치 INSERT.

    manifest path는 컨테이너 경로 (/nas/incoming/...) 기준.
    라벨 JSON은 INGEST 대상 아님 (LABEL 단계에서 별도 생성).

    MinIO raw_key는 source unit/rel_path 구조를 유지하여 폴더 구조를 보존한다.
    단, GCP auto-bootstrap unit(`gcp/<bucket>/...`)은 `gcp/` prefix를 제거한다.
    """
    results: list[dict] = []
    batch_id = manifest.get("manifest_id", f"batch_{datetime.now():%Y%m%d_%H%M%S}")
    source_unit_name = manifest.get("source_unit_name", "")
    source_unit_type = str(manifest.get("source_unit_type", "")).strip().lower()

    def _sanitize_path_parts(raw: str) -> str:
        parts = [p for p in Path(str(raw or "")).parts if p not in ("", ".", "..")]
        return "/".join(sanitize_path_component(p) for p in parts if p)

    for entry in manifest.get("files", []):
        filepath = entry.get("path", "")
        rel_path = entry.get("rel_path", "")
        try:
            # 검증
            vr = validate_incoming(filepath)
            if vr.level == "FAIL":
                media_type = detect_media_type(filepath)
                _append_ingest_rejection(
                    ingest_rejections,
                    source_path=filepath,
                    rel_path=rel_path,
                    media_type=media_type,
                    stage="register",
                    error_message=vr.message,
                    retryable=False,
                    error_code=vr.message,
                )
                context.log.warning(f"register 검증 실패(미삽입): {filepath}: {vr.message}")
                results.append({"path": filepath, "status": "failed", "message": vr.message})
                continue

            # 정규화
            original_name = Path(filepath).name
            sanitized_name = sanitize_filename(original_name)
            media_type = detect_media_type(filepath)

            # raw_key 생성: source_unit_name/rel_path (폴더 구조 유지)
            if rel_path:
                sanitized_rel_dir = _sanitize_path_parts(str(Path(rel_path).parent))
                if sanitized_rel_dir:
                    sanitized_rel = f"{sanitized_rel_dir}/{sanitized_name}"
                else:
                    sanitized_rel = sanitized_name
            else:
                sanitized_rel = sanitized_name

            sanitized_source_unit_name = storage_raw_key_prefix_from_source_unit(source_unit_name)
            if sanitized_source_unit_name and source_unit_type != "file":
                raw_key = f"{sanitized_source_unit_name}/{sanitized_rel}"
            else:
                raw_key = f"{sanitized_rel}"

            asset_id = str(uuid4())
            record = {
                "asset_id": asset_id,
                "source_path": filepath,
                "original_name": original_name,
                "media_type": media_type,
                "raw_key": raw_key,
                "ingest_batch_id": batch_id,
                "transfer_tool": manifest.get("transfer_tool", "manual"),
                "ingest_status": "pending",
                "error_message": vr.message if vr.level == "WARN" else None,
            }
            if source_unit_name:
                record["source_unit_name"] = source_unit_name

            results.append({
                "asset_id": asset_id,
                "path": filepath,
                "original_name": original_name,
                "sanitized_name": sanitized_name,
                "media_type": media_type,
                "raw_key": raw_key,
                "rel_path": rel_path,
                "status": "registered",
                "record": record,
            })

        except Exception as e:
            context.log.error(f"처리 실패: {filepath}: {e}")
            _append_ingest_rejection(
                ingest_rejections,
                source_path=filepath,
                rel_path=rel_path,
                media_type=detect_media_type(filepath),
                stage="register",
                error_message=str(e),
                retryable=False,
                error_code="register_exception",
            )
            results.append({"path": filepath, "status": "failed", "message": str(e)})

    return results


def normalize_and_archive(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    records: list[dict],
    archive_dir: str,
    ingest_rejections: list[dict] | None = None,
    retry_candidates: list[dict] | None = None,
    defer_video_env_classification: bool = False,
) -> list[dict]:
    """1-pass 로딩 → checksum → verify → image/video_metadata → MinIO 업로드.

    archive 이동은 assets 레이어에서 source unit 정책으로 처리한다.
    ★ NFS에서 파일 1회만 읽어 모든 메타 추출 (3회→1회 최적화).
    """
    uploaded: list[dict] = []
    upload_tasks: list[dict[str, Any]] = []
    max_workers_raw = os.getenv("INGEST_UPLOAD_WORKERS", str(DEFAULT_UPLOAD_WORKERS))
    try:
        max_workers = int(max_workers_raw)
    except ValueError:
        max_workers = DEFAULT_UPLOAD_WORKERS
    max_workers = max(1, min(MAX_UPLOAD_WORKERS, max_workers))

    try:
        reencode_workers = max(1, min(MAX_UPLOAD_WORKERS, int(os.getenv("INGEST_REENCODE_WORKERS", str(DEFAULT_REENCODE_WORKERS)))))
        reencode_threads = max(1, int(os.getenv("INGEST_REENCODE_THREADS", str(DEFAULT_REENCODE_THREADS))))
    except ValueError:
        reencode_workers = DEFAULT_REENCODE_WORKERS
        reencode_threads = DEFAULT_REENCODE_THREADS
    raw_insert_batch_size = 50
    candidate_records = [rec for rec in records if rec.get("status") == "registered"]
    total_candidates = len(candidate_records)
    processed_candidates = 0

    def _close_video_stream(meta: dict) -> None:
        stream = meta.get("file_stream")
        if stream is None:
            return
        try:
            stream.close()
        except Exception:
            pass

    def _mark_duplicate_skip(
        source_path: str,
        duplicate_asset_id: str,
        db_record: dict[str, Any],
        rec: dict[str, Any],
    ) -> None:
        """중복 파일은 raw_files에 신규 row를 남기지 않고 스킵 처리."""
        db_record["ingest_status"] = "skipped"
        db_record["error_message"] = f"duplicate_of:{duplicate_asset_id}"
        rec["status"] = "skipped"
        context.log.info(
            "중복 파일 스킵(raw_files insert 생략): "
            f"{source_path} -> {duplicate_asset_id}"
        )

    pending_db_rows: list[dict[str, Any]] = []
    pending_checksums: dict[str, str] = {}

    def _stage_upload_task(entry: dict[str, Any]) -> None:
        asset_id = entry["asset_id"]
        media_type = entry["media_type"]
        raw_key = entry["raw_key"]
        meta = entry["meta"]

        if media_type == "image" and "image_metadata" in meta:
            image_meta = dict(meta["image_metadata"])
            image_meta.update(
                {
                    "image_bucket": DEFAULT_RAW_BUCKET,
                    "image_key": raw_key,
                    "image_role": "source_image",
                    "checksum": meta.get("checksum"),
                    "file_size": meta.get("file_size"),
                }
            )
            db.insert_image_metadata(asset_id, image_meta)
        elif media_type == "video" and "video_metadata" in meta:
            db.insert_video_metadata(asset_id, meta["video_metadata"])

        upload_tasks.append(
            {
                "filepath": entry["filepath"],
                "source_path": entry["filepath"],
                "asset_id": asset_id,
                "media_type": media_type,
                "raw_key": raw_key,
                "db_record": entry["db_record"],
                "meta": meta,
                "reencode_required": (
                    meta["video_metadata"].get("reencode_required", False)
                    if media_type == "video"
                    else False
                ),
                "reencode_threads": reencode_threads,
            }
        )

    def _handle_pending_db_error(entry: dict[str, Any], exc: Exception) -> None:
        filepath = entry["filepath"]
        asset_id = entry["asset_id"]
        media_type = entry["media_type"]
        rel_path = entry.get("rel_path", "")
        meta = entry.get("meta")
        error_message = str(exc)
        is_transient = _is_transient_error(exc)

        context.log.error(f"normalize 실패: {filepath}: {exc}")
        entry["record_ref"]["status"] = "failed"
        if meta is not None:
            _close_video_stream(meta)
        if is_transient and retry_candidates is not None:
            retry_candidates.append(
                {
                    "source_path": filepath,
                    "rel_path": str(rel_path or Path(filepath).name),
                    "media_type": media_type,
                }
            )

        cleanup_error: Exception | None = None
        try:
            if db.has_raw_file(asset_id):
                db.delete_asset_for_reingest(asset_id)
        except Exception as cleanup_exc:  # noqa: BLE001
            cleanup_error = cleanup_exc

        if cleanup_error is not None:
            context.log.warning(
                "ingest 실패 레코드 정리 중 추가 오류: "
                f"asset_id={asset_id}, err={cleanup_error}"
            )

        _append_ingest_rejection(
            ingest_rejections,
            source_path=filepath,
            rel_path=rel_path,
            media_type=media_type,
            stage="db",
            error_message=error_message,
            retryable=is_transient,
            error_code="duckdb_lock_conflict" if is_transient else None,
        )

    def _flush_pending_db_rows() -> None:
        nonlocal pending_db_rows, pending_checksums
        if not pending_db_rows:
            return

        batch_entries = pending_db_rows
        pending_db_rows = []
        pending_checksums = {}

        try:
            db.insert_raw_files_batch([entry["db_record"] for entry in batch_entries])
        except Exception as batch_exc:  # noqa: BLE001
            context.log.warning(
                "raw_files batch insert 실패, 낱개 fallback: "
                f"count={len(batch_entries)} err={batch_exc}"
            )
            for entry in batch_entries:
                try:
                    db.insert_raw_files_batch([entry["db_record"]])
                    _stage_upload_task(entry)
                except Exception as single_exc:  # noqa: BLE001
                    _handle_pending_db_error(entry, single_exc)
            return

        for entry in batch_entries:
            try:
                _stage_upload_task(entry)
            except Exception as exc:  # noqa: BLE001
                _handle_pending_db_error(entry, exc)

    # ── Phase A: NAS I/O 병렬 메타 추출 (sha256 + ffprobe) ──────
    def _extract_meta(rec: dict) -> dict:
        """NAS I/O 집약 작업: checksum, ffprobe, reencode 판정. 스레드 안전."""
        filepath = rec["path"]
        media_type = rec.get("media_type", "image")
        try:
            if media_type == "image":
                meta = load_image_once(filepath)
            else:
                meta = load_video_once(
                    filepath,
                    include_file_stream=False,
                    include_env_metadata=not defer_video_env_classification,
                )
                reencode_req, reencode_rsn = needs_reencode(
                    meta["video_metadata"], Path(filepath)
                )
                meta["video_metadata"]["reencode_required"] = reencode_req
                meta["video_metadata"]["reencode_reason"] = reencode_rsn
            return {"rec": rec, "meta": meta, "error": None}
        except Exception as exc:
            return {"rec": rec, "meta": None, "error": exc}

    meta_workers_raw = os.getenv("INGEST_META_WORKERS", str(DEFAULT_META_WORKERS))
    try:
        meta_workers = int(meta_workers_raw)
    except ValueError:
        meta_workers = DEFAULT_META_WORKERS
    meta_workers = max(1, min(MAX_META_WORKERS, meta_workers))

    meta_results: list[dict] = []
    if total_candidates > 1 and meta_workers > 1:
        context.log.info(f"meta_extract:parallel workers={meta_workers} candidates={total_candidates}")
        with ThreadPoolExecutor(max_workers=meta_workers) as meta_pool:
            futures = {
                meta_pool.submit(_extract_meta, rec): rec
                for rec in candidate_records
            }
            for future in as_completed(futures):
                result = future.result()
                meta_results.append(result)
                processed_candidates += 1
                r = result["rec"]
                context.log.info(
                    f"video_meta_extract progress={processed_candidates}/{total_candidates} "
                    f"media_type={r.get('media_type', 'image')} path={r['path']}"
                )
                if result["error"] is None and result["meta"] and r.get("media_type") == "video":
                    vm = result["meta"].get("video_metadata", {})
                    if vm.get("reencode_required"):
                        context.log.info(f"reencode_required: {r['path']} reason={vm.get('reencode_reason')}")
    else:
        for rec in candidate_records:
            result = _extract_meta(rec)
            meta_results.append(result)
            processed_candidates += 1
            context.log.info(
                f"video_meta_extract progress={processed_candidates}/{total_candidates} "
                f"media_type={rec.get('media_type', 'image')} path={rec['path']}"
            )
            if result["error"] is None and result["meta"] and rec.get("media_type") == "video":
                vm = result["meta"].get("video_metadata", {})
                if vm.get("reencode_required"):
                    context.log.info(f"reencode_required: {rec['path']} reason={vm.get('reencode_reason')}")

    # ── Phase B: 순차 중복검출 + DB 등록 (DuckDB lock 최소화) ──
    for mr in meta_results:
        rec = mr["rec"]
        meta = mr["meta"]
        error = mr["error"]

        filepath = rec["path"]
        asset_id = rec["asset_id"]
        media_type = rec.get("media_type", "image")
        raw_key = rec["raw_key"]
        rel_path = rec.get("rel_path", "")
        db_record = rec.get("record", {})

        if error is not None:
            context.log.error(f"normalize 실패: {filepath}: {error}")
            error_message = str(error)
            rec["status"] = "failed"
            is_transient = _is_transient_error(error)
            if is_transient and retry_candidates is not None:
                retry_candidates.append(
                    {
                        "source_path": filepath,
                        "rel_path": str(rel_path or Path(filepath).name),
                        "media_type": media_type,
                    }
                )
            cleanup_error: Exception | None = None
            try:
                if db.has_raw_file(asset_id):
                    db.delete_asset_for_reingest(asset_id)
            except Exception as cleanup_exc:  # noqa: BLE001
                cleanup_error = cleanup_exc
            if cleanup_error is not None:
                context.log.warning(
                    f"ingest 실패 레코드 정리 중 추가 오류: asset_id={asset_id}, err={cleanup_error}"
                )
            _append_ingest_rejection(
                ingest_rejections,
                source_path=filepath,
                rel_path=rel_path,
                media_type=media_type,
                stage="normalize",
                error_message=error_message,
                retryable=is_transient,
                error_code="duckdb_lock_conflict" if is_transient else None,
            )
            continue

        try:
            existing = db.find_by_checksum(meta["checksum"])
            if existing:
                _close_video_stream(meta)
                context.log.warning(f"중복 건너뜀: {filepath} (기존: {existing['asset_id']})")
                _mark_duplicate_skip(filepath, str(existing["asset_id"]), db_record, rec)
                continue

            stale = db.find_any_by_checksum(meta["checksum"])
            if stale:
                stale_status = str(stale.get("ingest_status", "")).strip().lower()
                stale_asset_id = str(stale.get("asset_id", "")).strip()
                if stale_status == "failed" and stale_asset_id and _is_retryable_failed_record(stale):
                    context.log.warning(
                        "재시도 정리: retryable checksum 실패 레코드 삭제 "
                        f"(asset_id={stale_asset_id}, status={stale.get('ingest_status')}, "
                        f"error={stale.get('error_message')})"
                    )
                    db.delete_asset_for_reingest(stale_asset_id)
                elif stale_status and stale_status != "completed":
                    _close_video_stream(meta)
                    context.log.warning(f"중복 건너뜀: {filepath} (기존: {stale_asset_id})")
                    _mark_duplicate_skip(filepath, stale_asset_id, db_record, rec)
                    continue

            pending_duplicate_asset_id = pending_checksums.get(str(meta["checksum"]))
            if pending_duplicate_asset_id:
                _close_video_stream(meta)
                context.log.warning(
                    f"중복 건너뜀: {filepath} (동일 batch 기존: {pending_duplicate_asset_id})"
                )
                _mark_duplicate_skip(filepath, pending_duplicate_asset_id, db_record, rec)
                continue

            db_record["file_size"] = meta["file_size"]
            db_record["checksum"] = meta["checksum"]
            db_record["ingest_status"] = "uploading"
            pending_db_rows.append(
                {
                    "filepath": filepath,
                    "asset_id": asset_id,
                    "media_type": media_type,
                    "raw_key": raw_key,
                    "db_record": db_record,
                    "meta": meta,
                    "rel_path": rel_path,
                    "record_ref": rec,
                }
            )
            pending_checksums[str(meta["checksum"])] = str(asset_id)
            if len(pending_db_rows) >= raw_insert_batch_size:
                _flush_pending_db_rows()
        except Exception as e:
            context.log.error(f"normalize 실패: {filepath}: {e}")
            error_message = str(e)
            rec["status"] = "failed"
            is_transient = _is_transient_error(e)
            if meta is not None:
                _close_video_stream(meta)
            if is_transient and retry_candidates is not None:
                retry_candidates.append(
                    {
                        "source_path": filepath,
                        "rel_path": str(rel_path or Path(filepath).name),
                        "media_type": media_type,
                    }
                )
            cleanup_error_db: Exception | None = None
            try:
                if db.has_raw_file(asset_id):
                    db.delete_asset_for_reingest(asset_id)
            except Exception as cleanup_exc:  # noqa: BLE001
                cleanup_error_db = cleanup_exc
            if cleanup_error_db is not None:
                context.log.warning(
                    f"ingest 실패 레코드 정리 중 추가 오류: asset_id={asset_id}, err={cleanup_error_db}"
                )
            _append_ingest_rejection(
                ingest_rejections,
                source_path=filepath,
                rel_path=rel_path,
                media_type=media_type,
                stage="db",
                error_message=error_message,
                retryable=is_transient,
                error_code="duckdb_lock_conflict" if is_transient else None,
            )

    _flush_pending_db_rows()

    context.log.info(f"upload_prepare:done tasks={len(upload_tasks)}")
    context.log.info("upload_execute:start")

    def _upload_single(task: dict[str, Any]) -> None:
        media_type = task["media_type"]
        raw_key = task["raw_key"]
        meta = task["meta"]
        filepath = task["filepath"]
        
        if media_type == "image":
            content_type = f"image/{meta.get('image_metadata', {}).get('codec', DEFAULT_IMAGE_CODEC)}"
            minio.upload(DEFAULT_RAW_BUCKET, raw_key, meta["file_bytes"], content_type)
            return

        stream = meta.get("file_stream")
        content_type = DEFAULT_VIDEO_CONTENT_TYPE
        if stream is not None:
            minio.upload_fileobj(DEFAULT_RAW_BUCKET, raw_key, stream, content_type=content_type)
            return

        minio.upload_file(DEFAULT_RAW_BUCKET, raw_key, filepath, content_type=content_type)

    # 재인코딩이 필요한 task가 있으면 reencode_workers 수로 제한
    has_reencode_tasks = any(t.get("reencode_required") for t in upload_tasks)
    effective_workers = reencode_workers if has_reencode_tasks else max_workers

    # 2) MinIO 업로드 (병렬, 재인코딩 필요 시 reencode → 업로드 순서)
    def _execute_upload(task: dict) -> dict:
        """단일 업로드 실행 후 결과 dict 반환.

        video + reencode_required=True 인 경우:
          1) 임시 파일로 표준 스펙 재인코딩
          2) 재인코딩본으로 MinIO 업로드
          3) 임시 파일 정리 (finally)
        """
        tmp_path = None
        try:
            if task["media_type"] == "video" and task.get("reencode_required"):
                tmp_path = reencode_to_tmp(
                    Path(task["filepath"]),
                    threads=task.get("reencode_threads", 4),
                )
                task["filepath"] = str(tmp_path)
                task["reencode_info"] = {"reencode_preset": STANDARD_PRESET_NAME}
            _upload_single(task)
            return {"success": True, "task": task}
        except Exception as exc:
            return {"success": False, "task": task, "error": exc}
        finally:
            _close_video_stream(task["meta"])
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)

    completed_uploads = 0
    successful_uploads = 0
    first_upload_logged = False

    records_by_asset_id = {rec.get("asset_id"): rec for rec in records if rec.get("asset_id")}
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {executor.submit(_execute_upload, t): t for t in upload_tasks}
        for future in as_completed(futures):
            result = future.result()
            task = result["task"]
            filepath = task["filepath"]
            asset_id = task["asset_id"]
            raw_key = task["raw_key"]
            media_type = task["media_type"]
            meta = task["meta"]

            if result["success"]:
                context.log.info(f"MinIO 업로드 완료: vlm-raw/{raw_key}")
                successful_uploads += 1
                if not first_upload_logged:
                    context.log.info(f"first_upload_complete raw_key={raw_key}")
                    first_upload_logged = True
                # 재인코딩이 적용된 경우 video_metadata 업데이트
                reencode_info = task.get("reencode_info")
                if reencode_info and media_type == "video":
                    try:
                        db.update_video_reencode_applied(
                            asset_id,
                            reencode_preset=reencode_info.get(
                                "reencode_preset", STANDARD_PRESET_NAME
                            ),
                        )
                    except Exception as re_exc:  # noqa: BLE001
                        context.log.warning(
                            f"reencode_applied 업데이트 실패: {asset_id}: {re_exc}"
                        )
                uploaded.append(
                    {
                        "asset_id": asset_id,
                        "source_path": task.get("source_path") or filepath,
                        "raw_key": raw_key,
                        "media_type": media_type,
                        "checksum": meta["checksum"],
                        "file_size": meta["file_size"],
                    }
                )
            else:
                exc = result["error"]
                context.log.error(f"업로드 실패: {filepath}: {exc}")
                rec_by_id = records_by_asset_id.get(asset_id)
                if rec_by_id is not None:
                    rec_by_id["status"] = "failed"
                db.update_raw_file_status(asset_id, "failed", str(exc))

            completed_uploads += 1
            context.log.info(
                "upload progress="
                f"{completed_uploads}/{len(upload_tasks)} "
                f"success={successful_uploads}"
            )

    return uploaded


def ingest_summary(context, uploaded: list[dict], all_records: list[dict]) -> dict:
    """성공/실패/경고 건수 집계 → Dagster output metadata."""
    total = len(all_records)
    success = len(uploaded)
    failed = sum(1 for r in all_records if r.get("status") == "failed")
    skipped = total - success - failed

    summary = {
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
    }
    context.add_output_metadata(summary)
    context.log.info(f"INGEST 완료: {summary}")
    return summary
