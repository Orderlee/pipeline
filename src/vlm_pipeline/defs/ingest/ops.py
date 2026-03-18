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

from vlm_pipeline.lib.file_loader import load_image_once
from vlm_pipeline.lib.sanitizer import sanitize_filename, sanitize_path_component
from vlm_pipeline.lib.validator import detect_media_type, validate_incoming
from vlm_pipeline.lib.video_loader import load_video_once
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


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

    MinIO raw_key는 source_unit_name/rel_path 구조를 유지하여 폴더 구조를 보존한다.
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

            sanitized_source_unit_name = _sanitize_path_parts(source_unit_name)
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
) -> list[dict]:
    """1-pass 로딩 → checksum → verify → image/video_metadata → MinIO 업로드.

    archive 이동은 assets 레이어에서 source unit 정책으로 처리한다.
    ★ NFS에서 파일 1회만 읽어 모든 메타 추출 (3회→1회 최적화).
    """
    uploaded: list[dict] = []
    upload_tasks: list[dict[str, Any]] = []
    max_workers_raw = os.getenv("INGEST_UPLOAD_WORKERS", "4")
    try:
        max_workers = int(max_workers_raw)
    except ValueError:
        max_workers = 4
    max_workers = max(1, min(16, max_workers))

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

    # 1) 메타 추출/중복검출/DB 등록은 순차 처리 (DuckDB lock 최소화)
    for rec in records:
        if rec.get("status") != "registered":
            continue

        filepath = rec["path"]
        asset_id = rec["asset_id"]
        media_type = rec.get("media_type", "image")
        raw_key = rec["raw_key"]
        rel_path = rec.get("rel_path", "")
        db_record = rec.get("record", {})
        stage_hint = "normalize"
        meta: dict | None = None

        try:
            # 1-pass 로딩
            if media_type == "image":
                meta = load_image_once(filepath)
            else:
                # 대용량 파일 복사로 인한 로컬 디스크/메모리 I/O 병목 해소
                # boto3의 멀티파트 네이티브 병렬 읽기(upload_file) 활용을 위해 Spooled stream을 비활성
                meta = load_video_once(filepath, include_file_stream=False)

            stage_hint = "db"
            # 정확 중복 검출
            existing = db.find_by_checksum(meta["checksum"])
            if existing:
                _close_video_stream(meta)
                context.log.warning(f"중복 건너뜀: {filepath} (기존: {existing['asset_id']})")
                _mark_duplicate_skip(filepath, str(existing["asset_id"]), db_record, rec)
                continue

            # 과거 실패(run 중단/환경오류) 레코드가 checksum을 점유 중이면 재시도 전 정리
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
                    # 동일 batch 내 업로드중(uploading) 등은 중복으로 간주하고 스킵
                    _close_video_stream(meta)
                    context.log.warning(f"중복 건너뜀: {filepath} (기존: {stale_asset_id})")
                    _mark_duplicate_skip(filepath, stale_asset_id, db_record, rec)
                    continue

            # raw_files INSERT
            db_record["file_size"] = meta["file_size"]
            db_record["checksum"] = meta["checksum"]
            db_record["ingest_status"] = "uploading"
            db.insert_raw_files_batch([db_record])

            # image/video_metadata INSERT
            if media_type == "image" and "image_metadata" in meta:
                image_meta = dict(meta["image_metadata"])
                image_meta.update(
                    {
                        "image_bucket": "vlm-raw",
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
                    "filepath": filepath,
                    "asset_id": asset_id,
                    "media_type": media_type,
                    "raw_key": raw_key,
                    "db_record": db_record,
                    "meta": meta,
                }
            )
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

            rejection_stage = "db" if stage_hint == "db" else "normalize"
            _append_ingest_rejection(
                ingest_rejections,
                source_path=filepath,
                rel_path=rel_path,
                media_type=media_type,
                stage=rejection_stage,
                error_message=error_message,
                retryable=is_transient,
                error_code="duckdb_lock_conflict" if is_transient else None,
            )

    def _upload_single(task: dict[str, Any]) -> None:
        media_type = task["media_type"]
        raw_key = task["raw_key"]
        meta = task["meta"]
        filepath = task["filepath"]
        
        if media_type == "image":
            content_type = f"image/{meta.get('image_metadata', {}).get('codec', 'jpeg')}"
            minio.upload("vlm-raw", raw_key, meta["file_bytes"], content_type)
            return

        stream = meta.get("file_stream")
        content_type = "video/mp4"
        if stream is not None:
            minio.upload_fileobj("vlm-raw", raw_key, stream, content_type=content_type)
            return

        # SpooledTemporaryFile stream 대신 NAS에 위치한 원본 파일 경로를 직접 넘겨 병렬 업로드 최적화
        minio.upload_file("vlm-raw", raw_key, filepath, content_type=content_type)

    # 2) MinIO 업로드 (병렬)
    def _execute_upload(task: dict) -> dict:
        """단일 업로드 실행 후 결과 dict 반환."""
        try:
            _upload_single(task)
            return {"success": True, "task": task}
        except Exception as exc:
            return {"success": False, "task": task, "error": exc}
        finally:
            _close_video_stream(task["meta"])

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                uploaded.append(
                    {
                        "asset_id": asset_id,
                        "source_path": filepath,
                        "raw_key": raw_key,
                        "media_type": media_type,
                        "checksum": meta["checksum"],
                        "file_size": meta["file_size"],
                    }
                )
            else:
                exc = result["error"]
                context.log.error(f"업로드 실패: {filepath}: {exc}")
                for rec in records:
                    if rec.get("asset_id") == asset_id:
                        rec["status"] = "failed"
                        break
                db.update_raw_file_status(asset_id, "failed", str(exc))

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
