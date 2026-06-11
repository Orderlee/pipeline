"""INGEST @op — normalize_and_archive + ingest_summary.

Phase A: 병렬 NAS I/O 메타 추출 (checksum + ffprobe + reencode 판정).
Phase B: 순차 중복검출 + raw_files batch insert.
Phase C: MinIO 병렬 업로드 (재인코딩 필요 시 reencode→upload).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.file_loader import load_image_once
from vlm_pipeline.lib.video_loader import load_video_once
from vlm_pipeline.lib.video_reencode import (
    STANDARD_PRESET_NAME,
    needs_reencode,
    reencode_with_fallback,
)
from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.resources.postgres import PostgresResource
from vlm_pipeline.resources.minio import MinIOResource

from .ops_common import (
    DEFAULT_IMAGE_CODEC,
    DEFAULT_META_WORKERS,
    DEFAULT_RAW_BUCKET,
    DEFAULT_VIDEO_CONTENT_TYPE,
    MAX_META_WORKERS,
    _append_ingest_rejection,
    _is_retryable_failed_record,
    _is_transient_error,
    read_ingest_worker_config,
)


# ── Phase B 상태 컨테이너 ─────────────────────────────────────────
@dataclass
class _PhaseBState:
    """Phase B 순차 처리 중 누적되는 가변 상태."""

    pending_db_rows: list[dict[str, Any]] = field(default_factory=list)
    pending_checksums: dict[str, str] = field(default_factory=dict)
    upload_tasks: list[dict[str, Any]] = field(default_factory=list)


def _link_genai_asset_if_any(db, entry: dict, context) -> None:
    """raw_files INSERT 직후, 해당 row 가 GenAI source/output 이면 genai_jobs FK 연결.

    ops_register 가 manifest 의 items[seq].asset_id 매핑을 entry 에 _genai_batch_id /
    _genai_seq_in_batch / _genai_role 로 통과시킨 경우만 동작. 누락 시 no-op.
    """
    db_record = entry.get("db_record") or {}
    role = (db_record.get("source_type") or "").strip()
    if role not in ("genai_source", "genai_output"):
        return
    batch_id = entry.get("_genai_batch_id")
    seq = entry.get("_genai_seq_in_batch")
    if not batch_id or not seq:
        return
    asset_id = db_record.get("asset_id")
    if not asset_id:
        return
    update = getattr(db, "update_genai_job_assets", None)
    if not callable(update):
        # legacy DuckDB backend — Phase 3 은 PG 전제. 누락 시 warn 후 skip.
        context.log.warning(
            f"genai link skipped (no update_genai_job_assets on {type(db).__name__}): batch={batch_id} seq={seq}"
        )
        return
    kwargs = {"batch_id": batch_id, "seq_in_batch": int(seq)}
    if role == "genai_source":
        kwargs["input_asset_id"] = asset_id
    else:
        kwargs["output_asset_id"] = asset_id
    try:
        update(**kwargs)
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"update_genai_job_assets 실패 batch={batch_id} seq={seq} role={role}: {exc}")


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
    context,
) -> None:
    """중복 파일은 raw_files에 신규 row를 남기지 않고 스킵 처리."""
    db_record["ingest_status"] = "skipped"
    db_record["error_message"] = f"duplicate_of:{duplicate_asset_id}"
    rec["status"] = "skipped"
    context.log.info(f"중복 파일 스킵(raw_files insert 생략): {source_path} -> {duplicate_asset_id}")


def _stage_upload_task(
    entry: dict[str, Any],
    state: _PhaseBState,
    db,
    reencode_threads: int,
) -> None:
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

    state.upload_tasks.append(
        {
            "filepath": entry["filepath"],
            "source_path": entry["filepath"],
            "asset_id": asset_id,
            "media_type": media_type,
            "raw_key": raw_key,
            "db_record": entry["db_record"],
            "meta": meta,
            "reencode_required": (
                meta["video_metadata"].get("reencode_required", False) if media_type == "video" else False
            ),
            "reencode_threads": reencode_threads,
        }
    )


def _handle_pending_db_error(
    entry: dict[str, Any],
    exc: Exception,
    db,
    context,
    retry_candidates: list[dict] | None,
    ingest_rejections: list[dict] | None,
) -> None:
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
        context.log.warning(f"ingest 실패 레코드 정리 중 추가 오류: asset_id={asset_id}, err={cleanup_error}")

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


def _flush_pending_db_rows(
    state: _PhaseBState,
    db,
    context,
    retry_candidates: list[dict] | None,
    ingest_rejections: list[dict] | None,
    reencode_threads: int,
) -> None:
    if not state.pending_db_rows:
        return

    batch_entries = state.pending_db_rows
    state.pending_db_rows = []
    state.pending_checksums = {}

    try:
        db.insert_raw_files_batch([entry["db_record"] for entry in batch_entries])
    except Exception as batch_exc:  # noqa: BLE001
        context.log.warning(
            f"raw_files batch insert 실패, 낱개 fallback: count={len(batch_entries)} err={batch_exc}"
        )
        for entry in batch_entries:
            try:
                db.insert_raw_files_batch([entry["db_record"]])
                _link_genai_asset_if_any(db, entry, context)
                _stage_upload_task(entry, state, db, reencode_threads)
            except Exception as single_exc:  # noqa: BLE001
                _handle_pending_db_error(entry, single_exc, db, context, retry_candidates, ingest_rejections)
        return

    for entry in batch_entries:
        try:
            _link_genai_asset_if_any(db, entry, context)
            _stage_upload_task(entry, state, db, reencode_threads)
        except Exception as exc:  # noqa: BLE001
            _handle_pending_db_error(entry, exc, db, context, retry_candidates, ingest_rejections)


# ── Phase A: NAS I/O 병렬 메타 추출 ─────────────────────────────
def _phase_a_extract_metadata(
    context,
    candidate_records: list[dict],
    meta_workers: int,
    defer_video_env_classification: bool,
) -> list[dict]:
    """병렬 NAS I/O: checksum + ffprobe + reencode 판정.

    반환: list of {"rec": ..., "meta": ..., "error": ...}
    """
    total_candidates = len(candidate_records)

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
                # inline DQ #1: duration_sec 가 None/0/<1.0s 면 fail 처리.
                # silent 통과시 다운스트림(Gemini 청크 분할, frame_extract) 에서
                # ZeroDivisionError / 빈 events 로 표출 — 여기서 일찍 잡는다.
                # 이미지/짧은 GIF 처럼 합법적인 0.x sec 비디오는 매우 드물고, 그런 케이스
                # 는 Gemini 청크 분할이 어차피 실패하므로 reject 가 옳다.
                duration_raw = meta["video_metadata"].get("duration_sec")
                if duration_raw is None or (isinstance(duration_raw, (int, float)) and duration_raw < 1.0):
                    raise ValueError(f"invalid duration_sec={duration_raw!r} (must be >= 1.0s)")
                reencode_req, reencode_rsn = needs_reencode(meta["video_metadata"], Path(filepath))
                meta["video_metadata"]["reencode_required"] = reencode_req
                meta["video_metadata"]["reencode_reason"] = reencode_rsn
            return {"rec": rec, "meta": meta, "error": None}
        except Exception as exc:
            return {"rec": rec, "meta": None, "error": exc}

    def _log_result(result: dict, processed: int) -> None:
        r = result["rec"]
        context.log.info(
            f"video_meta_extract progress={processed}/{total_candidates} "
            f"media_type={r.get('media_type', 'image')} path={r['path']}"
        )
        if result["error"] is None and result["meta"] and r.get("media_type") == "video":
            vm = result["meta"].get("video_metadata", {})
            if vm.get("reencode_required"):
                context.log.info(f"reencode_required: {r['path']} reason={vm.get('reencode_reason')}")

    meta_results: list[dict] = []
    if total_candidates > 1 and meta_workers > 1:
        context.log.info(f"meta_extract:parallel workers={meta_workers} candidates={total_candidates}")
        with ThreadPoolExecutor(max_workers=meta_workers) as meta_pool:
            futures = {meta_pool.submit(_extract_meta, rec): rec for rec in candidate_records}
            processed_candidates = 0
            for future in as_completed(futures):
                result = future.result()
                meta_results.append(result)
                processed_candidates += 1
                _log_result(result, processed_candidates)
    else:
        for i, rec in enumerate(candidate_records, 1):
            result = _extract_meta(rec)
            meta_results.append(result)
            _log_result(result, i)

    return meta_results


# ── Phase B: 순차 중복검출 + DB 등록 ────────────────────────────
def _phase_b_dedup_and_insert(
    context,
    db,
    meta_results: list[dict],
    upload_enabled: bool,
    raw_insert_batch_size: int,
    retry_candidates: list[dict] | None,
    ingest_rejections: list[dict] | None,
    reencode_threads: int,
) -> _PhaseBState:
    """순차 dedup + raw_files batch insert.

    반환: _PhaseBState (upload_tasks 포함).
    """
    state = _PhaseBState()

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
                context.log.warning(f"ingest 실패 레코드 정리 중 추가 오류: asset_id={asset_id}, err={cleanup_error}")
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
                _mark_duplicate_skip(filepath, str(existing["asset_id"]), db_record, rec, context)
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
                    _mark_duplicate_skip(filepath, stale_asset_id, db_record, rec, context)
                    continue

            pending_duplicate_asset_id = state.pending_checksums.get(str(meta["checksum"]))
            if pending_duplicate_asset_id:
                _close_video_stream(meta)
                context.log.warning(f"중복 건너뜀: {filepath} (동일 batch 기존: {pending_duplicate_asset_id})")
                _mark_duplicate_skip(filepath, pending_duplicate_asset_id, db_record, rec, context)
                continue

            db_record["file_size"] = meta["file_size"]
            db_record["checksum"] = meta["checksum"]
            # upload_enabled=False 경로에서는 raw_files 에 잠시 "pending" 으로 insert 하고,
            # archive 이동 완료 시 호출자가 "archived" 로 전환한다.
            db_record["ingest_status"] = "uploading" if upload_enabled else "pending"
            state.pending_db_rows.append(
                {
                    "filepath": filepath,
                    "asset_id": asset_id,
                    "media_type": media_type,
                    "raw_key": raw_key,
                    "db_record": db_record,
                    "meta": meta,
                    "rel_path": rel_path,
                    "record_ref": rec,
                    # GenAI 연결 키 (ops_register 가 통과시킨 값) — 미설정 시 None
                    "_genai_batch_id": rec.get("_genai_batch_id"),
                    "_genai_seq_in_batch": rec.get("_genai_seq_in_batch"),
                }
            )
            state.pending_checksums[str(meta["checksum"])] = str(asset_id)
            if len(state.pending_db_rows) >= raw_insert_batch_size:
                _flush_pending_db_rows(state, db, context, retry_candidates, ingest_rejections, reencode_threads)
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

    _flush_pending_db_rows(state, db, context, retry_candidates, ingest_rejections, reencode_threads)
    return state


# ── Phase C: MinIO 병렬 업로드 ──────────────────────────────────
def _phase_c_upload_to_minio(
    context,
    db,
    minio: MinIOResource,
    upload_tasks: list[dict[str, Any]],
    max_workers: int,
    reencode_workers: int,
    records: list[dict],
) -> list[dict]:
    """MinIO 병렬 업로드 (재인코딩 필요 시 reencode → 업로드).

    반환: 성공 업로드 항목 list[dict].
    """
    uploaded: list[dict] = []

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

    def _execute_upload(task: dict) -> dict:
        """단일 업로드 실행 후 결과 dict 반환.

        video + reencode_required=True 인 경우:
          1) 임시 파일로 표준 스펙 재인코딩 (retry 3회, fallback to original)
          2) 재인코딩본(또는 원본 fallback)으로 MinIO 업로드
          3) 임시 파일 정리 (finally)
        """
        tmp_path = None
        asset_id = task["asset_id"]
        try:
            if task["media_type"] == "video" and task.get("reencode_required"):
                tmp_path, fallback_reason = reencode_with_fallback(
                    Path(task["filepath"]),
                    threads=task.get("reencode_threads", 4),
                )
                if tmp_path is not None:
                    task["filepath"] = str(tmp_path)
                    task["reencode_info"] = {"reencode_preset": STANDARD_PRESET_NAME}
                else:
                    context.log.warning(
                        f"reencode fallback to original after 3 retries: asset_id={asset_id} reason={fallback_reason}"
                    )
                    try:
                        db.update_video_reencode_reason(asset_id, fallback_reason)
                    except Exception as upd_exc:  # noqa: BLE001
                        context.log.warning(f"reencode_reason 갱신 실패: asset_id={asset_id}: {upd_exc}")
            _upload_single(task)
            return {"success": True, "task": task}
        except Exception as exc:
            return {"success": False, "task": task, "error": exc}
        finally:
            _close_video_stream(task["meta"])
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)

    has_reencode_tasks = any(t.get("reencode_required") for t in upload_tasks)
    effective_workers = reencode_workers if has_reencode_tasks else max_workers

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
                reencode_info = task.get("reencode_info")
                if reencode_info and media_type == "video":
                    try:
                        db.update_video_reencode_applied(
                            asset_id,
                            reencode_preset=reencode_info.get("reencode_preset", STANDARD_PRESET_NAME),
                        )
                    except Exception as re_exc:  # noqa: BLE001
                        context.log.warning(f"reencode_applied 업데이트 실패: {asset_id}: {re_exc}")
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
            context.log.info(f"upload progress={completed_uploads}/{len(upload_tasks)} success={successful_uploads}")

    return uploaded


def normalize_and_archive(
    context,
    db: PostgresResource,
    minio: MinIOResource,
    records: list[dict],
    archive_dir: str,
    ingest_rejections: list[dict] | None = None,
    retry_candidates: list[dict] | None = None,
    defer_video_env_classification: bool = False,
    upload_enabled: bool = True,
) -> list[dict]:
    """1-pass 로딩 → checksum → verify → image/video_metadata → MinIO 업로드.

    archive 이동은 assets 레이어에서 source unit 정책으로 처리한다.
    ★ NFS에서 파일 1회만 읽어 모든 메타 추출 (3회→1회 최적화).

    upload_enabled=False 면 Phase C(MinIO 업로드)를 스킵하고 "ready" 리스트만 반환.
    archive 이동과 'archived' 상태 마크는 호출자(orchestrator)가 별도 함수로 수행.
    """
    _wcfg = read_ingest_worker_config()
    max_workers, reencode_workers, reencode_threads = _wcfg.max_workers, _wcfg.reencode_workers, _wcfg.reencode_threads
    raw_insert_batch_size = 50
    candidate_records = [rec for rec in records if rec.get("status") == "registered"]

    meta_workers = min(MAX_META_WORKERS, int_env("INGEST_META_WORKERS", DEFAULT_META_WORKERS, minimum=1))

    # ── Phase A ──────────────────────────────────────────────────
    meta_results = _phase_a_extract_metadata(
        context,
        candidate_records,
        meta_workers=meta_workers,
        defer_video_env_classification=defer_video_env_classification,
    )

    # ── Phase B ──────────────────────────────────────────────────
    phase_b = _phase_b_dedup_and_insert(
        context,
        db,
        meta_results,
        upload_enabled=upload_enabled,
        raw_insert_batch_size=raw_insert_batch_size,
        retry_candidates=retry_candidates,
        ingest_rejections=ingest_rejections,
        reencode_threads=reencode_threads,
    )
    upload_tasks = phase_b.upload_tasks

    if not upload_enabled:
        # upload_enabled=False: upload_tasks 를 그대로 "ready" 리스트로 변환해 반환.
        # 호출자가 archive 이동 후 ingest_status='archived' 로 전환한다.
        ready: list[dict] = []
        for task in upload_tasks:
            _close_video_stream(task.get("meta") or {})
            m = task.get("meta") or {}
            ready.append(
                {
                    "asset_id": task["asset_id"],
                    "source_path": task.get("source_path") or task["filepath"],
                    "raw_key": task["raw_key"],
                    "media_type": task["media_type"],
                    "checksum": m.get("checksum"),
                    "file_size": m.get("file_size"),
                }
            )
        context.log.info(
            f"upload_skip:done (upload_enabled=False) ready={len(ready)} reason='MinIO 업로드는 dispatch 단계로 연기'"
        )
        return ready

    context.log.info(f"upload_prepare:done tasks={len(upload_tasks)}")
    context.log.info("upload_execute:start")

    # ── Phase C ──────────────────────────────────────────────────
    return _phase_c_upload_to_minio(
        context,
        db,
        minio,
        upload_tasks,
        max_workers=max_workers,
        reencode_workers=reencode_workers,
        records=records,
    )


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
