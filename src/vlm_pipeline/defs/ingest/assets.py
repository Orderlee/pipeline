"""INGEST @asset — NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw + DuckDB raw_files.

Layer 4: Dagster @asset, 같은 도메인의 ops.py만 import.
"""

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from dagster import AssetKey, asset

from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .ops import ingest_summary, normalize_and_archive, register_incoming


def _error_code_from_message(error_message: str, default_code: str = "unknown_error") -> str:
    message = str(error_message or "").strip()
    if not message:
        return default_code
    prefix = message.split(":", 1)[0].strip().lower()
    if not prefix:
        return default_code
    normalized = []
    for ch in prefix:
        if ch.isalnum() or ch in {"_", "-"}:
            normalized.append(ch)
        elif ch.isspace():
            normalized.append("_")
    return "".join(normalized).strip("_") or default_code


def _sanitize_manifest_identifier(raw: str, fallback: str = "unknown_manifest") -> str:
    value = str(raw or "").strip()
    if not value:
        return fallback
    normalized = [ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in value]
    collapsed = "".join(normalized).strip("_")
    return collapsed or fallback


def _resolve_failure_log_dir(config: PipelineConfig) -> Path:
    configured = str(os.getenv("INGEST_FAILURE_LOG_DIR", "")).strip()
    if configured:
        return Path(configured)
    return Path(config.manifest_dir) / "failed"


def _write_ingest_failure_logs(
    context,
    config: PipelineConfig,
    manifest: dict,
    ingest_rejections: list[dict],
) -> Path | None:
    if not ingest_rejections:
        return None

    manifest_id = _sanitize_manifest_identifier(str(manifest.get("manifest_id", "")))
    try:
        failure_dir = _resolve_failure_log_dir(config)
        failure_dir.mkdir(parents=True, exist_ok=True)
        log_path = failure_dir / f"{manifest_id}.jsonl"

        with log_path.open("a", encoding="utf-8") as fh:
            for row in ingest_rejections:
                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "manifest_id": str(manifest.get("manifest_id", "")),
                    "source_path": str(row.get("source_path", "")),
                    "rel_path": str(row.get("rel_path", "")),
                    "media_type": str(row.get("media_type", "unknown")),
                    "stage": str(row.get("stage", "unknown")),
                    "error_code": str(row.get("error_code", "unknown_error")),
                    "error_message": str(row.get("error_message", "")),
                    "retryable": bool(row.get("retryable", False)),
                }
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

        context.log.warning(
            "ingest 실패 로그 기록 완료: "
            f"{log_path} (count={len(ingest_rejections)})"
        )
        return log_path
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"ingest 실패 로그 기록 실패: {exc}")
        return None


def _build_retry_manifest(
    context,
    config: PipelineConfig,
    manifest: dict,
    retry_candidates: list[dict],
) -> Path | None:
    if not retry_candidates:
        return None

    max_attempt = max(1, int(os.getenv("INGEST_TRANSIENT_RETRY_MAX_ATTEMPTS", "3")))
    try:
        current_attempt = int(manifest.get("retry_attempt") or 0)
    except (TypeError, ValueError):
        current_attempt = 0
    if current_attempt >= max_attempt:
        context.log.warning(
            "retry manifest 생성 중단: retry 한도 초과 "
            f"(manifest_id={manifest.get('manifest_id')}, current={current_attempt}, max={max_attempt})"
        )
        return None

    dedup: dict[str, dict] = {}
    for row in retry_candidates:
        source_path = str(row.get("source_path", "")).strip()
        if not source_path:
            continue
        rel_path = str(row.get("rel_path", "")).strip() or Path(source_path).name
        media_type = str(row.get("media_type", "unknown")).strip() or "unknown"
        file_size = None
        try:
            if Path(source_path).exists():
                file_size = int(Path(source_path).stat().st_size)
        except OSError:
            file_size = None
        dedup[source_path] = {
            "path": source_path,
            "size": file_size,
            "rel_path": rel_path,
            "media_type": media_type,
        }

    files = [dedup[key] for key in sorted(dedup)]
    if not files:
        return None

    root_manifest_id = str(
        manifest.get("retry_of_manifest_id")
        or manifest.get("manifest_id")
        or "unknown_manifest"
    )
    root_manifest_slug = _sanitize_manifest_identifier(root_manifest_id)
    next_attempt = current_attempt + 1
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    retry_manifest_id = f"retry_{root_manifest_slug}_{timestamp}"

    pending_dir = Path(config.manifest_dir) / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    retry_manifest_path = pending_dir / f"{retry_manifest_id}.json"
    suffix = 2
    while retry_manifest_path.exists():
        retry_manifest_path = pending_dir / f"{retry_manifest_id}__{suffix}.json"
        suffix += 1

    source_unit_dispatch_key = str(
        manifest.get("source_unit_dispatch_key")
        or manifest.get("source_unit_path")
        or ""
    ).strip()
    if source_unit_dispatch_key:
        source_unit_dispatch_key = f"{source_unit_dispatch_key}#retry:{next_attempt:02d}"

    payload = {
        "manifest_id": retry_manifest_path.stem,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_dir": manifest.get("source_dir", config.incoming_dir),
        "source_unit_type": manifest.get("source_unit_type", "directory"),
        "source_unit_path": manifest.get("source_unit_path", ""),
        "source_unit_name": manifest.get("source_unit_name", ""),
        "source_unit_dispatch_key": source_unit_dispatch_key,
        "source_unit_total_file_count": len(files),
        "source_unit_chunk_index": 1,
        "source_unit_chunk_count": 1,
        "stable_signature": manifest.get("stable_signature", ""),
        "transfer_tool": "ingest_retry_manifest",
        "file_count": len(files),
        "files": files,
        "retry_of_manifest_id": root_manifest_id,
        "retry_attempt": next_attempt,
        "retry_reason": "transient_db_lock",
    }

    retry_manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    context.log.warning(
        "transient 오류 재시도 manifest 생성: "
        f"{retry_manifest_path.name} (files={len(files)}, retry_attempt={next_attempt})"
    )
    return retry_manifest_path


def _collect_duplicate_asset_file_map(records: list[dict]) -> dict[str, str]:
    """normalize 단계 duplicate_of 대상 asset_id별 중복 파일명(들)을 수집."""
    target_files: dict[str, set[str]] = {}
    for rec in records:
        db_record = rec.get("record")
        if not isinstance(db_record, dict):
            continue
        error_message = str(db_record.get("error_message") or "")
        if not error_message.startswith("duplicate_of:"):
            continue
        duplicate_asset_id = error_message.replace("duplicate_of:", "", 1).strip()
        if duplicate_asset_id:
            file_name = str(db_record.get("original_name") or "").strip()
            if not file_name:
                file_name = Path(str(rec.get("path") or "")).name
            if not file_name:
                file_name = "unknown_file"
            target_files.setdefault(duplicate_asset_id, set()).add(file_name)

    return {
        asset_id: ",".join(sorted(file_names))
        for asset_id, file_names in target_files.items()
        if file_names
    }


def _resolve_unique_directory(target_dir: Path, max_attempts: int = 100) -> Path:
    """경로 충돌 시 __2, __3 suffix를 붙여 유니크 디렉토리 경로 생성."""
    if not target_dir.exists():
        return target_dir
    for idx in range(2, max_attempts + 2):
        candidate = target_dir.with_name(f"{target_dir.name}__{idx}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Cannot find unique directory after {max_attempts} attempts: {target_dir}")


def _resolve_unique_file(target_file: Path, max_attempts: int = 100) -> Path:
    """경로 충돌 시 __2, __3 suffix를 붙여 유니크 파일 경로 생성."""
    if not target_file.exists():
        return target_file
    stem = target_file.stem
    suffix = target_file.suffix
    for idx in range(2, max_attempts + 2):
        candidate = target_file.with_name(f"{stem}__{idx}{suffix}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Cannot find unique file after {max_attempts} attempts: {target_file}")


def _find_existing_archive_candidate(base_target: Path, max_suffix: int = 10) -> Path | None:
    """archive 대상 경로(base + __N) 중 실제 존재 경로를 탐색.

    NFS 환경에서 exists() 호출은 네트워크 I/O를 발생시키므로 max_suffix를 작게 유지.
    """
    if base_target.exists():
        return base_target

    stem = base_target.stem
    suffix = base_target.suffix
    for idx in range(2, max_suffix + 1):
        candidate = base_target.with_name(f"{stem}__{idx}{suffix}")
        if candidate.exists():
            return candidate
    return None


def _find_existing_in_archive_unit_dirs(
    archive_root_dir: Path,
    source_unit_name: str,
    rel_path: str,
    max_suffix: int = 5,
) -> Path | None:
    """archive unit 디렉토리에서 파일 존재 여부 탐색.

    NFS 환경에서 exists() 호출은 네트워크 I/O를 발생시키므로 max_suffix를 작게 유지.
    """
    rel = Path(rel_path)
    base_dir = archive_root_dir / source_unit_name
    base_target = base_dir / rel

    found = _find_existing_archive_candidate(base_target, max_suffix=10)
    if found is not None:
        return found

    for idx in range(2, max_suffix + 1):
        candidate_dir = archive_root_dir / f"{source_unit_name}__{idx}"
        if not candidate_dir.exists():
            break
        found = _find_existing_archive_candidate(candidate_dir / rel, max_suffix=10)
        if found is not None:
            return found
    return None


def _is_source_missing_error(exc: Exception) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == 2:
        return True
    return "[Errno 2]" in str(exc)


def _cleanup_empty_tree(root: Path, max_depth: int = 5) -> None:
    """하위가 비어 있으면 상향식으로 빈 디렉토리를 정리.

    NFS 환경에서 os.walk는 느리므로 max_depth를 제한한다.
    """
    if not root.exists() or not root.is_dir():
        return

    def _cleanup_recursive(current: Path, depth: int) -> bool:
        """빈 디렉토리면 삭제하고 True 반환."""
        if depth > max_depth:
            return False
        try:
            children = list(current.iterdir())
        except OSError:
            return False

        if not children:
            try:
                current.rmdir()
                return True
            except OSError:
                return False

        all_removed = True
        for child in children:
            if child.is_dir():
                if not _cleanup_recursive(child, depth + 1):
                    all_removed = False
            else:
                all_removed = False

        if all_removed:
            try:
                current.rmdir()
                return True
            except OSError:
                pass
        return False

    _cleanup_recursive(root, 0)


def _cleanup_residual_source_file(context, source_path: str) -> None:
    """archive 완료 후 source 파일이 남아있으면 정리한다.

    move가 환경 이슈로 copy 동작처럼 끝나 source가 남는 케이스를 방어한다.
    """
    src = Path(source_path)
    try:
        if not src.exists():
            return
    except OSError:
        return

    try:
        if src.is_file() or src.is_symlink():
            src.unlink()
            context.log.warning(f"archive 완료 후 incoming 잔존 파일 정리: {src}")
        elif src.is_dir():
            shutil.rmtree(src)
            context.log.warning(f"archive 완료 후 incoming 잔존 디렉토리 정리: {src}")
    except OSError as exc:
        context.log.warning(f"incoming 잔존 정리 실패(수동 확인 필요): {src} ({exc})")


def _archive_uploaded_assets(
    context,
    db: DuckDBResource,
    manifest: dict,
    uploaded: list[dict],
    archive_dir: str,
    ingest_rejections: list[dict] | None = None,
) -> list[dict]:
    """업로드 완료 파일을 이전 정책(source unit 기반)으로 archive 이동."""
    if not uploaded:
        return []

    archive_root_dir = Path(archive_dir)
    archive_root_dir.mkdir(parents=True, exist_ok=True)

    files = manifest.get("files", [])
    rel_path_by_source: dict[str, str] = {}
    for entry in files:
        if not isinstance(entry, dict):
            continue
        source_path = str(entry.get("path", "")).strip()
        rel_path = str(entry.get("rel_path", "")).strip()
        if source_path:
            rel_path_by_source[source_path] = rel_path or Path(source_path).name

    source_unit_type = str(manifest.get("source_unit_type", "")).strip().lower()
    source_unit_name = str(manifest.get("source_unit_name", "")).strip()
    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    source_unit_path = Path(source_unit_path_raw) if source_unit_path_raw else None
    manifest_file_count = int(manifest.get("file_count") or len(files) or len(uploaded))
    source_unit_total_file_count = int(
        manifest.get("source_unit_total_file_count") or manifest_file_count
    )
    is_chunked_manifest = (
        int(manifest.get("source_unit_chunk_count") or 1) > 1
        or source_unit_total_file_count > manifest_file_count
    )

    archived_items: list[dict] = []

    def _mark_archive_result(item: dict, archive_path: Path | None, error: str | None = None) -> None:
        asset_id = str(item.get("asset_id", "")).strip()
        if not asset_id:
            return
        if archive_path is not None:
            source_path = str(item.get("source_path", "")).strip()
            if source_path:
                _cleanup_residual_source_file(context, source_path)
            db.update_raw_file_status(
                asset_id, "completed",
                archive_path=str(archive_path),
                raw_bucket="vlm-raw",
            )
            archived_items.append({**item, "archive_path": str(archive_path)})
            return
        error_message = error or "archive_move_failed"
        db.update_raw_file_status(asset_id, "failed", error_message)
        if ingest_rejections is not None:
            source_path = str(item.get("source_path", "")).strip()
            rel_path = rel_path_by_source.get(source_path) or Path(source_path).name
            ingest_rejections.append(
                {
                    "source_path": source_path,
                    "rel_path": rel_path,
                    "media_type": str(item.get("media_type", "unknown")),
                    "stage": "archive",
                    "error_code": _error_code_from_message(error_message, "archive_move_failed"),
                    "error_message": error_message,
                    "retryable": False,
                }
            )

    if source_unit_type == "directory" and source_unit_name:
        base_unit_archive_dir = archive_root_dir / source_unit_name

        # 이전 방식 우선: 폴더 전체 성공 시 폴더 단위 이동
        if (
            not is_chunked_manifest
            and source_unit_path
            and source_unit_path.exists()
            and len(uploaded) >= source_unit_total_file_count
        ):
            final_unit_archive_dir = _resolve_unique_directory(base_unit_archive_dir)
            try:
                shutil.move(str(source_unit_path), str(final_unit_archive_dir))
                context.log.info(
                    "폴더 단위 아카이브 이동 완료: "
                    f"{source_unit_path} -> {final_unit_archive_dir}"
                )
                for item in uploaded:
                    source_path = str(item.get("source_path", "")).strip()
                    rel_path = rel_path_by_source.get(source_path) or Path(source_path).name
                    archived_path = final_unit_archive_dir / rel_path
                    if archived_path.exists():
                        _mark_archive_result(item, archived_path)
                    else:
                        _mark_archive_result(
                            item,
                            None,
                            "archive_missing_after_directory_move",
                        )
                return archived_items
            except (OSError, shutil.Error) as exc:
                context.log.warning(f"폴더 단위 이동 실패, 파일 단위 이동으로 전환: {exc}")

        # 부분 이동 또는 폴더 이동 실패 fallback: 성공 파일만 unit 폴더 하위로 이동
        if is_chunked_manifest:
            unit_archive_dir = base_unit_archive_dir
        else:
            unit_archive_dir = _resolve_unique_directory(base_unit_archive_dir)
        try:
            unit_archive_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, shutil.Error) as exc:
            context.log.warning(f"아카이브 디렉토리 준비 실패: {unit_archive_dir}: {exc}")
            for item in uploaded:
                _mark_archive_result(item, None, f"archive_dir_prepare_failed:{exc}")
            return archived_items
        for item in uploaded:
            source_path = str(item.get("source_path", "")).strip()
            src = Path(source_path)
            rel_path = rel_path_by_source.get(source_path) or src.name
            base_dest = unit_archive_dir / rel_path
            dest: Path | None = None
            try:
                dest = _resolve_unique_file(base_dest)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dest))
                _mark_archive_result(item, dest)
            except (OSError, shutil.Error) as exc:
                if dest is not None and dest.exists():
                    context.log.warning(
                        "아카이브 이동 예외 이후 목적지 파일 존재 확인으로 completed 처리: "
                        f"{dest} ({exc})"
                    )
                    _mark_archive_result(item, dest)
                    continue
                if _is_source_missing_error(exc):
                    existing = _find_existing_in_archive_unit_dirs(
                        archive_root_dir=archive_root_dir,
                        source_unit_name=source_unit_name,
                        rel_path=rel_path,
                    )
                    if existing is not None:
                        context.log.info(
                            "소스 파일은 없지만 archive 존재 확인으로 completed 처리: "
                            f"{existing}"
                        )
                        _mark_archive_result(item, existing)
                        continue
                    context.log.warning(
                        "소스 파일을 찾을 수 없고 archive에도 없음: "
                        f"{src} ({exc})"
                    )
                    _mark_archive_result(item, None, "archive_source_missing")
                    continue
                context.log.warning(f"아카이브 이동 실패 (업로드는 성공): {exc}")
                _mark_archive_result(item, None, f"archive_move_failed:{exc}")

        if source_unit_path:
            _cleanup_empty_tree(source_unit_path)
        return archived_items

    # file unit/legacy manifest: 기존 파일 단위 이동
    for item in uploaded:
        source_path = str(item.get("source_path", "")).strip()
        src = Path(source_path)
        base_dest = archive_root_dir / src.name
        dest: Path | None = None
        try:
            dest = _resolve_unique_file(base_dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))
            _mark_archive_result(item, dest)
        except (OSError, shutil.Error) as exc:
            if dest is not None and dest.exists():
                context.log.warning(
                    "아카이브 이동 예외 이후 목적지 파일 존재 확인으로 completed 처리: "
                    f"{dest} ({exc})"
                )
                _mark_archive_result(item, dest)
                continue
            if _is_source_missing_error(exc):
                existing = _find_existing_archive_candidate(base_dest)
                if existing is not None:
                    context.log.info(
                        "소스 파일은 없지만 archive 존재 확인으로 completed 처리: "
                        f"{existing}"
                    )
                    _mark_archive_result(item, existing)
                    continue
                context.log.warning(
                    "소스 파일을 찾을 수 없고 archive에도 없음: "
                    f"{src} ({exc})"
                )
                _mark_archive_result(item, None, "archive_source_missing")
                continue
            context.log.warning(f"아카이브 이동 실패 (업로드는 성공): {exc}")
            _mark_archive_result(item, None, f"archive_move_failed:{exc}")

    return archived_items


@asset(
    description="NAS 미디어 → 검증 → 정규화 → MinIO vlm-raw + DuckDB raw_files",
    group_name="ingest",
    deps=[AssetKey(["pipeline", "gcs_download_to_incoming"])],
)
def ingested_raw_files(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST asset — manifest 기반 미디어 파일 수집.

    sensor에서 run_config tags로 manifest_path를 전달받음.
    manifest가 없으면 DuckDB 상태 요약만 반환.
    """
    config = PipelineConfig()

    # manifest_path는 sensor의 tags에서 전달
    manifest_path = context.run.tags.get("manifest_path")

    if manifest_path and Path(manifest_path).exists():
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    else:
        # manifest 없이 수동 실행 시 — 상태 요약만 반환
        context.log.warning("manifest_path가 없거나 파일이 존재하지 않습니다. 상태 요약만 반환합니다.")
        with db.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM raw_files").fetchone()[0]
            completed = conn.execute(
                "SELECT COUNT(*) FROM raw_files WHERE ingest_status = 'completed'"
            ).fetchone()[0]
        return {"total": int(total), "success": int(completed), "failed": 0, "skipped": 0}

    ingest_rejections: list[dict] = []
    retry_candidates: list[dict] = []

    # register
    records = register_incoming(
        context,
        db,
        manifest,
        ingest_rejections=ingest_rejections,
    )

    # normalize + upload
    uploaded = normalize_and_archive(
        context,
        db,
        minio,
        records,
        config.archive_dir,
        ingest_rejections=ingest_rejections,
        retry_candidates=retry_candidates,
    )

    # archive 이동(이전 source unit 정책)
    archived = _archive_uploaded_assets(
        context=context,
        db=db,
        manifest=manifest,
        uploaded=uploaded,
        archive_dir=config.archive_dir,
        ingest_rejections=ingest_rejections,
    )

    retry_manifest_path = _build_retry_manifest(
        context=context,
        config=config,
        manifest=manifest,
        retry_candidates=retry_candidates,
    )
    failure_log_path = _write_ingest_failure_logs(
        context=context,
        config=config,
        manifest=manifest,
        ingest_rejections=ingest_rejections,
    )

    duplicate_targets = _collect_duplicate_asset_file_map(records)
    if duplicate_targets:
        duplicate_files_count = sum(
            1
            for rec in records
            if str((rec.get("record") or {}).get("error_message") or "").startswith("duplicate_of:")
        )
        updated_assets = db.mark_duplicate_skipped_assets(duplicate_targets)
        context.log.warning(
            "중복 스킵 이력 반영(자산별): "
            f"duplicate_assets={len(duplicate_targets)}, "
            f"duplicate_files={duplicate_files_count}, "
            f"updated_assets={updated_assets}"
        )

    # summary
    summary = ingest_summary(context, archived, records)
    uploaded_count = len(uploaded)
    archived_count = len(archived)
    archive_missing_count = max(0, uploaded_count - archived_count)
    summary.update(
        {
            "uploaded_count": uploaded_count,
            "archived_count": archived_count,
            "archive_missing_count": archive_missing_count,
            "ingest_rejection_count": len(ingest_rejections),
            "retry_candidate_count": len(retry_candidates),
            "retry_manifest_created": bool(retry_manifest_path),
        }
    )
    output_metadata = {
        "uploaded_count": uploaded_count,
        "archived_count": archived_count,
        "archive_missing_count": archive_missing_count,
        "ingest_rejection_count": len(ingest_rejections),
        "retry_candidate_count": len(retry_candidates),
        "retry_manifest_created": bool(retry_manifest_path),
    }
    if retry_manifest_path is not None:
        output_metadata["retry_manifest_path"] = str(retry_manifest_path)
    if failure_log_path is not None:
        output_metadata["failure_log_path"] = str(failure_log_path)
    context.add_output_metadata(output_metadata)

    # manifest를 processed로 이동
    manifest_file = Path(manifest_path)
    processed_dir = Path(config.manifest_dir) / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(manifest_file), str(processed_dir / manifest_file.name))
    except Exception as e:
        context.log.warning(f"manifest 이동 실패: {e}")

    return summary
