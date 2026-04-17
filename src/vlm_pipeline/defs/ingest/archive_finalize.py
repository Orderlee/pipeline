"""archive 이동 실행 + completion handler.

업로드 완료 파일을 source unit 기반으로 archive 경로로 이동하거나, archive 없이
completed 상태만 확정한다. NAS hang / partial success 복구 로직이 포함되어 있어
운영 중 장애 대응에 중요하다.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from .archive_cleanup import (
    cleanup_empty_parent_chain,
    cleanup_empty_tree,
    cleanup_residual_source_file,
)
from .archive_move import (
    _move_with_timeout,
    find_existing_archive_candidate,
    find_existing_archive_directory,
    find_existing_in_archive_unit_dirs,
    is_source_missing_error,
    resolve_archive_source_unit_name,
    resolve_unique_file,
)

if TYPE_CHECKING:
    from vlm_pipeline.resources.duckdb import DuckDBResource


def complete_uploaded_assets_without_archive(
    context,
    db: "DuckDBResource",
    manifest: dict,
    uploaded: list[dict],
) -> list[dict]:
    """archive 이동 없이 업로드 완료 상태만 확정한다."""
    completed_items: list[dict] = []
    manifest_id = str(manifest.get("manifest_id", "")).strip() or "<unknown_manifest>"

    for item in uploaded:
        asset_id = str(item.get("asset_id", "")).strip()
        if not asset_id:
            continue
        db.update_raw_file_status(
            asset_id,
            "completed",
            archive_path=None,
            raw_bucket="vlm-raw",
        )
        completed_items.append(dict(item))

    context.log.info(
        "archive 이동 건너뜀: "
        f"manifest_id={manifest_id}, "
        f"transfer_tool={manifest.get('transfer_tool')}, "
        f"uploaded={len(uploaded)}, completed={len(completed_items)}"
    )
    return completed_items


def complete_uploaded_assets_in_archive(
    context,
    db: "DuckDBResource",
    manifest: dict,
    uploaded: list[dict],
) -> list[dict]:
    """이미 archive로 선이동된 소스를 기준으로 업로드 완료 상태를 확정한다."""
    completed_items: list[dict] = []
    manifest_id = str(manifest.get("manifest_id", "")).strip() or "<unknown_manifest>"

    for item in uploaded:
        asset_id = str(item.get("asset_id", "")).strip()
        archive_path = str(item.get("source_path", "")).strip() or None
        if not asset_id:
            continue
        db.update_raw_file_status(
            asset_id,
            "completed",
            archive_path=archive_path,
            raw_bucket="vlm-raw",
        )
        completed_items.append({**item, "archive_path": archive_path})

    context.log.info(
        "archive 선이동 업로드 완료 확정: "
        f"manifest_id={manifest_id}, "
        f"transfer_tool={manifest.get('transfer_tool')}, "
        f"uploaded={len(uploaded)}, completed={len(completed_items)}"
    )
    return completed_items


def archive_uploaded_assets(
    context,
    db: "DuckDBResource",
    manifest: dict,
    uploaded: list[dict],
    archive_dir: str,
    ingest_rejections: list[dict] | None = None,
) -> tuple[list[dict], Path | None]:
    """업로드 완료 파일을 source unit 기반으로 archive 이동."""
    from .duplicate import error_code_from_message

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
    archive_unit_name = resolve_archive_source_unit_name(source_unit_name) or source_unit_name
    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    source_dir_raw = str(manifest.get("source_dir", "")).strip()
    source_unit_path = Path(source_unit_path_raw) if source_unit_path_raw else None
    source_root_dir = Path(source_dir_raw) if source_dir_raw else None
    manifest_file_count = int(manifest.get("file_count") or len(files) or len(uploaded))
    source_unit_total_file_count = int(
        manifest.get("source_unit_total_file_count") or manifest_file_count
    )
    is_chunked_manifest = (
        int(manifest.get("source_unit_chunk_count") or 1) > 1
        or source_unit_total_file_count > manifest_file_count
    )

    archived_items: list[dict] = []
    archive_unit_dir_hint: Path | None = None

    if source_unit_type == "directory" and source_unit_name:
        archive_unit_dir_hint = archive_root_dir / archive_unit_name

    if not uploaded:
        return archived_items, archive_unit_dir_hint

    def _mark_archive_result(item: dict, archive_path: Path | None, error: str | None = None) -> None:
        asset_id = str(item.get("asset_id", "")).strip()
        if not asset_id:
            return
        if archive_path is not None:
            sp = str(item.get("source_path", "")).strip()
            if sp:
                cleanup_residual_source_file(context, sp)
                cleanup_empty_parent_chain(Path(sp).parent, stop_at=source_root_dir)
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
            sp = str(item.get("source_path", "")).strip()
            rel_path = rel_path_by_source.get(sp) or Path(sp).name
            ingest_rejections.append(
                {
                    "source_path": sp,
                    "rel_path": rel_path,
                    "media_type": str(item.get("media_type", "unknown")),
                    "stage": "archive",
                    "error_code": error_code_from_message(error_message, "archive_move_failed"),
                    "error_message": error_message,
                    "retryable": False,
                }
            )

    if source_unit_type == "directory" and source_unit_name:
        base_unit_archive_dir = archive_root_dir / archive_unit_name

        existing_archive = find_existing_archive_directory(base_unit_archive_dir)

        if (
            not is_chunked_manifest
            and source_unit_path
            and source_unit_path.exists()
            and len(uploaded) >= source_unit_total_file_count
            and existing_archive is None
        ):
            archive_unit_dir_hint = base_unit_archive_dir
            try:
                _move_with_timeout(str(source_unit_path), str(base_unit_archive_dir))
                context.log.info(
                    f"폴더 단위 아카이브 이동 완료: {source_unit_path} -> {base_unit_archive_dir}"
                )
                for item in uploaded:
                    sp = str(item.get("source_path", "")).strip()
                    rel_path = rel_path_by_source.get(sp) or Path(sp).name
                    archived_path = base_unit_archive_dir / rel_path
                    if archived_path.exists():
                        _mark_archive_result(item, archived_path)
                    else:
                        _mark_archive_result(item, None, "archive_missing_after_directory_move")
                return archived_items, archive_unit_dir_hint
            except (OSError, shutil.Error) as exc:
                context.log.warning(f"폴더 단위 이동 실패, 파일 단위 이동으로 전환: {exc}")

        if is_chunked_manifest:
            unit_archive_dir = base_unit_archive_dir
        else:
            unit_archive_dir = existing_archive if existing_archive is not None else base_unit_archive_dir
        archive_unit_dir_hint = unit_archive_dir
        try:
            unit_archive_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, shutil.Error) as exc:
            context.log.warning(f"아카이브 디렉토리 준비 실패: {unit_archive_dir}: {exc}")
            for item in uploaded:
                _mark_archive_result(item, None, f"archive_dir_prepare_failed:{exc}")
            return archived_items, archive_unit_dir_hint

        total = len(uploaded)
        for idx, item in enumerate(uploaded, 1):
            _move_single_file(context, item, unit_archive_dir, rel_path_by_source,
                              archive_root_dir, archive_unit_name, _mark_archive_result)
            if idx == 1 or idx == total or idx % 10 == 0:
                context.log.info(f"archive progress={idx}/{total} success={len(archived_items)}")

        if source_unit_path:
            cleanup_empty_tree(source_unit_path)
        return archived_items, archive_unit_dir_hint

    total = len(uploaded)
    for idx, item in enumerate(uploaded, 1):
        _move_single_file_fallback(context, item, archive_root_dir, _mark_archive_result)
        if idx == 1 or idx == total or idx % 10 == 0:
            context.log.info(f"archive progress={idx}/{total} success={len(archived_items)}")

    return archived_items, archive_unit_dir_hint


def _move_single_file(
    context,
    item: dict,
    unit_archive_dir: Path,
    rel_path_by_source: dict[str, str],
    archive_root_dir: Path,
    archive_unit_name: str,
    mark_fn,
) -> None:
    source_path = str(item.get("source_path", "")).strip()
    src = Path(source_path)
    rel_path = rel_path_by_source.get(source_path) or src.name
    base_dest = unit_archive_dir / rel_path
    dest: Path | None = None
    try:
        dest = resolve_unique_file(base_dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        _move_with_timeout(str(src), str(dest))
        mark_fn(item, dest)
    except (OSError, shutil.Error) as exc:
        if dest is not None and dest.exists():
            context.log.warning(
                f"아카이브 이동 예외 이후 목적지 파일 존재 확인으로 completed 처리: {dest} ({exc})"
            )
            mark_fn(item, dest)
            return
        if is_source_missing_error(exc):
            existing = find_existing_in_archive_unit_dirs(
                archive_root_dir=archive_root_dir,
                source_unit_name=archive_unit_name,
                rel_path=rel_path,
            )
            if existing is not None:
                context.log.info(f"소스 파일은 없지만 archive 존재 확인으로 completed 처리: {existing}")
                mark_fn(item, existing)
                return
            context.log.warning(f"소스 파일을 찾을 수 없고 archive에도 없음: {src} ({exc})")
            mark_fn(item, None, "archive_source_missing")
            return
        context.log.warning(f"아카이브 이동 실패 (업로드는 성공): {exc}")
        mark_fn(item, None, f"archive_move_failed:{exc}")


def _move_single_file_fallback(context, item: dict, archive_root_dir: Path, mark_fn) -> None:
    source_path = str(item.get("source_path", "")).strip()
    src = Path(source_path)
    base_dest = archive_root_dir / src.name
    dest: Path | None = None
    try:
        dest = resolve_unique_file(base_dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        _move_with_timeout(str(src), str(dest))
        mark_fn(item, dest)
    except (OSError, shutil.Error) as exc:
        if dest is not None and dest.exists():
            context.log.warning(
                f"아카이브 이동 예외 이후 목적지 파일 존재 확인으로 completed 처리: {dest} ({exc})"
            )
            mark_fn(item, dest)
            return
        if is_source_missing_error(exc):
            existing = find_existing_archive_candidate(base_dest)
            if existing is not None:
                context.log.info(f"소스 파일은 없지만 archive 존재 확인으로 completed 처리: {existing}")
                mark_fn(item, existing)
                return
            context.log.warning(f"소스 파일을 찾을 수 없고 archive에도 없음: {src} ({exc})")
            mark_fn(item, None, "archive_source_missing")
            return
        context.log.warning(f"아카이브 이동 실패 (업로드는 성공): {exc}")
        mark_fn(item, None, f"archive_move_failed:{exc}")
