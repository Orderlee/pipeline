"""INGEST archive 유틸리티 — archive 이동/경로 관련 함수들.

assets.py에서 분리된 archive 관련 헬퍼.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from vlm_pipeline.lib.env_utils import IS_STAGING

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig
    from vlm_pipeline.resources.duckdb import DuckDBResource


def resolve_unique_directory(target_dir: Path, max_attempts: int = 100) -> Path:
    """경로 충돌 시 __2, __3 suffix를 붙여 유니크 디렉토리 경로 생성."""
    if not target_dir.exists():
        return target_dir
    for idx in range(2, max_attempts + 2):
        candidate = target_dir.with_name(f"{target_dir.name}__{idx}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Cannot find unique directory after {max_attempts} attempts: {target_dir}")


def resolve_unique_file(target_file: Path, max_attempts: int = 100) -> Path:
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


def find_existing_archive_directory(base_dir: Path, max_suffix: int = 10) -> Path | None:
    """archive 대상 디렉토리(base + __N) 중 실제 존재 경로를 탐색."""
    if base_dir.exists() and base_dir.is_dir():
        return base_dir
    for idx in range(2, max_suffix + 1):
        candidate = base_dir.with_name(f"{base_dir.name}__{idx}")
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def find_existing_archive_candidate(base_target: Path, max_suffix: int = 10) -> Path | None:
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


def find_existing_in_archive_unit_dirs(
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

    found = find_existing_archive_candidate(base_target, max_suffix=10)
    if found is not None:
        return found

    for idx in range(2, max_suffix + 1):
        candidate_dir = archive_root_dir / f"{source_unit_name}__{idx}"
        if not candidate_dir.exists():
            break
        found = find_existing_archive_candidate(candidate_dir / rel, max_suffix=10)
        if found is not None:
            return found
    return None


def is_source_missing_error(exc: Exception) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == 2:
        return True
    return "[Errno 2]" in str(exc)


def cleanup_empty_tree(root: Path, max_depth: int = 5) -> None:
    """하위가 비어 있으면 상향식으로 빈 디렉토리를 정리.

    NFS 환경에서 os.walk는 느리므로 max_depth를 제한한다.
    """
    if not root.exists() or not root.is_dir():
        return

    def _cleanup_recursive(current: Path, depth: int) -> bool:
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


def cleanup_residual_source_file(context, source_path: str) -> None:
    """archive 완료 후 source 파일이 남아있으면 정리한다."""
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


def stage_source_unit_in_archive_pending(
    context,
    *,
    source_unit_type: str,
    source_unit_path: str,
    source_unit_name: str,
    archive_pending_dir: str,
) -> tuple[Path, str]:
    """source unit을 archive_pending으로 옮기고, 최종 경로/이름을 반환한다."""
    source_path = Path(str(source_unit_path).strip())
    archive_pending_root = Path(str(archive_pending_dir).strip())
    archive_pending_root.mkdir(parents=True, exist_ok=True)

    normalized_unit_name = str(source_unit_name or source_path.name).strip() or source_path.name
    base_dest = archive_pending_root / normalized_unit_name

    try:
        resolved_source = source_path.resolve()
        resolved_pending_root = archive_pending_root.resolve()
        resolved_source.relative_to(resolved_pending_root)
        final_path = source_path
    except Exception:
        if not source_path.exists():
            existing = (
                find_existing_archive_directory(base_dest)
                if str(source_unit_type).strip().lower() == "directory"
                else find_existing_archive_candidate(base_dest)
            )
            if existing is None:
                raise FileNotFoundError(source_path)
            final_path = existing
        else:
            if str(source_unit_type).strip().lower() == "directory":
                final_path = resolve_unique_directory(base_dest)
            else:
                final_path = resolve_unique_file(base_dest)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.rename(str(source_path), str(final_path))
            except OSError:
                shutil.move(str(source_path), str(final_path))
            context.log.info(
                f"staging auto_bootstrap 선이동: {source_path} -> {final_path}"
            )

    try:
        final_unit_name = str(final_path.resolve().relative_to(archive_pending_root.resolve()))
    except Exception:
        final_unit_name = final_path.name
    return final_path, final_unit_name


def should_archive_manifest(manifest: dict, *, config: PipelineConfig | None = None) -> bool:
    """manifest 정책에 따라 archive 이동 여부를 결정한다.

    staging에서는 dispatch JSON 기반 요청(및 그 재시도 manifest)만
    archive -> MinIO 업로드 흐름을 허용한다.
    legacy manifest 호환을 위해 archive_requested가 없으면 source_unit_path가
    archive_pending 하위인지와 transfer_tool을 함께 fallback 판단한다.
    """
    transfer_tool = str(manifest.get("transfer_tool", "")).strip().lower()
    staging_archive_allowed_tools = {"dispatch_sensor", "ingest_retry_manifest"}

    archive_requested = manifest.get("archive_requested")
    if archive_requested is not None:
        if isinstance(archive_requested, str):
            normalized = archive_requested.strip().lower()
            if normalized in {"1", "true", "t", "yes", "y", "on"}:
                if IS_STAGING and transfer_tool not in staging_archive_allowed_tools:
                    return False
                return True
            if normalized in {"0", "false", "f", "no", "n", "off", ""}:
                return False
        if not bool(archive_requested):
            return False
        if IS_STAGING and transfer_tool not in staging_archive_allowed_tools:
            return False
        return True

    if not IS_STAGING:
        return True

    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    if source_unit_path_raw:
        try:
            if config is not None:
                archive_pending_dir_raw = str(config.archive_pending_dir).strip()
            else:
                archive_pending_dir_raw = str(os.getenv("ARCHIVE_PENDING_DIR", "")).strip()
            if not archive_pending_dir_raw:
                from vlm_pipeline.resources.config import PipelineConfig

                config = PipelineConfig()
                archive_pending_dir_raw = str(config.archive_pending_dir).strip()
            resolved_source = Path(source_unit_path_raw).resolve()
            resolved_archive_pending = Path(archive_pending_dir_raw).resolve()
            resolved_source.relative_to(resolved_archive_pending)
            return transfer_tool in staging_archive_allowed_tools
        except Exception:
            pass

    return transfer_tool == "dispatch_sensor"


def prepare_manifest_for_archive_upload(
    context,
    manifest: dict,
    *,
    archive_dir: str,
) -> tuple[dict, Path | None, bool]:
    """archive_pending source unit을 먼저 archive로 옮기고 manifest 경로를 재작성한다.

    dispatch/staging archive_requested manifest는 archive 경로에서 MinIO 업로드되게 만든다.
    """
    source_unit_type = str(manifest.get("source_unit_type", "")).strip().lower()
    source_unit_name = str(manifest.get("source_unit_name", "")).strip()
    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    if source_unit_type != "directory" or not source_unit_name or not source_unit_path_raw:
        return manifest, None, False

    manifest_file_count = int(manifest.get("file_count") or len(manifest.get("files", [])) or 0)
    source_unit_total_file_count = int(
        manifest.get("source_unit_total_file_count") or manifest_file_count
    )
    is_chunked_manifest = (
        int(manifest.get("source_unit_chunk_count") or 1) > 1
        or source_unit_total_file_count > manifest_file_count
    )
    if is_chunked_manifest:
        return manifest, Path(archive_dir) / source_unit_name, False

    source_unit_path = Path(source_unit_path_raw)
    archive_root_dir = Path(archive_dir)
    archive_root_dir.mkdir(parents=True, exist_ok=True)
    base_archive_unit_dir = archive_root_dir / source_unit_name

    def _rewrite_manifest_paths(archive_unit_dir: Path) -> dict:
        updated_manifest = dict(manifest)
        updated_manifest["source_dir"] = str(archive_root_dir)
        updated_manifest["source_unit_path"] = str(archive_unit_dir)
        updated_manifest["archive_prepared_for_upload"] = True

        updated_files: list[dict] = []
        for entry in manifest.get("files", []):
            if not isinstance(entry, dict):
                continue
            updated_entry = dict(entry)
            source_path = str(entry.get("path", "")).strip()
            rel_path = str(entry.get("rel_path", "")).strip()
            if not rel_path:
                try:
                    rel_path = str(Path(source_path).relative_to(source_unit_path))
                except Exception:
                    rel_path = Path(source_path).name
            updated_entry["path"] = str(archive_unit_dir / rel_path)
            updated_entry["rel_path"] = rel_path
            updated_files.append(updated_entry)
        updated_manifest["files"] = updated_files
        return updated_manifest

    try:
        resolved_source = source_unit_path.resolve()
        resolved_archive_root = archive_root_dir.resolve()
        resolved_source.relative_to(resolved_archive_root)
        return _rewrite_manifest_paths(source_unit_path), source_unit_path, True
    except Exception:
        pass

    if not source_unit_path.exists():
        existing_archive_dir = find_existing_archive_directory(base_archive_unit_dir)
        if existing_archive_dir is not None:
            return _rewrite_manifest_paths(existing_archive_dir), existing_archive_dir, True
        return manifest, base_archive_unit_dir, False

    final_archive_unit_dir = resolve_unique_directory(base_archive_unit_dir)
    shutil.move(str(source_unit_path), str(final_archive_unit_dir))
    context.log.info(
        f"archive 선이동 완료(업로드는 archive 기준): {source_unit_path} -> {final_archive_unit_dir}"
    )
    return _rewrite_manifest_paths(final_archive_unit_dir), final_archive_unit_dir, True


def complete_uploaded_assets_without_archive(
    context,
    db: DuckDBResource,
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
    db: DuckDBResource,
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
    db: DuckDBResource,
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
    archive_unit_dir_hint: Path | None = None

    if source_unit_type == "directory" and source_unit_name:
        archive_unit_dir_hint = archive_root_dir / source_unit_name

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
        base_unit_archive_dir = archive_root_dir / source_unit_name

        if (
            not is_chunked_manifest
            and source_unit_path
            and source_unit_path.exists()
            and len(uploaded) >= source_unit_total_file_count
        ):
            final_unit_archive_dir = resolve_unique_directory(base_unit_archive_dir)
            archive_unit_dir_hint = final_unit_archive_dir
            try:
                shutil.move(str(source_unit_path), str(final_unit_archive_dir))
                context.log.info(
                    f"폴더 단위 아카이브 이동 완료: {source_unit_path} -> {final_unit_archive_dir}"
                )
                for item in uploaded:
                    sp = str(item.get("source_path", "")).strip()
                    rel_path = rel_path_by_source.get(sp) or Path(sp).name
                    archived_path = final_unit_archive_dir / rel_path
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
            unit_archive_dir = resolve_unique_directory(base_unit_archive_dir)
        archive_unit_dir_hint = unit_archive_dir
        try:
            unit_archive_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, shutil.Error) as exc:
            context.log.warning(f"아카이브 디렉토리 준비 실패: {unit_archive_dir}: {exc}")
            for item in uploaded:
                _mark_archive_result(item, None, f"archive_dir_prepare_failed:{exc}")
            return archived_items, archive_unit_dir_hint

        for item in uploaded:
            _move_single_file(context, item, unit_archive_dir, rel_path_by_source,
                              archive_root_dir, source_unit_name, _mark_archive_result)

        if source_unit_path:
            cleanup_empty_tree(source_unit_path)
        return archived_items, archive_unit_dir_hint

    for item in uploaded:
        _move_single_file_legacy(context, item, archive_root_dir, _mark_archive_result)

    return archived_items, archive_unit_dir_hint


def _move_single_file(
    context,
    item: dict,
    unit_archive_dir: Path,
    rel_path_by_source: dict[str, str],
    archive_root_dir: Path,
    source_unit_name: str,
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
        shutil.move(str(src), str(dest))
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
                source_unit_name=source_unit_name,
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


def _move_single_file_legacy(context, item: dict, archive_root_dir: Path, mark_fn) -> None:
    source_path = str(item.get("source_path", "")).strip()
    src = Path(source_path)
    base_dest = archive_root_dir / src.name
    dest: Path | None = None
    try:
        dest = resolve_unique_file(base_dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
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
