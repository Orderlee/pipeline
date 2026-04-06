"""INGEST archive 유틸리티 — archive 이동/경로 관련 함수들.

assets.py에서 분리된 archive 관련 헬퍼.
"""

from __future__ import annotations

import shutil
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from vlm_pipeline.lib.runtime_profile import RuntimeProfile, resolve_runtime_profile

if TYPE_CHECKING:
    from vlm_pipeline.resources.config import PipelineConfig
    from vlm_pipeline.resources.duckdb import DuckDBResource

# ── archive 경로 충돌 해결 상수 ──────────────────────────────────
ARCHIVE_MAX_SUFFIX_ATTEMPTS: int = 100
ARCHIVE_FIND_MAX_SUFFIX: int = 10
ARCHIVE_CLEANUP_MAX_DEPTH: int = 5
ARCHIVE_CLEANUP_MAX_PARENT_LEVELS: int = 8
TRUTHY_STRINGS: frozenset[str] = frozenset({"1", "true", "t", "yes", "y", "on"})
FALSY_STRINGS: frozenset[str] = frozenset({"0", "false", "f", "no", "n", "off", ""})


def resolve_archive_source_unit_name(source_unit_name: str) -> str:
    """archive 디렉토리용 source unit 이름을 정규화한다.

    GCP auto-bootstrap unit은 `gcp/<bucket>/...` 형태로 들어오지만,
    archive 저장은 `archive/<bucket>/...` 규칙을 사용한다.
    """
    raw = str(source_unit_name or "").strip().strip("/")
    if not raw:
        return ""

    parts = [part for part in PurePosixPath(raw).parts if part not in {"", ".", ".."}]
    if len(parts) >= 2 and parts[0].lower() == "gcp":
        parts = parts[1:]

    if not parts:
        return ""
    return str(PurePosixPath(*parts))


def resolve_unique_directory(target_dir: Path, max_attempts: int = ARCHIVE_MAX_SUFFIX_ATTEMPTS) -> Path:
    """경로 충돌 시 __2, __3 suffix를 붙여 유니크 디렉토리 경로 생성."""
    if not target_dir.exists():
        return target_dir
    for idx in range(2, max_attempts + 2):
        candidate = target_dir.with_name(f"{target_dir.name}__{idx}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Cannot find unique directory after {max_attempts} attempts: {target_dir}")


def resolve_unique_file(target_file: Path, max_attempts: int = ARCHIVE_MAX_SUFFIX_ATTEMPTS) -> Path:
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


def find_existing_archive_directory(base_dir: Path, max_suffix: int = ARCHIVE_FIND_MAX_SUFFIX) -> Path | None:
    """archive 대상 디렉토리(base + __N) 중 실제 존재 경로를 탐색."""
    if base_dir.exists() and base_dir.is_dir():
        return base_dir
    for idx in range(2, max_suffix + 1):
        candidate = base_dir.with_name(f"{base_dir.name}__{idx}")
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def find_existing_archive_candidate(base_target: Path, max_suffix: int = ARCHIVE_FIND_MAX_SUFFIX) -> Path | None:
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
    max_suffix: int = ARCHIVE_FIND_MAX_SUFFIX,
) -> Path | None:
    """archive unit 디렉토리에서 파일 존재 여부 탐색.

    NFS 환경에서 exists() 호출은 네트워크 I/O를 발생시키므로 max_suffix를 작게 유지.
    """
    rel = Path(rel_path)
    base_dir = archive_root_dir / source_unit_name
    base_target = base_dir / rel

    found = find_existing_archive_candidate(base_target, max_suffix=max_suffix)
    if found is not None:
        return found

    for idx in range(2, max_suffix + 1):
        candidate_dir = archive_root_dir / f"{source_unit_name}__{idx}"
        if not candidate_dir.exists():
            break
        found = find_existing_archive_candidate(candidate_dir / rel, max_suffix=max_suffix)
        if found is not None:
            return found
    return None


def is_source_missing_error(exc: Exception) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == 2:
        return True
    return "[Errno 2]" in str(exc)


def cleanup_empty_tree(root: Path, max_depth: int = ARCHIVE_CLEANUP_MAX_DEPTH) -> None:
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


def cleanup_empty_parent_chain(start: Path | None, *, stop_at: Path | None = None, max_levels: int = ARCHIVE_CLEANUP_MAX_PARENT_LEVELS) -> None:
    """비어 있는 부모 디렉토리를 상향식으로 정리한다.

    `stop_at` 디렉토리는 경계로 취급하고 삭제하지 않는다.
    """
    current = start
    boundary: Path | None = None
    if stop_at is not None:
        try:
            boundary = stop_at.resolve()
        except OSError:
            boundary = stop_at

    for _ in range(max_levels):
        if current is None:
            return
        try:
            resolved_current = current.resolve()
        except OSError:
            resolved_current = current

        if boundary is not None and resolved_current == boundary:
            return

        try:
            current.rmdir()
            current = current.parent
            continue
        except FileNotFoundError:
            current = current.parent
            continue
        except OSError:
            return


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


def manifest_source_under_gcp(manifest: dict, config: PipelineConfig | None = None) -> bool:
    """source_unit_path가 <incoming_dir>/gcp 하위인지 (GCS 다운로드 표준 경로)."""
    raw = str(manifest.get("source_unit_path", "")).strip()
    if not raw:
        return False
    if config is None:
        from vlm_pipeline.resources.config import PipelineConfig

        config = PipelineConfig()
    incoming = Path(str(config.incoming_dir).strip()).resolve()
    gcp_root = (incoming / "gcp").resolve()
    try:
        Path(raw).resolve().relative_to(gcp_root)
        return True
    except Exception:
        return False


def manifest_allows_auto_bootstrap_without_dispatch(
    manifest: dict,
    config: PipelineConfig | None = None,
    runtime_profile: RuntimeProfile | None = None,  # noqa: ARG001 - 향후 정책 확장용
) -> bool:
    """dispatch 트리거 JSON 없이 auto_bootstrap·ingest 허용: production/staging 모두 gcp 트리만."""
    return manifest_source_under_gcp(manifest, config)


def _staging_transfer_allows_archive(
    manifest: dict,
    transfer_tool: str,
    *,
    config: PipelineConfig | None,
) -> bool:
    """staging에서 archive+ingest 허용 transfer: dispatch·retry·또는 incoming/gcp auto_bootstrap."""
    if transfer_tool in {"dispatch_sensor", "ingest_retry_manifest"}:
        return True
    if transfer_tool == "auto_bootstrap_sensor" and manifest_source_under_gcp(manifest, config):
        return True
    return False


def should_archive_manifest(
    manifest: dict,
    *,
    config: PipelineConfig | None = None,
    runtime_profile: RuntimeProfile | None = None,
) -> bool:
    """manifest 정책에 따라 archive 이동 여부를 결정한다.

    staging: dispatch·ingest_retry 또는 **incoming/gcp** auto_bootstrap만 (트리거 JSON 없이 gcp만).

    production: dispatch 트리거 JSON이 없는 auto_bootstrap은 **incoming/gcp/** 경로만 허용.
    `incoming/tmp_data_2` 같은 직접 드롭 폴더는 dispatch JSON 없이 archive 이동하지 않는다.
    legacy manifest 호환을 위해 archive_requested가 없으면 transfer_tool 기준으로 fallback 판단한다.
    """
    profile = runtime_profile or resolve_runtime_profile()
    is_staging = profile.is_staging
    transfer_tool = str(manifest.get("transfer_tool", "")).strip().lower()

    def _production_auto_bootstrap_gate() -> bool:
        if transfer_tool != "auto_bootstrap_sensor":
            return True
        return manifest_allows_auto_bootstrap_without_dispatch(manifest, config)

    archive_requested = manifest.get("archive_requested")
    if archive_requested is not None:
        if isinstance(archive_requested, str):
            normalized = archive_requested.strip().lower()
            if normalized in TRUTHY_STRINGS:
                if is_staging and not _staging_transfer_allows_archive(manifest, transfer_tool, config=config):
                    return False
                if not is_staging and not _production_auto_bootstrap_gate():
                    return False
                return True
            if normalized in FALSY_STRINGS:
                return False
        if not bool(archive_requested):
            return False
        if is_staging and not _staging_transfer_allows_archive(manifest, transfer_tool, config=config):
            return False
        if not is_staging and not _production_auto_bootstrap_gate():
            return False
        return True

    if not is_staging:
        if not _production_auto_bootstrap_gate():
            return False
        return True

    return _staging_transfer_allows_archive(manifest, transfer_tool, config=config)


def prepare_manifest_for_archive_upload(
    context,
    manifest: dict,
    *,
    archive_dir: str,
) -> tuple[dict, Path | None, bool]:
    """source unit을 archive 기준 경로로 맞추고 manifest 경로를 재작성한다.

    dispatch/staging archive_requested manifest는 archive 경로에서 MinIO 업로드되게 만든다.
    """
    source_unit_type = str(manifest.get("source_unit_type", "")).strip().lower()
    source_unit_name = str(manifest.get("source_unit_name", "")).strip()
    archive_unit_name = resolve_archive_source_unit_name(source_unit_name) or source_unit_name
    source_unit_path_raw = str(manifest.get("source_unit_path", "")).strip()
    source_dir_raw = str(manifest.get("source_dir", "")).strip()
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
        return manifest, Path(archive_dir) / archive_unit_name, False

    source_unit_path = Path(source_unit_path_raw)
    archive_root_dir = Path(archive_dir)
    archive_root_dir.mkdir(parents=True, exist_ok=True)
    base_archive_unit_dir = archive_root_dir / archive_unit_name

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
    cleanup_empty_parent_chain(
        source_unit_path.parent,
        stop_at=Path(source_dir_raw) if source_dir_raw else None,
    )
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
                              archive_root_dir, archive_unit_name, _mark_archive_result)

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
