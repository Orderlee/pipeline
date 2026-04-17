"""archive pre-move + manifest rewriting.

archive 이전 단계에서 source unit을 archive 경로로 이동하고 manifest의 path·rel_path를
재작성한다. 이 로직은 chunked manifest / 충돌 경로 / 선이동 병합 처리를 포함하므로
변경 시 운영 안전성에 영향.
"""

from __future__ import annotations

from pathlib import Path

from .archive_cleanup import cleanup_empty_parent_chain, cleanup_empty_tree
from .archive_move import (
    _move_with_timeout,
    find_existing_archive_directory,
    resolve_archive_source_unit_name,
    resolve_unique_file,
)


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

    existing_archive_dir = find_existing_archive_directory(base_archive_unit_dir)
    if existing_archive_dir is not None:
        for child in source_unit_path.rglob("*"):
            if not child.is_file():
                continue
            rel = child.relative_to(source_unit_path)
            dest = existing_archive_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest = resolve_unique_file(dest)
            _move_with_timeout(str(child), str(dest))
        cleanup_empty_tree(source_unit_path)
        cleanup_empty_parent_chain(
            source_unit_path.parent,
            stop_at=Path(source_dir_raw) if source_dir_raw else None,
        )
        context.log.info(
            f"archive 선이동 완료(기존 디렉토리 병합): {source_unit_path} -> {existing_archive_dir}"
        )
        return _rewrite_manifest_paths(existing_archive_dir), existing_archive_dir, True

    final_archive_unit_dir = base_archive_unit_dir
    _move_with_timeout(str(source_unit_path), str(final_archive_unit_dir))
    cleanup_empty_parent_chain(
        source_unit_path.parent,
        stop_at=Path(source_dir_raw) if source_dir_raw else None,
    )
    context.log.info(
        f"archive 선이동 완료(업로드는 archive 기준): {source_unit_path} -> {final_archive_unit_dir}"
    )
    return _rewrite_manifest_paths(final_archive_unit_dir), final_archive_unit_dir, True
