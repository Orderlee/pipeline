"""auto_bootstrap sensor 공통 유틸 — dir 순회, dispatch 트리거, done marker, gcp 판정, chunking."""

from __future__ import annotations

import json
from pathlib import Path


def _iter_sorted_dir_entries(path: Path) -> list[Path]:
    try:
        entries = list(path.iterdir())
    except OSError:
        return []
    return sorted(entries, key=lambda row: row.name)


def _load_dispatch_requested_folders(dispatch_pending_dir: Path) -> set[str]:
    requested_folders: set[str] = set()
    if not dispatch_pending_dir.exists():
        return requested_folders

    for request_path in sorted(dispatch_pending_dir.glob("*.json")):
        try:
            payload = json.loads(request_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        folder_name = str(payload.get("folder_name", "")).strip()
        if folder_name:
            requested_folders.add(folder_name)
    return requested_folders


def _has_allowed_direct_file(dir_path: Path, allowed_exts: set[str]) -> bool:
    for entry in _iter_sorted_dir_entries(dir_path):
        if entry.name.startswith("."):
            continue
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        if entry.suffix.lower() in allowed_exts:
            return True
    return False


def _source_unit_sort_key(unit: dict) -> tuple[int, str]:
    unit_type = str(unit.get("unit_type", ""))
    scan_recursive = bool(unit.get("scan_recursive", True))
    if unit_type == "file":
        priority = 0
    elif not scan_recursive:
        priority = 1
    else:
        priority = 2
    return priority, str(unit.get("unit_key", ""))


def _has_done_marker(unit_path: str, marker_name: str) -> bool:
    return (Path(unit_path) / marker_name).exists()


def _is_gcp_unit_path(unit_path: str, incoming_dir: Path) -> bool:
    gcp_root = (incoming_dir / "gcp").resolve()
    try:
        Path(unit_path).resolve().relative_to(gcp_root)
        return True
    except Exception:
        return False


def _chunk_files(unit_files: list[dict], max_files_per_manifest: int) -> list[list[dict]]:
    if max_files_per_manifest <= 0:
        max_files_per_manifest = 1
    if not unit_files:
        return []
    return [
        unit_files[index:index + max_files_per_manifest]
        for index in range(0, len(unit_files), max_files_per_manifest)
    ]
