#!/usr/bin/env python3
"""
Watchdog FileSystemEventHandler for NAS folder tree incremental updates.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional


try:
    from watchdog.events import FileSystemEvent, FileSystemEventHandler
except Exception:  # pragma: no cover - runtime import guard
    print("Missing dependency: watchdog. Install with `pip install watchdog`.")
    raise

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from nas_folder_tree_db_helpers import (  # noqa: E402
    _clear_root_rows,
    _find_root,
    _iter_ancestors,
    _normalize,
    _rel_depth,
    _scan_root,
    _sort_key,
    _upsert_extension_row,
    _upsert_folder_row,
    _upsert_summary,
    _write_root_state,
)


class NasEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        conn,
        roots: List[str],
        max_depth: int,
        calculate_size: bool,
        recompute_tree_prefix: bool,
        dir_delete_delta: bool,
        fallback_rescan_on_dir_delete: bool,
        state_lock: Lock,
        state_by_root: Dict[str, Dict],
    ):
        self.conn = conn
        self.roots = roots
        self.max_depth = max_depth
        self.calculate_size = calculate_size
        self.recompute_tree_prefix = recompute_tree_prefix
        self.dir_delete_delta = dir_delete_delta
        self.fallback_rescan_on_dir_delete = fallback_rescan_on_dir_delete
        self.state_lock = state_lock
        self.state_by_root = state_by_root

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle_event(event, created=True)

    def on_deleted(self, event: FileSystemEvent) -> None:
        self._handle_event(event, deleted=True)

    def on_moved(self, event: FileSystemEvent) -> None:
        # Treat as delete + create
        self._handle_event(event, deleted=True, path_override=event.src_path)
        self._handle_event(event, created=True, path_override=event.dest_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._handle_event(event, modified=True)

    def _handle_event(
        self,
        event: FileSystemEvent,
        created: bool = False,
        deleted: bool = False,
        modified: bool = False,
        path_override: Optional[str] = None,
    ) -> None:
        with self.state_lock:
            path = _normalize(path_override or event.src_path)
            root = _find_root(path, self.roots)
            if not root:
                return

            depth = _rel_depth(path, root)
            if depth > self.max_depth and event.is_directory:
                return
            if depth > self.max_depth and not event.is_directory:
                # For files, compare depth of parent
                parent_depth = _rel_depth(os.path.dirname(path), root)
                if parent_depth > self.max_depth:
                    return

            if event.is_directory:
                if deleted:
                    if self.dir_delete_delta:
                        success = self._handle_dir_delete_delta(root, path)
                        if not success and self.fallback_rescan_on_dir_delete:
                            self._rescan_root(root)
                    else:
                        self._rescan_root(root)
                    return
                if created:
                    self._handle_dir_create(root, path)
                    return
                return

            if deleted:
                self._handle_file_delete(root, path)
            elif created:
                self._handle_file_create(root, path)
            elif modified:
                self._handle_file_modify(root, path)

    def _rescan_root(self, root: str) -> None:
        folders, ext_stats, files, summary = _scan_root(
            root, self.max_depth, self.calculate_size, self.recompute_tree_prefix
        )
        self.state_by_root[root] = {
            "folders": folders,
            "ext_stats": ext_stats,
            "files": files,
            "summary": summary,
        }
        scan_time = datetime.now()
        _write_root_state(self.conn, scan_time, root, folders, ext_stats, summary)

    def _handle_dir_delete_delta(self, root: str, path: str) -> bool:
        state = self.state_by_root.get(root)
        if not state:
            print(f"[WARN] No state for root {root}; cannot apply dir-delete delta.")
            return False

        if path == root:
            _clear_root_rows(self.conn, root)
            self.state_by_root.pop(root, None)
            print(f"[WARN] Root path removed: {root}")
            return True

        folders = state["folders"]
        files = state["files"]
        ext_stats = state["ext_stats"]
        summary = state["summary"]

        if path not in folders:
            print(f"[WARN] Deleted dir not in state: {path}")
            return False

        subtree_prefix = path + os.sep
        subtree_folders = [p for p in folders if p == path or p.startswith(subtree_prefix)]
        subtree_files = [p for p in files if p.startswith(subtree_prefix)]

        if not subtree_folders and not subtree_files:
            return True

        # Aggregate totals from the subtree root folder.
        subtree_root = folders.get(path)
        removed_files = subtree_root["total_file_count"] if subtree_root else len(subtree_files)
        removed_size = (
            subtree_root["total_size_bytes"] if subtree_root else sum(files[p]["size_bytes"] for p in subtree_files)
        )

        # Remove file entries and extension stats for folders in subtree.
        for file_path in subtree_files:
            entry = files.pop(file_path, None)
            if not entry:
                continue
            parent = entry["parent_path"]
            ext = entry["extension"]
            size = entry["size_bytes"]
            key = (parent, ext)
            if key in ext_stats:
                ext_stats[key]["count"] = max(0, ext_stats[key]["count"] - 1)
                ext_stats[key]["size"] = max(0, ext_stats[key]["size"] - size)

        # Drop extension stats for folders that are deleted.
        for key in list(ext_stats.keys()):
            folder_path, _ext = key
            if folder_path == path or folder_path.startswith(subtree_prefix):
                ext_stats.pop(key, None)

        # Remove folders in subtree from in-memory state.
        for folder_path in subtree_folders:
            folders.pop(folder_path, None)

        # Update parent + ancestors.
        parent = os.path.dirname(path)
        scan_time = datetime.now()
        if parent in folders:
            folders[parent]["folder_count"] = max(0, folders[parent]["folder_count"] - 1)

        for folder_path in _iter_ancestors(parent, root):
            folder = folders.get(folder_path)
            if not folder:
                continue
            folder["total_file_count"] = max(0, folder["total_file_count"] - removed_files)
            folder["total_size_bytes"] = max(0, folder["total_size_bytes"] - removed_size)
            _upsert_folder_row(self.conn, scan_time, folder)

        # Update summary from root entry.
        root_entry = folders.get(root)
        summary["total_folders"] = len(folders)
        summary["total_files"] = root_entry["total_file_count"] if root_entry else 0
        summary["total_size"] = root_entry["total_size_bytes"] if root_entry else 0
        summary["max_depth"] = max((f["depth"] for f in folders.values()), default=0)
        summary["unique_extensions"] = len({ext for (_, ext) in ext_stats.keys()})
        _upsert_summary(self.conn, scan_time, root, summary)

        # Remove DB rows for subtree.
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM nas_folder_tree WHERE path LIKE %s", (f"{path}%",))
            cur.execute("DELETE FROM nas_folder_extensions WHERE folder_path LIKE %s", (f"{path}%",))
        self.conn.commit()

        return True

    def _handle_dir_create(self, root: str, path: str) -> None:
        state = self.state_by_root.get(root)
        if not state:
            return
        folders = state["folders"]

        if path in folders:
            return

        parent = os.path.dirname(path)
        depth = _rel_depth(path, root)
        if depth > self.max_depth:
            return

        folders[path] = {
            "path": path,
            "parent_path": parent if path != root else None,
            "name": os.path.basename(path) or path,
            "depth": depth,
            "node_type": "folder",
            "size_bytes": 0,
            "file_count": 0,
            "folder_count": 0,
            "total_size_bytes": 0,
            "total_file_count": 0,
            "tree_prefix": "",
            "sort_key": _sort_key(path),
        }

        if parent in folders:
            folders[parent]["folder_count"] += 1
            scan_time = datetime.now()
            _upsert_folder_row(self.conn, scan_time, folders[parent])
            _upsert_folder_row(self.conn, scan_time, folders[path])

            root_entry = folders.get(root)
            if root_entry:
                state["summary"]["total_folders"] = len(folders)
                _upsert_summary(self.conn, scan_time, root, state["summary"])

    def _handle_file_create(self, root: str, path: str) -> None:
        state = self.state_by_root.get(root)
        if not state:
            return
        files = state["files"]
        folders = state["folders"]
        ext_stats = state["ext_stats"]

        if path in files:
            return

        parent = os.path.dirname(path)
        if parent not in folders:
            return

        size = 0
        if self.calculate_size:
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
        ext = os.path.splitext(path)[1].lower() or "(no ext)"

        files[path] = {
            "path": path,
            "parent_path": parent,
            "root_path": root,
            "size_bytes": size,
            "extension": ext,
        }

        key = (parent, ext)
        if key not in ext_stats:
            ext_stats[key] = {"count": 0, "size": 0}
        ext_stats[key]["count"] += 1
        ext_stats[key]["size"] += size

        scan_time = datetime.now()
        _upsert_extension_row(self.conn, scan_time, parent, ext, ext_stats[key])

        for folder_path in _iter_ancestors(parent, root):
            folder = folders.get(folder_path)
            if not folder:
                continue
            folder["total_file_count"] += 1
            folder["total_size_bytes"] += size
            if folder_path == parent:
                folder["file_count"] += 1
                folder["size_bytes"] += size
            _upsert_folder_row(self.conn, scan_time, folder)

        root_entry = folders.get(root)
        if root_entry:
            state["summary"]["total_files"] = root_entry["total_file_count"]
            state["summary"]["total_size"] = root_entry["total_size_bytes"]
            _upsert_summary(self.conn, scan_time, root, state["summary"])

    def _handle_file_delete(self, root: str, path: str) -> None:
        state = self.state_by_root.get(root)
        if not state:
            return
        files = state["files"]
        folders = state["folders"]
        ext_stats = state["ext_stats"]

        entry = files.pop(path, None)
        if not entry:
            return

        parent = entry["parent_path"]
        ext = entry["extension"]
        size = entry["size_bytes"]

        key = (parent, ext)
        if key in ext_stats:
            ext_stats[key]["count"] = max(0, ext_stats[key]["count"] - 1)
            ext_stats[key]["size"] = max(0, ext_stats[key]["size"] - size)
        scan_time = datetime.now()
        _upsert_extension_row(self.conn, scan_time, parent, ext, ext_stats.get(key, {"count": 0, "size": 0}))

        for folder_path in _iter_ancestors(parent, root):
            folder = folders.get(folder_path)
            if not folder:
                continue
            folder["total_file_count"] = max(0, folder["total_file_count"] - 1)
            folder["total_size_bytes"] = max(0, folder["total_size_bytes"] - size)
            if folder_path == parent:
                folder["file_count"] = max(0, folder["file_count"] - 1)
                folder["size_bytes"] = max(0, folder["size_bytes"] - size)
            _upsert_folder_row(self.conn, scan_time, folder)

        root_entry = folders.get(root)
        if root_entry:
            state["summary"]["total_files"] = root_entry["total_file_count"]
            state["summary"]["total_size"] = root_entry["total_size_bytes"]
            _upsert_summary(self.conn, scan_time, root, state["summary"])

    def _handle_file_modify(self, root: str, path: str) -> None:
        state = self.state_by_root.get(root)
        if not state:
            return
        files = state["files"]
        folders = state["folders"]
        ext_stats = state["ext_stats"]

        entry = files.get(path)
        if not entry:
            # treat as create if we didn't know it
            self._handle_file_create(root, path)
            return

        if not self.calculate_size:
            return

        try:
            new_size = os.path.getsize(path)
        except OSError:
            new_size = entry["size_bytes"]

        delta = new_size - entry["size_bytes"]
        if delta == 0:
            return

        entry["size_bytes"] = new_size

        parent = entry["parent_path"]
        ext = entry["extension"]
        key = (parent, ext)
        if key not in ext_stats:
            ext_stats[key] = {"count": 0, "size": 0}
        ext_stats[key]["size"] += delta

        scan_time = datetime.now()
        _upsert_extension_row(self.conn, scan_time, parent, ext, ext_stats[key])

        for folder_path in _iter_ancestors(parent, root):
            folder = folders.get(folder_path)
            if not folder:
                continue
            folder["total_size_bytes"] += delta
            if folder_path == parent:
                folder["size_bytes"] += delta
            _upsert_folder_row(self.conn, scan_time, folder)

        root_entry = folders.get(root)
        if root_entry:
            state["summary"]["total_size"] = root_entry["total_size_bytes"]
            _upsert_summary(self.conn, scan_time, root, state["summary"])
