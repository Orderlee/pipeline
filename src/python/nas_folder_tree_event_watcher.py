#!/usr/bin/env python3
"""
Event-driven NAS folder tree updater for PostgreSQL.

This script performs an initial full scan (bootstrap) and then applies
incremental updates based on filesystem events. It updates the same
tables used by the existing full-scan script, but treats them as the
"current state" (rows are replaced per path/root).

Usage examples:
  # Full bootstrap scan, then watch for changes (recommended)
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --max-depth 10

  # Skip initial scan and only process new events (state may be incomplete)
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --no-bootstrap

  # Faster watch: skip file size calculations
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --no-size

  # Directory delete delta mode (no full rescan on delete)
  # NOTE: Requires bootstrap to build in-memory state.
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --dir-delete-delta

  # Save state snapshots every 30s and restore on restart
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --state-save-interval 30

  # Use a custom snapshot path
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --state-path /data/nas_state.json

  # Disable state restore/save
  python nas_folder_tree_event_watcher.py --path /nas/datasets/projects --no-state-restore --no-state-save

Note:
  - By default, directory delete/move triggers a full rescan for that root.
  - Use --dir-delete-delta to update in-memory state + DB without rescan.
  - If you restart this script, it will bootstrap again unless you disable it.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

try:
    from watchdog.events import FileSystemEvent, FileSystemEventHandler
    from watchdog.observers import Observer
except Exception as exc:  # pragma: no cover - runtime import guard
    print("Missing dependency: watchdog. Install with `pip install watchdog`.")
    raise


# Reuse defaults/helpers from the full-scan script when available.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from nas_folder_tree_to_postgres import (  # type: ignore
        DEFAULT_MAX_DEPTH,
        DEFAULT_NAS_PATHS,
        DEFAULT_PG_DB,
        DEFAULT_PG_HOST,
        DEFAULT_PG_PASSWORD,
        DEFAULT_PG_PORT,
        DEFAULT_PG_USER,
        calculate_tree_prefixes,
        ensure_database_exists,
        ensure_schema,
        expand_glob_patterns,
        format_size,
    )
except Exception:  # pragma: no cover - fallback defaults if import fails
    DEFAULT_NAS_PATHS = ["/nas/datasets/projects"]
    DEFAULT_MAX_DEPTH = 10
    DEFAULT_PG_HOST = "postgres"
    DEFAULT_PG_PORT = 5432
    DEFAULT_PG_DB = "nas_tree"
    DEFAULT_PG_USER = "airflow"
    DEFAULT_PG_PASSWORD = "airflow"

    def ensure_database_exists(  # type: ignore
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ):
        raise RuntimeError("ensure_database_exists not available; keep nas_folder_tree_to_postgres.py importable.")

    def ensure_schema(conn):  # type: ignore
        raise RuntimeError("ensure_schema not available; keep nas_folder_tree_to_postgres.py importable.")

    def expand_glob_patterns(patterns: List[str]) -> List[str]:  # type: ignore
        expanded = []
        for pattern in patterns:
            if "*" in pattern or "?" in pattern:
                expanded.extend([p for p in sorted(Path().glob(pattern)) if p.is_dir()])
            else:
                p = Path(pattern)
                if p.is_dir():
                    expanded.append(str(p))
        return [str(p) for p in expanded]

    def calculate_tree_prefixes(items: List[Dict]) -> List[Dict]:  # type: ignore
        return items

    def format_size(size_bytes: int) -> str:  # type: ignore
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"


def _get_postgres_connection(host: str, port: int, database: str, user: str, password: str):
    ensure_database_exists(host, port, database, user, password)
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


def _normalize(path: str) -> str:
    return os.path.abspath(path)


def _rel_depth(path: str, root: str) -> int:
    rel = os.path.relpath(path, root)
    if rel == ".":
        return 0
    return len(rel.split(os.sep))


def _find_root(path: str, roots: List[str]) -> Optional[str]:
    path = _normalize(path)
    matches = [r for r in roots if path.startswith(r + os.sep) or path == r]
    if not matches:
        return None
    return max(matches, key=len)


def _iter_ancestors(path: str, root: str) -> Iterable[str]:
    current = path
    while True:
        yield current
        if current == root:
            break
        current = os.path.dirname(current)


def _sort_key(path: str) -> str:
    parts = []
    for part in path.split("/"):
        padded = part
        if any(ch.isdigit() for ch in part):
            padded = "".join([p.zfill(10) if p.isdigit() else p for p in _split_digits(part)])
        parts.append(padded)
    return "/".join(parts)


def _split_digits(value: str) -> List[str]:
    out: List[str] = []
    buf = ""
    last_is_digit = None
    for ch in value:
        is_digit = ch.isdigit()
        if last_is_digit is None or is_digit == last_is_digit:
            buf += ch
        else:
            out.append(buf)
            buf = ch
        last_is_digit = is_digit
    if buf:
        out.append(buf)
    return out


def _scan_root(
    root_path: str,
    max_depth: int,
    calculate_size: bool,
    recompute_tree_prefix: bool,
) -> Tuple[Dict[str, Dict], Dict[Tuple[str, str], Dict], Dict[str, Dict], Dict]:
    root_path = _normalize(root_path)

    folders: Dict[str, Dict] = {}
    ext_stats: Dict[Tuple[str, str], Dict] = {}
    files: Dict[str, Dict] = {}

    root_depth = root_path.rstrip(os.sep).count(os.sep)

    for dirpath, dirnames, filenames in os.walk(root_path):
        dirpath = _normalize(dirpath)
        depth = dirpath.rstrip(os.sep).count(os.sep) - root_depth
        if depth > max_depth:
            dirnames.clear()
            continue

        parent_path = os.path.dirname(dirpath) if dirpath != root_path else None
        name = os.path.basename(dirpath) or dirpath

        folder_size = 0
        file_count = 0
        for filename in filenames:
            file_path = _normalize(os.path.join(dirpath, filename))
            ext = os.path.splitext(filename)[1].lower() or "(no ext)"
            size = 0
            if calculate_size:
                try:
                    size = os.path.getsize(file_path)
                except OSError:
                    size = 0
            file_count += 1
            folder_size += size

            files[file_path] = {
                "path": file_path,
                "parent_path": dirpath,
                "root_path": root_path,
                "size_bytes": size,
                "extension": ext,
            }

            key = (dirpath, ext)
            if key not in ext_stats:
                ext_stats[key] = {"count": 0, "size": 0}
            ext_stats[key]["count"] += 1
            ext_stats[key]["size"] += size

        folders[dirpath] = {
            "path": dirpath,
            "parent_path": parent_path,
            "name": name,
            "depth": depth,
            "node_type": "folder",
            "size_bytes": folder_size,
            "file_count": file_count,
            "folder_count": len(dirnames),
            "total_size_bytes": folder_size,
            "total_file_count": file_count,
            "tree_prefix": "",
            "sort_key": _sort_key(dirpath),
        }

    # bottom-up aggregation
    if folders:
        sorted_by_depth = sorted(folders.values(), key=lambda x: -x["depth"])
        for item in sorted_by_depth:
            parent = item["parent_path"]
            if parent and parent in folders:
                folders[parent]["total_size_bytes"] += item["total_size_bytes"]
                folders[parent]["total_file_count"] += item["total_file_count"]

    if recompute_tree_prefix and folders:
        recalced = calculate_tree_prefixes(list(folders.values()))
        folders = {item["path"]: item for item in recalced}

    root_entry = folders.get(root_path)
    summary = {
        "total_folders": len(folders),
        "total_files": root_entry["total_file_count"] if root_entry else 0,
        "total_size": root_entry["total_size_bytes"] if root_entry else 0,
        "max_depth": max((f["depth"] for f in folders.values()), default=0),
        "unique_extensions": len({ext for (_, ext) in ext_stats.keys()}),
    }

    return folders, ext_stats, files, summary


def _serialize_state(state_by_root: Dict[str, Dict]) -> Dict:
    payload = {"schema_version": 1, "roots": []}
    for root, state in state_by_root.items():
        folders = list(state["folders"].values())
        files = list(state["files"].values())
        ext_stats = [
            {"folder_path": folder_path, "extension": ext, "count": stats["count"], "size": stats["size"]}
            for (folder_path, ext), stats in state["ext_stats"].items()
        ]
        payload["roots"].append(
            {
                "root": root,
                "summary": state["summary"],
                "folders": folders,
                "files": files,
                "ext_stats": ext_stats,
            }
        )
    return payload


def _deserialize_state(payload: Dict) -> Dict[str, Dict]:
    state_by_root: Dict[str, Dict] = {}
    for root_entry in payload.get("roots", []):
        root = root_entry.get("root")
        if not root:
            continue
        folders = {item["path"]: item for item in root_entry.get("folders", [])}
        files = {item["path"]: item for item in root_entry.get("files", [])}
        ext_stats = {
            (item["folder_path"], item["extension"]): {"count": item["count"], "size": item["size"]}
            for item in root_entry.get("ext_stats", [])
        }
        state_by_root[root] = {
            "folders": folders,
            "files": files,
            "ext_stats": ext_stats,
            "summary": root_entry.get("summary", {}),
        }
    return state_by_root


def _save_state(state_by_root: Dict[str, Dict], path: str) -> None:
    payload = _serialize_state(state_by_root)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        import json

        json.dump(payload, handle, ensure_ascii=True)
    os.replace(temp_path, path)


def _load_state(path: str) -> Optional[Dict[str, Dict]]:
    if not os.path.exists(path):
        return None
    import json

    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return _deserialize_state(payload)


def _clear_root_rows(conn, root_path: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM nas_folder_tree WHERE path LIKE %s", (f"{root_path}%",))
        cur.execute("DELETE FROM nas_folder_extensions WHERE folder_path LIKE %s", (f"{root_path}%",))
        cur.execute("DELETE FROM nas_folder_summary WHERE root_path = %s", (root_path,))
    conn.commit()


def _write_root_state(
    conn,
    scan_time: datetime,
    root_path: str,
    folders: Dict[str, Dict],
    ext_stats: Dict[Tuple[str, str], Dict],
    summary: Dict,
) -> None:
    _clear_root_rows(conn, root_path)

    tree_rows = [
        (
            scan_time,
            item["path"],
            item["parent_path"],
            item["name"],
            item["depth"],
            item["node_type"],
            item["size_bytes"],
            item["file_count"],
            item["folder_count"],
            item["total_size_bytes"],
            item["total_file_count"],
            item.get("tree_prefix", ""),
            item.get("sort_key", item["path"]),
        )
        for item in folders.values()
    ]

    ext_rows = [
        (scan_time, folder_path, ext, stats["count"], stats["size"])
        for (folder_path, ext), stats in ext_stats.items()
    ]

    with conn.cursor() as cur:
        if tree_rows:
            execute_values(
                cur,
                """
                INSERT INTO nas_folder_tree
                (scan_time, path, parent_path, name, depth, node_type, size_bytes,
                 file_count, folder_count, total_size_bytes, total_file_count,
                 tree_prefix, sort_key)
                VALUES %s
                """,
                tree_rows,
            )
        if ext_rows:
            execute_values(
                cur,
                """
                INSERT INTO nas_folder_extensions
                (scan_time, folder_path, extension, file_count, total_size_bytes)
                VALUES %s
                """,
                ext_rows,
            )
        cur.execute(
            """
            INSERT INTO nas_folder_summary
            (scan_time, root_path, total_folders, total_files, total_size_bytes,
             total_size_gb, max_depth, unique_extensions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                scan_time,
                root_path,
                summary["total_folders"],
                summary["total_files"],
                summary["total_size"],
                summary["total_size"] / (1024**3) if summary["total_size"] else 0,
                summary["max_depth"],
                summary["unique_extensions"],
            ),
        )
    conn.commit()


def _upsert_folder_row(conn, scan_time: datetime, folder: Dict) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM nas_folder_tree WHERE path = %s", (folder["path"],))
        cur.execute(
            """
            INSERT INTO nas_folder_tree
            (scan_time, path, parent_path, name, depth, node_type, size_bytes,
             file_count, folder_count, total_size_bytes, total_file_count,
             tree_prefix, sort_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                scan_time,
                folder["path"],
                folder["parent_path"],
                folder["name"],
                folder["depth"],
                folder["node_type"],
                folder["size_bytes"],
                folder["file_count"],
                folder["folder_count"],
                folder["total_size_bytes"],
                folder["total_file_count"],
                folder.get("tree_prefix", ""),
                folder.get("sort_key", folder["path"]),
            ),
        )
    conn.commit()


def _upsert_extension_row(conn, scan_time: datetime, folder_path: str, ext: str, stats: Dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM nas_folder_extensions WHERE folder_path = %s AND extension = %s",
            (folder_path, ext),
        )
        cur.execute(
            """
            INSERT INTO nas_folder_extensions
            (scan_time, folder_path, extension, file_count, total_size_bytes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (scan_time, folder_path, ext, stats["count"], stats["size"]),
        )
    conn.commit()


def _upsert_summary(conn, scan_time: datetime, root_path: str, summary: Dict) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM nas_folder_summary WHERE root_path = %s", (root_path,))
        cur.execute(
            """
            INSERT INTO nas_folder_summary
            (scan_time, root_path, total_folders, total_files, total_size_bytes,
             total_size_gb, max_depth, unique_extensions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                scan_time,
                root_path,
                summary["total_folders"],
                summary["total_files"],
                summary["total_size"],
                summary["total_size"] / (1024**3) if summary["total_size"] else 0,
                summary["max_depth"],
                summary["unique_extensions"],
            ),
        )
    conn.commit()


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
        removed_size = subtree_root["total_size_bytes"] if subtree_root else sum(
            files[p]["size_bytes"] for p in subtree_files
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Event-driven NAS folder tree updater.")
    parser.add_argument("--path", type=str, nargs="*", default=None, help="Root paths to watch")
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--postgres-host", type=str, default=DEFAULT_PG_HOST)
    parser.add_argument("--postgres-port", type=int, default=DEFAULT_PG_PORT)
    parser.add_argument("--postgres-db", type=str, default=DEFAULT_PG_DB)
    parser.add_argument("--postgres-user", type=str, default=DEFAULT_PG_USER)
    parser.add_argument("--postgres-password", type=str, default=DEFAULT_PG_PASSWORD)
    parser.add_argument("--no-size", action="store_true", help="Skip file size calculations")
    parser.add_argument("--bootstrap", action="store_true", default=True, help="Run initial full scan")
    parser.add_argument("--no-bootstrap", action="store_true", help="Skip initial full scan")
    parser.add_argument("--recompute-tree-prefix", action="store_true", default=False)
    parser.add_argument(
        "--dir-delete-delta",
        action="store_true",
        default=False,
        help="Handle directory deletes without full rescan (requires bootstrap).",
    )
    parser.add_argument(
        "--no-rescan-on-dir-delete",
        action="store_true",
        default=False,
        help="Do not fallback to full rescan if delta delete fails.",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        default="/data/nas_folder_tree_state.json",
        help="Path to save/restore watcher state snapshot.",
    )
    parser.add_argument(
        "--state-save-interval",
        type=int,
        default=60,
        help="Seconds between state snapshot saves.",
    )
    parser.add_argument(
        "--no-state-restore",
        action="store_true",
        default=False,
        help="Do not restore state from snapshot on startup.",
    )
    parser.add_argument(
        "--no-state-save",
        action="store_true",
        default=False,
        help="Do not save state snapshots while running.",
    )
    parser.add_argument(
        "--force-bootstrap",
        action="store_true",
        default=False,
        help="Run bootstrap even if a saved state is available.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    bootstrap = args.bootstrap and not args.no_bootstrap
    calculate_size = not args.no_size

    patterns = args.path if args.path else list(DEFAULT_NAS_PATHS)
    roots = [str(Path(p).resolve()) for p in expand_glob_patterns(patterns)]

    if not roots:
        print("No matching root folders found.")
        return

    conn = _get_postgres_connection(
        args.postgres_host,
        args.postgres_port,
        args.postgres_db,
        args.postgres_user,
        args.postgres_password,
    )

    ensure_schema(conn)

    state_by_root: Dict[str, Dict] = {}
    state_lock = Lock()

    if not args.no_state_restore and not args.force_bootstrap:
        loaded = _load_state(args.state_path)
        if loaded:
            state_by_root.update(loaded)
            print(f"Loaded state snapshot: {args.state_path}")

    if bootstrap and (args.force_bootstrap or not state_by_root):
        print("Bootstrap scan starting...")
        for root in roots:
            folders, ext_stats, files, summary = _scan_root(
                root, args.max_depth, calculate_size, args.recompute_tree_prefix
            )
            state_by_root[root] = {
                "folders": folders,
                "ext_stats": ext_stats,
                "files": files,
                "summary": summary,
            }
            scan_time = datetime.now()
            _write_root_state(conn, scan_time, root, folders, ext_stats, summary)
            print(
                f"OK {root}: folders={summary['total_folders']:,}, "
                f"files={summary['total_files']:,}, size={format_size(summary['total_size'])}"
            )
    else:
        missing = [r for r in roots if r not in state_by_root]
        if missing:
            print("Warning: state missing for roots; consider running with --bootstrap:")
            for r in missing:
                print(f"  - {r}")

    handler = NasEventHandler(
        conn=conn,
        roots=roots,
        max_depth=args.max_depth,
        calculate_size=calculate_size,
        recompute_tree_prefix=args.recompute_tree_prefix,
        dir_delete_delta=args.dir_delete_delta,
        fallback_rescan_on_dir_delete=not args.no_rescan_on_dir_delete,
        state_lock=state_lock,
        state_by_root=state_by_root,
    )

    observer = Observer()
    for root in roots:
        observer.schedule(handler, root, recursive=True)
        print(f"Watching: {root}")

    observer.start()
    last_save = time.time()
    try:
        while True:
            time.sleep(1)
            if args.no_state_save:
                continue
            if args.state_save_interval <= 0:
                continue
            if time.time() - last_save >= args.state_save_interval:
                with state_lock:
                    _save_state(state_by_root, args.state_path)
                last_save = time.time()
    except KeyboardInterrupt:
        if not args.no_state_save:
            with state_lock:
                _save_state(state_by_root, args.state_path)
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
