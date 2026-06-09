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
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional

try:
    from watchdog.observers import Observer
except Exception:  # pragma: no cover - runtime import guard
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

    def format_size(size_bytes: int) -> str:  # type: ignore
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"


from nas_folder_tree_db_helpers import (  # noqa: E402
    _get_postgres_connection,
    _scan_root,
    _write_root_state,
)


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
    from nas_folder_tree_event_handler import NasEventHandler

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
