#!/usr/bin/env python3
"""
Shared utilities and PostgreSQL persistence helpers for NAS folder tree tables.

Pure path utilities (no DB) are also housed here so that both
nas_folder_tree_event_watcher.py and nas_folder_tree_event_handler.py can
import them without creating a circular dependency.
"""

from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

_DEFAULT_PG_ADMIN_DB = "postgres"


def ensure_database_exists(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    admin_db: str = _DEFAULT_PG_ADMIN_DB,
) -> None:
    if not database or database == admin_db:
        return

    admin_conn = psycopg2.connect(
        host=host,
        port=port,
        database=admin_db,
        user=user,
        password=password,
    )
    admin_conn.autocommit = True
    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                print(f"PostgreSQL DB 생성 완료: {database}")
    finally:
        admin_conn.close()


def calculate_tree_prefixes(folder_tree: List[Dict]) -> List[Dict]:
    if not folder_tree:
        return folder_tree

    sorted_tree = sorted(folder_tree, key=lambda x: x["path"])

    children_by_parent: Dict[str, List[str]] = defaultdict(list)
    for item in sorted_tree:
        parent = item["parent_path"] or ""
        children_by_parent[parent].append(item["path"])

    is_last_child: Dict[str, bool] = {}
    for parent, children in children_by_parent.items():
        sorted_children = sorted(children)
        for i, child in enumerate(sorted_children):
            is_last_child[child] = i == len(sorted_children) - 1

    path_to_item = {item["path"]: item for item in sorted_tree}

    for item in sorted_tree:
        path = item["path"]
        depth = item["depth"]

        if depth == 0:
            item["tree_prefix"] = ""
            item["sort_key"] = "0"
            continue

        if is_last_child.get(path, False):
            current_prefix = "└── "
        else:
            current_prefix = "├── "

        current_path = item["parent_path"]
        ancestors = []
        while current_path and current_path in path_to_item:
            parent_item = path_to_item[current_path]
            if parent_item["depth"] > 0:
                if is_last_child.get(current_path, False):
                    ancestors.append("    ")
                else:
                    ancestors.append("│   ")
            current_path = parent_item["parent_path"]

        ancestor_prefix = "".join(reversed(ancestors))
        item["tree_prefix"] = ancestor_prefix + current_prefix

        sort_parts = []
        for part in path.split("/"):
            padded = re.sub(r"(\d+)", lambda m: m.group(1).zfill(10), part)
            sort_parts.append(padded)
        item["sort_key"] = "/".join(sort_parts)

    return sorted_tree


# ---------------------------------------------------------------------------
# Pure path utilities (no I/O beyond os.path)
# ---------------------------------------------------------------------------

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


def _sort_key(path: str) -> str:
    parts = []
    for part in path.split("/"):
        padded = part
        if any(ch.isdigit() for ch in part):
            padded = "".join([p.zfill(10) if p.isdigit() else p for p in _split_digits(part)])
        parts.append(padded)
    return "/".join(parts)


# ---------------------------------------------------------------------------
# Filesystem scan
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------

def _get_postgres_connection(host: str, port: int, database: str, user: str, password: str):
    ensure_database_exists(host, port, database, user, password)
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


# ---------------------------------------------------------------------------
# PostgreSQL persistence helpers
# ---------------------------------------------------------------------------

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
        (scan_time, folder_path, ext, stats["count"], stats["size"]) for (folder_path, ext), stats in ext_stats.items()
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
