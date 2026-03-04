#!/usr/bin/env python3
"""
Local DuckDB(MVP tables) -> MotherDuck(MVP tables) sync.

목표:
- 로컬 DuckDB의 MVP 운영 테이블을 MotherDuck로 동기화
- 테이블 단위 CREATE OR REPLACE로 스키마/데이터를 로컬과 동일하게 정렬
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import duckdb

# 공유 모듈 경로 설정
REPO_ROOT = Path(__file__).resolve().parent.parent
for candidate in (
    REPO_ROOT / "src" / "python",
    Path("/src/python"),
    Path(__file__).resolve().parent,
):
    if candidate.exists():
        sys.path.insert(0, str(candidate))

from common.config import get_settings  # noqa: E402
from common.motherduck import (  # noqa: E402
    connect_motherduck,
    connect_motherduck_root,
    database_exists,
    load_env_for_motherduck,
    quote_identifier,
    quote_string,
)


MVP_TABLES = [
    "raw_files",
    "image_metadata",
    "video_metadata",
    "labels",
    "processed_clips",
    "datasets",
    "dataset_clips",
]

# 운영 대상에서 제거된 레거시 테이블
LEGACY_TABLES_TO_DROP = [
    "clip_metadata",  # processed_clips에 흡수됨
]


def _normalize_sync_tables(tables: Optional[list[str]]) -> list[str]:
    """사용자 요청 동기화 테이블 정규화/검증."""
    if not tables:
        return list(MVP_TABLES)

    normalized: list[str] = []
    seen: set[str] = set()
    invalid: list[str] = []
    valid_set = set(MVP_TABLES)

    for raw in tables:
        table = str(raw).strip()
        if not table:
            continue
        if table not in valid_set:
            invalid.append(table)
            continue
        if table in seen:
            continue
        normalized.append(table)
        seen.add(table)

    if invalid:
        raise ValueError(
            "Invalid table(s): "
            + ", ".join(sorted(set(invalid)))
            + f". Allowed: {', '.join(MVP_TABLES)}"
        )

    if not normalized:
        raise ValueError("No valid table selected for sync.")

    return normalized


def _bootstrap_database_from_local(
    token: str,
    database: str,
    local_db_path: str,
    dry_run: bool,
) -> str:
    """
    MotherDuck의 "Add data > From local DB"에 해당하는 SQL 부트스트랩.

    Returns:
        "exists" | "created" | "would_create"
    """
    con = connect_motherduck_root(token)
    try:
        if database_exists(con, database):
            return "exists"

        create_sql = (
            f"CREATE DATABASE {quote_identifier(database)} "
            f"FROM '{quote_string(str(Path(local_db_path).resolve()))}'"
        )
        if dry_run:
            print(f"[DRY-RUN] Would bootstrap MotherDuck DB via local DB: {database}")
            print(f"[DRY-RUN] SQL: {create_sql}")
            return "would_create"

        con.execute(create_sql)
        print(f"[BOOTSTRAP] Created MotherDuck DB '{database}' from local DB '{local_db_path}'")
        return "created"
    finally:
        con.close()


def _ensure_org_share(
    token: str,
    database: str,
    share_name: str,
    share_update_mode: str,
    dry_run: bool,
) -> None:
    """
    MotherDuck 데이터베이스 공유를 Organization + Discoverable로 보장.
    """
    create_share_sql = (
        f"CREATE SHARE IF NOT EXISTS {quote_identifier(share_name)} "
        f"FROM {quote_identifier(database)} "
        f"(ACCESS ORGANIZATION, VISIBILITY DISCOVERABLE, UPDATE {share_update_mode})"
    )

    if dry_run:
        print(
            f"[DRY-RUN] Would ensure organization share: "
            f"db='{database}' share='{share_name}'"
        )
        print(f"[DRY-RUN] SQL: {create_share_sql}")
        return

    con = connect_motherduck(database, token)
    try:
        row = con.execute(create_share_sql).fetchone()
        share_url = row[0] if row and len(row) > 0 else None
        print(
            f"[SHARE] Ensured organization share: db='{database}' "
            f"share='{share_name}'"
        )
        if share_url:
            print(f"[SHARE] URL: {share_url}")
    finally:
        con.close()


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _local_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = 'local_db'
          AND table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _count_rows(con: duckdb.DuckDBPyConnection, relation_sql: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {relation_sql}").fetchone()[0])


def _sync_mvp_tables(
    con: duckdb.DuckDBPyConnection,
    dry_run: bool,
    tables: Optional[list[str]] = None,
) -> list[tuple[str, int, int]]:
    results: list[tuple[str, int, int]] = []
    target_tables = _normalize_sync_tables(tables)

    for table in target_tables:
        if not _local_table_exists(con, table):
            print(f"[SKIP] local_db.{table} table not found")
            continue

        src_rel = f"local_db.{quote_identifier(table)}"
        dst_rel = quote_identifier(table)

        local_count = _count_rows(con, src_rel)
        motherduck_before = _count_rows(con, dst_rel) if _table_exists(con, table) else 0

        if dry_run:
            print(
                f"[DRY-RUN] table={table} "
                f"local={local_count} motherduck_before={motherduck_before} action=REPLACE"
            )
            results.append((table, local_count, motherduck_before))
            continue

        con.execute(f"CREATE OR REPLACE TABLE {dst_rel} AS SELECT * FROM {src_rel}")
        motherduck_after = _count_rows(con, dst_rel)
        print(
            f"[SYNC] table={table} "
            f"local={local_count} motherduck_before={motherduck_before} motherduck_after={motherduck_after}"
        )
        results.append((table, local_count, motherduck_before))

    return results


def _drop_legacy_tables(con: duckdb.DuckDBPyConnection, dry_run: bool) -> list[str]:
    dropped: list[str] = []

    for table in LEGACY_TABLES_TO_DROP:
        if not _table_exists(con, table):
            continue

        if dry_run:
            print(f"[DRY-RUN] table={table} action=DROP")
        else:
            con.execute(f"DROP TABLE IF EXISTS {quote_identifier(table)}")
            print(f"[DROP] table={table}")
        dropped.append(table)

    return dropped


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync local DuckDB MVP tables to MotherDuck"
    )
    parser.add_argument("--config", default=None, help="Path to configs/global.yaml")
    parser.add_argument("--db", default="pipeline_db", help="MotherDuck database name")
    parser.add_argument(
        "--local-db-path",
        default=None,
        help="Local DuckDB path (default: settings/env)",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=None,
        help="MotherDuck token (or MOTHERDUCK_TOKEN env)",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=None,
        help="Optional .env path for MOTHERDUCK_TOKEN",
    )
    parser.add_argument(
        "--ensure-org-share",
        "--share-on-create",
        dest="ensure_org_share",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Ensure org-level share is enabled "
            "(default: true)."
        ),
    )
    parser.add_argument(
        "--share-name",
        default=None,
        help="MotherDuck share name (default: same as --db)",
    )
    parser.add_argument(
        "--share-update",
        choices=["MANUAL", "AUTOMATIC"],
        default="MANUAL",
        help="MotherDuck share update mode (default: MANUAL)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help=(
            "Subset of MVP tables to sync. "
            f"Allowed: {', '.join(MVP_TABLES)}"
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Read/compare only")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        selected_tables = _normalize_sync_tables(args.tables)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 1

    load_env_for_motherduck(repo_root=REPO_ROOT, env_file=args.env_file)

    settings = get_settings(args.config)
    local_db_path = (
        args.local_db_path
        or os.getenv("DATAOPS_DUCKDB_PATH")
        or os.getenv("DUCKDB_PATH")
        or settings.duckdb_path
    )

    if not local_db_path or not os.path.exists(local_db_path):
        print(f"[ERROR] Local DuckDB not found: {local_db_path}")
        return 1

    md_token = args.token or os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        print("[ERROR] MotherDuck token is required. Set MOTHERDUCK_TOKEN or pass --token/--env-file.")
        return 1

    bootstrap_state = _bootstrap_database_from_local(
        token=md_token,
        database=args.db,
        local_db_path=local_db_path,
        dry_run=args.dry_run,
    )
    if bootstrap_state == "would_create":
        if args.ensure_org_share:
            _ensure_org_share(
                token=md_token,
                database=args.db,
                share_name=args.share_name or args.db,
                share_update_mode=args.share_update,
                dry_run=True,
            )
        return 0

    if args.ensure_org_share:
        _ensure_org_share(
            token=md_token,
            database=args.db,
            share_name=args.share_name or args.db,
            share_update_mode=args.share_update,
            dry_run=args.dry_run,
        )

    con = connect_motherduck(args.db, md_token)
    try:
        attach_path = quote_string(str(Path(local_db_path).resolve()))
        con.execute(f"ATTACH '{attach_path}' AS local_db (READ_ONLY)")

        dropped = _drop_legacy_tables(con, dry_run=args.dry_run)
        if dropped:
            mode = "DRY-RUN" if args.dry_run else "SYNC"
            print(f"[{mode}] dropped_legacy_tables={len(dropped)}")

        synced = _sync_mvp_tables(con, dry_run=args.dry_run, tables=selected_tables)
        if not synced:
            print("[INFO] No local MVP tables found. Nothing to sync.")
            con.execute("DETACH local_db")
            return 0

        mode = "DRY-RUN" if args.dry_run else "SYNC"
        synced_table_names = [table for table, _, _ in synced]
        print(
            f"[{mode}] synced_tables={len(synced)} "
            f"tables={','.join(synced_table_names)}"
        )

        con.execute("DETACH local_db")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
