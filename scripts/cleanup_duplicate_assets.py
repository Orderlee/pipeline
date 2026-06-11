#!/usr/bin/env python3
"""Remove duplicate raw assets globally by checksum.

Behavior:
1. Find duplicate checksum groups across all `raw_files`.
2. Choose one canonical row per checksum group.
3. Rebuild keeper `error_message` as `duplicate_skipped_in_manifest:...`.
4. Delete duplicate rows from DB and duplicate objects from MinIO/archive.

Run inside container:
  python3 /scripts/cleanup_duplicate_assets.py --apply

DSN via env: DATAOPS_POSTGRES_DSN=postgresql://user:pass@host:port/dbname
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import boto3
import psycopg2
import psycopg2.extras
from botocore.config import Config


@dataclass(frozen=True)
class DuplicateRow:
    checksum: str
    asset_id: str
    raw_key: str
    source_path: str
    archive_path: str
    original_name: str
    error_message: str
    ingest_status: str
    created_at: object


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN (overrides DATAOPS_POSTGRES_DSN)")
    parser.add_argument("--db", default=None, help="[DEPRECATED] DuckDB path — ignored, kept for CLI compat")
    parser.add_argument("--bucket", default="vlm-raw", help="MinIO bucket name")
    parser.add_argument("--apply", action="store_true", help="Apply cleanup changes (alias for --no-dry-run)")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", default=False, help="Print plan only, no DB/MinIO changes")
    parser.add_argument("--log-every", type=int, default=100, help="Progress log interval")
    return parser.parse_args()


def resolve_dsn(args: argparse.Namespace) -> str:
    if args.dsn:
        return args.dsn
    dsn = os.getenv("DATAOPS_POSTGRES_DSN", "").strip()
    if not dsn:
        print("[ERROR] DATAOPS_POSTGRES_DSN 미설정. --dsn 또는 환경변수 필요.", file=sys.stderr)
        sys.exit(2)
    return dsn


def connect_pg(dsn: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def load_duplicate_rows(conn: psycopg2.extensions.connection) -> list[DuplicateRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH dup AS (
                SELECT checksum
                FROM raw_files
                WHERE checksum IS NOT NULL AND length(trim(checksum)) > 0
                GROUP BY checksum
                HAVING COUNT(*) > 1
            )
            SELECT
                rf.checksum,
                rf.asset_id,
                COALESCE(rf.raw_key, ''),
                COALESCE(rf.source_path, ''),
                COALESCE(rf.archive_path, ''),
                COALESCE(rf.original_name, ''),
                COALESCE(rf.error_message, ''),
                COALESCE(rf.ingest_status, ''),
                rf.created_at
            FROM raw_files rf
            WHERE rf.checksum IN (SELECT checksum FROM dup)
            ORDER BY rf.checksum, rf.created_at, rf.raw_key, rf.asset_id
            """
        )
        rows = cur.fetchall()
    return [DuplicateRow(*row) for row in rows]


def file_name_for_row(row: DuplicateRow) -> str:
    if row.original_name.strip():
        return row.original_name.strip()
    for candidate in (row.raw_key, row.source_path, row.archive_path):
        name = Path(str(candidate or "")).name.strip()
        if name:
            return name
    return "unknown_file"


def parse_marker_counter(error_message: str) -> Counter[str]:
    message = str(error_message or "").strip()
    if not message.startswith("duplicate_skipped_in_manifest:"):
        return Counter()
    payload = message.replace("duplicate_skipped_in_manifest:", "", 1)
    names = [name.strip() for name in payload.split(",") if name.strip()]
    return Counter(names)


def build_marker(rows: list[DuplicateRow], keeper: DuplicateRow) -> str:
    merged_counts: Counter[str] = Counter()
    removed_counts = Counter(file_name_for_row(row) for row in rows if row.asset_id != keeper.asset_id)
    for row in rows:
        marker_counts = parse_marker_counter(row.error_message)
        for name, count in marker_counts.items():
            if merged_counts[name] < count:
                merged_counts[name] = count
    for name, count in removed_counts.items():
        if merged_counts[name] < count:
            merged_counts[name] = count
    payload = [
        name
        for name in sorted(merged_counts)
        for _ in range(int(merged_counts[name]))
    ]
    return f"duplicate_skipped_in_manifest:{','.join(payload)}" if payload else ""


def keeper_sort_key(row: DuplicateRow) -> tuple:
    return (
        0 if row.archive_path.strip() else 1,
        0 if row.error_message.startswith("duplicate_skipped_in_manifest:") else 1,
        row.created_at,
        row.raw_key,
        row.asset_id,
    )


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(items), size):
        yield items[idx:idx + size]


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def delete_dependent_rows(
    conn: psycopg2.extensions.connection,
    remove_asset_ids: list[str],
) -> dict[str, int]:
    """FK 순서에 따라 remove_asset_ids 에 연결된 하위 테이블 행을 삭제.

    삭제 순서: dataset_clips → image_labels → image_metadata → processed_clips → labels → video_metadata
    raw_files 삭제는 호출자가 마지막에 수행.
    반환: 각 테이블별 삭제 행 수 dict.
    """
    counts: dict[str, int] = {}
    if not remove_asset_ids:
        return counts

    placeholders = ", ".join(["%s"] * len(remove_asset_ids))
    params = tuple(remove_asset_ids)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM dataset_clips
            WHERE clip_id IN (
                SELECT clip_id FROM processed_clips
                WHERE source_asset_id IN ({placeholders})
            )
            """,
            params,
        )
        counts["dataset_clips"] = cur.rowcount

        cur.execute(
            f"""
            DELETE FROM image_labels
            WHERE image_id IN (
                SELECT image_id FROM image_metadata
                WHERE source_asset_id IN ({placeholders})
            )
            """,
            params,
        )
        counts["image_labels"] = cur.rowcount

        cur.execute(
            f"DELETE FROM image_metadata WHERE source_asset_id IN ({placeholders})",
            params,
        )
        counts["image_metadata"] = cur.rowcount

        cur.execute(
            f"DELETE FROM processed_clips WHERE source_asset_id IN ({placeholders})",
            params,
        )
        counts["processed_clips"] = cur.rowcount

        cur.execute(
            f"DELETE FROM labels WHERE asset_id IN ({placeholders})",
            params,
        )
        counts["labels"] = cur.rowcount

        cur.execute(
            f"DELETE FROM video_metadata WHERE asset_id IN ({placeholders})",
            params,
        )
        counts["video_metadata"] = cur.rowcount

    return counts


def main() -> int:
    args = parse_args()

    if args.db is not None:
        print("[WARN] --db 는 무시됩니다. PG 포팅 이후 DSN(--dsn / DATAOPS_POSTGRES_DSN)을 사용하세요.", file=sys.stderr)

    is_dry_run = args.dry_run or not args.apply

    dsn = resolve_dsn(args)
    conn = connect_pg(dsn)

    try:
        duplicate_rows = load_duplicate_rows(conn)
    finally:
        conn.close()

    by_checksum: dict[str, list[DuplicateRow]] = defaultdict(list)
    for row in duplicate_rows:
        by_checksum[row.checksum].append(row)

    keepers: dict[str, DuplicateRow] = {}
    keeper_updates: list[tuple[str, str]] = []
    remove_rows: list[DuplicateRow] = []
    protected_archive_paths: set[str] = set()
    all_archive_counts: Counter[str] = Counter(
        row.archive_path for row in duplicate_rows if row.archive_path.strip()
    )

    for checksum, rows in sorted(by_checksum.items()):
        keeper = min(rows, key=keeper_sort_key)
        keepers[checksum] = keeper
        protected_archive_paths.add(keeper.archive_path)
        marker = build_marker(rows, keeper)
        if marker != str(keeper.error_message or "").strip():
            keeper_updates.append((marker, keeper.asset_id))
        remove_rows.extend(row for row in rows if row.asset_id != keeper.asset_id)

    remove_asset_ids = [row.asset_id for row in remove_rows]
    remove_raw_keys = [row.raw_key for row in remove_rows if row.raw_key.strip()]
    remove_archive_path_counts = Counter(
        row.archive_path for row in remove_rows if row.archive_path.strip()
    )

    delete_archive_paths: list[str] = []
    for archive_path, remove_count in sorted(remove_archive_path_counts.items()):
        if archive_path in protected_archive_paths:
            continue
        if all_archive_counts[archive_path] == remove_count:
            delete_archive_paths.append(archive_path)

    print(
        f"[PLAN] duplicate_groups={len(by_checksum)} "
        f"keeper_updates={len(keeper_updates)} remove_rows={len(remove_rows)} "
        f"remove_minio_objects={len(remove_raw_keys)} remove_archive_files={len(delete_archive_paths)}"
    )
    for checksum, keeper in list(keepers.items())[:10]:
        group_size = len(by_checksum[checksum])
        print(
            f"[KEEPER] checksum={checksum} count={group_size} "
            f"asset_id={keeper.asset_id} raw_key={keeper.raw_key}"
        )

    if is_dry_run:
        print("[DONE] dry-run complete (no changes applied; pass --apply to execute)")
        return 0

    conn = connect_pg(dsn)
    try:
        dep_counts = delete_dependent_rows(conn, remove_asset_ids)

        with conn.cursor() as cur:
            if remove_asset_ids:
                placeholders = ", ".join(["%s"] * len(remove_asset_ids))
                cur.execute(
                    f"DELETE FROM raw_files WHERE asset_id IN ({placeholders})",
                    tuple(remove_asset_ids),
                )
                dep_counts["raw_files"] = cur.rowcount

            if keeper_updates:
                now = datetime.now()
                psycopg2.extras.execute_batch(
                    cur,
                    "UPDATE raw_files SET error_message = %s, updated_at = %s WHERE asset_id = %s",
                    [(marker, now, asset_id) for marker, asset_id in keeper_updates],
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"[INFO] DB rows deleted: {dep_counts}")

    s3 = build_s3_client()
    for idx, batch in enumerate(chunked(remove_raw_keys, 1000), start=1):
        result = s3.delete_objects(
            Bucket=args.bucket,
            Delete={"Objects": [{"Key": raw_key} for raw_key in batch], "Quiet": True},
        )
        errors = result.get("Errors", [])
        if errors:
            print(f"[ERROR] minio delete batch={idx} errors={errors[:5]}", file=sys.stderr)
            return 1
        if idx % max(1, args.log_every) == 0:
            print(f"[PROGRESS] deleted_minio_batches={idx}")

    archive_deleted = 0
    archive_missing = 0
    for archive_path in delete_archive_paths:
        path = Path(archive_path)
        if not path.exists():
            archive_missing += 1
            continue
        path.unlink()
        archive_deleted += 1

    conn = connect_pg(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT checksum
                    FROM raw_files
                    WHERE checksum IS NOT NULL AND length(trim(checksum)) > 0
                    GROUP BY checksum
                    HAVING COUNT(*) > 1
                ) sub
                """
            )
            remaining_dup_groups = cur.fetchone()[0]

            cur.execute(
                """
                WITH dup AS (
                    SELECT checksum
                    FROM raw_files
                    WHERE checksum IS NOT NULL AND length(trim(checksum)) > 0
                    GROUP BY checksum
                    HAVING COUNT(*) > 1
                )
                SELECT COUNT(*)
                FROM raw_files
                WHERE checksum IN (SELECT checksum FROM dup)
                  AND COALESCE(error_message, '') NOT LIKE 'duplicate_skipped_in_manifest:%'
                """
            )
            marker_mismatch_groups = cur.fetchone()[0]
    finally:
        conn.close()

    print(
        f"[DONE] removed_rows={len(remove_rows)} "
        f"deleted_minio_objects={len(remove_raw_keys)} "
        f"deleted_archive_files={archive_deleted} archive_missing={archive_missing} "
        f"remaining_dup_groups={remaining_dup_groups} marker_mismatch_rows_in_dup_groups={marker_mismatch_groups}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
