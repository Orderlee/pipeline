#!/usr/bin/env python3
"""
Purge pipeline data across PostgreSQL and MinIO in one command.

Safety:
- Dry-run by default.
- Use --yes to apply deletions.

Examples:
  # Dry-run by created_at date
  python3 /scripts/purge_pipeline_data.py --created-date 2026-02-25

  # Apply by created_at date
  python3 /scripts/purge_pipeline_data.py --created-date 2026-02-25 --yes

  # Apply by raw_key pattern
  python3 /scripts/purge_pipeline_data.py --raw-key-like '%yeongjun%_-_%' --yes

  # Full wipe (all tables + selected MinIO buckets)
  python3 /scripts/purge_pipeline_data.py --all --yes
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Sequence

import psycopg2
import psycopg2.extensions

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src" / "vlm_pipeline"))

if TYPE_CHECKING:
    from minio import Minio


FULL_DELETE_TABLE_ORDER = [
    "dataset_clips",
    "datasets",
    "image_metadata",
    "processed_clips",
    "labels",
    "video_metadata",
    "raw_files",
]

DEFAULT_BUCKETS = ["vlm-raw", "vlm-labels", "vlm-processed", "vlm-dataset"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete pipeline data from PostgreSQL + MinIO."
    )
    parser.add_argument("--all", action="store_true", help="Delete all pipeline data.")
    parser.add_argument("--created-date", default=None, help="Filter by raw_files.created_at date (YYYY-MM-DD).")
    parser.add_argument("--asset-id", action="append", default=[], help="Filter by raw_files.asset_id. Repeatable.")
    parser.add_argument("--raw-key-like", default=None, help="Filter by raw_files.raw_key LIKE pattern.")
    parser.add_argument("--source-path-like", default=None, help="Filter by raw_files.source_path LIKE pattern.")
    parser.add_argument("--name-like", default=None, help="Filter by raw_files.original_name LIKE pattern.")
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN (postgresql://user:pass@host:port/dbname). Falls back to DATAOPS_POSTGRES_DSN env.",
    )
    parser.add_argument("--no-minio", action="store_true", help="Skip MinIO object deletion.")
    parser.add_argument("--buckets", nargs="+", default=DEFAULT_BUCKETS, help="MinIO buckets to purge in --all mode.")
    parser.add_argument("--yes", action="store_true", help="Actually apply deletions.")
    return parser.parse_args()


def _normalize_like(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    return v or None


def _selector_parts(args: argparse.Namespace) -> tuple[str, list[str]]:
    where: list[str] = []
    params: list[str] = []

    if args.created_date:
        where.append("CAST(created_at AS DATE) = CAST(%s AS DATE)")
        params.append(args.created_date)
    if args.raw_key_like:
        where.append("raw_key LIKE %s")
        params.append(args.raw_key_like)
    if args.source_path_like:
        where.append("source_path LIKE %s")
        params.append(args.source_path_like)
    if args.name_like:
        where.append("original_name LIKE %s")
        params.append(args.name_like)
    if args.asset_id:
        where.append("asset_id = ANY(%s)")
        params.append(args.asset_id)  # type: ignore[arg-type]

    if not where:
        return "1=1", []
    return " AND ".join(where), params


def _build_minio_client() -> "Minio":
    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

    secure = endpoint.startswith("https://")
    if endpoint.startswith("http://"):
        endpoint = endpoint[len("http://"):]
    elif endpoint.startswith("https://"):
        endpoint = endpoint[len("https://"):]

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _connect_pg(dsn: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def _table_count(conn: psycopg2.extensions.connection, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _collect_asset_ids(
    conn: psycopg2.extensions.connection,
    where_sql: str,
    params: Sequence[Any],
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT asset_id
            FROM raw_files
            WHERE {where_sql}
            ORDER BY created_at
            """,
            list(params),
        )
        rows = cur.fetchall()
    return [str(row[0]) for row in rows]


def _collect_object_refs_for_assets(
    conn: psycopg2.extensions.connection,
    asset_ids: Sequence[str],
) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    if not asset_ids:
        return refs

    ids_list = list(asset_ids)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT clip_id FROM processed_clips WHERE source_asset_id = ANY(%s)",
            (ids_list,),
        )
        clip_ids = [str(row[0]) for row in cur.fetchall() if row[0]]

        cur.execute(
            """
            SELECT COALESCE(NULLIF(raw_bucket, ''), 'vlm-raw') AS bucket, raw_key
            FROM raw_files
            WHERE asset_id = ANY(%s)
              AND raw_key IS NOT NULL
              AND raw_key <> ''
            """,
            (ids_list,),
        )
        for bucket, key in cur.fetchall():
            refs.add((str(bucket), str(key)))

        if clip_ids:
            cur.execute(
                """
                SELECT COALESCE(NULLIF(image_bucket, ''), 'vlm-raw') AS bucket, image_key
                FROM image_metadata
                WHERE (source_asset_id = ANY(%s) OR source_clip_id = ANY(%s))
                  AND image_key IS NOT NULL
                  AND image_key <> ''
                """,
                (ids_list, clip_ids),
            )
        else:
            cur.execute(
                """
                SELECT COALESCE(NULLIF(image_bucket, ''), 'vlm-raw') AS bucket, image_key
                FROM image_metadata
                WHERE source_asset_id = ANY(%s)
                  AND image_key IS NOT NULL
                  AND image_key <> ''
                """,
                (ids_list,),
            )
        for bucket, key in cur.fetchall():
            refs.add((str(bucket), str(key)))

        cur.execute(
            """
            SELECT COALESCE(NULLIF(labels_bucket, ''), 'vlm-labels') AS bucket, labels_key
            FROM labels
            WHERE asset_id = ANY(%s)
              AND labels_key IS NOT NULL
              AND labels_key <> ''
            """,
            (ids_list,),
        )
        for bucket, key in cur.fetchall():
            refs.add((str(bucket), str(key)))

        cur.execute(
            """
            SELECT COALESCE(NULLIF(processed_bucket, ''), 'vlm-processed') AS bucket, clip_key, label_key
            FROM processed_clips
            WHERE source_asset_id = ANY(%s)
            """,
            (ids_list,),
        )
        for bucket, clip_key, label_key in cur.fetchall():
            bucket_name = str(bucket)
            if clip_key:
                refs.add((bucket_name, str(clip_key)))
            if label_key:
                refs.add((bucket_name, str(label_key)))

        cur.execute(
            """
            SELECT COALESCE(NULLIF(d.dataset_bucket, ''), 'vlm-dataset') AS bucket, dc.dataset_key
            FROM dataset_clips dc
            JOIN processed_clips pc ON dc.clip_id = pc.clip_id
            JOIN datasets d ON dc.dataset_id = d.dataset_id
            WHERE pc.source_asset_id = ANY(%s)
              AND dc.dataset_key IS NOT NULL
              AND dc.dataset_key <> ''
            """,
            (ids_list,),
        )
        for bucket, dataset_key in cur.fetchall():
            refs.add((str(bucket), str(dataset_key)))

    return refs


def _delete_assets_from_db(conn: psycopg2.extensions.connection, asset_ids: Sequence[str]) -> None:
    if not asset_ids:
        return
    ids_list = list(asset_ids)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT clip_id FROM processed_clips WHERE source_asset_id = ANY(%s)",
            (ids_list,),
        )
        clip_ids = [str(row[0]) for row in cur.fetchall() if row[0]]

        cur.execute(
            """
            DELETE FROM dataset_clips
            WHERE clip_id IN (
                SELECT clip_id FROM processed_clips
                WHERE source_asset_id = ANY(%s)
            )
            """,
            (ids_list,),
        )

        if clip_ids:
            cur.execute(
                "DELETE FROM image_metadata WHERE source_asset_id = ANY(%s) OR source_clip_id = ANY(%s)",
                (ids_list, clip_ids),
            )
        else:
            cur.execute("DELETE FROM image_metadata WHERE source_asset_id = ANY(%s)", (ids_list,))

        cur.execute("DELETE FROM processed_clips WHERE source_asset_id = ANY(%s)", (ids_list,))
        cur.execute("DELETE FROM labels WHERE asset_id = ANY(%s)", (ids_list,))
        cur.execute("DELETE FROM video_metadata WHERE asset_id = ANY(%s)", (ids_list,))
        cur.execute("DELETE FROM raw_files WHERE asset_id = ANY(%s)", (ids_list,))


def _delete_all_from_db(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        for table in FULL_DELETE_TABLE_ORDER:
            cur.execute(f"DELETE FROM {table}")


def _collect_all_minio_refs(minio_client: "Minio", buckets: Iterable[str]) -> set[tuple[str, str]]:
    from minio.error import S3Error

    refs: set[tuple[str, str]] = set()
    for bucket in buckets:
        try:
            for obj in minio_client.list_objects(bucket, recursive=True):
                refs.add((bucket, obj.object_name))
        except S3Error as exc:
            print(f"[WARN] Skip bucket '{bucket}': {exc}")
    return refs


def _delete_minio_refs(
    minio_client: "Minio",
    refs: Iterable[tuple[str, str]],
    apply: bool,
) -> tuple[int, int, int]:
    from minio.error import S3Error

    deleted = 0
    missing = 0
    failed = 0

    for bucket, key in refs:
        if not apply:
            continue
        try:
            minio_client.remove_object(bucket, key)
            deleted += 1
        except S3Error as exc:
            code = (exc.code or "").strip()
            if code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                missing += 1
            else:
                failed += 1
                print(f"[MINIO][ERROR] {bucket}/{key}: {exc}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[MINIO][ERROR] {bucket}/{key}: {exc}")

    return deleted, missing, failed


def _print_table_counts(conn: psycopg2.extensions.connection, label: str) -> None:
    counts = {table: _table_count(conn, table) for table in FULL_DELETE_TABLE_ORDER}
    print(f"[{label}] {counts}")


def main() -> int:
    args = _parse_args()
    args.raw_key_like = _normalize_like(args.raw_key_like)
    args.source_path_like = _normalize_like(args.source_path_like)
    args.name_like = _normalize_like(args.name_like)

    if not args.all and not any(
        [args.created_date, args.asset_id, args.raw_key_like, args.source_path_like, args.name_like]
    ):
        print(
            "[ERROR] Provide at least one selector "
            "(--created-date / --asset-id / --raw-key-like / --source-path-like / --name-like), "
            "or use --all."
        )
        return 1

    dsn = args.dsn or os.getenv("DATAOPS_POSTGRES_DSN", "").strip() or None
    if not dsn:
        print("[ERROR] PostgreSQL DSN required. Set DATAOPS_POSTGRES_DSN env or pass --dsn.")
        return 1

    conn: psycopg2.extensions.connection | None = None
    minio_client: Any = None

    try:
        try:
            conn = _connect_pg(dsn)
        except psycopg2.Error as exc:
            print(f"[ERROR] PostgreSQL connection failed: {exc}")
            return 1

        if not args.no_minio:
            try:
                minio_client = _build_minio_client()
            except ModuleNotFoundError as exc:
                print(f"[ERROR] MinIO client dependency missing: {exc}")
                print("Install 'minio' package or use --no-minio.")
                return 1

        where_sql, where_params = _selector_parts(args)
        target_asset_ids: list[str] = []

        if not args.all:
            ids = _collect_asset_ids(conn, where_sql, where_params)
            print(f"[TARGET] assets={len(ids)}")
            target_asset_ids = sorted(set(ids))
            if not target_asset_ids:
                print("[INFO] No matching assets found. Nothing to delete.")
                return 0

        object_refs: set[tuple[str, str]] = set()
        if minio_client:
            if args.all:
                object_refs = _collect_all_minio_refs(minio_client, args.buckets)
            else:
                object_refs = _collect_object_refs_for_assets(conn, target_asset_ids)
            print(f"[TARGET][MINIO] objects={len(object_refs)}")

        _print_table_counts(conn, "BEFORE")

        if not args.yes:
            print("[DRY-RUN] Use --yes to apply deletion.")
            return 0

        try:
            if args.all:
                _delete_all_from_db(conn)
            else:
                _delete_assets_from_db(conn, target_asset_ids)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        minio_deleted = 0
        minio_missing = 0
        minio_failed = 0
        if minio_client and object_refs:
            minio_deleted, minio_missing, minio_failed = _delete_minio_refs(
                minio_client,
                sorted(object_refs),
                apply=True,
            )

        _print_table_counts(conn, "AFTER")
        if minio_client:
            print(
                f"[AFTER_MINIO] deleted={minio_deleted} missing={minio_missing} failed={minio_failed}"
            )

        if minio_failed > 0:
            return 2
        return 0
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
