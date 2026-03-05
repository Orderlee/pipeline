#!/usr/bin/env python3
"""
Purge pipeline data across local DuckDB, MotherDuck, and MinIO in one command.

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
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence

import duckdb


REPO_ROOT = Path(__file__).resolve().parent.parent

if TYPE_CHECKING:
    from minio import Minio


def _load_dotenv_if_present(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if key in os.environ and os.environ[key] != "":
            continue
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ[key] = value


def load_env_for_motherduck(repo_root: Optional[Path] = None, env_file: Optional[str] = None) -> None:
    if os.getenv("MOTHERDUCK_TOKEN"):
        return

    if repo_root is None:
        repo_root = REPO_ROOT

    if env_file:
        p = Path(env_file)
        if not p.is_absolute():
            p = repo_root / p
        _load_dotenv_if_present(p)
        return

    _load_dotenv_if_present(repo_root / ".env")
    if not os.getenv("MOTHERDUCK_TOKEN"):
        _load_dotenv_if_present(repo_root / "docker" / ".env")


def connect_motherduck(database: str, token: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    md_token = token or os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError("MotherDuck token is required. Set MOTHERDUCK_TOKEN or pass --token.")
    return duckdb.connect(f"md:{database}?motherduck_token={md_token}")


FULL_DELETE_TABLE_ORDER = [
    "dataset_clips",
    "datasets",
    "processed_clips",
    "labels",
    "image_metadata",
    "video_metadata",
    "raw_files",
]

DEFAULT_BUCKETS = ["vlm-raw", "vlm-labels", "vlm-processed", "vlm-dataset"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete pipeline data from local DuckDB + MotherDuck + MinIO."
    )
    parser.add_argument("--all", action="store_true", help="Delete all pipeline data.")
    parser.add_argument("--created-date", default=None, help="Filter by raw_files.created_at date (YYYY-MM-DD).")
    parser.add_argument("--asset-id", action="append", default=[], help="Filter by raw_files.asset_id. Repeatable.")
    parser.add_argument("--raw-key-like", default=None, help="Filter by raw_files.raw_key LIKE pattern.")
    parser.add_argument("--source-path-like", default=None, help="Filter by raw_files.source_path LIKE pattern.")
    parser.add_argument("--name-like", default=None, help="Filter by raw_files.original_name LIKE pattern.")
    parser.add_argument("--local-db-path", default="/data/pipeline.duckdb", help="Local DuckDB path.")
    parser.add_argument(
        "--local-lock-timeout-sec",
        type=float,
        default=30.0,
        help="Local DuckDB write lock wait timeout seconds (only used with --yes).",
    )
    parser.add_argument(
        "--local-lock-retry-interval-sec",
        type=float,
        default=1.0,
        help="Local DuckDB write lock retry interval seconds (only used with --yes).",
    )
    parser.add_argument("--db", default="pipeline_db", help="MotherDuck database name.")
    parser.add_argument("--token", default=None, help="MotherDuck token (or MOTHERDUCK_TOKEN env).")
    parser.add_argument("--env-file", default=None, help="Optional .env path for MotherDuck token.")
    parser.add_argument("--no-local", action="store_true", help="Skip local DuckDB deletion.")
    parser.add_argument("--no-motherduck", action="store_true", help="Skip MotherDuck deletion.")
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
        where.append("CAST(created_at AS DATE) = CAST(? AS DATE)")
        params.append(args.created_date)
    if args.raw_key_like:
        where.append("raw_key LIKE ?")
        params.append(args.raw_key_like)
    if args.source_path_like:
        where.append("source_path LIKE ?")
        params.append(args.source_path_like)
    if args.name_like:
        where.append("original_name LIKE ?")
        params.append(args.name_like)
    if args.asset_id:
        where.append("asset_id IN (" + ", ".join(["?"] * len(args.asset_id)) + ")")
        params.extend(args.asset_id)

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


def _connect_local_duckdb(
    db_path: str,
    write_mode: bool,
    lock_timeout_sec: float,
    retry_interval_sec: float,
) -> duckdb.DuckDBPyConnection:
    if not write_mode:
        return duckdb.connect(db_path, read_only=True)

    timeout_sec = max(0.0, float(lock_timeout_sec))
    interval_sec = max(0.1, float(retry_interval_sec))
    deadline = time.monotonic() + timeout_sec
    last_exc: Exception | None = None

    while True:
        try:
            return duckdb.connect(db_path)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if "Could not set lock on file" not in str(exc):
                raise
            if time.monotonic() >= deadline:
                break
            time.sleep(interval_sec)

    raise RuntimeError(
        f"Local DuckDB lock timeout after {timeout_sec:.1f}s: {db_path}\n"
        "Stop concurrent writer jobs (e.g., Dagster ingest/sync) and retry."
    ) from last_exc


def _table_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _collect_asset_ids(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    params: Sequence[str],
) -> list[str]:
    rows = con.execute(
        f"""
        SELECT asset_id
        FROM raw_files
        WHERE {where_sql}
        ORDER BY created_at
        """,
        list(params),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _collect_object_refs_for_assets(
    con: duckdb.DuckDBPyConnection,
    asset_ids: Sequence[str],
) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    if not asset_ids:
        return refs

    placeholders = ", ".join(["?"] * len(asset_ids))

    for bucket, key in con.execute(
        f"""
        SELECT COALESCE(NULLIF(raw_bucket, ''), 'vlm-raw') AS bucket, raw_key
        FROM raw_files
        WHERE asset_id IN ({placeholders})
          AND raw_key IS NOT NULL
          AND raw_key <> ''
        """,
        list(asset_ids),
    ).fetchall():
        refs.add((str(bucket), str(key)))

    for bucket, key in con.execute(
        f"""
        SELECT COALESCE(NULLIF(labels_bucket, ''), 'vlm-labels') AS bucket, labels_key
        FROM labels
        WHERE asset_id IN ({placeholders})
          AND labels_key IS NOT NULL
          AND labels_key <> ''
        """,
        list(asset_ids),
    ).fetchall():
        refs.add((str(bucket), str(key)))

    for bucket, clip_key, label_key in con.execute(
        f"""
        SELECT COALESCE(NULLIF(processed_bucket, ''), 'vlm-processed') AS bucket, clip_key, label_key
        FROM processed_clips
        WHERE source_asset_id IN ({placeholders})
        """,
        list(asset_ids),
    ).fetchall():
        bucket_name = str(bucket)
        if clip_key:
            refs.add((bucket_name, str(clip_key)))
        if label_key:
            refs.add((bucket_name, str(label_key)))

    for bucket, dataset_key in con.execute(
        f"""
        SELECT COALESCE(NULLIF(d.dataset_bucket, ''), 'vlm-dataset') AS bucket, dc.dataset_key
        FROM dataset_clips dc
        JOIN processed_clips pc ON dc.clip_id = pc.clip_id
        JOIN datasets d ON dc.dataset_id = d.dataset_id
        WHERE pc.source_asset_id IN ({placeholders})
          AND dc.dataset_key IS NOT NULL
          AND dc.dataset_key <> ''
        """,
        list(asset_ids),
    ).fetchall():
        refs.add((str(bucket), str(dataset_key)))

    return refs


def _delete_assets_from_db(con: duckdb.DuckDBPyConnection, asset_ids: Sequence[str]) -> None:
    if not asset_ids:
        return
    placeholders = ", ".join(["?"] * len(asset_ids))
    params = list(asset_ids)

    con.execute(
        f"""
        DELETE FROM dataset_clips
        WHERE clip_id IN (
            SELECT clip_id FROM processed_clips
            WHERE source_asset_id IN ({placeholders})
        )
        """,
        params,
    )
    con.execute(f"DELETE FROM processed_clips WHERE source_asset_id IN ({placeholders})", params)
    con.execute(f"DELETE FROM labels WHERE asset_id IN ({placeholders})", params)
    con.execute(f"DELETE FROM image_metadata WHERE asset_id IN ({placeholders})", params)
    con.execute(f"DELETE FROM video_metadata WHERE asset_id IN ({placeholders})", params)
    con.execute(f"DELETE FROM raw_files WHERE asset_id IN ({placeholders})", params)


def _delete_all_from_db(con: duckdb.DuckDBPyConnection) -> None:
    for table in FULL_DELETE_TABLE_ORDER:
        con.execute(f"DELETE FROM {table}")


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


def _print_table_counts(con: duckdb.DuckDBPyConnection, label: str) -> None:
    counts = {table: _table_count(con, table) for table in FULL_DELETE_TABLE_ORDER}
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

    local_con: duckdb.DuckDBPyConnection | None = None
    md_con: duckdb.DuckDBPyConnection | None = None
    minio_client: Any = None

    try:
        if not args.no_local:
            if not Path(args.local_db_path).exists():
                print(f"[ERROR] Local DuckDB not found: {args.local_db_path}")
                return 1
            local_con = _connect_local_duckdb(
                db_path=args.local_db_path,
                write_mode=bool(args.yes),
                lock_timeout_sec=args.local_lock_timeout_sec,
                retry_interval_sec=args.local_lock_retry_interval_sec,
            )

        if not args.no_motherduck:
            load_env_for_motherduck(repo_root=REPO_ROOT, env_file=args.env_file)
            md_con = connect_motherduck(args.db, args.token)

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
            if local_con:
                ids = _collect_asset_ids(local_con, where_sql, where_params)
                print(f"[TARGET][LOCAL] assets={len(ids)}")
                target_asset_ids.extend(ids)
            if md_con:
                ids = _collect_asset_ids(md_con, where_sql, where_params)
                print(f"[TARGET][MD] assets={len(ids)}")
                target_asset_ids.extend(ids)
            target_asset_ids = sorted(set(target_asset_ids))
            print(f"[TARGET][UNION] assets={len(target_asset_ids)}")
            if not target_asset_ids:
                print("[INFO] No matching assets found. Nothing to delete.")
                return 0

        object_refs: set[tuple[str, str]] = set()
        if minio_client:
            if args.all:
                object_refs = _collect_all_minio_refs(minio_client, args.buckets)
            else:
                if local_con:
                    object_refs |= _collect_object_refs_for_assets(local_con, target_asset_ids)
                if md_con:
                    object_refs |= _collect_object_refs_for_assets(md_con, target_asset_ids)
            print(f"[TARGET][MINIO] objects={len(object_refs)}")

        if local_con:
            _print_table_counts(local_con, "BEFORE_LOCAL")
        if md_con:
            _print_table_counts(md_con, "BEFORE_MD")

        if not args.yes:
            print("[DRY-RUN] Use --yes to apply deletion.")
            return 0

        if local_con:
            local_con.execute("BEGIN")
            try:
                if args.all:
                    _delete_all_from_db(local_con)
                else:
                    _delete_assets_from_db(local_con, target_asset_ids)
                local_con.execute("COMMIT")
            except Exception:
                local_con.execute("ROLLBACK")
                raise

        if md_con:
            md_con.execute("BEGIN")
            try:
                if args.all:
                    _delete_all_from_db(md_con)
                else:
                    _delete_assets_from_db(md_con, target_asset_ids)
                md_con.execute("COMMIT")
            except Exception:
                md_con.execute("ROLLBACK")
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

        if local_con:
            _print_table_counts(local_con, "AFTER_LOCAL")
        if md_con:
            _print_table_counts(md_con, "AFTER_MD")
        if minio_client:
            print(
                f"[AFTER_MINIO] deleted={minio_deleted} missing={minio_missing} failed={minio_failed}"
            )

        if minio_failed > 0:
            return 2
        return 0
    finally:
        if local_con:
            local_con.close()
        if md_con:
            md_con.close()


if __name__ == "__main__":
    raise SystemExit(main())
