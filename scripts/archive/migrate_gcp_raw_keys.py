#!/usr/bin/env python3
"""Migrate legacy GCP raw_keys and MinIO objects from `gcp/...` to `<bucket>/...`.

Safety:
- Dry-run by default.
- Use --apply to perform MinIO copy/delete and raw_files.raw_key updates.

This script intentionally updates only raw_files.raw_key. Archive paths and
source_unit_name remain unchanged.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
from botocore.config import Config
from botocore.exceptions import ClientError


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src" / "python"))

from common.motherduck import connect_motherduck, load_env_for_motherduck  # noqa: E402


DEFAULT_LOCAL_DB_PATH = "/data/pipeline.duckdb"
DEFAULT_MINIO_BUCKET = "vlm-raw"
DEFAULT_OLD_PREFIX = "gcp/"


@dataclass(frozen=True)
class RawRow:
    asset_id: str
    old_key: str
    new_key: str


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    etag: str


@dataclass(frozen=True)
class PlanEntry:
    old_key: str
    new_key: str
    needs_db_update: bool
    needs_copy: bool
    delete_old: bool
    reason: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy GCP raw_keys in DuckDB/MotherDuck and vlm-raw MinIO objects."
    )
    parser.add_argument("--apply", action="store_true", help="Actually perform the migration.")
    parser.add_argument("--local-db-path", default=DEFAULT_LOCAL_DB_PATH, help="Local DuckDB path.")
    parser.add_argument("--db", default="pipeline_db", help="MotherDuck database name.")
    parser.add_argument("--token", default=None, help="MotherDuck token (or MOTHERDUCK_TOKEN env).")
    parser.add_argument("--env-file", default=None, help="Optional .env path for MotherDuck token.")
    parser.add_argument("--no-motherduck", action="store_true", help="Skip MotherDuck checks/updates.")
    parser.add_argument("--bucket", default=DEFAULT_MINIO_BUCKET, help="MinIO bucket containing raw objects.")
    parser.add_argument("--old-prefix", default=DEFAULT_OLD_PREFIX, help="Legacy raw_key/object prefix to migrate.")
    parser.add_argument(
        "--local-lock-timeout-sec",
        type=float,
        default=30.0,
        help="Local DuckDB write lock wait timeout when --apply is used.",
    )
    parser.add_argument(
        "--local-lock-retry-interval-sec",
        type=float,
        default=1.0,
        help="Local DuckDB write lock retry interval when --apply is used.",
    )
    return parser.parse_args()


def _normalize_etag(value: object) -> str:
    rendered = str(value or "").strip()
    if rendered.startswith('"') and rendered.endswith('"'):
        rendered = rendered[1:-1]
    return rendered


def _build_s3_client():
    import boto3

    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY must be set.")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            max_pool_connections=64,
        ),
    )


def _connect_local_duckdb(
    db_path: str,
    write_mode: bool,
    lock_timeout_sec: float,
    retry_interval_sec: float,
) -> duckdb.DuckDBPyConnection:
    if not write_mode:
        return duckdb.connect(db_path, read_only=True)

    deadline = time.monotonic() + max(0.0, lock_timeout_sec)
    interval_sec = max(0.1, retry_interval_sec)
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
        f"Local DuckDB lock timeout after {lock_timeout_sec:.1f}s: {db_path}\n"
        "Stop concurrent writer jobs and retry."
    ) from last_exc


def _fetch_raw_rows(con: duckdb.DuckDBPyConnection, old_prefix: str) -> list[RawRow]:
    rows = con.execute(
        """
        SELECT asset_id, raw_key
        FROM raw_files
        WHERE raw_key LIKE ?
        ORDER BY raw_key
        """,
        [f"{old_prefix}%"],
    ).fetchall()
    prefix_len = len(old_prefix)
    return [
        RawRow(asset_id=str(asset_id), old_key=str(raw_key), new_key=str(raw_key)[prefix_len:])
        for asset_id, raw_key in rows
    ]


def _fetch_db_collisions(con: duckdb.DuckDBPyConnection, old_prefix: str) -> list[tuple[str, str, str]]:
    prefix_len = len(old_prefix)
    return [
        (str(old_key), str(new_key), str(existing_asset_id))
        for old_key, new_key, existing_asset_id in con.execute(
            """
            WITH prefixed AS (
              SELECT asset_id, raw_key, substr(raw_key, ?) AS new_key
              FROM raw_files
              WHERE raw_key LIKE ?
            ),
            plain AS (
              SELECT asset_id, raw_key
              FROM raw_files
              WHERE raw_key NOT LIKE ?
            )
            SELECT prefixed.raw_key AS old_key, prefixed.new_key, plain.asset_id AS existing_asset_id
            FROM prefixed
            JOIN plain ON plain.raw_key = prefixed.new_key
            ORDER BY prefixed.raw_key
            """,
            [prefix_len + 1, f"{old_prefix}%", f"{old_prefix}%"],
        ).fetchall()
    ]


def _head_object(s3, bucket: str, key: str) -> ObjectInfo | None:
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code") or "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return ObjectInfo(
        key=key,
        size=int(response.get("ContentLength") or 0),
        etag=_normalize_etag(response.get("ETag")),
    )


def _list_old_objects(s3, bucket: str, old_prefix: str) -> dict[str, ObjectInfo]:
    out: dict[str, ObjectInfo] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=old_prefix):
        for obj in page.get("Contents", []):
            key = str(obj.get("Key") or "")
            if not key:
                continue
            out[key] = ObjectInfo(
                key=key,
                size=int(obj.get("Size") or 0),
                etag=_normalize_etag(obj.get("ETag")),
            )
    return out


def _same_object(lhs: ObjectInfo | None, rhs: ObjectInfo | None) -> bool:
    if lhs is None or rhs is None:
        return False
    if lhs.size != rhs.size:
        return False
    if lhs.etag and rhs.etag:
        if lhs.etag == rhs.etag:
            return True
        # MinIO server-side copy can legitimately rewrite multipart ETags while
        # preserving object bytes. When either side is multipart, treat
        # size-equality as sufficient for idempotent migration/retry behavior.
        if "-" in lhs.etag or "-" in rhs.etag:
            return True
        return False
    return True


def _build_plan(
    rows: list[RawRow],
    old_objects: dict[str, ObjectInfo],
    s3,
    bucket: str,
    old_prefix: str,
) -> tuple[list[PlanEntry], list[str], list[str], list[str]]:
    plan: list[PlanEntry] = []
    divergent_collisions: list[str] = []
    missing_old_objects: list[str] = []
    orphan_old_objects: list[str] = []

    db_old_keys = {row.old_key for row in rows}
    for row in rows:
        old_obj = old_objects.get(row.old_key)
        if old_obj is None:
            missing_old_objects.append(row.old_key)
            continue
        new_obj = _head_object(s3, bucket, row.new_key)
        if new_obj is None:
            plan.append(
                PlanEntry(
                    old_key=row.old_key,
                    new_key=row.new_key,
                    needs_db_update=True,
                    needs_copy=True,
                    delete_old=True,
                    reason="copy_old_to_new",
                )
            )
            continue
        if not _same_object(old_obj, new_obj):
            divergent_collisions.append(row.old_key)
            continue
        plan.append(
            PlanEntry(
                old_key=row.old_key,
                new_key=row.new_key,
                needs_db_update=True,
                needs_copy=False,
                delete_old=True,
                reason="reuse_existing_new",
            )
        )

    for old_key, old_obj in old_objects.items():
        if old_key in db_old_keys:
            continue
        new_key = old_key[len(old_prefix):] if old_key.startswith(old_prefix) else old_key
        new_obj = _head_object(s3, bucket, new_key)
        if new_obj is None:
            orphan_old_objects.append(old_key)
            continue
        if not _same_object(old_obj, new_obj):
            divergent_collisions.append(old_key)
            continue
        plan.append(
            PlanEntry(
                old_key=old_key,
                new_key=new_key,
                needs_db_update=False,
                needs_copy=False,
                delete_old=True,
                reason="delete_orphan_old",
            )
        )

    return plan, divergent_collisions, missing_old_objects, orphan_old_objects


def _copy_and_verify(s3, bucket: str, entry: PlanEntry) -> None:
    s3.copy_object(
        Bucket=bucket,
        Key=entry.new_key,
        CopySource={"Bucket": bucket, "Key": entry.old_key},
    )
    old_obj = _head_object(s3, bucket, entry.old_key)
    new_obj = _head_object(s3, bucket, entry.new_key)
    if not _same_object(old_obj, new_obj):
        raise RuntimeError(
            f"Copy verification failed for {entry.old_key} -> {entry.new_key}: "
            f"old={old_obj} new={new_obj}"
        )


def _apply_updates(con: duckdb.DuckDBPyConnection, rows: list[RawRow], old_prefix: str) -> int:
    if not rows:
        return 0
    con.execute("BEGIN TRANSACTION")
    try:
        con.executemany(
            "UPDATE raw_files SET raw_key = ? WHERE raw_key = ?",
            [(row.new_key, row.old_key) for row in rows],
        )
        con.execute("COMMIT")
    except Exception:  # noqa: BLE001
        con.execute("ROLLBACK")
        raise
    remaining = int(con.execute("SELECT COUNT(*) FROM raw_files WHERE raw_key LIKE ?", [f"{old_prefix}%"]).fetchone()[0])
    return remaining


def _key_set(rows: list[RawRow] | None) -> set[str]:
    return {row.old_key for row in rows or []}


def _print_summary(
    local_rows: list[RawRow],
    local_collisions: list[tuple[str, str, str]],
    md_rows: list[RawRow] | None,
    md_collisions: list[tuple[str, str, str]] | None,
    md_key_mismatch_count: int,
    old_objects: dict[str, ObjectInfo],
    plan: list[PlanEntry],
    divergent_collisions: list[str],
    missing_old_objects: list[str],
    orphan_old_objects: list[str],
) -> None:
    print(f"[SUMMARY] local_db_old_rows={len(local_rows)}")
    if md_rows is not None:
        print(f"[SUMMARY] motherduck_old_rows={len(md_rows)}")
    print(f"[SUMMARY] minio_old_objects={len(old_objects)}")
    print(f"[SUMMARY] db_collisions_local={len(local_collisions)}")
    if md_collisions is not None:
        print(f"[SUMMARY] db_collisions_motherduck={len(md_collisions)}")
        print(f"[SUMMARY] motherduck_key_mismatch_count={md_key_mismatch_count}")
    print(f"[SUMMARY] divergent_collisions={len(divergent_collisions)}")
    print(f"[SUMMARY] missing_old_objects={len(missing_old_objects)}")
    print(f"[SUMMARY] orphan_old_without_new={len(orphan_old_objects)}")
    print(f"[SUMMARY] plan_entries={len(plan)}")
    print(f"[SUMMARY] plan_copy={sum(1 for entry in plan if entry.needs_copy)}")
    print(f"[SUMMARY] plan_db_update={sum(1 for entry in plan if entry.needs_db_update)}")
    print(f"[SUMMARY] plan_delete_old={sum(1 for entry in plan if entry.delete_old)}")
    for entry in plan[:10]:
        print(f"[SAMPLE] {entry.reason}: {entry.old_key} -> {entry.new_key}")
    for old_key, new_key, asset_id in local_collisions[:10]:
        print(f"[COLLISION][local] old={old_key} new={new_key} existing_asset_id={asset_id}")
    if md_collisions:
        for old_key, new_key, asset_id in md_collisions[:10]:
            print(f"[COLLISION][motherduck] old={old_key} new={new_key} existing_asset_id={asset_id}")
    for key in divergent_collisions[:10]:
        print(f"[DIVERGENT] {key}")
    for key in missing_old_objects[:10]:
        print(f"[MISSING_OLD] {key}")
    for key in orphan_old_objects[:10]:
        print(f"[ORPHAN_OLD_WITHOUT_NEW] {key}")


def main() -> int:
    args = _parse_args()
    old_prefix = str(args.old_prefix or DEFAULT_OLD_PREFIX)
    if not old_prefix.endswith("/"):
        raise SystemExit("--old-prefix must end with '/'")

    local_con = _connect_local_duckdb(
        args.local_db_path,
        write_mode=args.apply,
        lock_timeout_sec=args.local_lock_timeout_sec,
        retry_interval_sec=args.local_lock_retry_interval_sec,
    )
    md_con = None
    try:
        load_env_for_motherduck(REPO_ROOT, env_file=args.env_file)
        if not args.no_motherduck:
            md_con = connect_motherduck(args.db, token=args.token)

        local_rows = _fetch_raw_rows(local_con, old_prefix)
        local_collisions = _fetch_db_collisions(local_con, old_prefix)

        md_rows: list[RawRow] | None = None
        md_collisions: list[tuple[str, str, str]] | None = None
        md_key_mismatch_count = 0
        if md_con is not None:
            md_rows = _fetch_raw_rows(md_con, old_prefix)
            md_collisions = _fetch_db_collisions(md_con, old_prefix)
            md_key_mismatch_count = len(_key_set(local_rows) ^ _key_set(md_rows))

        s3 = _build_s3_client()
        old_objects = _list_old_objects(s3, args.bucket, old_prefix)
        plan, divergent_collisions, missing_old_objects, orphan_old_objects = _build_plan(
            rows=local_rows,
            old_objects=old_objects,
            s3=s3,
            bucket=args.bucket,
            old_prefix=old_prefix,
        )

        _print_summary(
            local_rows=local_rows,
            local_collisions=local_collisions,
            md_rows=md_rows,
            md_collisions=md_collisions,
            md_key_mismatch_count=md_key_mismatch_count,
            old_objects=old_objects,
            plan=plan,
            divergent_collisions=divergent_collisions,
            missing_old_objects=missing_old_objects,
            orphan_old_objects=orphan_old_objects,
        )

        hard_failures = (
            list(local_collisions)
            or list(md_collisions or [])
            or md_key_mismatch_count
            or divergent_collisions
            or missing_old_objects
            or orphan_old_objects
        )
        if not args.apply:
            print("[RESULT] dry-run complete")
            return 1 if hard_failures else 0
        if hard_failures:
            raise RuntimeError("Refusing to apply because dry-run reported collisions or missing objects.")

        print("[APPLY] starting MinIO copy/reuse phase")
        for idx, entry in enumerate(plan, start=1):
            if entry.needs_copy:
                _copy_and_verify(s3, args.bucket, entry)
            if idx % 100 == 0 or idx == len(plan):
                print(f"[APPLY] prepared {idx}/{len(plan)}")

        db_rows_to_update = [entry for entry in plan if entry.needs_db_update]
        local_update_rows = [RawRow(asset_id="", old_key=entry.old_key, new_key=entry.new_key) for entry in db_rows_to_update]

        print(f"[APPLY] updating local DuckDB rows={len(local_update_rows)}")
        local_remaining = _apply_updates(local_con, local_update_rows, old_prefix)
        print(f"[APPLY] local DuckDB remaining_gcp_rows={local_remaining}")

        if md_con is not None:
            print(f"[APPLY] updating MotherDuck rows={len(local_update_rows)}")
            md_remaining = _apply_updates(md_con, local_update_rows, old_prefix)
            print(f"[APPLY] MotherDuck remaining_gcp_rows={md_remaining}")

        print("[APPLY] deleting old MinIO objects")
        delete_batch: list[dict[str, str]] = []
        deleted = 0
        for idx, entry in enumerate((entry for entry in plan if entry.delete_old), start=1):
            delete_batch.append({"Key": entry.old_key})
            if len(delete_batch) >= 1000:
                s3.delete_objects(Bucket=args.bucket, Delete={"Objects": delete_batch, "Quiet": True})
                deleted += len(delete_batch)
                delete_batch = []
            if idx % 100 == 0:
                print(f"[APPLY] queued_delete {idx}")
        if delete_batch:
            s3.delete_objects(Bucket=args.bucket, Delete={"Objects": delete_batch, "Quiet": True})
            deleted += len(delete_batch)
        print(f"[APPLY] deleted_old_objects={deleted}")

        remaining_old_objects = len(_list_old_objects(s3, args.bucket, old_prefix))
        print(f"[RESULT] remaining_old_objects={remaining_old_objects}")
        print("[RESULT] apply complete")
        return 0
    finally:
        try:
            local_con.close()
        except Exception:  # noqa: BLE001
            pass
        if md_con is not None:
            try:
                md_con.close()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    raise SystemExit(main())
