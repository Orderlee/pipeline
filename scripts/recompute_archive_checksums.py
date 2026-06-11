#!/usr/bin/env python3
"""Recompute `raw_files.checksum` from archived files.

Run inside container:
  python3 /scripts/recompute_archive_checksums.py --apply

Env:
  DATAOPS_POSTGRES_DSN  postgresql://user:pass@host:port/dbname
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psycopg2

try:
    from vlm_pipeline.lib.checksum import sha256sum
except Exception as exc:  # pragma: no cover
    print(f"[ERROR] Failed to import checksum helper: {exc}", file=sys.stderr)
    sys.exit(1)

try:
    from vlm_pipeline.lib.env_utils import default_postgres_dsn
except Exception as exc:  # pragma: no cover
    print(f"[ERROR] Failed to import env_utils: {exc}", file=sys.stderr)
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN. Falls back to DATAOPS_POSTGRES_DSN env var.",
    )
    parser.add_argument(
        "--scope",
        default="all",
        choices=["all", "duplicate_checksums", "duplicate_archive_paths", "suspects"],
        help="Target row scope",
    )
    parser.add_argument(
        "--statuses",
        default="",
        help="Optional comma-separated ingest_status filter. Empty means all statuses.",
    )
    parser.add_argument("--workers", type=int, default=4, help="Checksum worker count")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    parser.add_argument("--log-every", type=int, default=100, help="Progress logging interval")
    parser.add_argument("--max-errors", type=int, default=200, help="Abort after this many errors")
    parser.add_argument("--apply", action="store_true", help="Write recomputed checksum to DB")
    parser.add_argument("--dry-run", action="store_true", help="Alias for omitting --apply (no DB writes)")
    return parser.parse_args()


def resolve_dsn(args: argparse.Namespace) -> str:
    dsn = args.dsn or default_postgres_dsn()
    if not dsn:
        print(
            "[ERROR] No DSN configured. Set DATAOPS_POSTGRES_DSN env var or pass --dsn.",
            file=sys.stderr,
        )
        sys.exit(2)
    return dsn


def connect_pg(dsn: str) -> psycopg2.extensions.connection:
    retry_count = max(0, int(os.getenv("PG_LOCK_RETRY_COUNT", "5")))
    delay_s = max(0.5, float(os.getenv("PG_LOCK_RETRY_DELAY_S", "1.0")))
    last_exc: Exception | None = None
    for attempt in range(retry_count + 1):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retry_count:
                break
            time.sleep(delay_s)
    if last_exc is None:
        raise RuntimeError("PostgreSQL connection failed unexpectedly")
    raise last_exc


def load_targets(
    conn: psycopg2.extensions.connection,
    scope: str,
    statuses: set[str],
    limit: int | None,
) -> list[tuple[str, str, str | None, str | None, int | None]]:
    where_clauses = [
        "archive_path IS NOT NULL",
        "length(trim(archive_path)) > 0",
    ]
    if scope == "duplicate_checksums":
        where_clauses.append(
            """
            checksum IN (
                SELECT checksum
                FROM raw_files
                WHERE checksum IS NOT NULL AND length(trim(checksum)) > 0
                GROUP BY checksum
                HAVING COUNT(*) > 1
            )
            """
        )
    elif scope == "duplicate_archive_paths":
        where_clauses.append(
            """
            archive_path IN (
                SELECT archive_path
                FROM raw_files
                WHERE archive_path IS NOT NULL AND length(trim(archive_path)) > 0
                GROUP BY archive_path
                HAVING COUNT(*) > 1
            )
            """
        )
    elif scope == "suspects":
        where_clauses.append(
            """
            (
                checksum IN (
                    SELECT checksum
                    FROM raw_files
                    WHERE checksum IS NOT NULL AND length(trim(checksum)) > 0
                    GROUP BY checksum
                    HAVING COUNT(*) > 1
                )
                OR archive_path IN (
                    SELECT archive_path
                    FROM raw_files
                    WHERE archive_path IS NOT NULL AND length(trim(archive_path)) > 0
                    GROUP BY archive_path
                    HAVING COUNT(*) > 1
                )
            )
            """
        )

    sql = f"""
        SELECT asset_id, raw_key, ingest_status, archive_path, file_size, checksum
        FROM raw_files
        WHERE {" AND ".join(where_clauses)}
        ORDER BY created_at, asset_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    filtered: list[tuple[str, str, str | None, str | None, int | None]] = []
    for asset_id, raw_key, ingest_status, archive_path, file_size, checksum in rows:
        normalized_status = str(ingest_status or "").strip()
        if statuses and normalized_status not in statuses:
            continue
        filtered.append(
            (
                str(asset_id),
                str(raw_key or ""),
                str(archive_path or ""),
                str(checksum or ""),
                int(file_size) if file_size is not None else None,
            )
        )
    if limit is not None:
        return filtered[:limit]
    return filtered


def recompute_one(
    row: tuple[str, str, str, str, int | None],
) -> dict[str, object]:
    asset_id, raw_key, archive_path, old_checksum, old_file_size = row
    path = Path(archive_path)
    if not path.is_file():
        return {
            "status": "missing_file",
            "asset_id": asset_id,
            "raw_key": raw_key,
            "archive_path": archive_path,
        }

    new_checksum = sha256sum(path)
    file_size = path.stat().st_size
    return {
        "status": "ok",
        "asset_id": asset_id,
        "raw_key": raw_key,
        "archive_path": archive_path,
        "old_checksum": old_checksum,
        "new_checksum": new_checksum,
        "checksum_changed": new_checksum != old_checksum,
        "old_file_size": old_file_size,
        "new_file_size": file_size,
        "file_size_changed": old_file_size != file_size,
    }


def main() -> int:
    args = parse_args()
    statuses = {item.strip() for item in str(args.statuses or "").split(",") if item.strip()}
    workers = max(1, min(32, int(args.workers)))
    apply = args.apply and not args.dry_run

    dsn = resolve_dsn(args)

    read_conn = connect_pg(dsn)
    try:
        targets = load_targets(read_conn, scope=args.scope, statuses=statuses, limit=args.limit)
    finally:
        read_conn.close()

    print(
        f"[INFO] scope={args.scope} targets={len(targets)} workers={workers} "
        f"statuses={sorted(statuses) if statuses else 'ALL'} apply={apply}"
    )
    if not targets:
        print("[DONE] nothing to process")
        return 0

    started_at = time.time()
    results: list[dict[str, object]] = []
    updates: list[tuple[str, int, str]] = []
    duplicate_map: dict[str, list[str]] = defaultdict(list)
    missing_file = 0
    checksum_changed = 0
    file_size_changed = 0
    unchanged = 0
    failed = 0
    err_samples: list[str] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(recompute_one, row) for row in targets]
        total = len(futures)
        for idx, future in enumerate(as_completed(futures), start=1):
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if len(err_samples) < 20:
                    err_samples.append(f"worker_failed:{type(exc).__name__}:{exc}")
                if failed >= args.max_errors:
                    print("[ERROR] too many checksum worker errors; aborting early")
                    break
                continue

            status = str(result.get("status"))
            results.append(result)
            if status == "missing_file":
                missing_file += 1
                if len(err_samples) < 20:
                    err_samples.append(
                        f"missing_file asset_id={result['asset_id']} path={result['archive_path']}"
                    )
            elif status == "ok":
                new_checksum = str(result["new_checksum"])
                raw_key = str(result["raw_key"])
                duplicate_map[new_checksum].append(raw_key)
                if bool(result["checksum_changed"]):
                    checksum_changed += 1
                    updates.append(
                        (
                            new_checksum,
                            int(result["new_file_size"]),
                            str(result["asset_id"]),
                        )
                    )
                else:
                    unchanged += 1
                if bool(result["file_size_changed"]):
                    file_size_changed += 1
            else:
                failed += 1
                if len(err_samples) < 20:
                    err_samples.append(
                        f"unexpected_status asset_id={result.get('asset_id')} status={status}"
                    )

            if idx % max(1, args.log_every) == 0 or idx == total:
                elapsed = time.time() - started_at
                print(
                    f"[PROGRESS] {idx}/{total} unchanged={unchanged} "
                    f"checksum_changed={checksum_changed} file_size_changed={file_size_changed} "
                    f"missing_file={missing_file} failed={failed} elapsed_sec={elapsed:.1f}",
                    flush=True,
                )

    duplicate_groups = [
        (checksum, sorted(keys))
        for checksum, keys in duplicate_map.items()
        if len(keys) > 1
    ]
    duplicate_groups.sort(key=lambda item: len(item[1]), reverse=True)
    duplicate_rows = sum(len(keys) for _, keys in duplicate_groups)

    print(
        f"[SUMMARY] scanned={len(results)} unchanged={unchanged} "
        f"checksum_changed={checksum_changed} file_size_changed={file_size_changed} "
        f"missing_file={missing_file} failed={failed}"
    )
    print(
        f"[SUMMARY] duplicate_checksum_groups={len(duplicate_groups)} "
        f"duplicate_checksum_rows={duplicate_rows}"
    )
    for checksum, keys in duplicate_groups[:10]:
        print(
            f"[DUPLICATE] checksum={checksum} count={len(keys)} samples={keys[:5]}",
            flush=True,
        )

    if apply and updates:
        print(f"[INFO] applying checksum updates={len(updates)}", flush=True)
        write_conn = connect_pg(dsn)
        try:
            with write_conn.cursor() as cur:
                for new_checksum, new_file_size, asset_id in updates:
                    cur.execute(
                        """
                        UPDATE raw_files
                        SET checksum = %s, file_size = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE asset_id = %s
                        """,
                        (new_checksum, new_file_size, asset_id),
                    )
            write_conn.commit()
        except Exception:
            write_conn.rollback()
            raise
        finally:
            write_conn.close()
        print(f"[DONE] applied_updates={len(updates)}", flush=True)
    elif apply:
        print("[DONE] no checksum changes to apply", flush=True)
    else:
        print("[DONE] dry-run complete", flush=True)

    if err_samples:
        print("[SAMPLES]")
        for item in err_samples:
            print(f"  - {item}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
