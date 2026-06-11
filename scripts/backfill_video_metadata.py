#!/usr/bin/env python3
"""Backfill missing `video_metadata` rows from `raw_files` video records.

Run inside docker container:
  python3 /src/vlm/scripts/backfill_video_metadata.py
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Any

import psycopg2

try:
    from vlm_pipeline.lib.env_utils import default_postgres_dsn
    from vlm_pipeline.lib.video_loader import load_video_once
except Exception as exc:  # pragma: no cover
    print(f"[ERROR] Failed to import vlm modules: {exc}", file=sys.stderr)
    sys.exit(1)


INSERT_SQL = """
INSERT INTO video_metadata (
    asset_id, width, height, duration_sec, fps,
    codec, bitrate, frame_count, has_audio,
    environment_type, daynight_type, outdoor_score,
    avg_brightness, env_method, extracted_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (asset_id) DO NOTHING
"""


def _existing_path(archive_path: str | None, source_path: str | None) -> str | None:
    if archive_path and os.path.isfile(archive_path):
        return archive_path
    if source_path and os.path.isfile(source_path):
        return source_path
    return None


def _missing_count(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM raw_files rf
            LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id
            WHERE rf.media_type = 'video' AND vm.asset_id IS NULL
            """
        )
        return cur.fetchone()[0]


def _iter_targets(
    conn: Any, statuses: set[str], limit: int | None
) -> list[tuple[str, str, str | None, str | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rf.asset_id, rf.ingest_status, rf.archive_path, rf.source_path
            FROM raw_files rf
            LEFT JOIN video_metadata vm ON rf.asset_id = vm.asset_id
            WHERE rf.media_type = 'video' AND vm.asset_id IS NULL
            ORDER BY CASE WHEN rf.ingest_status = 'completed' THEN 0 ELSE 1 END, rf.created_at
            """
        )
        rows = cur.fetchall()
    filtered = [r for r in rows if r[1] in statuses]
    if limit is not None:
        return filtered[:limit]
    return filtered


def _insert_meta(
    conn: Any,
    asset_id: str,
    meta: dict,
    now: datetime,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            INSERT_SQL,
            (
                asset_id,
                meta.get("width"),
                meta.get("height"),
                meta.get("duration_sec"),
                meta.get("fps"),
                meta.get("codec"),
                meta.get("bitrate"),
                meta.get("frame_count"),
                meta.get("has_audio", False),
                meta.get("environment_type"),
                meta.get("daynight_type"),
                meta.get("outdoor_score"),
                meta.get("avg_brightness"),
                meta.get("env_method"),
                meta.get("extracted_at", now),
            ),
        )
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN (default: $DATAOPS_POSTGRES_DSN)",
    )
    parser.add_argument(
        "--statuses",
        default="completed,failed",
        help="Comma-separated ingest_status values to include",
    )
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    parser.add_argument("--log-every", type=int, default=25, help="Progress logging interval")
    parser.add_argument("--max-errors", type=int, default=200, help="Abort after this many errors")
    parser.add_argument("--dry-run", action="store_true", help="Do not write DB")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    statuses = {s.strip() for s in args.statuses.split(",") if s.strip()}
    if not statuses:
        print("[ERROR] --statuses produced empty set", file=sys.stderr)
        return 2

    dsn = args.dsn or default_postgres_dsn()
    if not dsn:
        print(
            "[ERROR] No DSN: set DATAOPS_POSTGRES_DSN or pass --dsn",
            file=sys.stderr,
        )
        return 2

    try:
        conn = psycopg2.connect(dsn)
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] DB connection failed: {exc}", file=sys.stderr)
        return 2

    try:
        start_missing = _missing_count(conn)
        targets = _iter_targets(conn, statuses=statuses, limit=args.limit)
        total = len(targets)
        print(
            f"[INFO] start_missing={start_missing} targets={total} "
            f"statuses={sorted(statuses)} dry_run={args.dry_run}"
        )

        no_file = 0
        loaded = 0
        inserted_or_ignored = 0
        load_failed = 0
        insert_failed = 0
        err_samples: list[str] = []
        started_at = time.time()

        for idx, (asset_id, ingest_status, archive_path, source_path) in enumerate(targets, start=1):
            path = _existing_path(archive_path, source_path)
            if not path:
                no_file += 1
                if len(err_samples) < 15:
                    err_samples.append(f"no_file asset_id={asset_id} status={ingest_status}")
                continue

            try:
                meta = load_video_once(path, include_file_stream=False)["video_metadata"]
                loaded += 1
            except Exception as exc:  # pragma: no cover
                load_failed += 1
                if len(err_samples) < 15:
                    err_samples.append(f"load_failed asset_id={asset_id} err={type(exc).__name__}:{exc}")
                if (load_failed + insert_failed) >= args.max_errors:
                    print("[ERROR] too many errors during load; aborting early")
                    break
                continue

            if not args.dry_run:
                try:
                    _insert_meta(conn, asset_id, meta, datetime.now())
                    inserted_or_ignored += 1
                except Exception as exc:  # pragma: no cover
                    conn.rollback()
                    insert_failed += 1
                    if len(err_samples) < 15:
                        err_samples.append(
                            f"insert_failed asset_id={asset_id} err={type(exc).__name__}:{exc}"
                        )
                    if (load_failed + insert_failed) >= args.max_errors:
                        print("[ERROR] too many errors during insert; aborting early")
                        break
                    continue

            if idx % max(1, args.log_every) == 0:
                elapsed = time.time() - started_at
                print(
                    f"[PROGRESS] {idx}/{total} loaded={loaded} inserted_or_ignored={inserted_or_ignored} "
                    f"no_file={no_file} load_failed={load_failed} insert_failed={insert_failed} "
                    f"elapsed_sec={elapsed:.1f}"
                )

        end_missing = _missing_count(conn)
        elapsed = time.time() - started_at
        print(
            f"[DONE] targets={total} loaded={loaded} inserted_or_ignored={inserted_or_ignored} "
            f"no_file={no_file} load_failed={load_failed} insert_failed={insert_failed} "
            f"end_missing={end_missing} elapsed_sec={elapsed:.1f}"
        )
        if err_samples:
            print("[SAMPLES]")
            for line in err_samples:
                print(f"  - {line}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
