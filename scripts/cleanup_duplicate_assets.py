#!/usr/bin/env python3
"""Remove duplicate raw assets globally by checksum.

Behavior:
1. Find duplicate checksum groups across all `raw_files`.
2. Choose one canonical row per checksum group.
3. Rebuild keeper `error_message` as `duplicate_skipped_in_manifest:...`.
4. Delete duplicate rows from DB and duplicate objects from MinIO/archive.

Run inside container:
  python3 /scripts/cleanup_duplicate_assets.py --db /data/pipeline.duckdb --apply
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import boto3
import duckdb
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
    parser.add_argument("--db", default="/data/pipeline.duckdb", help="DuckDB file path")
    parser.add_argument("--bucket", default="vlm-raw", help="MinIO bucket name")
    parser.add_argument("--apply", action="store_true", help="Apply cleanup changes")
    parser.add_argument("--log-every", type=int, default=100, help="Progress log interval")
    return parser.parse_args()


def connect_with_retry(db_path: str, *, read_only: bool) -> duckdb.DuckDBPyConnection:
    retry_count = max(0, int(os.getenv("DUCKDB_LOCK_RETRY_COUNT", "20")))
    delay_ms = max(50, int(os.getenv("DUCKDB_LOCK_RETRY_DELAY_MS", "100")))
    last_exc: Exception | None = None
    for attempt in range(retry_count + 1):
        try:
            con = duckdb.connect(db_path, read_only=read_only)
            con.execute("PRAGMA disable_progress_bar")
            return con
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retry_count:
                break
            time.sleep(delay_ms / 1000.0)
    if last_exc is None:
        raise RuntimeError("DuckDB connection failed unexpectedly")
    raise last_exc


def load_duplicate_rows(con: duckdb.DuckDBPyConnection) -> list[DuplicateRow]:
    rows = con.execute(
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
    ).fetchall()
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


def schema_sql() -> str:
    candidates = [
        Path(__file__).resolve().parents[1] / "src" / "vlm_pipeline" / "sql" / "schema.sql",
        Path.cwd() / "src" / "vlm_pipeline" / "sql" / "schema.sql",
        Path("/app/src/vlm_pipeline/sql/schema.sql"),
        Path("/src/vlm/vlm_pipeline/sql/schema.sql"),
    ]
    for schema_path in candidates:
        if schema_path.exists():
            return schema_path.read_text(encoding="utf-8")
    raise FileNotFoundError(
        "schema.sql not found in any known path: "
        + ", ".join(str(path) for path in candidates)
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] DB not found: {db_path}", file=sys.stderr)
        return 2

    read_con = connect_with_retry(str(db_path), read_only=True)
    duplicate_rows = load_duplicate_rows(read_con)
    read_con.close()

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

    if not args.apply:
        print("[DONE] dry-run complete")
        return 0

    keep_asset_ids = {row.asset_id for row in duplicate_rows} - set(remove_asset_ids)
    keeper_error_map = {asset_id: marker for marker, asset_id in keeper_updates}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_db_path = db_path.with_name(f"{db_path.stem}.dedup_tmp{db_path.suffix}")
    backup_db_path = db_path.with_name(f"{db_path.stem}.pre_dedup_{timestamp}{db_path.suffix}")
    db_wal_path = Path(f"{db_path}.wal")
    tmp_db_wal_path = Path(f"{tmp_db_path}.wal")
    backup_db_wal_path = Path(f"{backup_db_path}.wal")
    if tmp_db_path.exists():
        tmp_db_path.unlink()
    if tmp_db_wal_path.exists():
        tmp_db_wal_path.unlink()

    source_con = connect_with_retry(str(db_path), read_only=True)
    raw_rows = source_con.execute(
        """
        SELECT
            asset_id, source_path, original_name, media_type, file_size, checksum, phash, dup_group_id,
            archive_path, raw_bucket, raw_key, ingest_batch_id, transfer_tool, ingest_status,
            error_message, created_at, updated_at
        FROM raw_files
        ORDER BY created_at, asset_id
        """
    ).fetchall()
    image_rows = source_con.execute(
        """
        SELECT
            image_id, source_asset_id, source_clip_id, image_bucket, image_key, image_role,
            frame_index, frame_sec, checksum, file_size,
            width, height, color_mode, bit_depth, has_alpha, orientation, extracted_at
        FROM image_metadata
        """
    ).fetchall()
    video_rows = source_con.execute(
        """
        SELECT
            asset_id, width, height, duration_sec, fps, codec, bitrate, frame_count, has_audio,
            environment_type, daynight_type, outdoor_score, avg_brightness, env_method, extracted_at,
            frame_extract_status, frame_extract_count, frame_extract_error, frame_extracted_at
        FROM video_metadata
        """
    ).fetchall()
    label_rows = source_con.execute(
        """
        SELECT asset_id, label_id, labels_bucket, labels_key, label_format, label_tool, event_count,
               label_status, created_at
        FROM labels
        """
    ).fetchall()
    processed_rows = source_con.execute(
        """
        SELECT clip_id, source_asset_id, source_label_id, event_index, checksum, file_size,
               processed_bucket, clip_key, label_key, width, height, codec, process_status, created_at
        FROM processed_clips
        """
    ).fetchall()
    dataset_rows = source_con.execute(
        """
        SELECT dataset_id, name, version, config, split_ratio, dataset_bucket, dataset_prefix,
               build_status, created_at
        FROM datasets
        """
    ).fetchall()
    dataset_clip_rows = source_con.execute(
        "SELECT dataset_id, clip_id, split, dataset_key FROM dataset_clips"
    ).fetchall()
    source_con.close()

    kept_raw_rows = []
    for row in raw_rows:
        asset_id = str(row[0])
        if asset_id in remove_asset_ids:
            continue
        mutable = list(row)
        if asset_id in keeper_error_map:
            mutable[14] = keeper_error_map[asset_id]
            mutable[16] = datetime.now()
        kept_raw_rows.append(tuple(mutable))

    kept_asset_ids = {str(row[0]) for row in kept_raw_rows}
    kept_label_rows = [row for row in label_rows if str(row[0]) in kept_asset_ids]
    kept_label_ids = {str(row[1]) for row in kept_label_rows}
    kept_processed_rows = [
        row
        for row in processed_rows
        if str(row[1]) in kept_asset_ids and (not row[2] or str(row[2]) in kept_label_ids)
    ]
    kept_clip_ids = {str(row[0]) for row in kept_processed_rows}
    kept_dataset_clip_rows = [row for row in dataset_clip_rows if str(row[1]) in kept_clip_ids]

    target_con = duckdb.connect(str(tmp_db_path))
    target_con.execute("PRAGMA disable_progress_bar")
    target_con.execute(schema_sql())
    target_con.executemany(
        """
        INSERT INTO raw_files (
            asset_id, source_path, original_name, media_type, file_size, checksum, phash, dup_group_id,
            archive_path, raw_bucket, raw_key, ingest_batch_id, transfer_tool, ingest_status,
            error_message, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        kept_raw_rows,
    )
    if video_rows:
        target_con.executemany(
            """
            INSERT INTO video_metadata (
                asset_id, width, height, duration_sec, fps, codec, bitrate, frame_count, has_audio,
                environment_type, daynight_type, outdoor_score, avg_brightness, env_method, extracted_at,
                frame_extract_status, frame_extract_count, frame_extract_error, frame_extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row for row in video_rows if str(row[0]) in kept_asset_ids],
        )
    if kept_label_rows:
        target_con.executemany(
            """
            INSERT INTO labels (
                asset_id, label_id, labels_bucket, labels_key, label_format, label_tool, event_count,
                label_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            kept_label_rows,
        )
    if kept_processed_rows:
        target_con.executemany(
            """
            INSERT INTO processed_clips (
                clip_id, source_asset_id, source_label_id, event_index, checksum, file_size,
                processed_bucket, clip_key, label_key, width, height, codec, process_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            kept_processed_rows,
        )
    if image_rows:
        target_con.executemany(
            """
            INSERT INTO image_metadata (
                image_id, source_asset_id, source_clip_id, image_bucket, image_key, image_role,
                frame_index, frame_sec, checksum, file_size,
                width, height, color_mode, bit_depth, has_alpha, orientation, extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row
                for row in image_rows
                if str(row[1]) in kept_asset_ids and (row[2] is None or str(row[2]) in kept_clip_ids)
            ],
        )
    if dataset_rows:
        target_con.executemany(
            """
            INSERT INTO datasets (
                dataset_id, name, version, config, split_ratio, dataset_bucket, dataset_prefix,
                build_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            dataset_rows,
        )
    if kept_dataset_clip_rows:
        target_con.executemany(
            "INSERT INTO dataset_clips (dataset_id, clip_id, split, dataset_key) VALUES (?, ?, ?, ?)",
            kept_dataset_clip_rows,
        )
    # Flush changes into the main DB file so the temp WAL does not need to move with it.
    target_con.execute("CHECKPOINT")
    target_con.close()

    shutil.move(str(db_path), str(backup_db_path))
    if db_wal_path.exists():
        shutil.move(str(db_wal_path), str(backup_db_wal_path))
    shutil.move(str(tmp_db_path), str(db_path))

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

    verify_con = connect_with_retry(str(db_path), read_only=True)
    remaining_dup_groups = verify_con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT checksum
            FROM raw_files
            WHERE checksum IS NOT NULL AND length(trim(checksum)) > 0
            GROUP BY checksum
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    marker_mismatch_groups = verify_con.execute(
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
    ).fetchone()[0]
    verify_con.close()

    print(
        f"[DONE] removed_rows={len(remove_rows)} "
        f"deleted_minio_objects={len(remove_raw_keys)} "
        f"deleted_archive_files={archive_deleted} archive_missing={archive_missing} "
        f"remaining_dup_groups={remaining_dup_groups} marker_mismatch_rows_in_dup_groups={marker_mismatch_groups}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
