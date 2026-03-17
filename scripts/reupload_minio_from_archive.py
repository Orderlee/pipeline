#!/usr/bin/env python3
"""Re-upload completed archive files to MinIO using raw_key from DuckDB."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import duckdb
from botocore.config import Config
from botocore.exceptions import ClientError


DB_PATH = os.getenv("DATAOPS_DUCKDB_PATH", "/data/pipeline.duckdb")
BUCKET = os.getenv("MINIO_REUPLOAD_BUCKET", "vlm-raw")
WORKERS = max(1, int(os.getenv("MINIO_REUPLOAD_WORKERS", "8")))
PROGRESS_EVERY = max(1, int(os.getenv("MINIO_REUPLOAD_PROGRESS_EVERY", "50")))


def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            max_pool_connections=max(32, WORKERS * 4),
        ),
    )


def _ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)


def _list_existing_keys(s3) -> set[str]:
    keys: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            keys.add(str(obj.get("Key", "")))
    return keys


def _load_candidates(existing_keys: set[str]):
    con = duckdb.connect(DB_PATH)
    rows = con.execute(
        """
        SELECT asset_id, archive_path, raw_key, COALESCE(file_size, 0) AS file_size
        FROM raw_files
        WHERE ingest_status = 'completed'
          AND archive_path IS NOT NULL
          AND length(trim(archive_path)) > 0
          AND raw_key IS NOT NULL
          AND length(trim(raw_key)) > 0
        ORDER BY created_at
        """
    ).fetchall()
    con.close()

    candidates = []
    skipped_existing = 0
    missing_source = 0
    for asset_id, archive_path, raw_key, file_size in rows:
        key = str(raw_key)
        if key in existing_keys:
            skipped_existing += 1
            continue
        src = Path(str(archive_path))
        if not src.exists() or not src.is_file():
            missing_source += 1
            continue
        candidates.append((str(asset_id), str(src), key, int(file_size)))

    return rows, candidates, skipped_existing, missing_source


def _upload_all(s3, candidates):
    uploaded = 0
    failed = 0
    uploaded_bytes = 0
    fail_samples: list[tuple[str, str, str]] = []

    def _upload_one(item):
        asset_id, src, key, file_size = item
        try:
            s3.upload_file(src, BUCKET, key)
            return True, file_size, asset_id, key, ""
        except Exception as exc:  # noqa: BLE001
            return False, 0, asset_id, key, str(exc)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(_upload_one, item) for item in candidates]
        total = len(futures)
        for idx, fut in enumerate(as_completed(futures), start=1):
            ok, file_size, asset_id, key, err = fut.result()
            if ok:
                uploaded += 1
                uploaded_bytes += int(file_size)
            else:
                failed += 1
                if len(fail_samples) < 20:
                    fail_samples.append((asset_id, key, err))

            if idx % PROGRESS_EVERY == 0 or idx == total:
                print(
                    f"[PROGRESS] {idx}/{total} uploaded={uploaded} "
                    f"failed={failed} uploaded_gb={uploaded_bytes / (1024**3):.3f}",
                    flush=True,
                )

    return uploaded, failed, uploaded_bytes, fail_samples


def main() -> int:
    s3 = _build_s3_client()
    _ensure_bucket(s3)

    existing = _list_existing_keys(s3)
    rows, candidates, skipped_existing, missing_source = _load_candidates(existing)

    print(
        "[INFO] "
        f"rows={len(rows)} existing={len(existing)} "
        f"skipped_existing={skipped_existing} missing_source={missing_source} "
        f"candidates={len(candidates)} workers={WORKERS}",
        flush=True,
    )
    for _, _, sample_key, _ in candidates[:5]:
        print(f"[INFO] sample_key={sample_key}", flush=True)

    if not candidates:
        print("[RESULT] nothing to upload", flush=True)
        return 0

    uploaded, failed, uploaded_bytes, fail_samples = _upload_all(s3, candidates)

    print("[RESULT] done", flush=True)
    print(
        f"[RESULT] uploaded={uploaded} failed={failed} "
        f"uploaded_gb={uploaded_bytes / (1024**3):.3f}",
        flush=True,
    )
    if fail_samples:
        print("[RESULT] fail_samples", flush=True)
        for asset_id, key, err in fail_samples:
            print(f"  - {asset_id} {key} {err}", flush=True)

    final_count = len(_list_existing_keys(s3))
    print(f"[RESULT] bucket_object_count={final_count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
