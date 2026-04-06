#!/usr/bin/env python3
"""Clean up production label/preprocess data for fixed source prefixes.

Scope:
- Delete only label/preprocess artifacts in local production DuckDB + production MinIO.
- Keep raw_files, video_metadata, vlm-raw, NAS archive, staging, MotherDuck untouched.

Targets are intentionally fixed to:
- khon-kaen-rtsp-bucket/
- adlib-hotel-202512/

Safety:
- Dry-run by default.
- Use --apply to execute deletions.
- Intended to run after production writers are stopped.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import boto3
import duckdb
from botocore.config import Config
from botocore.exceptions import ClientError


ALLOWED_TARGET_PREFIXES = (
    "khon-kaen-rtsp-bucket/",
    "adlib-hotel-202512/",
)
KNOWN_BUCKET_PREFIXES = (
    "vlm-raw/",
    "vlm-labels/",
    "vlm-processed/",
    "vlm-dataset/",
)
PROTECTED_SCOPE = "label_preprocess"
PROTECTED_IMAGE_ROLES = ("processed_clip_frame", "raw_video_frame")
REPORT_TABLES = (
    "raw_files",
    "video_metadata",
    "labels",
    "processed_clips",
    "image_metadata",
    "image_labels",
    "dataset_clips",
)
PREFIX_SCAN_BUCKETS = ("vlm-labels", "vlm-processed")
INTERNAL_SNAPSHOT_KEYS = {
    "label_asset_ids",
    "processed_asset_ids",
    "relevant_asset_ids",
    "label_ids",
    "clip_ids",
    "image_ids",
    "minio_refs",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-prefix",
        action="append",
        default=[],
        help="Repeatable. Allowed values are fixed to khon-kaen-rtsp-bucket/ and adlib-hotel-202512/.",
    )
    parser.add_argument(
        "--labels-prefix",
        action="append",
        default=[],
        help="Repeatable. Delete vlm-labels/<prefix> data plus dependent DB rows.",
    )
    parser.add_argument(
        "--processed-prefix",
        action="append",
        default=[],
        help="Repeatable. Delete vlm-processed/<prefix> data plus related DB rows.",
    )
    parser.add_argument(
        "--scope",
        default=PROTECTED_SCOPE,
        choices=[PROTECTED_SCOPE],
        help="Cleanup scope. Only label_preprocess is supported.",
    )
    parser.add_argument("--db", default="/data/pipeline.duckdb", help="Production DuckDB path.")
    parser.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT") or "http://127.0.0.1:9000",
        help="Production MinIO endpoint.",
    )
    parser.add_argument(
        "--minio-access-key",
        default=os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or "minioadmin",
        help="MinIO access key.",
    )
    parser.add_argument(
        "--minio-secret-key",
        default=os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin",
        help="MinIO secret key.",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional JSON report path. Defaults to docs/references when available, else scripts/reports.",
    )
    parser.add_argument(
        "--lock-timeout-sec",
        type=float,
        default=30.0,
        help="DuckDB write-lock wait timeout when --apply is used.",
    )
    parser.add_argument(
        "--lock-retry-interval-sec",
        type=float,
        default=1.0,
        help="DuckDB write-lock retry interval when --apply is used.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", dest="apply", action="store_false", help="Preview only (default).")
    mode.add_argument("--apply", dest="apply", action="store_true", help="Execute deletions.")
    parser.set_defaults(apply=False)
    return parser.parse_args()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normalize_target_prefix(raw_value: str) -> str:
    normalized = str(raw_value or "").strip().strip("/")
    if not normalized:
        raise ValueError("empty_target_prefix")
    for bucket_prefix in KNOWN_BUCKET_PREFIXES:
        if normalized.startswith(bucket_prefix.rstrip("/")):
            normalized = normalized[len(bucket_prefix.rstrip("/")) :].lstrip("/")
            break
    normalized = normalized.strip("/")
    if not normalized:
        raise ValueError("empty_target_prefix")
    canonical = f"{normalized}/"
    if canonical not in ALLOWED_TARGET_PREFIXES:
        raise ValueError(
            f"unsupported_target_prefix:{raw_value} (allowed={', '.join(ALLOWED_TARGET_PREFIXES)})"
        )
    return canonical


def _resolve_target_prefixes(raw_values: Sequence[str]) -> list[str]:
    if raw_values:
        values = raw_values
    else:
        values = list(ALLOWED_TARGET_PREFIXES)
    return sorted({_normalize_target_prefix(value) for value in values})


def _resolve_scope_prefixes(args: argparse.Namespace) -> dict[str, list[str]]:
    if args.target_prefix and (args.labels_prefix or args.processed_prefix):
        raise ValueError("use_target_prefix_or_scope_prefixes_not_both")
    if args.target_prefix:
        resolved = _resolve_target_prefixes(args.target_prefix)
        return {"labels": list(resolved), "processed": list(resolved)}
    if args.labels_prefix or args.processed_prefix:
        return {
            "labels": _resolve_target_prefixes(args.labels_prefix) if args.labels_prefix else [],
            "processed": _resolve_target_prefixes(args.processed_prefix) if args.processed_prefix else [],
        }
    resolved = list(ALLOWED_TARGET_PREFIXES)
    return {"labels": list(resolved), "processed": list(resolved)}


def _default_report_dir() -> Path:
    file_based = Path(__file__).resolve().parent.parent / "docs" / "references"
    cwd_based = Path.cwd() / "docs" / "references"
    scripts_reports = Path(__file__).resolve().parent / "reports"
    for candidate in (file_based, cwd_based):
        if candidate.exists():
            return candidate
    scripts_reports.mkdir(parents=True, exist_ok=True)
    return scripts_reports


def _resolve_report_path(raw_path: str | None, *, apply: bool) -> Path:
    if raw_path:
        path = Path(raw_path)
    else:
        suffix = "apply" if apply else "dry_run"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"production_label_preprocess_cleanup_{suffix}_{timestamp}.json"
        path = _default_report_dir() / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect_duckdb(
    db_path: str,
    *,
    write_mode: bool,
    lock_timeout_sec: float,
    retry_interval_sec: float,
) -> duckdb.DuckDBPyConnection:
    if not write_mode:
        con = duckdb.connect(db_path, read_only=True)
        con.execute("PRAGMA disable_progress_bar")
        return con

    timeout_sec = max(0.0, float(lock_timeout_sec))
    interval_sec = max(0.1, float(retry_interval_sec))
    deadline = time.monotonic() + timeout_sec
    last_exc: Exception | None = None
    while True:
        try:
            con = duckdb.connect(db_path)
            con.execute("PRAGMA disable_progress_bar")
            return con
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if "Could not set lock on file" not in str(exc):
                raise
            if time.monotonic() >= deadline:
                break
            time.sleep(interval_sec)
    raise RuntimeError(
        f"DuckDB write lock timeout after {timeout_sec:.1f}s: {db_path}. "
        "Stop production writers (dispatch/label/process/yolo/sync) and retry."
    ) from last_exc


def _build_s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _table_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _placeholders(items: Sequence[str]) -> str:
    return ", ".join(["?"] * len(items))


def _select_scalar(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: Sequence[Any],
) -> int:
    return int(con.execute(sql, list(params)).fetchone()[0] or 0)


def _target_sql_for_prefix(prefix: str) -> tuple[str, list[str]]:
    return (
        "COALESCE(raw_key, '') LIKE ? OR COALESCE(archive_path, '') LIKE ?",
        [f"{prefix}%", f"/nas/archive/{prefix}%"],
    )


def _target_sql_for_prefixes(prefixes: Sequence[str]) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []
    for prefix in prefixes:
        clause, clause_params = _target_sql_for_prefix(prefix)
        clauses.append(f"({clause})")
        params.extend(clause_params)
    return " OR ".join(clauses) if clauses else "1=0", params


def _load_target_asset_ids(con: duckdb.DuckDBPyConnection, prefixes: Sequence[str]) -> list[str]:
    where_sql, params = _target_sql_for_prefixes(prefixes)
    rows = con.execute(
        f"""
        SELECT asset_id
        FROM raw_files
        WHERE {where_sql}
        ORDER BY created_at, asset_id
        """,
        params,
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


def _load_target_clip_ids(con: duckdb.DuckDBPyConnection, asset_ids: Sequence[str]) -> list[str]:
    if not asset_ids:
        return []
    rows = con.execute(
        f"""
        SELECT clip_id
        FROM processed_clips
        WHERE source_asset_id IN ({_placeholders(asset_ids)})
        ORDER BY created_at, clip_id
        """,
        list(asset_ids),
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


def _load_target_label_ids(con: duckdb.DuckDBPyConnection, asset_ids: Sequence[str]) -> list[str]:
    if not asset_ids:
        return []
    rows = con.execute(
        f"""
        SELECT label_id
        FROM labels
        WHERE asset_id IN ({_placeholders(asset_ids)})
        ORDER BY created_at, label_id
        """,
        list(asset_ids),
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


def _load_clip_ids_for_label_ids(
    con: duckdb.DuckDBPyConnection,
    label_ids: Sequence[str],
) -> list[str]:
    if not label_ids:
        return []
    rows = con.execute(
        f"""
        SELECT clip_id
        FROM processed_clips
        WHERE source_label_id IN ({_placeholders(label_ids)})
        ORDER BY created_at, clip_id
        """,
        list(label_ids),
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


def _load_source_asset_ids_for_clip_ids(
    con: duckdb.DuckDBPyConnection,
    clip_ids: Sequence[str],
) -> list[str]:
    if not clip_ids:
        return []
    rows = con.execute(
        f"""
        SELECT DISTINCT source_asset_id
        FROM processed_clips
        WHERE clip_id IN ({_placeholders(clip_ids)})
          AND source_asset_id IS NOT NULL
        ORDER BY source_asset_id
        """,
        list(clip_ids),
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


def _load_target_image_ids(
    con: duckdb.DuckDBPyConnection,
    asset_ids: Sequence[str],
    clip_ids: Sequence[str],
) -> list[str]:
    clauses: list[str] = []
    params: list[str] = []
    if clip_ids:
        clauses.append(f"source_clip_id IN ({_placeholders(clip_ids)})")
        params.extend(clip_ids)
    if asset_ids:
        role_placeholders = _placeholders(PROTECTED_IMAGE_ROLES)
        clauses.append(
            f"(source_asset_id IN ({_placeholders(asset_ids)}) "
            f"AND COALESCE(image_role, '') IN ({role_placeholders}))"
        )
        params.extend(asset_ids)
        params.extend(PROTECTED_IMAGE_ROLES)
    if not clauses:
        return []
    rows = con.execute(
        f"""
        SELECT image_id
        FROM image_metadata
        WHERE {' OR '.join(clauses)}
        ORDER BY extracted_at, image_id
        """,
        params,
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


def _count_target_video_metadata(con: duckdb.DuckDBPyConnection, asset_ids: Sequence[str]) -> int:
    if not asset_ids:
        return 0
    return _select_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM video_metadata
        WHERE asset_id IN ({_placeholders(asset_ids)})
        """,
        list(asset_ids),
    )


def _count_target_labels(con: duckdb.DuckDBPyConnection, label_ids: Sequence[str]) -> int:
    if not label_ids:
        return 0
    return _select_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM labels
        WHERE label_id IN ({_placeholders(label_ids)})
        """,
        list(label_ids),
    )


def _count_target_processed_clips(con: duckdb.DuckDBPyConnection, clip_ids: Sequence[str]) -> int:
    if not clip_ids:
        return 0
    return _select_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM processed_clips
        WHERE clip_id IN ({_placeholders(clip_ids)})
        """,
        list(clip_ids),
    )


def _count_target_image_metadata(con: duckdb.DuckDBPyConnection, image_ids: Sequence[str]) -> int:
    if not image_ids:
        return 0
    return _select_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM image_metadata
        WHERE image_id IN ({_placeholders(image_ids)})
        """,
        list(image_ids),
    )


def _count_target_image_labels(
    con: duckdb.DuckDBPyConnection,
    clip_ids: Sequence[str],
    image_ids: Sequence[str],
) -> int:
    clauses: list[str] = []
    params: list[str] = []
    if clip_ids:
        clauses.append(f"source_clip_id IN ({_placeholders(clip_ids)})")
        params.extend(clip_ids)
    if image_ids:
        clauses.append(f"image_id IN ({_placeholders(image_ids)})")
        params.extend(image_ids)
    if not clauses:
        return 0
    return _select_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM image_labels
        WHERE {' OR '.join(clauses)}
        """,
        params,
    )


def _count_target_dataset_clips(con: duckdb.DuckDBPyConnection, clip_ids: Sequence[str]) -> int:
    if not clip_ids:
        return 0
    return _select_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM dataset_clips
        WHERE clip_id IN ({_placeholders(clip_ids)})
        """,
        list(clip_ids),
    )


def _collect_requested_scope_breakdown(
    con: duckdb.DuckDBPyConnection,
    prefix: str,
    *,
    scope_type: str,
) -> dict[str, Any]:
    where_sql, params = _target_sql_for_prefix(prefix)
    asset_ids = [
        str(row[0])
        for row in con.execute(
            f"""
            SELECT asset_id
            FROM raw_files
            WHERE {where_sql}
            ORDER BY created_at, asset_id
            """,
            params,
        ).fetchall()
        if row[0]
    ]
    label_ids = _load_target_label_ids(con, asset_ids) if scope_type == "labels" else []
    explicit_clip_ids = _load_target_clip_ids(con, asset_ids) if scope_type == "processed" else []
    dependent_clip_ids = _load_clip_ids_for_label_ids(con, label_ids) if scope_type == "labels" else []
    clip_ids = sorted(set(explicit_clip_ids) | set(dependent_clip_ids))
    image_asset_ids = sorted(set(asset_ids) | set(_load_source_asset_ids_for_clip_ids(con, clip_ids)))
    image_ids = _load_target_image_ids(con, image_asset_ids, clip_ids)
    sample_rows = con.execute(
        f"""
        SELECT asset_id, source_unit_name, raw_key, archive_path
        FROM raw_files
        WHERE {where_sql}
        ORDER BY created_at, asset_id
        LIMIT 5
        """,
        params,
    ).fetchall()
    return {
        "scope_type": scope_type,
        "target_prefix": prefix,
        "asset_count": len(asset_ids),
        "counts": {
            "raw_files": len(asset_ids),
            "video_metadata": _count_target_video_metadata(con, asset_ids),
            "labels": _count_target_labels(con, label_ids),
            "processed_clips": _count_target_processed_clips(con, clip_ids),
            "image_metadata": _count_target_image_metadata(con, image_ids),
            "image_labels": _count_target_image_labels(con, clip_ids, image_ids),
            "dataset_clips": _count_target_dataset_clips(con, clip_ids),
        },
        "samples": [
            {
                "asset_id": row[0],
                "source_unit_name": row[1],
                "raw_key": row[2],
                "archive_path": row[3],
            }
            for row in sample_rows
        ],
    }


def _collect_image_role_breakdown(
    con: duckdb.DuckDBPyConnection,
    asset_ids: Sequence[str],
) -> list[dict[str, Any]]:
    if not asset_ids:
        return []
    rows = con.execute(
        f"""
        SELECT COALESCE(image_role, '<null>') AS image_role, COUNT(*)
        FROM image_metadata
        WHERE source_asset_id IN ({_placeholders(asset_ids)})
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """,
        list(asset_ids),
    ).fetchall()
    return [{"image_role": row[0], "count": int(row[1])} for row in rows]


def _collect_minio_refs(
    con: duckdb.DuckDBPyConnection,
    label_ids: Sequence[str],
    clip_ids: Sequence[str],
    image_ids: Sequence[str],
) -> dict[str, set[str]]:
    refs: dict[str, set[str]] = defaultdict(set)
    if label_ids:
        for bucket, key in con.execute(
            f"""
            SELECT COALESCE(NULLIF(labels_bucket, ''), 'vlm-labels') AS bucket, labels_key
            FROM labels
            WHERE label_id IN ({_placeholders(label_ids)})
              AND labels_key IS NOT NULL
              AND labels_key <> ''
            """,
            list(label_ids),
        ).fetchall():
            refs[str(bucket)].add(str(key))

    if clip_ids or image_ids:
        clauses: list[str] = []
        params: list[str] = []
        if clip_ids:
            clauses.append(f"source_clip_id IN ({_placeholders(clip_ids)})")
            params.extend(clip_ids)
        if image_ids:
            clauses.append(f"image_id IN ({_placeholders(image_ids)})")
            params.extend(image_ids)
        for bucket, key in con.execute(
            f"""
            SELECT COALESCE(NULLIF(labels_bucket, ''), 'vlm-labels') AS bucket, labels_key
            FROM image_labels
            WHERE {' OR '.join(clauses)}
              AND labels_key IS NOT NULL
              AND labels_key <> ''
            """,
            params,
        ).fetchall():
            refs[str(bucket)].add(str(key))

    if clip_ids:
        for bucket, key in con.execute(
            f"""
            SELECT COALESCE(NULLIF(processed_bucket, ''), 'vlm-processed') AS bucket, clip_key
            FROM processed_clips
            WHERE clip_id IN ({_placeholders(clip_ids)})
              AND clip_key IS NOT NULL
              AND clip_key <> ''
            """,
            list(clip_ids),
        ).fetchall():
            refs[str(bucket)].add(str(key))

        for bucket, key in con.execute(
            f"""
            SELECT COALESCE(NULLIF(d.dataset_bucket, ''), 'vlm-dataset') AS bucket, dc.dataset_key
            FROM dataset_clips dc
            JOIN datasets d ON d.dataset_id = dc.dataset_id
            WHERE dc.clip_id IN ({_placeholders(clip_ids)})
              AND dc.dataset_key IS NOT NULL
              AND dc.dataset_key <> ''
            """,
            list(clip_ids),
        ).fetchall():
            refs[str(bucket)].add(str(key))

    if image_ids:
        for bucket, key in con.execute(
            f"""
            SELECT COALESCE(NULLIF(image_bucket, ''), 'vlm-processed') AS bucket, image_key
            FROM image_metadata
            WHERE image_id IN ({_placeholders(image_ids)})
              AND image_key IS NOT NULL
              AND image_key <> ''
            """,
            list(image_ids),
        ).fetchall():
            refs[str(bucket)].add(str(key))

    return refs


def _summarize_minio_refs(refs: dict[str, set[str]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for bucket in sorted(refs):
        keys = sorted(refs[bucket])
        summary.append(
            {
                "bucket": bucket,
                "candidate_count": len(keys),
                "sample_keys": keys[:5],
            }
        )
    return summary


def _scan_prefix_objects(
    s3_client,
    scan_plan: Sequence[tuple[str, Sequence[str]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket, prefixes in scan_plan:
        for prefix in prefixes:
            count = 0
            samples: list[str] = []
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects = page.get("Contents", [])
                count += len(objects)
                for obj in objects:
                    if len(samples) < 5:
                        samples.append(str(obj["Key"]))
            rows.append(
                {
                    "bucket": bucket,
                    "prefix": prefix,
                    "count": count,
                    "samples": samples,
                }
            )
    return rows


def _collect_scope_snapshot(
    con: duckdb.DuckDBPyConnection,
    s3_client,
    scope_prefixes: dict[str, Sequence[str]],
) -> dict[str, Any]:
    label_prefixes = list(scope_prefixes.get("labels", []))
    processed_prefixes = list(scope_prefixes.get("processed", []))

    label_asset_ids = _load_target_asset_ids(con, label_prefixes)
    processed_asset_ids = _load_target_asset_ids(con, processed_prefixes)
    label_ids = _load_target_label_ids(con, label_asset_ids)
    dependent_clip_ids = _load_clip_ids_for_label_ids(con, label_ids)
    explicit_clip_ids = _load_target_clip_ids(con, processed_asset_ids)
    clip_ids = sorted(set(explicit_clip_ids) | set(dependent_clip_ids))
    clip_asset_ids = _load_source_asset_ids_for_clip_ids(con, clip_ids)
    relevant_asset_ids = sorted(set(label_asset_ids) | set(processed_asset_ids) | set(clip_asset_ids))
    image_asset_ids = sorted(set(processed_asset_ids) | set(clip_asset_ids))
    image_ids = _load_target_image_ids(con, image_asset_ids, clip_ids)
    refs = _collect_minio_refs(con, label_ids, clip_ids, image_ids)
    requested_scope_breakdown = {
        "labels": [
            _collect_requested_scope_breakdown(con, prefix, scope_type="labels")
            for prefix in label_prefixes
        ],
        "processed": [
            _collect_requested_scope_breakdown(con, prefix, scope_type="processed")
            for prefix in processed_prefixes
        ],
    }
    target_counts = {
        "raw_files": len(relevant_asset_ids),
        "video_metadata": _count_target_video_metadata(con, relevant_asset_ids),
        "labels": _count_target_labels(con, label_ids),
        "processed_clips": _count_target_processed_clips(con, clip_ids),
        "image_metadata": _count_target_image_metadata(con, image_ids),
        "image_labels": _count_target_image_labels(con, clip_ids, image_ids),
        "dataset_clips": _count_target_dataset_clips(con, clip_ids),
    }
    table_total_counts = {table: _table_count(con, table) for table in REPORT_TABLES}
    processed_scan_prefixes = sorted(set(label_prefixes) | set(processed_prefixes))
    scan_plan: list[tuple[str, Sequence[str]]] = []
    if label_prefixes:
        scan_plan.append(("vlm-labels", label_prefixes))
    if processed_scan_prefixes:
        scan_plan.append(("vlm-processed", processed_scan_prefixes))
    return {
        "label_asset_ids": label_asset_ids,
        "processed_asset_ids": processed_asset_ids,
        "relevant_asset_ids": relevant_asset_ids,
        "label_ids": label_ids,
        "clip_ids": clip_ids,
        "image_ids": image_ids,
        "requested_scope_breakdown": requested_scope_breakdown,
        "target_counts": target_counts,
        "table_total_counts": table_total_counts,
        "delete_candidates": {
            "label_asset_id_count": len(label_asset_ids),
            "processed_asset_id_count": len(processed_asset_ids),
            "asset_id_count": len(relevant_asset_ids),
            "label_id_count": len(label_ids),
            "clip_id_count": len(clip_ids),
            "image_id_count": len(image_ids),
            "bucket_candidates": _summarize_minio_refs(refs),
        },
        "image_role_breakdown": _collect_image_role_breakdown(con, image_asset_ids),
        "prefix_object_counts": _scan_prefix_objects(s3_client, scan_plan),
        "minio_refs": refs,
    }


def _delete_dataset_clips(con: duckdb.DuckDBPyConnection, clip_ids: Sequence[str]) -> None:
    if not clip_ids:
        return
    con.execute(
        f"DELETE FROM dataset_clips WHERE clip_id IN ({_placeholders(clip_ids)})",
        list(clip_ids),
    )


def _delete_image_labels(
    con: duckdb.DuckDBPyConnection,
    clip_ids: Sequence[str],
    image_ids: Sequence[str],
) -> None:
    clauses: list[str] = []
    params: list[str] = []
    if clip_ids:
        clauses.append(f"source_clip_id IN ({_placeholders(clip_ids)})")
        params.extend(clip_ids)
    if image_ids:
        clauses.append(f"image_id IN ({_placeholders(image_ids)})")
        params.extend(image_ids)
    if not clauses:
        return
    con.execute(f"DELETE FROM image_labels WHERE {' OR '.join(clauses)}", params)


def _delete_image_metadata(con: duckdb.DuckDBPyConnection, image_ids: Sequence[str]) -> None:
    if not image_ids:
        return
    con.execute(
        f"DELETE FROM image_metadata WHERE image_id IN ({_placeholders(image_ids)})",
        list(image_ids),
    )


def _detach_image_metadata_clip_refs(
    con: duckdb.DuckDBPyConnection,
    clip_ids: Sequence[str],
) -> None:
    if not clip_ids:
        return
    con.execute(
        f"""
        UPDATE image_metadata
        SET source_clip_id = NULL
        WHERE source_clip_id IN ({_placeholders(clip_ids)})
        """,
        list(clip_ids),
    )


def _delete_processed_clips(con: duckdb.DuckDBPyConnection, clip_ids: Sequence[str]) -> None:
    if not clip_ids:
        return
    con.execute(
        f"DELETE FROM processed_clips WHERE clip_id IN ({_placeholders(clip_ids)})",
        list(clip_ids),
    )


def _delete_labels(con: duckdb.DuckDBPyConnection, label_ids: Sequence[str]) -> None:
    if not label_ids:
        return
    con.execute(
        f"DELETE FROM labels WHERE label_id IN ({_placeholders(label_ids)})",
        list(label_ids),
    )


def _is_missing_client_error(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code", "")).strip()
    status = str(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", "")).strip()
    return code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"} or status == "404"


def _delete_minio_refs(s3_client, refs: dict[str, set[str]]) -> dict[str, Any]:
    deleted = 0
    missing = 0
    failed = 0
    per_bucket: dict[str, dict[str, int]] = defaultdict(lambda: {"deleted": 0, "missing": 0, "failed": 0})
    samples = {"missing": [], "failed": []}

    for bucket in sorted(refs):
        for key in sorted(refs[bucket]):
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                if _is_missing_client_error(exc):
                    missing += 1
                    per_bucket[bucket]["missing"] += 1
                    if len(samples["missing"]) < 10:
                        samples["missing"].append({"bucket": bucket, "key": key})
                    continue
                failed += 1
                per_bucket[bucket]["failed"] += 1
                if len(samples["failed"]) < 10:
                    samples["failed"].append({"bucket": bucket, "key": key, "error": str(exc)})
                continue
            except Exception as exc:  # noqa: BLE001
                failed += 1
                per_bucket[bucket]["failed"] += 1
                if len(samples["failed"]) < 10:
                    samples["failed"].append({"bucket": bucket, "key": key, "error": str(exc)})
                continue

            try:
                s3_client.delete_object(Bucket=bucket, Key=key)
                deleted += 1
                per_bucket[bucket]["deleted"] += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                per_bucket[bucket]["failed"] += 1
                if len(samples["failed"]) < 10:
                    samples["failed"].append({"bucket": bucket, "key": key, "error": str(exc)})

    return {
        "deleted": deleted,
        "missing": missing,
        "failed": failed,
        "per_bucket": [{"bucket": bucket, **stats} for bucket, stats in sorted(per_bucket.items())],
        "samples": samples,
    }


def _run_write_stage(
    con: duckdb.DuckDBPyConnection,
    *,
    stage_name: str,
    operations: Sequence[Any],
) -> None:
    con.execute("BEGIN TRANSACTION")
    try:
        for operation in operations:
            operation()
        con.execute("COMMIT")
    except Exception as exc:  # noqa: BLE001
        con.execute("ROLLBACK")
        raise RuntimeError(f"cleanup_stage_failed:{stage_name}") from exc


def _build_verification(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_counts = before["target_counts"]
    after_counts = after["target_counts"]
    minio_scan = after["prefix_object_counts"]
    zero_tables = ["labels", "processed_clips", "image_metadata", "image_labels", "dataset_clips"]
    zero_checks = {table: after_counts.get(table, 0) == 0 for table in zero_tables}
    preserved_checks = {
        "raw_files_preserved": after_counts.get("raw_files", 0) == before_counts.get("raw_files", 0),
        "video_metadata_preserved": after_counts.get("video_metadata", 0)
        == before_counts.get("video_metadata", 0),
    }
    minio_checks = {
        f"{row['bucket']}::{row['prefix']}": row["count"] == 0
        for row in minio_scan
    }
    return {
        "after_target_counts": after_counts,
        "after_prefix_object_counts": minio_scan,
        "zero_checks": zero_checks,
        "preserved_checks": preserved_checks,
        "minio_checks": minio_checks,
        "all_passed": all(zero_checks.values()) and all(preserved_checks.values()) and all(minio_checks.values()),
    }


def _strip_internal_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in snapshot.items()
        if key not in INTERNAL_SNAPSHOT_KEYS
    }


def _print_summary(report: dict[str, Any]) -> None:
    mode = report["mode"]
    requested = report["requested_prefixes"]
    print(
        f"[MODE] {mode} scope={report['scope']} "
        f"labels={', '.join(requested['labels']) or '-'} "
        f"processed={', '.join(requested['processed']) or '-'}"
    )
    counts = report["before"]["target_counts"]
    print(
        "[TARGETS] "
        f"label_asset_ids={report['before']['delete_candidates']['label_asset_id_count']} "
        f"processed_asset_ids={report['before']['delete_candidates']['processed_asset_id_count']} "
        f"asset_ids={report['before']['delete_candidates']['asset_id_count']} "
        f"label_ids={report['before']['delete_candidates']['label_id_count']} "
        f"clip_ids={report['before']['delete_candidates']['clip_id_count']} "
        f"image_ids={report['before']['delete_candidates']['image_id_count']}"
    )
    print(
        "[BEFORE] "
        f"labels={counts['labels']} processed_clips={counts['processed_clips']} "
        f"image_metadata={counts['image_metadata']} image_labels={counts['image_labels']} "
        f"dataset_clips={counts['dataset_clips']}"
    )
    for bucket_row in report["before"]["delete_candidates"]["bucket_candidates"]:
        print(
            f"[MINIO_PLAN] bucket={bucket_row['bucket']} "
            f"candidate_count={bucket_row['candidate_count']} "
            f"samples={bucket_row['sample_keys'][:2]}"
        )
    if mode == "apply":
        apply_stats = report["apply"]["minio"]
        print(
            "[MINIO_APPLY] "
            f"deleted={apply_stats['deleted']} missing={apply_stats['missing']} failed={apply_stats['failed']}"
        )
        print(f"[VERIFY] all_passed={report['verification']['all_passed']}")
    print(f"[REPORT] {report['report_path']}")


def main() -> int:
    args = parse_args()
    requested_prefixes = _resolve_scope_prefixes(args)
    report_path = _resolve_report_path(args.report_path, apply=bool(args.apply))
    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"db_not_found:{db_path}")

    s3_client = _build_s3_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key)
    before_con = _connect_duckdb(
        str(db_path),
        write_mode=False,
        lock_timeout_sec=args.lock_timeout_sec,
        retry_interval_sec=args.lock_retry_interval_sec,
    )
    try:
        before = _collect_scope_snapshot(before_con, s3_client, requested_prefixes)
    finally:
        before_con.close()

    report: dict[str, Any] = {
        "generated_at": _now_iso(),
        "mode": "apply" if args.apply else "dry_run",
        "scope": args.scope,
        "requested_prefixes": requested_prefixes,
        "target_prefixes": sorted(set(requested_prefixes["labels"]) | set(requested_prefixes["processed"])),
        "db_path": str(db_path),
        "minio_endpoint": args.minio_endpoint,
        "report_path": str(report_path),
        "before": _strip_internal_snapshot(before),
        "warnings": [],
    }

    if not before["relevant_asset_ids"]:
        report["warnings"].append("no_matching_raw_files")

    if not args.apply:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        _print_summary(report)
        return 0

    write_con = _connect_duckdb(
        str(db_path),
        write_mode=True,
        lock_timeout_sec=args.lock_timeout_sec,
        retry_interval_sec=args.lock_retry_interval_sec,
    )
    db_stages = [
        {
            "stage": "detach_clip_children",
            "operations": [
                lambda: _delete_dataset_clips(write_con, before["clip_ids"]),
                lambda: _delete_image_labels(write_con, before["clip_ids"], before["image_ids"]),
                lambda: _detach_image_metadata_clip_refs(write_con, before["clip_ids"]),
            ],
        },
        {
            "stage": "delete_processed_clips",
            "operations": [
                lambda: _delete_processed_clips(write_con, before["clip_ids"]),
            ],
        },
        {
            "stage": "delete_image_metadata",
            "operations": [
                lambda: _delete_image_metadata(write_con, before["image_ids"]),
            ],
        },
        {
            "stage": "delete_labels",
            "operations": [
                lambda: _delete_labels(write_con, before["label_ids"]),
            ],
        },
    ]
    try:
        for stage in db_stages:
            _run_write_stage(
                write_con,
                stage_name=stage["stage"],
                operations=stage["operations"],
            )
    finally:
        write_con.close()

    minio_stats = _delete_minio_refs(s3_client, before["minio_refs"])

    after_con = _connect_duckdb(
        str(db_path),
        write_mode=False,
        lock_timeout_sec=args.lock_timeout_sec,
        retry_interval_sec=args.lock_retry_interval_sec,
    )
    try:
        after = _collect_scope_snapshot(after_con, s3_client, requested_prefixes)
    finally:
        after_con.close()

    report["apply"] = {
        "db_transaction": {
            "mode": "staged_commits_for_duckdb_fk_limitation",
            "stages": [stage["stage"] for stage in db_stages],
            "detached_foreign_keys": [
                "image_metadata.source_clip_id",
            ],
            "tables_touched_in_order": [
                "dataset_clips",
                "image_labels",
                "image_metadata.source_clip_id (set NULL)",
                "processed_clips",
                "image_metadata",
                "labels",
            ],
        },
        "minio": minio_stats,
    }
    report["after"] = _strip_internal_snapshot(after)
    report["verification"] = _build_verification(before, after)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _print_summary(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
