#!/usr/bin/env python3
"""Migrate legacy YOLO detection JSON objects in MinIO to COCO JSON (in-place).

Scope:
- `image_labels.label_tool = 'yolo-world'`
- `image_labels.label_format = 'yolo_detection_json'`

Behavior:
- Dry-run (default): read + convert + report only.
- Apply: backup MinIO object once, overwrite same key with COCO JSON, then update
  `image_labels.label_format='coco'` and `object_count`.
- Fail-forward: errors are reported and migration continues with next key.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
import duckdb
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from vlm_pipeline.lib.yolo_coco import convert_detection_payload_to_coco, is_coco_detection_payload


@dataclass(frozen=True)
class RuntimeSettings:
    env: str
    db_path: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str


def _load_dotenv_if_present(dotenv_path: Path, *, override: bool) -> None:
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
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        if value == "":
            continue
        if not override and os.getenv(key):
            continue
        os.environ[key] = value


def _load_target_env_defaults(target_env: str) -> None:
    _load_dotenv_if_present(REPO_ROOT / "docker" / ".env", override=False)
    if target_env == "staging":
        _load_dotenv_if_present(REPO_ROOT / "docker" / ".env.staging", override=True)


def _normalize_endpoint(endpoint: str) -> str:
    rendered = str(endpoint or "").strip()
    if not rendered:
        return rendered
    if rendered.startswith("http://") or rendered.startswith("https://"):
        return rendered
    return f"http://{rendered}"


def _resolve_runtime_settings(args: argparse.Namespace) -> RuntimeSettings:
    default_db = "/data/staging.duckdb" if args.env == "staging" else "/data/pipeline.duckdb"
    db_path = str(args.db_path or os.getenv("DATAOPS_DUCKDB_PATH") or os.getenv("DUCKDB_PATH") or default_db)

    endpoint = str(args.minio_endpoint or os.getenv("MINIO_ENDPOINT") or "http://127.0.0.1:9000")
    access_key = str(
        args.minio_access_key
        or os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
        or "minioadmin"
    )
    secret_key = str(
        args.minio_secret_key
        or os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
        or "minioadmin"
    )

    return RuntimeSettings(
        env=args.env,
        db_path=db_path,
        minio_endpoint=_normalize_endpoint(endpoint),
        minio_access_key=access_key,
        minio_secret_key=secret_key,
    )


def _build_s3_client(settings: RuntimeSettings):
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _normalize_key(raw_key: str) -> str:
    return str(raw_key or "").strip().lstrip("/")


def _build_backup_key(labels_key: str) -> str:
    return f"_backup_yolo_detection_json/{_normalize_key(labels_key)}"


def _object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _backup_object_once(client, bucket: str, key: str, backup_key: str) -> None:
    if _object_exists(client, bucket, backup_key):
        return
    client.copy_object(
        Bucket=bucket,
        Key=backup_key,
        CopySource={"Bucket": bucket, "Key": key},
        ContentType="application/json",
        MetadataDirective="COPY",
    )


def _restore_object_from_backup(client, bucket: str, key: str, backup_key: str) -> None:
    client.copy_object(
        Bucket=bucket,
        Key=key,
        CopySource={"Bucket": bucket, "Key": backup_key},
        ContentType="application/json",
        MetadataDirective="COPY",
    )


def _download_json(client, bucket: str, key: str) -> dict[str, Any]:
    response = client.get_object(Bucket=bucket, Key=key)
    payload = response["Body"].read()
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("label payload is not a JSON object")
    return decoded


def _upload_json(client, bucket: str, key: str, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=encoded,
        ContentType="application/json",
    )


def _select_targets(
    conn: duckdb.DuckDBPyConnection,
    *,
    resume_from_key: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    query = [
        """
        SELECT
          il.image_label_id,
          il.image_id,
          il.source_clip_id,
          COALESCE(NULLIF(il.labels_bucket, ''), 'vlm-labels') AS labels_bucket,
          il.labels_key,
          il.label_format,
          il.object_count,
          il.created_at,
          im.image_key,
          im.width,
          im.height
        FROM image_labels il
        LEFT JOIN image_metadata im ON im.image_id = il.image_id
        WHERE il.label_tool = 'yolo-world'
          AND il.label_format = 'yolo_detection_json'
          AND il.labels_key IS NOT NULL
          AND TRIM(il.labels_key) <> ''
        """
    ]
    params: list[object] = []

    if resume_from_key:
        query.append(" AND il.labels_key >= ?")
        params.append(resume_from_key)

    query.append(" ORDER BY il.labels_key, il.image_label_id")

    if limit is not None and limit > 0:
        query.append(" LIMIT ?")
        params.append(limit)

    rows = conn.execute("".join(query), params).fetchall()
    columns = [
        "image_label_id",
        "image_id",
        "source_clip_id",
        "labels_bucket",
        "labels_key",
        "label_format",
        "object_count",
        "created_at",
        "image_key",
        "width",
        "height",
    ]
    return [dict(zip(columns, row)) for row in rows]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _default_report_path(env_name: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "scripts" / "reports" / f"migrate_yolo_detection_json_to_coco_{env_name}_{ts}.jsonl"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate legacy yolo_detection_json objects to COCO format.")
    parser.add_argument("--env", choices=["production", "staging"], required=True)

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Conversion simulation only (default).")
    mode.add_argument("--apply", action="store_true", help="Apply MinIO overwrite + DB update.")

    parser.add_argument("--limit", type=int, default=None, help="Max rows to process.")
    parser.add_argument(
        "--resume-from-key",
        default=None,
        help="Resume from this labels_key (inclusive).",
    )
    parser.add_argument("--report-path", default=None, help="JSONL report output path.")

    parser.add_argument("--db-path", default=None, help="Override DuckDB path.")
    parser.add_argument("--minio-endpoint", default=None, help="Override MinIO endpoint.")
    parser.add_argument("--minio-access-key", default=None, help="Override MinIO access key.")
    parser.add_argument("--minio-secret-key", default=None, help="Override MinIO secret key.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _load_target_env_defaults(args.env)
    settings = _resolve_runtime_settings(args)

    apply_mode = bool(args.apply)
    report_path = Path(args.report_path) if args.report_path else _default_report_path(args.env)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        json.dumps(
            {
                "mode": "apply" if apply_mode else "dry_run",
                "env": settings.env,
                "db_path": settings.db_path,
                "minio_endpoint": settings.minio_endpoint,
                "report_path": str(report_path),
            },
            ensure_ascii=False,
        )
    )

    try:
        conn = duckdb.connect(settings.db_path, read_only=not apply_mode)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"DuckDB connect failed: {settings.db_path}: {exc}") from exc

    try:
        targets = _select_targets(
            conn,
            resume_from_key=(str(args.resume_from_key).strip() if args.resume_from_key else None),
            limit=args.limit,
        )

        if not targets:
            print(json.dumps({"targets": 0, "message": "no yolo_detection_json rows found"}, ensure_ascii=False))
            report_path.write_text("", encoding="utf-8")
            return 0

        if apply_mode:
            backup_path = report_path.with_name(f"{report_path.stem}.db_backup.jsonl")
            _write_jsonl(backup_path, targets)
            print(json.dumps({"db_backup_path": str(backup_path), "rows": len(targets)}, ensure_ascii=False))

        s3_client = _build_s3_client(settings)

        summary = {
            "targets": len(targets),
            "converted": 0,
            "already_coco_payload": 0,
            "failed": 0,
        }

        with report_path.open("w", encoding="utf-8") as report_fp:
            for row in targets:
                image_label_id = str(row.get("image_label_id") or "")
                image_id = str(row.get("image_id") or "")
                source_clip_id = row.get("source_clip_id")
                labels_bucket = str(row.get("labels_bucket") or "vlm-labels").strip() or "vlm-labels"
                labels_key = _normalize_key(str(row.get("labels_key") or ""))

                report = {
                    "image_label_id": image_label_id,
                    "image_id": image_id,
                    "labels_bucket": labels_bucket,
                    "labels_key": labels_key,
                }

                try:
                    current_payload = _download_json(s3_client, labels_bucket, labels_key)
                    if is_coco_detection_payload(current_payload):
                        summary["already_coco_payload"] += 1

                    coco_payload = convert_detection_payload_to_coco(
                        current_payload,
                        fallback_image_id=image_id,
                        fallback_source_clip_id=(str(source_clip_id).strip() or None) if source_clip_id is not None else None,
                        fallback_image_key=str(row.get("image_key") or labels_key).strip() or labels_key,
                        fallback_image_width=row.get("width"),
                        fallback_image_height=row.get("height"),
                        default_class_source="migration",
                    )
                    annotation_count = len(coco_payload.get("annotations") or [])

                    if apply_mode:
                        backup_key = _build_backup_key(labels_key)
                        _backup_object_once(s3_client, labels_bucket, labels_key, backup_key)
                        _upload_json(s3_client, labels_bucket, labels_key, coco_payload)

                        try:
                            conn.execute(
                                """
                                UPDATE image_labels
                                SET label_format = 'coco',
                                    object_count = ?
                                WHERE image_label_id = ?
                                """,
                                [annotation_count, image_label_id],
                            )
                        except Exception:  # noqa: BLE001
                            _restore_object_from_backup(s3_client, labels_bucket, labels_key, backup_key)
                            raise

                        report.update(
                            {
                                "status": "applied",
                                "annotation_count": annotation_count,
                                "backup_key": backup_key,
                            }
                        )
                    else:
                        report.update(
                            {
                                "status": "dry_run",
                                "annotation_count": annotation_count,
                            }
                        )

                    summary["converted"] += 1
                except Exception as exc:  # noqa: BLE001
                    summary["failed"] += 1
                    report.update({"status": "failed", "error": str(exc)})

                report_fp.write(json.dumps(report, ensure_ascii=False, default=str) + "\n")

        print(json.dumps(summary, ensure_ascii=False))
        print(json.dumps({"report_path": str(report_path)}, ensure_ascii=False))
        return 0 if summary["failed"] == 0 else 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
