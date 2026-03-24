"""GCP download asset.

GCS -> /nas/incoming/gcp 다운로드 (auto_bootstrap·staging 허용 경로와 일치).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from dagster import AssetKey, Field, asset

from vlm_pipeline.lib.env_utils import as_int

DEFAULT_GCP_SCRIPT_PATH = "/gcp/download_from_gcs_rclone.py"
DEFAULT_GCP_DOWNLOAD_DIR = "/nas/incoming/gcp"
DEFAULT_GCP_BUCKETS = ["adlibhotel-event-bucket", "kkpolice-event-bucket"]


@asset(
    key=AssetKey(["pipeline", "incoming_nas"]),
    description="GCS 버킷 → /nas/incoming/gcp 미디어 다운로드",
    group_name="gcp",
    config_schema={
        "script_path": Field(str, default_value=DEFAULT_GCP_SCRIPT_PATH),
        "mode": Field(str, default_value="date-folders"),
        "download_dir": Field(str, default_value=DEFAULT_GCP_DOWNLOAD_DIR),
        "archive_dir": Field(str, default_value=os.getenv("ARCHIVE_DIR", "/nas/archive")),
        "backend": Field(str, default_value=os.getenv("GCS_BACKEND", "gcloud")),
        "bucket": Field(str, default_value=os.getenv("BUCKET_NAME", DEFAULT_GCP_BUCKETS[0])),
        "buckets": Field([str], default_value=DEFAULT_GCP_BUCKETS),
        "bucket_subdir": Field(bool, default_value=True),
        "date_folders": Field([str], default_value=[]),
        "skip_existing": Field(bool, default_value=True),
        "skip_archived_done": Field(bool, default_value=True),
        "dry_run": Field(bool, default_value=False),
        "list_only": Field(bool, default_value=False),
        "config": Field(str, default_value=os.getenv("RCLONE_CONFIG", "")),
        "extra_args": Field(str, default_value=os.getenv("RCLONE_EXTRA_ARGS", "")),
        "stall_seconds": Field(
            int,
            default_value=as_int(os.getenv("GCS_STALL_SECONDS"), 300),
        ),
        "max_restarts": Field(
            int,
            default_value=as_int(os.getenv("GCS_MAX_RESTARTS"), 3),
        ),
        "zero_byte_retries": Field(
            int,
            default_value=as_int(os.getenv("GCS_ZERO_BYTE_RETRIES"), 2),
        ),
        "timeout_sec": Field(int, default_value=60 * 60 * 6),
    },
)
def gcs_download_to_incoming(context):
    cfg = context.op_config

    script_path = Path(cfg.get("script_path") or DEFAULT_GCP_SCRIPT_PATH)
    if not script_path.exists():
        raise FileNotFoundError(f"GCP download script not found: {script_path}")

    cmd = [
        "python3",
        str(script_path),
        "--download",
        "--mode",
        str(cfg.get("mode", "date-folders")),
        "--download-dir",
        str(cfg.get("download_dir", DEFAULT_GCP_DOWNLOAD_DIR)),
        "--archive-dir",
        str(cfg.get("archive_dir", "/nas/archive")),
        "--backend",
        str(cfg.get("backend", "auto")),
        "--stall-seconds",
        str(as_int(cfg.get("stall_seconds"), 300)),
        "--max-restarts",
        str(as_int(cfg.get("max_restarts"), 3)),
        "--zero-byte-retries",
        str(max(0, as_int(cfg.get("zero_byte_retries"), 2))),
    ]

    buckets = [str(item).strip() for item in (cfg.get("buckets") or []) if str(item).strip()]
    if buckets:
        cmd.extend(["--buckets", *buckets])
    else:
        bucket = str(cfg.get("bucket") or "").strip()
        if bucket:
            cmd.extend(["--bucket", bucket])

    if bool(cfg.get("bucket_subdir", True)):
        cmd.append("--bucket-subdir")
    else:
        cmd.append("--no-bucket-subdir")

    if bool(cfg.get("skip_existing", True)):
        cmd.append("--skip-existing")
    else:
        cmd.append("--overwrite")

    if bool(cfg.get("skip_archived_done", True)):
        cmd.append("--skip-archived-done")
    else:
        cmd.append("--no-skip-archived-done")

    if bool(cfg.get("dry_run", False)):
        cmd.append("--dry-run")
    if bool(cfg.get("list_only", False)):
        cmd.append("--list-only")

    date_folders = [
        str(item).strip() for item in (cfg.get("date_folders") or []) if str(item).strip()
    ]
    if date_folders:
        cmd.extend(["--date-folders", *date_folders])

    rclone_config = str(cfg.get("config") or "").strip()
    if rclone_config:
        cmd.extend(["--config", rclone_config])

    extra_args = str(cfg.get("extra_args") or "").strip()
    if extra_args:
        cmd.extend(["--extra-args", extra_args])

    timeout_sec = max(60, as_int(cfg.get("timeout_sec"), 60 * 60 * 6))

    context.log.info(f"Running GCP download: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        if exc.stdout:
            context.log.warning(str(exc.stdout)[-4000:].strip())
        if exc.stderr:
            context.log.warning(str(exc.stderr)[-4000:].strip())
        raise RuntimeError(f"gcs_download_to_incoming timeout after {timeout_sec}s")

    if result.stdout:
        context.log.info(result.stdout[-4000:].strip())
    if result.stderr:
        context.log.warning(result.stderr[-4000:].strip())

    if result.returncode != 0:
        raise RuntimeError(
            f"gcs_download_to_incoming failed (exit={result.returncode})"
        )

    return {
        "status": "ok",
        "download_dir": str(cfg.get("download_dir", DEFAULT_GCP_DOWNLOAD_DIR)),
        "backend": str(cfg.get("backend", "auto")),
        "mode": str(cfg.get("mode", "date-folders")),
    }
