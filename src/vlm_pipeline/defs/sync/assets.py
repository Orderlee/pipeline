"""SYNC @asset — local DuckDB -> MotherDuck 동기화 (옵션).

Layer 4: Dagster @asset.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from dagster import AssetKey, Field, asset


DEFAULT_MOTHERDUCK_DB = "pipeline_db"
DEFAULT_SYNC_SCRIPT_PATH = "/src/python/local_duckdb_to_motherduck_sync.py"


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _as_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _default_duckdb_path() -> str:
    return (
        os.getenv("DATAOPS_DUCKDB_PATH")
        or os.getenv("DUCKDB_PATH")
        or "/data/pipeline.duckdb"
    )


def _is_duckdb_lock_conflict(text: str) -> bool:
    normalized = str(text or "").lower()
    return (
        "could not set lock on file" in normalized
        and "conflicting lock is held" in normalized
    )


def _extract_lock_owner_pid(text: str) -> str | None:
    match = re.search(r"\(PID\s+(\d+)\)", str(text or ""), flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def _resolve_script_path(configured_path: str) -> Path:
    candidates = [
        configured_path,
        DEFAULT_SYNC_SCRIPT_PATH,
    ]
    for candidate in candidates:
        candidate_path = Path(candidate)
        if candidate_path.exists():
            return candidate_path
    return Path(configured_path)


@asset(
    key=AssetKey(["pipeline", "motherduck_sync"]),
    deps=[AssetKey(["built_dataset"])],
    description="SYNC — local DuckDB MVP tables → MotherDuck 동기화 (옵션)",
    group_name="sync",
    config_schema={
        "enabled": Field(bool, default_value=False),
        "db": Field(str, default_value=DEFAULT_MOTHERDUCK_DB),
        "dry_run": Field(bool, default_value=False),
        "ensure_org_share": Field(bool, default_value=True),
        "share_name": Field(str, default_value=""),
        "share_update": Field(str, default_value="MANUAL"),
        "timeout_sec": Field(int, default_value=600),
        "tables": Field([str], default_value=[]),
        "trigger_table": Field(str, default_value=""),
        "script_path": Field(str, default_value=DEFAULT_SYNC_SCRIPT_PATH),
    },
)
def motherduck_sync(context):
    """SYNC 단계: local DuckDB MVP tables를 MotherDuck로 동기화."""
    cfg = context.op_config

    enabled = bool(cfg.get("enabled", False)) or _env_bool("MOTHERDUCK_SYNC_ENABLED", False)
    if not enabled:
        context.log.info(
            "motherduck_sync: disabled "
            "(set MOTHERDUCK_SYNC_ENABLED=true or op config enabled=true)"
        )
        return {"status": "skipped", "reason": "disabled"}

    if not os.getenv("MOTHERDUCK_TOKEN"):
        context.log.warning("motherduck_sync: MOTHERDUCK_TOKEN not set. sync를 건너뜁니다.")
        return {"status": "skipped", "reason": "missing_token"}

    db_path = Path(_default_duckdb_path())
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB not found: {db_path}")

    db_name = (cfg.get("db") or os.getenv("MOTHERDUCK_DB") or DEFAULT_MOTHERDUCK_DB).strip()
    dry_run = bool(cfg.get("dry_run", False)) or _env_bool("MOTHERDUCK_SYNC_DRY_RUN", False)
    ensure_org_share = bool(cfg.get("ensure_org_share", True))
    share_name = (cfg.get("share_name") or os.getenv("MOTHERDUCK_SHARE_NAME") or db_name).strip()
    tables = [str(item).strip() for item in (cfg.get("tables") or []) if str(item).strip()]
    trigger_table = str(cfg.get("trigger_table") or "").strip()
    timeout_sec = max(
        60,
        _as_int(
            cfg.get("timeout_sec") or os.getenv("MOTHERDUCK_SYNC_TIMEOUT_SEC"),
            600,
        ),
    )
    share_update = (
        (cfg.get("share_update") or os.getenv("MOTHERDUCK_SHARE_UPDATE") or "MANUAL")
        .strip()
        .upper()
    )
    if share_update not in {"MANUAL", "AUTOMATIC"}:
        context.log.warning(
            f"motherduck_sync: invalid share_update={share_update}, MANUAL로 대체합니다."
        )
        share_update = "MANUAL"

    script_path = _resolve_script_path(
        str(cfg.get("script_path") or DEFAULT_SYNC_SCRIPT_PATH).strip()
    )
    if not script_path.exists():
        raise FileNotFoundError(
            "MotherDuck sync script not found. "
            f"checked={cfg.get('script_path')},{DEFAULT_SYNC_SCRIPT_PATH}"
        )

    cmd = [
        "python3",
        str(script_path),
        "--db",
        db_name,
        "--local-db-path",
        str(db_path),
        "--share-update",
        share_update,
    ]
    if tables:
        cmd.extend(["--tables", *tables])
    if dry_run:
        cmd.append("--dry-run")
    if ensure_org_share:
        if share_name:
            cmd.extend(["--share-name", share_name])
    else:
        cmd.append("--no-ensure-org-share")

    context.log.info(f"Running motherduck sync: {' '.join(cmd)}")
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
        raise RuntimeError(f"motherduck sync timeout after {timeout_sec}s")

    if result.stdout:
        context.log.info(result.stdout[-4000:].strip())
    if result.stderr:
        context.log.warning(result.stderr[-4000:].strip())

    if result.returncode != 0:
        combined_output = "\n".join(part for part in [result.stdout, result.stderr] if part)
        if _is_duckdb_lock_conflict(combined_output):
            owner_pid = _extract_lock_owner_pid(combined_output)
            if owner_pid:
                context.log.warning(
                    "motherduck_sync: duckdb lock conflict detected "
                    f"(owner_pid={owner_pid}). 이번 tick 동기화를 건너뜁니다."
                )
            else:
                context.log.warning(
                    "motherduck_sync: duckdb lock conflict detected. "
                    "이번 tick 동기화를 건너뜁니다."
                )
            summary = {
                "status": "skipped",
                "reason": "duckdb_lock_conflict",
                "db": db_name,
                "dry_run": dry_run,
                "ensure_org_share": ensure_org_share,
                "tables": tables,
                "trigger_table": trigger_table,
            }
            context.add_output_metadata(summary)
            return summary
        raise RuntimeError(f"motherduck sync failed (code={result.returncode})")

    if trigger_table:
        context.log.info(f"motherduck_sync trigger_table={trigger_table}")
    context.log.info("MotherDuck sync completed successfully")
    summary = {
        "status": "completed",
        "db": db_name,
        "dry_run": dry_run,
        "ensure_org_share": ensure_org_share,
        "tables": tables,
        "trigger_table": trigger_table,
    }
    context.add_output_metadata(summary)
    return summary
