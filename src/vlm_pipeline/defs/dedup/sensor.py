"""DEDUP sensor — raw image pHash backlog 감지 후 dedup_job 트리거."""

from __future__ import annotations

import json
import time
from hashlib import sha1
from pathlib import Path

import duckdb
from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.env_utils import bool_env, default_duckdb_path, int_env, is_duckdb_lock_conflict

DEDUP_SENSOR_TARGET_JOBS = {
    "dedup_job",
    "mvp_stage_job",
}


def _parse_cursor_state(raw_cursor: str | None) -> tuple[int | None, str | None, int]:
    if not raw_cursor:
        return None, None, 0
    try:
        payload = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return None, None, 0
    if not isinstance(payload, dict):
        return None, None, 0

    try:
        last_count = int(payload.get("last_count"))
    except (TypeError, ValueError):
        last_count = None

    try:
        event_seq = int(payload.get("event_seq", 0))
    except (TypeError, ValueError):
        event_seq = 0

    last_state_token = payload.get("last_state_token")
    if last_state_token is None:
        return last_count, None, max(0, event_seq)
    return last_count, str(last_state_token), max(0, event_seq)


def _read_dedup_backlog_snapshot() -> dict[str, int | str | None]:
    db_path = Path(default_duckdb_path())
    if not db_path.exists():
        raise FileNotFoundError(str(db_path))

    retry_count = int_env("DUCKDB_SENSOR_LOCK_RETRY_COUNT", 5, 0)
    retry_delay_ms = int_env("DUCKDB_SENSOR_LOCK_RETRY_DELAY_MS", 200, 10)
    max_retry_delay_ms = int_env("DUCKDB_SENSOR_LOCK_RETRY_MAX_DELAY_MS", 2000, retry_delay_ms)

    for attempt in range(retry_count + 1):
        conn = None
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS backlog_count,
                    CAST(MAX(updated_at) AS VARCHAR) AS latest_updated_at
                FROM raw_files
                WHERE media_type = 'image'
                  AND ingest_status = 'completed'
                  AND phash IS NULL
                """
            ).fetchone()
            backlog_count = int(row[0]) if row and row[0] is not None else 0
            latest_updated_at = str(row[1]) if row and row[1] is not None else None
            state_token = f"count={backlog_count}|updated_at={latest_updated_at or ''}"
            return {
                "backlog_count": backlog_count,
                "latest_updated_at": latest_updated_at,
                "state_token": state_token,
            }
        except Exception as exc:  # noqa: BLE001
            if is_duckdb_lock_conflict(exc) and attempt < retry_count:
                delay_ms = min(max_retry_delay_ms, retry_delay_ms * (2**attempt))
                time.sleep(delay_ms / 1000.0)
                continue
            raise
        finally:
            if conn is not None:
                conn.close()

    raise RuntimeError("dedup sensor snapshot retry exhausted unexpectedly")


@sensor(
    job_name="dedup_job",
    minimum_interval_seconds=int_env("DEDUP_SENSOR_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="새 raw image pHash backlog 감지 시 dedup_job 자동 실행",
)
def dedup_backlog_sensor(context):
    if not bool_env("DEDUP_SENSOR_ENABLED", True):
        yield SkipReason("dedup_backlog_sensor 비활성화됨")
        return

    try:
        in_flight_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
            limit=200,
        )
    except Exception as exc:  # noqa: BLE001
        yield SkipReason(f"dedup in-flight run 조회 실패: {exc}")
        return

    active_jobs = sorted(
        {
            str(run.job_name)
            for run in in_flight_runs
            if str(getattr(run, "job_name", "") or "") in DEDUP_SENSOR_TARGET_JOBS
        }
    )
    if active_jobs:
        yield SkipReason("dedup job already queued/running: " + ", ".join(active_jobs))
        return

    try:
        snapshot = _read_dedup_backlog_snapshot()
    except FileNotFoundError as exc:
        yield SkipReason(f"DuckDB not found: {exc}")
        return
    except Exception as exc:  # noqa: BLE001
        yield SkipReason(f"dedup backlog read failed: {exc}")
        return

    current_count = int(snapshot["backlog_count"])
    current_updated_at = str(snapshot["latest_updated_at"] or "")
    current_state_token = str(snapshot["state_token"] or "")

    previous_count, previous_state_token, previous_event_seq = _parse_cursor_state(context.cursor)
    next_cursor = {
        "last_count": current_count,
        "last_state_token": current_state_token,
        "event_seq": previous_event_seq,
    }

    if previous_count is None:
        if current_count <= 0:
            context.update_cursor(json.dumps(next_cursor, sort_keys=True))
            yield SkipReason("dedup backlog baseline initialized: 0")
            return

        event_seq = previous_event_seq + 1
        next_cursor["event_seq"] = event_seq
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]
        yield RunRequest(
            run_key=f"dedup-backlog-e{event_seq}-c{current_count}-{update_token}",
            run_config={
                "ops": {
                    "dedup_results": {
                        "config": {
                            "limit": int_env("DEDUP_SENSOR_LIMIT", 200, 1),
                            "threshold": int_env("DEDUP_SENSOR_THRESHOLD", 5, 0),
                        }
                    }
                }
            },
            tags={
                "trigger": "dedup_backlog_sensor",
                "baseline": "true",
                "backlog_count": str(current_count),
                "backlog_updated_at": current_updated_at,
            },
        )
        return

    backlog_changed = current_count != previous_count
    state_changed = current_state_token != (previous_state_token or "")

    if current_count <= 0:
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        yield SkipReason("dedup backlog 없음")
        return

    if not backlog_changed and not state_changed:
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        yield SkipReason(f"dedup backlog unchanged: count={current_count}")
        return

    event_seq = previous_event_seq + 1
    next_cursor["event_seq"] = event_seq
    context.update_cursor(json.dumps(next_cursor, sort_keys=True))
    update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]
    yield RunRequest(
        run_key=f"dedup-backlog-e{event_seq}-c{current_count}-{update_token}",
        run_config={
            "ops": {
                "dedup_results": {
                    "config": {
                        "limit": int_env("DEDUP_SENSOR_LIMIT", 200, 1),
                        "threshold": int_env("DEDUP_SENSOR_THRESHOLD", 5, 0),
                    }
                }
            }
        },
        tags={
            "trigger": "dedup_backlog_sensor",
            "prev_backlog_count": str(previous_count),
            "backlog_count": str(current_count),
            "delta": str(current_count - previous_count),
            "event_seq": str(event_seq),
            "prev_backlog_state": previous_state_token or "",
            "backlog_state": current_state_token,
            "backlog_updated_at": current_updated_at,
        },
    )
