"""Detection backlog sensor 팩토리.

YOLO/SAM3 등 detection 모델별로 거의 동일한 backlog 센서를
파라미터화해서 코드 중복 없이 생성한다.
"""

from __future__ import annotations

import json
import time
from hashlib import sha1
from pathlib import Path

import duckdb
from dagster import DefaultSensorStatus, RunRequest, SensorDefinition, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.env_utils import (
    bool_env,
    default_duckdb_path,
    int_env,
    is_duckdb_lock_conflict,
)


# ---------------------------------------------------------------------------
# Backlog snapshot (DuckDB read-only)
# ---------------------------------------------------------------------------

def read_backlog_snapshot(label_tool: str) -> dict[str, int | str | None]:
    """processed_clip_frame 중 *label_tool* 결과가 없는 이미지 수를 읽는다."""
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

            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = 'image_labels'"
            ).fetchall()
            if not tables:
                return {"backlog_count": 0, "state_token": "no_table"}

            row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS backlog_count,
                    CAST(MAX(im.extracted_at) AS VARCHAR) AS latest_extracted_at
                FROM image_metadata im
                WHERE im.image_role IN ('processed_clip_frame', 'raw_video_frame')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM image_labels il
                      WHERE il.image_id = im.image_id
                        AND il.label_tool = '{label_tool}'
                  )
                """
            ).fetchone()
            backlog_count = int(row[0]) if row and row[0] is not None else 0
            latest = str(row[1]) if row and row[1] is not None else None
            state_token = f"count={backlog_count}|latest={latest or ''}"
            return {
                "backlog_count": backlog_count,
                "latest_extracted_at": latest,
                "state_token": state_token,
            }
        except Exception as exc:
            if is_duckdb_lock_conflict(exc) and attempt < retry_count:
                delay_ms = min(max_retry_delay_ms, retry_delay_ms * (2 ** attempt))
                time.sleep(delay_ms / 1000.0)
                continue
            raise
        finally:
            if conn is not None:
                conn.close()

    raise RuntimeError(f"{label_tool} sensor snapshot retry exhausted")


# ---------------------------------------------------------------------------
# Cursor 파싱
# ---------------------------------------------------------------------------

def parse_sensor_cursor(raw_cursor: str | None) -> tuple[int | None, str | None, int]:
    """센서 cursor JSON을 (last_count, last_state_token, event_seq) 로 파싱."""
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
    return last_count, str(last_state_token) if last_state_token else None, max(0, event_seq)


# ---------------------------------------------------------------------------
# Sensor 팩토리
# ---------------------------------------------------------------------------

def build_detection_backlog_sensor(
    *,
    name: str,
    label_tool: str,
    job_name: str,
    target_jobs: set[str],
    env_enable_key: str | None = None,
    env_interval_key: str = "DETECTION_SENSOR_INTERVAL_SEC",
    default_interval_sec: int = 120,
    default_running: bool = True,
    description: str = "",
    run_key_prefix: str = "detect",
) -> SensorDefinition:
    """Detection backlog 센서를 파라미터화해서 생성한다.

    ``env_enable_key`` 가 지정되면 해당 환경변수가 false일 때 센서를 skip한다.
    """
    if env_enable_key:
        enabled_now = bool_env(env_enable_key, False)
        default_status = DefaultSensorStatus.RUNNING if (default_running and enabled_now) else DefaultSensorStatus.STOPPED
    else:
        default_status = DefaultSensorStatus.RUNNING if default_running else DefaultSensorStatus.STOPPED

    @sensor(
        name=name,
        job_name=job_name,
        minimum_interval_seconds=int_env(env_interval_key, default_interval_sec, 30),
        default_status=default_status,
        description=description,
    )
    def _sensor_fn(context):
        if env_enable_key and not bool_env(env_enable_key, False):
            yield SkipReason(f"{label_tool} detection disabled ({env_enable_key}=false)")
            return

        try:
            in_flight_runs = context.instance.get_runs(
                filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
                limit=200,
            )
        except Exception as exc:
            yield SkipReason(f"{label_tool} in-flight run 조회 실패: {exc}")
            return

        active_jobs = sorted({
            str(run.job_name)
            for run in in_flight_runs
            if str(getattr(run, "job_name", "") or "") in target_jobs
        })
        if active_jobs:
            yield SkipReason(f"{label_tool} job already running: {', '.join(active_jobs)}")
            return

        try:
            snapshot = read_backlog_snapshot(label_tool)
        except FileNotFoundError as exc:
            yield SkipReason(f"DuckDB not found: {exc}")
            return
        except Exception as exc:
            yield SkipReason(f"{label_tool} backlog read failed: {exc}")
            return

        current_count = int(snapshot["backlog_count"])
        current_state_token = str(snapshot["state_token"] or "")

        prev_count, prev_state_token, prev_seq = parse_sensor_cursor(context.cursor)
        next_cursor = {
            "last_count": current_count,
            "last_state_token": current_state_token,
            "event_seq": prev_seq,
        }

        if current_count <= 0:
            context.update_cursor(json.dumps(next_cursor, sort_keys=True))
            yield SkipReason(f"{label_tool} backlog 없음")
            return

        if (
            prev_count is not None
            and current_count == prev_count
            and current_state_token == (prev_state_token or "")
        ):
            context.update_cursor(json.dumps(next_cursor, sort_keys=True))
            yield SkipReason(f"{label_tool} backlog unchanged: count={current_count}")
            return

        event_seq = prev_seq + 1
        next_cursor["event_seq"] = event_seq
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]

        yield RunRequest(
            run_key=f"{run_key_prefix}-e{event_seq}-c{current_count}-{update_token}",
            tags={
                "trigger": name,
                "backlog_count": str(current_count),
                "event_seq": str(event_seq),
            },
        )

    return _sensor_fn
