"""MotherDuck table sensors — 테이블 row count 증가 기반 sync 트리거."""

from __future__ import annotations

import json
import time
from hashlib import sha1
from pathlib import Path

import duckdb
from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.env_utils import (
    default_duckdb_path,
    has_any_duckdb_writer_tag,
    int_env,
    is_duckdb_lock_conflict,
)
from vlm_pipeline.resources.runtime_settings import load_motherduck_sensor_settings

_SENSOR_SETTINGS = load_motherduck_sensor_settings()
WATCHED_TABLES = list(_SENSOR_SETTINGS.watched_tables)


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

    last_updated_at = payload.get("last_updated_at")
    if last_updated_at is None:
        return last_count, None, max(0, event_seq)
    return last_count, str(last_updated_at), max(0, event_seq)


def _read_table_snapshot(table_name: str) -> tuple[int, str | None]:
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
            exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_name = ?
                """,
                [table_name],
            ).fetchone()[0]
            if int(exists) <= 0:
                raise LookupError(f"table not found: {table_name}")

            escaped = table_name.replace('"', '""')
            if table_name == "raw_files":
                row = conn.execute(
                    f'SELECT COUNT(*), CAST(MAX(updated_at) AS VARCHAR) FROM "{escaped}"'
                ).fetchone()
                return int(row[0]), (str(row[1]) if row[1] is not None else None)
            return int(conn.execute(f'SELECT COUNT(*) FROM "{escaped}"').fetchone()[0]), None
        except Exception as exc:  # noqa: BLE001
            if is_duckdb_lock_conflict(exc) and attempt < retry_count:
                delay_ms = min(max_retry_delay_ms, retry_delay_ms * (2**attempt))
                time.sleep(delay_ms / 1000.0)
                continue
            raise
        finally:
            if conn is not None:
                conn.close()

    raise RuntimeError("duckdb sensor snapshot retry exhausted unexpectedly")


def _build_table_sensor(table_name: str):
    sensor_name = f"motherduck_{table_name}_sensor"
    description = (
        "raw_files row count/updated_at 변화 시 MotherDuck 부분 동기화 트리거"
        if table_name == "raw_files"
        else f"{table_name} row count 증가 시 MotherDuck 부분 동기화 트리거"
    )

    def _has_inflight_duckdb_writer(context) -> bool:
        if not _SENSOR_SETTINGS.skip_during_duckdb_writer:
            return False
        runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]),
            limit=50,
        )
        for run in runs:
            if run.job_name == "motherduck_sync_job":
                continue
            tags = getattr(run, "tags", {}) or {}
            if has_any_duckdb_writer_tag(tags):
                return True
        return False

    @sensor(
        name=sensor_name,
        job_name="motherduck_sync_job",
        minimum_interval_seconds=_SENSOR_SETTINGS.interval_sec,
        default_status=DefaultSensorStatus.RUNNING,
        description=description,
    )
    def _table_sensor(context):
        if _has_inflight_duckdb_writer(context):
            yield SkipReason("duckdb_writer run in progress: motherduck sensor skipped")
            return

        in_flight = context.instance.get_runs(
            filters=RunsFilter(
                job_name="motherduck_sync_job",
                statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED],
            ),
            limit=1,
        )
        if in_flight:
            yield SkipReason("motherduck_sync_job already queued/running")
            return

        try:
            current_count, current_updated_at = _read_table_snapshot(table_name)
        except FileNotFoundError as exc:
            yield SkipReason(f"DuckDB not found: {exc}")
            return
        except LookupError as exc:
            yield SkipReason(str(exc))
            return
        except Exception as exc:  # noqa: BLE001
            yield SkipReason(f"{table_name} count read failed: {exc}")
            return

        previous_count, previous_updated_at, previous_event_seq = _parse_cursor_state(context.cursor)
        next_cursor = {
            "last_count": current_count,
            "event_seq": previous_event_seq,
        }
        if table_name == "raw_files":
            next_cursor["last_updated_at"] = current_updated_at or ""

        if previous_count is None:
            if current_count <= 0:
                context.update_cursor(json.dumps(next_cursor, sort_keys=True))
                yield SkipReason(f"{table_name} baseline initialized: {current_count}")
                return

            # 초기 baseline이 non-zero면 1회 catch-up sync 실행
            event_seq = previous_event_seq + 1
            next_cursor["event_seq"] = event_seq
            context.update_cursor(json.dumps(next_cursor, sort_keys=True))
            if table_name == "raw_files":
                update_token = sha1((current_updated_at or "none").encode("utf-8")).hexdigest()[:10]
                run_key = f"motherduck-{table_name}-e{event_seq}-c{current_count}-{update_token}"
            else:
                run_key = f"motherduck-{table_name}-e{event_seq}-c{current_count}"

            yield RunRequest(
                run_key=run_key,
                run_config={
                    "ops": {
                        "pipeline__motherduck_sync": {
                            "config": {
                                "enabled": True,
                                "tables": [table_name],
                                "trigger_table": table_name,
                            }
                        }
                    }
                },
                tags={
                    "trigger": sensor_name,
                    "table": table_name,
                    "prev_count": "None",
                    "current_count": str(current_count),
                    "delta": str(current_count),
                    "baseline": "true",
                },
            )
            return

        count_increased = current_count > previous_count
        updated_changed = False
        if table_name == "raw_files":
            updated_changed = (current_updated_at or "") != (previous_updated_at or "")

        if current_count < previous_count:
            context.update_cursor(json.dumps(next_cursor, sort_keys=True))
            yield SkipReason(
                f"{table_name} row count decreased: {previous_count}->{current_count}"
            )
            return

        if not count_increased and not updated_changed:
            context.update_cursor(json.dumps(next_cursor, sort_keys=True))
            if table_name == "raw_files":
                yield SkipReason(
                    f"{table_name} unchanged: {current_count}, updated_at={current_updated_at or ''}"
                )
            else:
                yield SkipReason(f"{table_name} unchanged: {current_count}")
            return

        event_seq = previous_event_seq + 1
        next_cursor["event_seq"] = event_seq
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))

        delta = current_count - previous_count
        if table_name == "raw_files":
            update_token = sha1((current_updated_at or "none").encode("utf-8")).hexdigest()[:10]
            run_key = f"motherduck-{table_name}-e{event_seq}-c{current_count}-{update_token}"
        else:
            run_key = f"motherduck-{table_name}-e{event_seq}-c{current_count}"
        tags = {
            "trigger": sensor_name,
            "table": table_name,
            "prev_count": str(previous_count),
            "current_count": str(current_count),
            "delta": str(delta),
            "event_seq": str(event_seq),
        }
        if table_name == "raw_files":
            tags["prev_updated_at"] = previous_updated_at or ""
            tags["current_updated_at"] = current_updated_at or ""

        yield RunRequest(
            run_key=run_key,
            run_config={
                "ops": {
                    "pipeline__motherduck_sync": {
                        "config": {
                            "enabled": True,
                            "tables": [table_name],
                            "trigger_table": table_name,
                        }
                    }
                }
            },
            tags=tags,
        )

    return _table_sensor


(
    motherduck_raw_files_sensor,
    motherduck_video_metadata_sensor,
    motherduck_labels_sensor,
    motherduck_processed_clips_sensor,
) = [_build_table_sensor(table_name) for table_name in WATCHED_TABLES]


MOTHERDUCK_TABLE_SENSORS = [
    motherduck_raw_files_sensor,
    motherduck_video_metadata_sensor,
    motherduck_labels_sensor,
    motherduck_processed_clips_sensor,
]
