"""LABEL sensor — auto-labeling 전체 backlog 감지."""

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
    int_env,
    is_duckdb_lock_conflict,
)

AUTO_LABELING_TARGET_JOBS = {
    "auto_labeling_job",
    "mvp_stage_job",
}


def _read_auto_label_backlog_snapshot() -> dict[str, int | str | None]:
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
                WITH gemini_pending AS (
                    SELECT
                        r.asset_id,
                        CAST(r.updated_at AS VARCHAR) AS updated_token
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND COALESCE(vm.auto_label_status, 'pending') = 'pending'
                ),
                caption_pending AS (
                    SELECT
                        r.asset_id,
                        CAST(COALESCE(vm.auto_labeled_at, vm.extracted_at) AS VARCHAR) AS updated_token
                    FROM raw_files r
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND vm.auto_label_status = 'generated'
                ),
                clip_pending AS (
                    SELECT
                        l.label_id,
                        CAST(l.created_at AS VARCHAR) AS updated_token
                    FROM raw_files r
                    JOIN labels l ON l.asset_id = r.asset_id
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND l.label_status = 'completed'
                      AND l.label_tool = 'gemini'
                      AND l.label_source = 'auto'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM processed_clips pc
                          WHERE pc.source_label_id = l.label_id
                            AND pc.process_status = 'completed'
                      )
                )
                SELECT
                    (SELECT COUNT(*) FROM gemini_pending)
                    + (SELECT COUNT(*) FROM caption_pending)
                    + (SELECT COUNT(*) FROM clip_pending) AS backlog_count,
                    CAST((SELECT MAX(updated_token) FROM gemini_pending) AS VARCHAR) AS gemini_updated_at,
                    CAST((SELECT MAX(updated_token) FROM caption_pending) AS VARCHAR) AS caption_updated_at,
                    CAST((SELECT MAX(updated_token) FROM clip_pending) AS VARCHAR) AS clip_updated_at,
                    (SELECT COUNT(*) FROM gemini_pending) AS gemini_pending_count,
                    (SELECT COUNT(*) FROM caption_pending) AS caption_pending_count,
                    (SELECT COUNT(*) FROM clip_pending) AS clip_pending_count
                """
            ).fetchone()
            backlog_count = int(row[0]) if row and row[0] is not None else 0
            gemini_updated = str(row[1]) if row and row[1] is not None else None
            caption_updated = str(row[2]) if row and row[2] is not None else None
            clip_updated = str(row[3]) if row and row[3] is not None else None
            gemini_pending_count = int(row[4]) if row and row[4] is not None else 0
            caption_pending_count = int(row[5]) if row and row[5] is not None else 0
            clip_pending_count = int(row[6]) if row and row[6] is not None else 0
            state_token = (
                f"total={backlog_count}|gemini={gemini_pending_count}|caption={caption_pending_count}"
                f"|clip={clip_pending_count}|"
                f"g={gemini_updated or ''}|c={caption_updated or ''}|p={clip_updated or ''}"
            )
            return {
                "backlog_count": backlog_count,
                "latest_updated_at": max(
                    gemini_updated or "",
                    caption_updated or "",
                    clip_updated or "",
                ) or None,
                "gemini_pending_count": gemini_pending_count,
                "caption_pending_count": caption_pending_count,
                "clip_pending_count": clip_pending_count,
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

    raise RuntimeError("auto_labeling sensor snapshot retry exhausted")


@sensor(
    job_name="auto_labeling_job",
    minimum_interval_seconds=int_env("AUTO_LABELING_SENSOR_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="Gemini 생성, JSON 정규화, clip 생성 backlog 감지 시 auto_labeling_job 실행",
)
def auto_labeling_sensor(context):
    try:
        in_flight_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
            limit=200,
        )
    except Exception as exc:
        yield SkipReason(f"auto_labeling in-flight run 조회 실패: {exc}")
        return

    active_jobs = sorted({
        str(run.job_name)
        for run in in_flight_runs
        if str(getattr(run, "job_name", "") or "") in AUTO_LABELING_TARGET_JOBS
    })
    if active_jobs:
        yield SkipReason(f"auto_labeling job already running: {', '.join(active_jobs)}")
        return

    try:
        snapshot = _read_auto_label_backlog_snapshot()
    except FileNotFoundError as exc:
        yield SkipReason(f"DuckDB not found: {exc}")
        return
    except Exception as exc:
        yield SkipReason(f"auto_labeling backlog read failed: {exc}")
        return

    current_count = int(snapshot["backlog_count"])
    current_state_token = str(snapshot["state_token"] or "")

    prev_count, prev_state_token, prev_seq = _parse_cursor(context.cursor)
    next_cursor = {
        "last_count": current_count,
        "last_state_token": current_state_token,
        "event_seq": prev_seq,
    }

    if current_count <= 0:
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        yield SkipReason("auto_labeling backlog 없음")
        return

    if prev_count is not None and current_count == prev_count and current_state_token == (prev_state_token or ""):
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        yield SkipReason(f"auto_labeling backlog unchanged: count={current_count}")
        return

    event_seq = prev_seq + 1
    next_cursor["event_seq"] = event_seq
    context.update_cursor(json.dumps(next_cursor, sort_keys=True))
    update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]

    yield RunRequest(
        run_key=f"auto-labeling-e{event_seq}-c{current_count}-{update_token}",
        tags={
            "trigger": "auto_labeling_sensor",
            "backlog_count": str(current_count),
            "gemini_pending_count": str(snapshot.get("gemini_pending_count", 0)),
            "caption_pending_count": str(snapshot.get("caption_pending_count", 0)),
            "clip_pending_count": str(snapshot.get("clip_pending_count", 0)),
            "event_seq": str(event_seq),
        },
    )


def _parse_cursor(raw_cursor: str | None) -> tuple[int | None, str | None, int]:
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
