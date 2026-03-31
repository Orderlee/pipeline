"""PROCESS sensor — pending/stale video frame backlog 감지 후 추출 job 트리거."""

from __future__ import annotations

import json
import time
from hashlib import sha1
from pathlib import Path

import duckdb
from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.env_utils import (
    bool_env,
    default_duckdb_path,
    int_env,
    is_duckdb_lock_conflict,
)

VIDEO_FRAME_SENSOR_TARGET_JOBS = {
    "auto_labeling_job",
    "video_frame_extract_job",
    "mvp_stage_job",
    "process_build_job",
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


def _read_video_frame_backlog_snapshot() -> dict[str, int | str | None]:
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
                WITH existing_frames AS (
                    SELECT
                        source_asset_id,
                        frame_index,
                        COUNT(*) AS frame_row_count
                    FROM image_metadata
                    WHERE image_role = 'video_event_frame'
                      AND source_clip_id IS NULL
                    GROUP BY source_asset_id, frame_index
                ),
                missing_label_events AS (
                    SELECT
                        r.asset_id,
                        l.label_id,
                        l.created_at
                    FROM raw_files r
                    JOIN labels l ON l.asset_id = r.asset_id
                    JOIN video_metadata vm ON vm.asset_id = r.asset_id
                    LEFT JOIN existing_frames ef
                      ON ef.source_asset_id = r.asset_id
                     AND ef.frame_index = COALESCE(l.event_index, 0) + 1
                    WHERE r.media_type = 'video'
                      AND r.ingest_status = 'completed'
                      AND l.label_status = 'completed'
                      AND (l.timestamp_start_sec IS NOT NULL OR l.timestamp_end_sec IS NOT NULL)
                      AND COALESCE(ef.frame_row_count, 0) = 0
                )
                SELECT
                    COUNT(DISTINCT asset_id) AS backlog_asset_count,
                    COUNT(*) AS backlog_label_count,
                    CAST(MAX(created_at) AS VARCHAR) AS latest_label_created_at
                FROM missing_label_events
                """
            ).fetchone()
            backlog_asset_count = int(row[0]) if row and row[0] is not None else 0
            backlog_label_count = int(row[1]) if row and row[1] is not None else 0
            latest_created_at = str(row[2]) if row and row[2] is not None else None
            state_token = (
                f"assets={backlog_asset_count}|labels={backlog_label_count}|created_at={latest_created_at or ''}"
            )
            return {
                "backlog_count": backlog_asset_count,
                "pending_count": backlog_label_count,
                "processing_count": 0,
                "latest_raw_updated_at": latest_created_at,
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

    raise RuntimeError("video frame sensor snapshot retry exhausted unexpectedly")


# Future activation checklist for processed clip based auto extraction:
# 1. Uncomment the snapshot helper below.
# 2. Uncomment the processed_clip_frame_extract_sensor definition below.
# 3. Register the sensor and companion job in src/vlm_pipeline/definitions.py.
# 4. Disable or remove video_frame_extract_sensor once processed_clips becomes the source of truth.
#
# def _read_processed_clip_frame_backlog_snapshot() -> dict[str, int | str | None]:
#     db_path = Path(default_duckdb_path())
#     if not db_path.exists():
#         raise FileNotFoundError(str(db_path))
#
#     retry_count = int_env("DUCKDB_SENSOR_LOCK_RETRY_COUNT", 5, 0)
#     retry_delay_ms = int_env("DUCKDB_SENSOR_LOCK_RETRY_DELAY_MS", 200, 10)
#     max_retry_delay_ms = int_env("DUCKDB_SENSOR_LOCK_RETRY_MAX_DELAY_MS", 2000, retry_delay_ms)
#
#     for attempt in range(retry_count + 1):
#         conn = None
#         try:
#             conn = duckdb.connect(str(db_path), read_only=True)
#             row = conn.execute(
#                 """
#                 WITH extracted_frames AS (
#                     SELECT source_clip_id, COUNT(*) AS frame_row_count
#                     FROM image_metadata
#                     WHERE image_role = 'processed_clip_frame'
#                       AND source_clip_id IS NOT NULL
#                     GROUP BY source_clip_id
#                 )
#                 SELECT
#                     COUNT(*) AS backlog_count,
#                     CAST(MAX(pc.created_at) AS VARCHAR) AS latest_clip_created_at
#                 FROM processed_clips pc
#                 JOIN raw_files r ON r.asset_id = pc.source_asset_id
#                 LEFT JOIN extracted_frames ef ON ef.source_clip_id = pc.clip_id
#                 WHERE pc.process_status = 'completed'
#                   AND r.media_type = 'video'
#                   AND r.ingest_status = 'completed'
#                   AND COALESCE(ef.frame_row_count, 0) = 0
#                 """
#             ).fetchone()
#             backlog_count = int(row[0]) if row and row[0] is not None else 0
#             latest_created_at = str(row[1]) if row and row[1] is not None else ""
#             state_token = f"count={backlog_count}|created_at={latest_created_at}"
#             return {
#                 "backlog_count": backlog_count,
#                 "latest_clip_created_at": latest_created_at,
#                 "state_token": state_token,
#             }
#         except Exception as exc:  # noqa: BLE001
#             if is_duckdb_lock_conflict(exc) and attempt < retry_count:
#                 delay_ms = min(max_retry_delay_ms, retry_delay_ms * (2**attempt))
#                 time.sleep(delay_ms / 1000.0)
#                 continue
#             raise
#         finally:
#             if conn is not None:
#                 conn.close()
#
#     raise RuntimeError("processed clip frame sensor snapshot retry exhausted unexpectedly")
#
#
# @sensor(
#     job_name="processed_clip_frame_extract_job",
#     minimum_interval_seconds=int_env("PROCESSED_CLIP_FRAME_SENSOR_INTERVAL_SEC", 60, 30),
#     default_status=DefaultSensorStatus.STOPPED,
#     description="processed_clips completed video backlog 감지 시 extracted_processed_clip_frames 자동 실행",
# )
# def processed_clip_frame_extract_sensor(context):
#     if not bool_env("PROCESSED_CLIP_FRAME_SENSOR_ENABLED", False):
#         yield SkipReason("processed_clip_frame_extract_sensor 비활성화됨")
#         return
#
#     target_jobs = {
#         "video_frame_extract_job",
#         "processed_clip_frame_extract_job",
#         "process_build_job",
#         "mvp_stage_job",
#     }
#     try:
#         in_flight_runs = context.instance.get_runs(
#             filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
#             limit=200,
#         )
#     except Exception as exc:  # noqa: BLE001
#         yield SkipReason(f"processed clip in-flight run 조회 실패: {exc}")
#         return
#
#     active_jobs = sorted(
#         {
#             str(run.job_name)
#             for run in in_flight_runs
#             if str(getattr(run, "job_name", "") or "") in target_jobs
#         }
#     )
#     if active_jobs:
#         yield SkipReason(
#             "processed clip frame extraction job already queued/running: " + ", ".join(active_jobs)
#         )
#         return
#
#     try:
#         snapshot = _read_processed_clip_frame_backlog_snapshot()
#     except FileNotFoundError as exc:
#         yield SkipReason(f"DuckDB not found: {exc}")
#         return
#     except Exception as exc:  # noqa: BLE001
#         yield SkipReason(f"processed clip frame backlog read failed: {exc}")
#         return
#
#     current_count = int(snapshot["backlog_count"])
#     current_state_token = str(snapshot["state_token"] or "")
#     previous_count, previous_state_token, previous_event_seq = _parse_cursor_state(context.cursor)
#     next_cursor = {
#         "last_count": current_count,
#         "last_state_token": current_state_token,
#         "event_seq": previous_event_seq,
#     }
#
#     if current_count <= 0:
#         context.update_cursor(json.dumps(next_cursor, sort_keys=True))
#         yield SkipReason("processed clip frame backlog 없음")
#         return
#
#     if previous_count is not None and current_count == previous_count and current_state_token == (previous_state_token or ""):
#         context.update_cursor(json.dumps(next_cursor, sort_keys=True))
#         yield SkipReason(f"processed clip frame backlog unchanged: count={current_count}")
#         return
#
#     event_seq = previous_event_seq + 1
#     next_cursor["event_seq"] = event_seq
#     context.update_cursor(json.dumps(next_cursor, sort_keys=True))
#     update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]
#     yield RunRequest(
#         run_key=f"processed-clip-frame-extract-e{event_seq}-c{current_count}-{update_token}",
#         run_config={
#             "ops": {
#                 "extracted_processed_clip_frames": {
#                     "config": {
#                         "limit": int_env("PROCESSED_CLIP_FRAME_SENSOR_LIMIT", 200, 1),
#                         "jpeg_quality": int_env("PROCESSED_CLIP_FRAME_SENSOR_JPEG_QUALITY", 90, 1),
#                         "max_frames_per_video": int_env(
#                             "PROCESSED_CLIP_FRAME_SENSOR_MAX_FRAMES_PER_VIDEO", 12, 1
#                         ),
#                         "overwrite_existing": bool_env(
#                             "PROCESSED_CLIP_FRAME_SENSOR_OVERWRITE_EXISTING", False
#                         ),
#                     }
#                 }
#             }
#         },
#         tags={
#             "trigger": "processed_clip_frame_extract_sensor",
#             "backlog_count": str(current_count),
#             "event_seq": str(event_seq),
#         },
#     )


@sensor(
    job_name="video_frame_extract_job",
    minimum_interval_seconds=int_env("VIDEO_FRAME_SENSOR_INTERVAL_SEC", 60, 30),
    default_status=DefaultSensorStatus.STOPPED,
    description="[DEPRECATED] auto_labeling_sensor로 대체됨. 수동 실행만 가능.",
)
def video_frame_extract_sensor(context):
    if not bool_env("VIDEO_FRAME_SENSOR_ENABLED", True):
        yield SkipReason("video_frame_extract_sensor 비활성화됨")
        return

    try:
        in_flight_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED]),
            limit=200,
        )
    except Exception as exc:  # noqa: BLE001
        yield SkipReason(f"video frame in-flight run 조회 실패: {exc}")
        return

    active_video_jobs = [
        run
        for run in in_flight_runs
        if str(getattr(run, "job_name", "") or "") in VIDEO_FRAME_SENSOR_TARGET_JOBS
    ]
    if active_video_jobs:
        active_jobs = sorted({str(run.job_name) for run in active_video_jobs})
        yield SkipReason(
            "video frame extraction job already queued/running: " + ", ".join(active_jobs)
        )
        return

    try:
        snapshot = _read_video_frame_backlog_snapshot()
    except FileNotFoundError as exc:
        yield SkipReason(f"DuckDB not found: {exc}")
        return
    except Exception as exc:  # noqa: BLE001
        yield SkipReason(f"video frame backlog read failed: {exc}")
        return

    current_count = int(snapshot["backlog_count"])
    current_pending = int(snapshot["pending_count"])
    current_processing = int(snapshot["processing_count"])
    current_updated_at = str(snapshot["latest_raw_updated_at"] or "")
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
            yield SkipReason("video frame backlog baseline initialized: 0")
            return

        event_seq = previous_event_seq + 1
        next_cursor["event_seq"] = event_seq
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]
        yield RunRequest(
            run_key=f"video-frame-extract-e{event_seq}-c{current_count}-{update_token}",
            run_config={
                "ops": {
                    "clip_captioning": {
                        "config": {
                            "limit": int_env("VIDEO_FRAME_SENSOR_LIMIT", 200, 1),
                            "jpeg_quality": int_env("VIDEO_FRAME_SENSOR_JPEG_QUALITY", 90, 1),
                            "overwrite_existing": bool_env(
                                "VIDEO_FRAME_SENSOR_OVERWRITE_EXISTING", False
                            ),
                        }
                    }
                }
            },
            tags={
                "trigger": "video_frame_extract_sensor",
                "baseline": "true",
                "backlog_count": str(current_count),
                "pending_count": str(current_pending),
                "processing_count": str(current_processing),
                "backlog_updated_at": current_updated_at,
            },
        )
        return

    backlog_changed = current_count != previous_count
    state_changed = current_state_token != (previous_state_token or "")

    if current_count <= 0:
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        yield SkipReason("video frame backlog 없음")
        return

    if not backlog_changed and not state_changed:
        context.update_cursor(json.dumps(next_cursor, sort_keys=True))
        yield SkipReason(
            "video frame backlog unchanged: "
            f"count={current_count}, pending={current_pending}, processing={current_processing}"
        )
        return

    event_seq = previous_event_seq + 1
    next_cursor["event_seq"] = event_seq
    context.update_cursor(json.dumps(next_cursor, sort_keys=True))
    update_token = sha1(current_state_token.encode("utf-8")).hexdigest()[:10]

    yield RunRequest(
        run_key=f"video-frame-extract-e{event_seq}-c{current_count}-{update_token}",
        run_config={
            "ops": {
                "clip_captioning": {
                    "config": {
                        "limit": int_env("VIDEO_FRAME_SENSOR_LIMIT", 200, 1),
                        "jpeg_quality": int_env("VIDEO_FRAME_SENSOR_JPEG_QUALITY", 90, 1),
                        "overwrite_existing": bool_env(
                            "VIDEO_FRAME_SENSOR_OVERWRITE_EXISTING", False
                        ),
                    }
                }
            }
        },
        tags={
            "trigger": "video_frame_extract_sensor",
            "prev_backlog_count": str(previous_count),
            "backlog_count": str(current_count),
            "pending_count": str(current_pending),
            "processing_count": str(current_processing),
            "delta": str(current_count - previous_count),
            "event_seq": str(event_seq),
            "prev_backlog_state": previous_state_token or "",
            "backlog_state": current_state_token,
            "backlog_updated_at": current_updated_at,
        },
    )
