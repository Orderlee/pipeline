"""Detection backlog sensor 팩토리 (Dagster 의존).

순수 헬퍼(`read_backlog_snapshot`, `parse_sensor_cursor`)는
vlm_pipeline.lib.detection_sensor_utils 에 위치.
"""

from __future__ import annotations

import json
from hashlib import sha1

from dagster import DefaultSensorStatus, RunRequest, SensorDefinition, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.detection_sensor_utils import parse_sensor_cursor, read_backlog_snapshot
from vlm_pipeline.lib.env_utils import bool_env, int_env


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
    asset_name: str,
    sensor_limit_env: str = "DETECTION_SENSOR_LIMIT",
    default_sensor_limit: int = 200,
) -> SensorDefinition:
    """Detection backlog 센서를 파라미터화해서 생성한다.

    ``env_enable_key`` 가 지정되면 해당 환경변수가 false일 때 센서를 skip한다.
    """
    if env_enable_key:
        enabled_now = bool_env(env_enable_key, False)
        default_status = (
            DefaultSensorStatus.RUNNING if (default_running and enabled_now) else DefaultSensorStatus.STOPPED
        )
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

        active_jobs = sorted(
            {str(run.job_name) for run in in_flight_runs if str(getattr(run, "job_name", "") or "") in target_jobs}
        )
        if active_jobs:
            yield SkipReason(f"{label_tool} job already running: {', '.join(active_jobs)}")
            return

        try:
            snapshot = read_backlog_snapshot(label_tool)
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

        if prev_count is not None and current_count == prev_count and current_state_token == (prev_state_token or ""):
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
            run_config={
                "ops": {
                    asset_name: {
                        "config": {
                            "limit": int_env(sensor_limit_env, default_sensor_limit, 1),
                        }
                    }
                }
            },
        )

    return _sensor_fn
