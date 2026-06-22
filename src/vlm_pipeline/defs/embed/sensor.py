"""frame_embedding_backlog_sensor — 미임베딩 프레임 감지 → frame_embedding_job.

순수 결정(decide_frame_embedding_run)은 helpers.py — dagster 비의존 단위 테스트.
in-flight 가드로 중복 run 방지, monotonic seq 기반 고유 run_key 로 backlog 미감소 시에도 재시도 보장.
"""

from __future__ import annotations

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    SensorResult,
    sensor,
)

from vlm_pipeline.defs.embed.assets import DEFAULT_MODEL, FRAME_ROLES, VIDEO_FRAME_ROLES
from vlm_pipeline.defs.embed.helpers import decide_frame_embedding_run, decide_video_embedding_run
from vlm_pipeline.lib.video_embedding import METHOD_FRAME_POOL, video_model_name

_SENSOR_LIMIT = 200
_JOB_NAME = "frame_embedding_job"
_CAPTION_JOB_NAME = "caption_embedding_job"
_VIDEO_JOB_NAME = "video_embedding_job"
_ACTIVE_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]


def _has_in_flight_run(context, job_name: str) -> bool:
    """지정 job 의 진행 중 run 존재 여부 (중복 run pile-up 방지)."""
    try:
        runs = context.instance.get_runs(
            filters=RunsFilter(job_name=job_name, statuses=_ACTIVE_STATUSES),
            limit=1,
        )
        return len(runs) > 0
    except Exception as exc:  # noqa: BLE001 — 조회 실패 시 보수적으로 in_flight 취급
        context.log.warning("%s in-flight check failed: %s", job_name, exc)
        return True


@sensor(
    name="frame_embedding_backlog_sensor",
    job_name=_JOB_NAME,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=120,
    required_resource_keys={"db"},
    description="미임베딩 프레임 backlog 감지 → frame_embedding_job 트리거 (기본 OFF)",
)
def frame_embedding_backlog_sensor(context) -> SensorResult:
    db = context.resources.db
    count = db.count_frame_backlog(model_name=DEFAULT_MODEL, image_roles=FRAME_ROLES)
    in_flight = _has_in_flight_run(context, _JOB_NAME)
    run_config, cursor, run_key = decide_frame_embedding_run(
        backlog_count=count,
        prev_cursor=context.cursor,
        in_flight=in_flight,
        limit=_SENSOR_LIMIT,
        model_name=DEFAULT_MODEL,
        image_roles=FRAME_ROLES,
    )
    if run_config is None:
        return SensorResult(run_requests=[], cursor=cursor)
    return SensorResult(
        run_requests=[RunRequest(run_key=run_key, run_config=run_config)],
        cursor=cursor,
    )


@sensor(
    name="caption_embedding_backlog_sensor",
    job_name=_CAPTION_JOB_NAME,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=120,
    required_resource_keys={"db"},
    description="미임베딩 caption backlog 감지 → caption_embedding_job 트리거 (기본 OFF)",
)
def caption_embedding_backlog_sensor(context) -> SensorResult:
    db = context.resources.db
    count = db.count_caption_backlog(model_name=DEFAULT_MODEL)
    in_flight = _has_in_flight_run(context, _CAPTION_JOB_NAME)
    run_config, cursor, run_key = decide_frame_embedding_run(
        backlog_count=count,
        prev_cursor=context.cursor,
        in_flight=in_flight,
        limit=_SENSOR_LIMIT,
        model_name=DEFAULT_MODEL,
    )
    if run_config is None:
        return SensorResult(run_requests=[], cursor=cursor)
    # decide_frame_embedding_run returns run_config keyed for frame_embedding op;
    # replace op key with caption_embedding
    caption_run_config = {"ops": {"caption_embedding": run_config["ops"]["frame_embedding"]}}
    # replace run_key prefix
    caption_run_key = run_key.replace("frame-embed-", "caption-embed-") if run_key else run_key
    return SensorResult(
        run_requests=[RunRequest(run_key=caption_run_key, run_config=caption_run_config)],
        cursor=cursor,
    )


_DEFAULT_VIDEO_MODEL = video_model_name(DEFAULT_MODEL, METHOD_FRAME_POOL)


@sensor(
    name="video_embedding_backlog_sensor",
    job_name=_VIDEO_JOB_NAME,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=120,
    required_resource_keys={"db"},
    description="미임베딩 video backlog 감지 → video_embedding_job 트리거 (기본 OFF)",
)
def video_embedding_backlog_sensor(context) -> SensorResult:
    db = context.resources.db
    count = db.count_video_backlog(
        video_model_name=_DEFAULT_VIDEO_MODEL,
        frame_model_name=DEFAULT_MODEL,
        video_roles=VIDEO_FRAME_ROLES,
    )
    in_flight = _has_in_flight_run(context, _VIDEO_JOB_NAME)
    run_config, cursor, run_key = decide_video_embedding_run(
        backlog_count=count,
        prev_cursor=context.cursor,
        in_flight=in_flight,
        limit=_SENSOR_LIMIT,
        video_model_name=_DEFAULT_VIDEO_MODEL,
        frame_model_name=DEFAULT_MODEL,
        video_roles=VIDEO_FRAME_ROLES,
    )
    if run_config is None:
        return SensorResult(run_requests=[], cursor=cursor)
    return SensorResult(
        run_requests=[RunRequest(run_key=run_key, run_config=run_config)],
        cursor=cursor,
    )
