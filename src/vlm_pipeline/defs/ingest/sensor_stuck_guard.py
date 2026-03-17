"""stuck_run_guard_sensor — 오래 정체된 STARTED run cancel + 자동 재큐잉."""

from __future__ import annotations

import os
import time

from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.env_utils import bool_env, int_env


@sensor(
    minimum_interval_seconds=int_env("STUCK_RUN_GUARD_INTERVAL_SEC", 120, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="오래 정체된 STARTED run cancel + (옵션) 자동 재큐잉",
)
def stuck_run_guard_sensor(context):
    if not bool_env("STUCK_RUN_GUARD_ENABLED", True):
        return SkipReason("stuck run guard 비활성화됨")

    timeout_sec = int_env("STUCK_RUN_GUARD_TIMEOUT_SEC", 3 * 60 * 60, 60)
    max_cancels = int_env("STUCK_RUN_GUARD_MAX_CANCELS_PER_TICK", 1, 1)
    auto_requeue_enabled = bool_env("STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED", True)
    max_requeues = int_env("STUCK_RUN_GUARD_MAX_REQUEUES_PER_TICK", 1, 1)
    target_jobs_raw = os.getenv(
        "STUCK_RUN_GUARD_TARGET_JOBS",
        "mvp_stage_job,ingest_job,label_job,auto_labeling_job,process_build_job,video_frame_extract_job,motherduck_sync_job",
    )
    target_jobs = {item.strip() for item in target_jobs_raw.split(",") if item.strip()}
    now_ts = time.time()

    try:
        started_runs = context.instance.get_runs(
            filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]),
            limit=200,
        )
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"stuck run 조회 실패: {exc}")

    canceled = 0
    inspected = 0
    for run in started_runs:
        if run.job_name not in target_jobs:
            continue
        tags = getattr(run, "tags", {}) or {}
        if str(tags.get("duckdb_writer", "")).lower() != "true":
            continue

        inspected += 1
        try:
            stats = context.instance.get_run_stats(run.run_id)
            start_time = getattr(stats, "start_time", None)
        except Exception:  # noqa: BLE001
            start_time = None
        if not start_time:
            continue

        age_sec = int(now_ts - float(start_time))
        if age_sec < timeout_sec:
            continue

        try:
            cancel_requested = bool(context.instance.run_coordinator.cancel_run(run.run_id))
            context.instance.add_run_tags(
                run.run_id,
                {"stuck_guard_cancel_requested_at": str(int(now_ts))},
            )
            context.log.warning(
                f"stuck run cancel 요청: run_id={run.run_id}, job={run.job_name}, "
                f"age={age_sec}s, cancel_requested={cancel_requested}"
            )
            canceled += 1
        except Exception as exc:  # noqa: BLE001
            context.log.warning(f"stuck run cancel 실패: run_id={run.run_id}: {exc}")

        if canceled >= max_cancels:
            break

    requeued = 0
    delegated_to_incoming = 0
    if auto_requeue_enabled:
        try:
            canceled_runs = context.instance.get_runs(
                filters=RunsFilter(statuses=[DagsterRunStatus.CANCELED]),
                limit=200,
            )
        except Exception as exc:  # noqa: BLE001
            context.log.warning(f"canceled run 조회 실패: {exc}")
            canceled_runs = []

        for run in canceled_runs:
            if requeued >= max_requeues:
                break
            if run.job_name not in target_jobs:
                continue

            tags = getattr(run, "tags", {}) or {}
            if "stuck_guard_cancel_requested_at" not in tags:
                continue
            if "stuck_guard_requeued_at" in tags or "stuck_guard_requeue_delegated_at" in tags:
                continue

            if str(tags.get("trigger", "")).strip() == "incoming_manifest_sensor":
                delegated_to_incoming += 1
                try:
                    context.instance.add_run_tags(
                        run.run_id,
                        {"stuck_guard_requeue_delegated_at": str(int(now_ts))},
                    )
                except Exception:  # noqa: BLE001
                    pass
                continue

            try:
                next_tags = dict(tags)
                next_tags["trigger"] = "stuck_run_guard_requeue"
                next_tags["stuck_guard_requeue_of"] = run.run_id
                next_tags["stuck_guard_requeue_ts"] = str(int(now_ts))

                run_config = getattr(run, "run_config", None) or {}
                run_key = f"stuck-requeue-{run.run_id}-{int(now_ts)}"

                yield RunRequest(
                    job_name=run.job_name,
                    run_key=run_key,
                    run_config=run_config,
                    tags=next_tags,
                )
                context.instance.add_run_tags(
                    run.run_id,
                    {"stuck_guard_requeued_at": str(int(now_ts))},
                )
                requeued += 1
                context.log.warning(
                    f"stuck run 자동 재큐잉: from_run={run.run_id}, job={run.job_name}, run_key={run_key}"
                )
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"stuck run 재큐잉 실패: run_id={run.run_id}: {exc}")

    if canceled == 0 and requeued == 0 and delegated_to_incoming == 0:
        return SkipReason(
            f"stuck run 없음 (inspected={inspected}, timeout={timeout_sec}s, jobs={sorted(target_jobs)})"
        )

    return SkipReason(
        f"stuck run guard 처리 완료: canceled={canceled}, requeued={requeued}, "
        f"delegated={delegated_to_incoming}, timeout={timeout_sec}s, jobs={sorted(target_jobs)}"
    )
