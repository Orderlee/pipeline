"""stuck_run_guard_sensor — 오래 정체된 STARTED run cancel + 자동 재큐잉."""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.defs.dispatch.service import resolve_dispatch_request_id_from_tags
from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.runtime_settings import load_stuck_run_guard_settings

_PID_PATTERNS = (
    re.compile(r"\bpid:\s*(\d+)\b"),
    re.compile(r"\bprocess \(pid:\s*(\d+)\)"),
    re.compile(r"\bparent process \(pid:\s*(\d+)\)"),
)


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _extract_run_process_state(context, run_id: str) -> tuple[float | None, list[int], list[int]]:
    try:
        records = context.instance.get_records_for_run(run_id, of_type=None, limit=100)
    except Exception:  # noqa: BLE001
        return None, [], []

    latest_event_ts = None
    observed_pids: set[int] = set()
    for record in records.records:
        entry = record.event_log_entry
        latest_event_ts = max(latest_event_ts or 0.0, float(entry.timestamp or 0.0))
        message = str(entry.user_message or "")
        for pattern in _PID_PATTERNS:
            for matched in pattern.findall(message):
                try:
                    observed_pids.add(int(matched))
                except ValueError:
                    continue

    live_pids = sorted(pid for pid in observed_pids if _pid_is_alive(pid))
    return latest_event_ts, sorted(observed_pids), live_pids


def _log_orphaned_dispatch_context(context, run, *, reason: str, latest_event_age_sec: int) -> None:
    db_resource = getattr(context.resources, "db", None)
    if db_resource is None:
        return

    tags = getattr(run, "tags", {}) or {}
    folder_name = str(tags.get("folder_name_original") or tags.get("folder_name") or "").strip()
    manifest_path = str(tags.get("manifest_path") or "").strip()
    if not folder_name and not manifest_path:
        return

    raw_file_count = None
    try:
        raw_file_count = db_resource.count_raw_files_for_source_unit_name(folder_name)
    except Exception:  # noqa: BLE001
        raw_file_count = None

    config = PipelineConfig()
    incoming_exists = False
    archive_exists = False
    if folder_name:
        incoming_exists = (Path(config.incoming_dir) / folder_name).exists()
        archive_exists = (Path(config.archive_dir) / folder_name).exists()

    context.log.warning(
        "orphaned dispatch run 감지: "
        f"run_id={run.run_id}, job={run.job_name}, reason={reason}, "
        f"folder={folder_name or '<unknown>'}, "
        f"manifest_exists={Path(manifest_path).exists() if manifest_path else False}, "
        f"raw_files={raw_file_count}, incoming_exists={incoming_exists}, "
        f"archive_exists={archive_exists}, latest_event_age={latest_event_age_sec}s"
    )


def _decide_stuck_run_cancel_reason(
    *,
    job_name: str,
    age_sec: int,
    latest_event_age_sec: int,
    observed_pids: list[int],
    live_pids: list[int],
    timeout_sec: int,
    orphan_timeout_sec: int,
    orphan_only_jobs: set[str],
) -> str | None:
    orphaned = bool(observed_pids) and not live_pids and latest_event_age_sec >= orphan_timeout_sec
    orphan_only = job_name in orphan_only_jobs
    timed_out = age_sec >= timeout_sec and not orphan_only
    if timed_out:
        return "timeout"
    if orphaned:
        return "orphaned_no_live_worker"
    return None


def _should_skip_stuck_guard_requeue(*, job_name: str, trigger: str, orphan_only_jobs: set[str]) -> bool:
    if job_name in orphan_only_jobs:
        return True
    return trigger.strip() == "incoming_manifest_sensor"


@sensor(
    minimum_interval_seconds=int_env("STUCK_RUN_GUARD_INTERVAL_SEC", 120, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="오래 정체된 STARTED run cancel + (옵션) 자동 재큐잉",
    required_resource_keys={"db"},
)
def stuck_run_guard_sensor(context):
    settings = load_stuck_run_guard_settings()
    if not settings.enabled:
        return SkipReason("stuck run guard 비활성화됨")

    timeout_sec = settings.timeout_sec
    orphan_timeout_sec = settings.orphan_timeout_sec
    max_cancels = settings.max_cancels
    auto_requeue_enabled = settings.auto_requeue_enabled
    max_requeues = settings.max_requeues
    target_jobs = settings.target_jobs
    orphan_only_jobs = settings.orphan_only_jobs
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
        latest_event_ts, observed_pids, live_pids = _extract_run_process_state(context, run.run_id)
        latest_event_age_sec = (
            int(now_ts - latest_event_ts)
            if latest_event_ts is not None and latest_event_ts > 0
            else age_sec
        )
        cancel_reason = _decide_stuck_run_cancel_reason(
            job_name=str(run.job_name or ""),
            age_sec=age_sec,
            latest_event_age_sec=latest_event_age_sec,
            observed_pids=observed_pids,
            live_pids=live_pids,
            timeout_sec=timeout_sec,
            orphan_timeout_sec=orphan_timeout_sec,
            orphan_only_jobs=orphan_only_jobs,
        )
        if cancel_reason is None:
            continue

        if cancel_reason == "orphaned_no_live_worker":
            _log_orphaned_dispatch_context(
                context,
                run,
                reason=cancel_reason,
                latest_event_age_sec=latest_event_age_sec,
            )

        try:
            cancel_requested = bool(context.instance.run_coordinator.cancel_run(run.run_id))
            context.instance.add_run_tags(
                run.run_id,
                {
                    "stuck_guard_cancel_requested_at": str(int(now_ts)),
                    "stuck_guard_cancel_reason": cancel_reason,
                },
            )
            request_id = resolve_dispatch_request_id_from_tags(tags)
            if request_id:
                db_resource = getattr(context.resources, "db", None)
                if db_resource is not None:
                    db_resource.close_dispatch_request(
                        request_id,
                        status="canceled",
                        error_message=f"stuck_guard:{cancel_reason}",
                    )
            context.log.warning(
                f"stuck run cancel 요청: run_id={run.run_id}, job={run.job_name}, "
                f"age={age_sec}s, latest_event_age={latest_event_age_sec}s, "
                f"observed_pids={observed_pids}, live_pids={live_pids}, "
                f"cancel_reason={cancel_reason}, cancel_requested={cancel_requested}"
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
            if "stuck_guard_requeue_skipped_at" in tags:
                continue

            trigger = str(tags.get("trigger", "")).strip()
            if _should_skip_stuck_guard_requeue(
                job_name=str(run.job_name or ""),
                trigger=trigger,
                orphan_only_jobs=orphan_only_jobs,
            ):
                if trigger == "incoming_manifest_sensor":
                    delegated_to_incoming += 1
                try:
                    context.instance.add_run_tags(
                        run.run_id,
                        {"stuck_guard_requeue_skipped_at": str(int(now_ts))},
                    )
                except Exception:  # noqa: BLE001
                    pass
                if run.job_name in orphan_only_jobs:
                    context.log.warning(
                        "stuck run 재큐잉 생략: run_id=%s, job=%s, reason=orphan_only_job",
                        run.run_id,
                        run.job_name,
                    )
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
            "stuck run 없음 "
            f"(inspected={inspected}, timeout={timeout_sec}s, orphan_timeout={orphan_timeout_sec}s, "
            f"jobs={sorted(target_jobs)})"
        )

    return SkipReason(
        f"stuck run guard 처리 완료: canceled={canceled}, requeued={requeued}, "
        f"delegated={delegated_to_incoming}, timeout={timeout_sec}s, "
        f"orphan_timeout={orphan_timeout_sec}s, jobs={sorted(target_jobs)}"
    )
