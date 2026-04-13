"""incoming_manifest_sensor — NFS .manifests/pending/ 폴링."""

from __future__ import annotations

import json
import time
from pathlib import Path

from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.runtime_profile import resolve_runtime_profile
from vlm_pipeline.resources.config import PipelineConfig

from .runtime_policy import pending_manifest_allowed
from .sensor_helpers import (
    build_source_unit_run_key,
    collect_in_flight_runs,
    collect_in_flight_source_units,
    load_pending_manifest_entries,
    manifest_retry_state,
    move_superseded_manifests,
    parse_cursor,
    select_latest_per_source_unit,
    supersede_by_stable_signature,
)


def _move_staging_blocked_manifest(manifest_path: Path, processed_dir: Path, context) -> None:
    destination = processed_dir / f"{manifest_path.stem}.staging_blocked.json"
    suffix = 2
    while destination.exists():
        destination = processed_dir / f"{manifest_path.stem}.staging_blocked__{suffix}.json"
        suffix += 1

    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.rename(destination)
        context.log.info(
            f"staging auto_bootstrap manifest 무시: {manifest_path.name} -> {destination.name}"
        )
    except OSError as exc:
        context.log.warning(
            f"staging auto_bootstrap manifest 이동 실패: {manifest_path} -> {destination}: {exc}"
        )


@sensor(
    job_name="ingest_job",
    minimum_interval_seconds=int_env("INCOMING_SENSOR_INTERVAL_SEC", 180, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="NFS 마운트된 .manifests/pending/ 폴링 — NFS 장애 시 graceful skip",
)
def incoming_manifest_sensor(context):
    """NFS 마운트된 .manifests/pending/ 폴링.

    NFS 장애 시 SkipReason 반환 → 다음 폴링에서 자동 재시도.
    cursor 기반 중복 방지: 이미 처리된 manifest는 건너뜀.
    """
    config = PipelineConfig()
    runtime_profile = resolve_runtime_profile()
    pending_dir = Path(config.manifest_dir) / "pending"
    processed_dir = Path(config.manifest_dir) / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    try:
        manifests = sorted(pending_dir.glob("*.json"))
    except (OSError, PermissionError, TimeoutError) as exc:
        context.log.warning(f"NFS 접근 실패 (다음 폴링에서 재시도): {exc}")
        yield SkipReason(f"NFS 접근 실패: {exc}")
        return

    previous_state = parse_cursor(context.cursor)
    current_state: dict[str, int] = {}
    new_entries: list[dict] = []

    existing_manifest_keys = {str(m) for m in manifests}

    if not manifests:
        if previous_state:
            context.log.info(f"pending 비어있음 — stale cursor 정리: {len(previous_state)}개 항목 제거")
        context.update_cursor("{}")
        yield SkipReason("pending manifest 없음")
        return

    manifest_entries = load_pending_manifest_entries(manifests, context)
    if manifest_entries:
        allowed_entries: list[dict] = []
        blocked_entries = 0
        for entry in manifest_entries:
            payload = entry.get("payload") or {}
            if pending_manifest_allowed(
                payload,
                config=config,
                runtime_profile=runtime_profile,
            ):
                allowed_entries.append(entry)
                continue
            _move_staging_blocked_manifest(entry["path"], processed_dir, context)
            blocked_entries += 1
        manifest_entries = allowed_entries
        if blocked_entries > 0:
            context.log.info(
                f"runtime policy pending manifest 차단: {blocked_entries}개"
            )

    context.log.info(f"pending manifest 발견: {len(manifests)}개, entries: {len(manifest_entries)}개")

    selected_entries, superseded_entries = select_latest_per_source_unit(manifest_entries)
    selected_entries, sig_superseded = supersede_by_stable_signature(selected_entries)
    superseded_entries.extend(sig_superseded)
    superseded_count = move_superseded_manifests(superseded_entries, processed_dir, context)

    context.log.info(
        f"selected: {len(selected_entries)}개, superseded: {len(superseded_entries)}개"
        f" (sig_superseded: {len(sig_superseded)}개), previous_cursor: {len(previous_state)}개"
    )

    stale_keys = [k for k in previous_state if k not in existing_manifest_keys]
    if stale_keys:
        context.log.info(f"stale cursor 항목 정리: {len(stale_keys)}개 (삭제된 manifest)")

    max_in_flight_runs = max(1, int_env("INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS", 2, 1))
    max_new_run_requests_per_tick = max(
        1, int_env("INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK", 2, 1),
    )
    max_retry_per_manifest = int_env("INCOMING_SENSOR_MAX_RETRY_PER_MANIFEST", 3, 1)
    in_flight_runs = collect_in_flight_runs(context)
    in_flight_run_count = len(in_flight_runs)

    for entry in selected_entries:
        manifest_path = entry["path"]
        manifest_id = str(entry.get("manifest_id", "")).strip()
        mtime_ns = int(entry["mtime_ns"])
        key = str(manifest_path)

        prev_mtime = previous_state.get(key)
        is_new = key not in previous_state
        is_modified = prev_mtime != mtime_ns
        should_retry_failed, failed_run_count, latest_manifest_status = manifest_retry_state(
            context, manifest_id,
        )

        if should_retry_failed and failed_run_count > max_retry_per_manifest:
            current_state[key] = mtime_ns
            context.log.warning(
                f"retry 한도 초과로 manifest 재시도를 중단합니다: {manifest_path.name} "
                f"(failed_runs={failed_run_count}, max_retry={max_retry_per_manifest}, "
                f"latest_status={latest_manifest_status})"
            )
            continue

        if should_retry_failed:
            is_modified = True

        if is_new or is_modified:
            if should_retry_failed:
                context.log.warning(
                    f"실패 이력 manifest 자동 재시도 대상: {manifest_path.name} "
                    f"(failed_runs={failed_run_count}, latest_status={latest_manifest_status})"
                )
            new_entries.append(entry)
            context.log.info(f"새 manifest 감지: {manifest_path.name} (new={is_new}, modified={is_modified})")
        else:
            current_state[key] = mtime_ns
            context.log.info(f"이미 처리된 manifest: {manifest_path.name} (mtime={mtime_ns}, prev={prev_mtime})")

    if not new_entries:
        context.update_cursor(json.dumps(current_state, sort_keys=True))
        if superseded_count > 0:
            yield SkipReason(f"새로운 pending manifest 없음 (중복 manifest 정리={superseded_count})")
        else:
            yield SkipReason("새로운 pending manifest 없음")
        return

    if in_flight_run_count >= max_in_flight_runs:
        context.update_cursor(json.dumps(current_state, sort_keys=True))
        yield SkipReason(
            f"backpressure: ingest manifest job in-flight run이 임계치 이상임 "
            f"(in_flight={in_flight_run_count}, limit={max_in_flight_runs})"
        )
        return

    in_flight_source_units = collect_in_flight_source_units(context, runs=in_flight_runs)
    run_requests: list[RunRequest] = []
    launched_manifest_keys: set[str] = set()
    deferred_in_flight_source = 0
    deferred_global_limit = 0
    deferred_tick_limit = 0

    for entry in new_entries:
        manifest_path = entry["path"]
        source_unit_path = entry["source_unit_path"]
        source_unit_dispatch_key = entry["source_unit_dispatch_key"]
        stable_signature = entry["stable_signature"]
        manifest_id = entry["manifest_id"]
        retry_of_manifest_id = str(entry.get("retry_of_manifest_id", "")).strip()
        manifest_retry_attempt = int(entry.get("retry_attempt", 0) or 0)
        retry_reason = str(entry.get("retry_reason", "")).strip()
        should_retry_failed, failed_run_count, _ = manifest_retry_state(context, manifest_id)

        if source_unit_dispatch_key and source_unit_dispatch_key in in_flight_source_units:
            context.log.info(
                f"in-flight source unit 감지로 RunRequest 건너뜀: "
                f"{source_unit_dispatch_key} (manifest={manifest_path.name})"
            )
            deferred_in_flight_source += 1
            continue

        if in_flight_run_count + len(run_requests) >= max_in_flight_runs:
            deferred_global_limit += 1
            continue
        if len(run_requests) >= max_new_run_requests_per_tick:
            deferred_tick_limit += 1
            continue

        try:
            run_key = build_source_unit_run_key(
                source_unit_path, stable_signature,
                source_unit_dispatch_key=source_unit_dispatch_key,
                manifest_id=manifest_id,
            )
            if should_retry_failed:
                run_key = f"{run_key}-retry-{failed_run_count + 1}-{int(time.time())}"
            retry_attempt_tag = (
                failed_run_count + 1 if should_retry_failed else max(0, manifest_retry_attempt)
            )
            retry_flag = should_retry_failed or (manifest_retry_attempt > 0)
            tags = {
                "trigger": "incoming_manifest_sensor",
                "manifest_path": str(manifest_path),
                "manifest_name": manifest_path.name,
                "source_unit_path": source_unit_path,
                "source_unit_dispatch_key": source_unit_dispatch_key,
                "stable_signature": stable_signature,
                "manifest_id": manifest_id,
                "retry_failed_manifest": "true" if retry_flag else "false",
                "retry_attempt": str(retry_attempt_tag),
            }
            if retry_of_manifest_id:
                tags["retry_of_manifest_id"] = retry_of_manifest_id
            if retry_reason:
                tags["retry_reason"] = retry_reason
            run_requests.append(
                RunRequest(
                    run_key=run_key,
                    run_config={},
                    tags=tags,
                )
            )
            launched_manifest_keys.add(str(manifest_path))
            if source_unit_dispatch_key:
                in_flight_source_units.add(source_unit_dispatch_key)
            context.log.info(f"RunRequest 생성: {manifest_path.name} (run_key={run_key})")
        except Exception as exc:
            context.log.error(f"RunRequest 생성 실패: {manifest_path}: {exc}")

    for entry in new_entries:
        key = str(entry["path"])
        if key in launched_manifest_keys:
            current_state[key] = int(entry["mtime_ns"])

    context.update_cursor(json.dumps(current_state, sort_keys=True))

    if not run_requests:
        details = [
            f"in_flight_source={deferred_in_flight_source}",
            f"global_limit={deferred_global_limit}",
            f"limit={max_in_flight_runs}",
            f"tick_limit={deferred_tick_limit}",
            f"max_new_per_tick={max_new_run_requests_per_tick}",
        ]
        suffix = f", 중복 manifest 정리={superseded_count}" if superseded_count > 0 else ""
        yield SkipReason("신규 manifest enqueue 없음 (" + ", ".join(details) + f"){suffix}")
        return

    for request in run_requests:
        yield request
