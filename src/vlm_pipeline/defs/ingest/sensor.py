"""INGEST sensors — manifest 폴링 + incoming auto bootstrap.

Layer 4: Dagster sensor.
data_pipeline_job 제거됨 — mvp_stage_job으로 트리거.
"""

import json
import os
import time
from hashlib import sha1
from datetime import datetime
from pathlib import Path

from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS
from vlm_pipeline.resources.config import PipelineConfig


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, value)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@sensor(
    job_name="mvp_stage_job",
    minimum_interval_seconds=_int_env("INCOMING_SENSOR_INTERVAL_SEC", 180, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="NFS 마운트된 .manifests/pending/ 폴링 — NFS 장애 시 graceful skip",
)
def incoming_manifest_sensor(context):
    """NFS 마운트된 .manifests/pending/ 폴링.

    NFS 장애 시 SkipReason 반환 → 다음 폴링에서 자동 재시도.
    cursor 기반 중복 방지: 이미 처리된 manifest는 건너뜀.
    """
    config = PipelineConfig()
    pending_dir = Path(config.manifest_dir) / "pending"
    processed_dir = Path(config.manifest_dir) / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    try:
        manifests = sorted(pending_dir.glob("*.json"))
    except (OSError, PermissionError, TimeoutError) as e:
        # NFS 장애 시 skip → 다음 폴링에서 자동 재시도
        context.log.warning(f"NFS 접근 실패 (다음 폴링에서 재시도): {e}")
        yield SkipReason(f"NFS 접근 실패: {e}")
        return

    # cursor 기반 중복 방지
    previous_state = _parse_cursor(context.cursor)
    current_state: dict[str, int] = {}
    new_entries: list[dict] = []

    # 실제 존재하는 파일만 current_state에 반영 (stale cursor 자동 정리)
    existing_manifest_keys = {str(m) for m in manifests}

    if not manifests:
        # pending이 비어있으면 cursor를 완전히 리셋
        if previous_state:
            context.log.info(f"pending 비어있음 — stale cursor 정리: {len(previous_state)}개 항목 제거")
        context.update_cursor("{}")
        yield SkipReason("pending manifest 없음")
        return

    manifest_entries = _load_pending_manifest_entries(manifests, context)
    context.log.info(f"pending manifest 발견: {len(manifests)}개, entries: {len(manifest_entries)}개")

    selected_entries, superseded_entries = _select_latest_per_source_unit(manifest_entries)
    superseded_count = _move_superseded_manifests(superseded_entries, processed_dir, context)

    context.log.info(f"selected: {len(selected_entries)}개, superseded: {len(superseded_entries)}개, previous_cursor: {len(previous_state)}개")

    # stale cursor 항목 정리 (삭제된 파일은 cursor에서 제거)
    stale_keys = [k for k in previous_state if k not in existing_manifest_keys]
    if stale_keys:
        context.log.info(f"stale cursor 항목 정리: {len(stale_keys)}개 (삭제된 manifest)")

    max_in_flight_runs = max(1, _int_env("INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS", 2, 1))
    max_new_run_requests_per_tick = max(
        1,
        _int_env("INCOMING_SENSOR_MAX_NEW_RUN_REQUESTS_PER_TICK", 2, 1),
    )
    max_retry_per_manifest = _int_env("INCOMING_SENSOR_MAX_RETRY_PER_MANIFEST", 3, 1)
    in_flight_runs = _collect_in_flight_runs(context)
    in_flight_run_count = len(in_flight_runs)

    for entry in selected_entries:
        manifest_path = entry["path"]
        manifest_id = str(entry.get("manifest_id", "")).strip()
        mtime_ns = int(entry["mtime_ns"])
        key = str(manifest_path)

        prev_mtime = previous_state.get(key)
        is_new = key not in previous_state
        is_modified = prev_mtime != mtime_ns
        should_retry_failed, failed_run_count, latest_manifest_status = _manifest_retry_state(
            context,
            manifest_id,
        )

        if should_retry_failed and failed_run_count > max_retry_per_manifest:
            current_state[key] = mtime_ns
            context.log.warning(
                "retry 한도 초과로 manifest 재시도를 중단합니다: "
                f"{manifest_path.name} "
                f"(failed_runs={failed_run_count}, max_retry={max_retry_per_manifest}, "
                f"latest_status={latest_manifest_status})"
            )
            continue

        if should_retry_failed:
            is_modified = True

        if is_new or is_modified:
            if should_retry_failed:
                context.log.warning(
                    "실패 이력 manifest 자동 재시도 대상: "
                    f"{manifest_path.name} "
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
            yield SkipReason(
                f"새로운 pending manifest 없음 (중복 manifest 정리={superseded_count})"
            )
        else:
            yield SkipReason("새로운 pending manifest 없음")
        return

    if in_flight_run_count >= max_in_flight_runs:
        context.update_cursor(json.dumps(current_state, sort_keys=True))
        yield SkipReason(
            "backpressure: mvp_stage_job in-flight run이 임계치 이상임 "
            f"(in_flight={in_flight_run_count}, limit={max_in_flight_runs})"
        )
        return

    in_flight_source_units = _collect_in_flight_source_units(context, runs=in_flight_runs)
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
        should_retry_failed, failed_run_count, _ = _manifest_retry_state(context, manifest_id)

        if source_unit_dispatch_key and source_unit_dispatch_key in in_flight_source_units:
            context.log.info(
                "in-flight source unit 감지로 RunRequest 건너뜀: "
                f"{source_unit_dispatch_key} (manifest={manifest_path.name})"
            )
            deferred_in_flight_source += 1
            continue

        # 전역 in-flight 임계치 보호: 이번 tick에서 생성하는 요청까지 포함해 제한
        if in_flight_run_count + len(run_requests) >= max_in_flight_runs:
            deferred_global_limit += 1
            continue
        # tick당 enqueue 상한: backlog가 빠르게 커지는 것을 완화한다.
        if len(run_requests) >= max_new_run_requests_per_tick:
            deferred_tick_limit += 1
            continue

        try:
            run_key = _build_source_unit_run_key(
                source_unit_path,
                stable_signature,
                source_unit_dispatch_key=source_unit_dispatch_key,
            )
            if should_retry_failed:
                run_key = f"{run_key}-retry-{failed_run_count + 1}-{int(time.time())}"
            retry_attempt_tag = (
                failed_run_count + 1
                if should_retry_failed
                else max(0, manifest_retry_attempt)
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
                    run_config={
                        "ops": {
                            "dedup_results": {"config": {"limit": 200, "threshold": 5}},
                            "processed_clips": {"config": {"limit": 1000}},
                        }
                    },
                    tags=tags,
                )
            )
            launched_manifest_keys.add(str(manifest_path))
            if source_unit_dispatch_key:
                # 같은 tick에서 동일 dispatch key 추가 enqueue 방지
                in_flight_source_units.add(source_unit_dispatch_key)
            context.log.info(f"RunRequest 생성: {manifest_path.name} (run_key={run_key})")
        except Exception as e:
            context.log.error(f"RunRequest 생성 실패: {manifest_path}: {e}")

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
        yield SkipReason(
            "신규 manifest enqueue 없음 ("
            + ", ".join(details)
            + f"){suffix}"
        )
        return

    for request in run_requests:
        yield request


@sensor(
    minimum_interval_seconds=_int_env("STUCK_RUN_GUARD_INTERVAL_SEC", 120, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="오래 정체된 STARTED run cancel + (옵션) 자동 재큐잉",
)
def stuck_run_guard_sensor(context):
    if not _bool_env("STUCK_RUN_GUARD_ENABLED", True):
        return SkipReason("stuck run guard 비활성화됨")

    timeout_sec = _int_env("STUCK_RUN_GUARD_TIMEOUT_SEC", 3 * 60 * 60, 60)
    max_cancels = _int_env("STUCK_RUN_GUARD_MAX_CANCELS_PER_TICK", 1, 1)
    auto_requeue_enabled = _bool_env("STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED", True)
    max_requeues = _int_env("STUCK_RUN_GUARD_MAX_REQUEUES_PER_TICK", 1, 1)
    target_jobs_raw = os.getenv(
        "STUCK_RUN_GUARD_TARGET_JOBS",
        "mvp_stage_job,ingest_job,motherduck_sync_job",
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
                "stuck run cancel 요청: "
                f"run_id={run.run_id}, job={run.job_name}, age={age_sec}s, "
                f"cancel_requested={cancel_requested}"
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

            # incoming_manifest_sensor가 생성한 run은 기존 retry 로직이 자동 재큐잉한다.
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
                    "stuck run 자동 재큐잉: "
                    f"from_run={run.run_id}, job={run.job_name}, run_key={run_key}"
                )
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"stuck run 재큐잉 실패: run_id={run.run_id}: {exc}")

    if canceled == 0 and requeued == 0 and delegated_to_incoming == 0:
        return SkipReason(
            f"stuck run 없음 (inspected={inspected}, timeout={timeout_sec}s, jobs={sorted(target_jobs)})"
        )

    return SkipReason(
        "stuck run guard 처리 완료: "
        f"canceled={canceled}, requeued={requeued}, delegated={delegated_to_incoming}, "
        f"timeout={timeout_sec}s, jobs={sorted(target_jobs)}"
    )


def _parse_cursor(raw_cursor: str | None) -> dict[str, int]:
    """cursor JSON 파싱 — 이전 상태 복원."""
    if not raw_cursor:
        return {}
    try:
        data = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    parsed: dict[str, int] = {}
    for key, value in data.items():
        try:
            parsed[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return parsed


def _build_source_unit_run_key(
    source_unit_path: str,
    stable_signature: str,
    source_unit_dispatch_key: str = "",
) -> str:
    """source unit + signature 기반 run_key 생성."""
    source_value = source_unit_dispatch_key or source_unit_path or "<unknown_source_unit>"
    signature_value = stable_signature or "<unknown_signature>"
    base = f"{source_value}|{signature_value}"
    source_hash = sha1(base.encode("utf-8")).hexdigest()[:20]
    return f"incoming-unit-{source_hash}"


def _read_manifest_payload(manifest_path: Path, context) -> dict:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"manifest JSON 파싱 실패: {manifest_path}: {exc}")
        return {}

    if not isinstance(payload, dict):
        context.log.warning(f"manifest 형식 오류(객체 아님): {manifest_path}")
        return {}
    return payload


def _load_pending_manifest_entries(manifests: list[Path], context) -> list[dict]:
    entries: list[dict] = []
    for manifest_path in manifests:
        payload = _read_manifest_payload(manifest_path, context)
        source_unit_path = str(payload.get("source_unit_path", "")).strip()
        source_unit_dispatch_key = (
            str(payload.get("source_unit_dispatch_key", "")).strip() or source_unit_path
        )
        stable_signature = str(payload.get("stable_signature", "")).strip()
        manifest_id = str(payload.get("manifest_id", "") or manifest_path.stem).strip()
        retry_of_manifest_id = str(payload.get("retry_of_manifest_id", "")).strip()
        retry_reason = str(payload.get("retry_reason", "")).strip()
        try:
            retry_attempt = int(payload.get("retry_attempt", 0) or 0)
        except (TypeError, ValueError):
            retry_attempt = 0
        try:
            mtime_ns = int(manifest_path.stat().st_mtime_ns)
        except OSError:
            mtime_ns = 0
        entries.append(
            {
                "path": manifest_path,
                "payload": payload,
                "source_unit_path": source_unit_path,
                "source_unit_dispatch_key": source_unit_dispatch_key,
                "stable_signature": stable_signature,
                "manifest_id": manifest_id,
                "retry_of_manifest_id": retry_of_manifest_id,
                "retry_attempt": retry_attempt,
                "retry_reason": retry_reason,
                "mtime_ns": mtime_ns,
            }
        )
    return entries


def _source_unit_group_key(entry: dict) -> str:
    source_unit_dispatch_key = str(entry.get("source_unit_dispatch_key", "")).strip()
    if source_unit_dispatch_key:
        return source_unit_dispatch_key
    return f"manifest_path:{entry['path']}"


def _select_latest_per_source_unit(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for entry in entries:
        grouped.setdefault(_source_unit_group_key(entry), []).append(entry)

    selected: list[dict] = []
    superseded: list[dict] = []
    for group_entries in grouped.values():
        ordered = sorted(
            group_entries,
            key=lambda row: (int(row.get("mtime_ns", 0)), str(row["path"])),
            reverse=True,
        )
        selected.append(ordered[0])
        superseded.extend(ordered[1:])
    return selected, superseded


def _resolve_superseded_path(processed_dir: Path, manifest_path: Path) -> Path:
    base = processed_dir / f"{manifest_path.stem}.superseded.json"
    if not base.exists():
        return base
    index = 2
    while True:
        candidate = processed_dir / f"{manifest_path.stem}.superseded__{index}.json"
        if not candidate.exists():
            return candidate
        index += 1


def _move_superseded_manifests(entries: list[dict], processed_dir: Path, context) -> int:
    moved = 0
    for entry in entries:
        manifest_path = entry["path"]
        if not manifest_path.exists():
            continue
        destination = _resolve_superseded_path(processed_dir, manifest_path)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.rename(destination)
            moved += 1
            context.log.info(
                "중복 pending manifest 정리(superseded): "
                f"{manifest_path.name} -> {destination.name}"
            )
        except OSError as exc:
            context.log.warning(
                f"superseded manifest 이동 실패: {manifest_path} -> {destination}: {exc}"
            )
    return moved


def _collect_in_flight_runs(context) -> list:
    try:
        return context.instance.get_runs(
            filters=RunsFilter(
                job_name="mvp_stage_job",
                # QUEUED까지 포함하면 run coordinator 대기열만으로도 sensor가 과도하게 멈춘다.
                # 실제 실행 압력(backpressure)은 STARTED 기준으로만 판단한다.
                statuses=[DagsterRunStatus.STARTED],
            ),
            limit=200,
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"in-flight run 조회 실패(백프레셔 약화): {exc}")
        return []


def _collect_in_flight_source_units(context, runs: list | None = None) -> set[str]:
    if runs is None:
        runs = _collect_in_flight_runs(context)
    if not runs:
        return set()

    try:
        source_units: set[str] = set()
        for run in runs:
            tags = getattr(run, "tags", {}) or {}
            source_unit_path = str(
                tags.get("source_unit_dispatch_key", "")
                or tags.get("source_unit_path", "")
            ).strip()
            if source_unit_path:
                source_units.add(source_unit_path)
        return source_units
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"in-flight source_unit 수집 실패(중복 방어 약화): {exc}")
        return set()


def _manifest_retry_state(context, manifest_id: str) -> tuple[bool, int, str]:
    """manifest_id 기준 최근 실행 상태를 바탕으로 재시도 여부를 계산."""
    normalized_id = str(manifest_id or "").strip()
    if not normalized_id:
        return False, 0, "NONE"

    try:
        recent_runs = context.instance.get_runs(
            filters=RunsFilter(
                job_name="mvp_stage_job",
                tags={"manifest_id": normalized_id},
            ),
            limit=20,
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"manifest retry state 조회 실패: {normalized_id}: {exc}")
        return False, 0, "LOOKUP_ERROR"

    if not recent_runs:
        return False, 0, "NONE"

    latest_status = getattr(recent_runs[0], "status", None)
    failed_statuses = {DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED}
    failed_run_count = sum(1 for run in recent_runs if getattr(run, "status", None) in failed_statuses)
    should_retry_failed = latest_status in failed_statuses
    latest_status_name = str(latest_status.name if hasattr(latest_status, "name") else latest_status)
    return should_retry_failed, int(failed_run_count), latest_status_name


def _build_auto_bootstrap_cursor_payload(units: dict[str, dict], scan_offset: int = 0) -> dict:
    return {
        "version": 3,
        "scan_offset": max(scan_offset, 0),
        "units": units,
    }


def _parse_auto_bootstrap_cursor(raw_cursor: str | None) -> tuple[dict[str, dict], int]:
    """auto_bootstrap_manifest_sensor cursor 파싱.

    스키마:
      {"version": 3, "scan_offset": 12, "units": {"directory:/nas/incoming/a": {"signature": "...", "stable_cycles": 2, "manifested_signature": "..."}}}
    """
    if not raw_cursor:
        return {}, 0
    try:
        data = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return {}, 0
    if not isinstance(data, dict):
        return {}, 0

    try:
        scan_offset = int(data.get("scan_offset", 0))
    except (TypeError, ValueError):
        scan_offset = 0

    units = data.get("units")
    if not isinstance(units, dict):
        return {}, max(scan_offset, 0)

    parsed: dict[str, dict] = {}
    for key, value in units.items():
        if not isinstance(value, dict):
            continue
        signature = str(value.get("signature", ""))
        manifested_signature = str(value.get("manifested_signature", ""))
        try:
            stable_cycles = int(value.get("stable_cycles", 0))
        except (TypeError, ValueError):
            stable_cycles = 0
        parsed[str(key)] = {
            "signature": signature,
            "manifested_signature": manifested_signature,
            "stable_cycles": max(stable_cycles, 0),
        }
    return parsed, max(scan_offset, 0)


def _iter_sorted_dir_entries(path: Path) -> list[Path]:
    try:
        entries = list(path.iterdir())
    except OSError:
        return []
    return sorted(entries, key=lambda row: row.name)


def _has_allowed_direct_file(dir_path: Path, allowed_exts: set[str]) -> bool:
    for entry in _iter_sorted_dir_entries(dir_path):
        if entry.name.startswith(".partial__"):
            continue
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        if entry.suffix.lower() in allowed_exts:
            return True
    return False


def _discover_source_units(incoming_dir: Path, allowed_exts: set[str]) -> list[dict]:
    """unit 경로를 경량 탐색한다(파일 전수 스캔 없음)."""
    discovered: list[dict] = []
    seen_unit_keys: set[str] = set()

    def _add_unit(unit_type: str, unit_path: Path, unit_name: str, scan_recursive: bool) -> None:
        unit_key = f"{unit_type}:{unit_path}"
        if unit_key in seen_unit_keys:
            return
        seen_unit_keys.add(unit_key)
        discovered.append(
            {
                "unit_key": unit_key,
                "unit_type": unit_type,
                "unit_name": unit_name,
                "unit_path": str(unit_path),
                "scan_recursive": scan_recursive,
            }
        )

    for top_entry in _iter_sorted_dir_entries(incoming_dir):
        top_name = top_entry.name
        if top_name == ".manifests" or top_name.startswith(".partial__"):
            continue

        try:
            is_dir = top_entry.is_dir()
            is_file = top_entry.is_file()
        except OSError:
            continue

        if is_file:
            if top_entry.suffix.lower() in allowed_exts:
                _add_unit(
                    unit_type="file",
                    unit_path=top_entry,
                    unit_name=top_name,
                    scan_recursive=False,
                )
            continue

        if not is_dir:
            continue

        if top_name == "gcp":
            for bucket_entry in _iter_sorted_dir_entries(top_entry):
                if bucket_entry.name.startswith(".partial__"):
                    continue
                try:
                    if not bucket_entry.is_dir():
                        continue
                except OSError:
                    continue

                bucket_unit_name = str(Path("gcp") / bucket_entry.name)
                if _has_allowed_direct_file(bucket_entry, allowed_exts):
                    _add_unit(
                        unit_type="directory",
                        unit_path=bucket_entry,
                        unit_name=bucket_unit_name,
                        scan_recursive=False,
                    )

                for date_entry in _iter_sorted_dir_entries(bucket_entry):
                    if date_entry.name.startswith(".partial__"):
                        continue
                    try:
                        if not date_entry.is_dir():
                            continue
                    except OSError:
                        continue
                    _add_unit(
                        unit_type="directory",
                        unit_path=date_entry,
                        unit_name=str(Path(bucket_unit_name) / date_entry.name),
                        scan_recursive=True,
                    )
            continue

        if _has_allowed_direct_file(top_entry, allowed_exts):
            _add_unit(
                unit_type="directory",
                unit_path=top_entry,
                unit_name=top_name,
                scan_recursive=False,
            )

        for child_entry in _iter_sorted_dir_entries(top_entry):
            if child_entry.name.startswith(".partial__"):
                continue
            try:
                if not child_entry.is_dir():
                    continue
            except OSError:
                continue
            _add_unit(
                unit_type="directory",
                unit_path=child_entry,
                unit_name=str(Path(top_name) / child_entry.name),
                scan_recursive=True,
            )

    return sorted(discovered, key=lambda row: row["unit_key"])


def _scan_unit_media_files(unit: dict, allowed_exts: set[str]) -> list[dict]:
    unit_type = str(unit.get("unit_type", ""))
    unit_path = Path(str(unit.get("unit_path", "")))
    files: list[dict] = []

    if unit_type == "file":
        try:
            if not unit_path.is_file():
                return []
            if unit_path.suffix.lower() not in allowed_exts:
                return []
            stat = unit_path.stat()
        except OSError:
            return []

        return [
            {
                "path": str(unit_path),
                "size": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "rel_path": unit_path.name,
            }
        ]

    try:
        if not unit_path.is_dir():
            return []
    except OSError:
        return []

    scan_recursive = bool(unit.get("scan_recursive", True))
    if scan_recursive:
        for root, dirs, names in os.walk(unit_path):
            dirs[:] = sorted([name for name in dirs if not name.startswith(".partial__")])
            root_path = Path(root)
            for name in sorted(names):
                path_obj = root_path / name
                if path_obj.suffix.lower() not in allowed_exts:
                    continue
                try:
                    stat = path_obj.stat()
                except OSError:
                    continue
                try:
                    rel_path = str(path_obj.relative_to(unit_path))
                except Exception:
                    rel_path = path_obj.name
                files.append(
                    {
                        "path": str(path_obj),
                        "size": int(stat.st_size),
                        "mtime_ns": int(stat.st_mtime_ns),
                        "rel_path": rel_path,
                    }
                )
    else:
        for entry in _iter_sorted_dir_entries(unit_path):
            if entry.name.startswith(".partial__"):
                continue
            try:
                if not entry.is_file():
                    continue
            except OSError:
                continue
            if entry.suffix.lower() not in allowed_exts:
                continue
            try:
                stat = entry.stat()
            except OSError:
                continue
            files.append(
                {
                    "path": str(entry),
                    "size": int(stat.st_size),
                    "mtime_ns": int(stat.st_mtime_ns),
                    "rel_path": entry.name,
                }
            )

    files.sort(key=lambda row: row["path"])
    return files


def _scan_discovered_units(units_to_scan: list[dict], allowed_exts: set[str]) -> dict[str, dict]:
    units: dict[str, dict] = {}
    for unit in units_to_scan:
        unit_files = _scan_unit_media_files(unit, allowed_exts)
        if not unit_files:
            continue

        total_size = sum(int(row.get("size", 0)) for row in unit_files)
        max_mtime_ns = max(int(row.get("mtime_ns", 0)) for row in unit_files)
        file_count = len(unit_files)
        unit_key = str(unit["unit_key"])
        units[unit_key] = {
            "unit_type": str(unit["unit_type"]),
            "unit_name": str(unit["unit_name"]),
            "unit_path": str(unit["unit_path"]),
            "files": unit_files,
            "total_size": total_size,
            "max_mtime_ns": max_mtime_ns,
            "file_count": file_count,
            "signature": f"{file_count}:{total_size}:{max_mtime_ns}",
        }
    return units


def _select_units_for_tick(
    discovered_units: list[dict],
    scan_offset: int,
    max_units_per_tick: int,
) -> tuple[list[dict], int]:
    if not discovered_units:
        return [], 0
    total = len(discovered_units)
    limit = min(total, max(1, max_units_per_tick))
    start = scan_offset % total
    selected = [discovered_units[(start + index) % total] for index in range(limit)]
    next_scan_offset = (start + limit) % total
    return selected, next_scan_offset


def _is_gcp_unit_path(unit_path: str, incoming_dir: Path) -> bool:
    try:
        Path(unit_path).resolve().relative_to((incoming_dir / "gcp").resolve())
        return True
    except Exception:
        return False


def _has_done_marker(unit_path: str, marker_name: str) -> bool:
    return (Path(unit_path) / marker_name).exists()


def _chunk_files(unit_files: list[dict], max_files_per_manifest: int) -> list[list[dict]]:
    if max_files_per_manifest <= 0:
        max_files_per_manifest = 1
    if not unit_files:
        return []
    return [
        unit_files[index:index + max_files_per_manifest]
        for index in range(0, len(unit_files), max_files_per_manifest)
    ]


@sensor(
    job_name="mvp_stage_job",
    minimum_interval_seconds=_int_env("AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC", 180, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="incoming 미디어 파일 유입 시 안정화 후 pending manifest 자동 생성",
)
def auto_bootstrap_manifest_sensor(context):
    """incoming 파일을 감지해 pending manifest를 자동 생성한다.

    조건:
      1) 동일 signature가 stable_cycles 이상 연속 관측
      2) 마지막 수정 시각이 stable_age_sec 이상 경과
    """
    config = PipelineConfig()
    incoming_dir = Path(config.incoming_dir)
    pending_dir = Path(config.manifest_dir) / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    max_pending_manifests = max(
        1,
        int(os.getenv("AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS", "200")),
    )
    max_new_manifests_per_tick = max(
        1,
        int(os.getenv("AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK", "20")),
    )

    pending_count = sum(1 for _ in pending_dir.glob("*.json"))
    if pending_count >= max_pending_manifests:
        return SkipReason(
            "pending backlog 보호: auto bootstrap 중단 "
            f"(pending={pending_count}, limit={max_pending_manifests})"
        )

    stable_cycles_required = max(1, int(os.getenv("AUTO_BOOTSTRAP_STABLE_CYCLES", "2")))
    stable_age_sec = max(0, int(os.getenv("AUTO_BOOTSTRAP_STABLE_AGE_SEC", "120")))
    stable_age_ns = stable_age_sec * 1_000_000_000
    max_units_per_tick = max(
        1,
        int(os.getenv("AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK", "5")),
    )
    max_ready_units_per_tick = max(
        1,
        int(os.getenv("AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK", "5")),
    )
    require_done_marker = os.getenv("AUTO_BOOTSTRAP_REQUIRE_DONE_MARKER", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    done_marker_gcp_only = os.getenv("AUTO_BOOTSTRAP_DONE_MARKER_GCP_ONLY", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    done_marker_name = os.getenv("AUTO_BOOTSTRAP_DONE_MARKER_NAME", "_DONE").strip() or "_DONE"
    max_files_per_manifest = max(
        1,
        int(os.getenv("AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST", "100")),
    )
    allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
    previous_units, previous_scan_offset = _parse_auto_bootstrap_cursor(context.cursor)

    if not incoming_dir.exists():
        context.update_cursor(
            json.dumps(_build_auto_bootstrap_cursor_payload({}, scan_offset=0), ensure_ascii=False)
        )
        return SkipReason(f"incoming 디렉토리 없음: {incoming_dir}")

    scan_elapsed_sec = 0.0
    try:
        scan_started = time.perf_counter()
        discovered_units = _discover_source_units(incoming_dir, allowed_exts)
        units_to_scan, next_scan_offset = _select_units_for_tick(
            discovered_units,
            scan_offset=previous_scan_offset,
            max_units_per_tick=max_units_per_tick,
        )
        units = _scan_discovered_units(units_to_scan, allowed_exts)
        scan_elapsed_sec = time.perf_counter() - scan_started
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"incoming 스캔 실패: {exc}")

    if not discovered_units:
        context.update_cursor(
            json.dumps(_build_auto_bootstrap_cursor_payload({}, scan_offset=0), ensure_ascii=False)
        )
        return SkipReason("incoming에 처리 가능한 미디어 unit 없음")

    # 이미 pending에 동일 signature manifest가 있으면 중복 생성 방지
    pending_unit_signatures: set[tuple[str, str]] = set()
    pending_unit_paths: set[str] = set()
    for pending_manifest in sorted(pending_dir.glob("*.json"), key=lambda p: str(p)):
        try:
            payload = json.loads(pending_manifest.read_text(encoding="utf-8"))
        except Exception:
            continue
        pending_unit_path = str(payload.get("source_unit_path", "")).strip()
        pending_signature = str(payload.get("stable_signature", "")).strip()
        if pending_unit_path:
            pending_unit_paths.add(pending_unit_path)
            if pending_signature:
                pending_unit_signatures.add((pending_unit_path, pending_signature))

    if scan_elapsed_sec >= 5:
        scanned_file_count = sum(len(unit.get("files", [])) for unit in units.values())
        context.log.info(
            "auto_bootstrap scan 통계: "
            f"elapsed={scan_elapsed_sec:.2f}s, discovered_units={len(discovered_units)}, "
            f"scan_window={len(units_to_scan)}, scanned_units={len(units)}, "
            f"scanned_files={scanned_file_count}"
        )

    now_ns = time.time_ns()
    active_unit_keys = {str(unit["unit_key"]) for unit in discovered_units}
    scanned_unit_keys = {str(unit["unit_key"]) for unit in units_to_scan}
    next_units: dict[str, dict] = {}
    for unit_key, previous in previous_units.items():
        if unit_key not in active_unit_keys or unit_key in scanned_unit_keys:
            continue
        try:
            stable_cycles = int(previous.get("stable_cycles", 0))
        except (TypeError, ValueError):
            stable_cycles = 0
        next_units[unit_key] = {
            "signature": str(previous.get("signature", "")),
            "stable_cycles": max(stable_cycles, 0),
            "manifested_signature": str(previous.get("manifested_signature", "")),
        }

    ready_units: list[tuple[str, dict]] = []
    waiting_done_marker_count = 0

    for unit_key, unit in sorted(units.items(), key=lambda row: row[0]):
        signature = str(unit["signature"])
        previous = previous_units.get(unit_key, {})
        previous_signature = str(previous.get("signature", ""))
        previous_cycles = int(previous.get("stable_cycles", 0))
        manifested_signature = str(previous.get("manifested_signature", ""))

        stable_cycles = previous_cycles + 1 if previous_signature == signature else 1
        max_mtime_ns = int(unit.get("max_mtime_ns", 0))
        age_ns = (now_ns - max_mtime_ns) if max_mtime_ns > 0 else stable_age_ns
        is_stable = stable_cycles >= stable_cycles_required and age_ns >= stable_age_ns
        has_pending_manifest = (
            unit["unit_path"] in pending_unit_paths
            or (unit["unit_path"], signature) in pending_unit_signatures
        )
        already_manifested = manifested_signature == signature or has_pending_manifest
        needs_done_marker = (
            require_done_marker
            and unit["unit_type"] == "directory"
            and (not done_marker_gcp_only or _is_gcp_unit_path(str(unit["unit_path"]), incoming_dir))
        )
        has_done_marker = _has_done_marker(str(unit["unit_path"]), done_marker_name) if needs_done_marker else True

        next_units[unit_key] = {
            "signature": signature,
            "stable_cycles": stable_cycles,
            "manifested_signature": manifested_signature,
        }

        if is_stable and not has_done_marker:
            waiting_done_marker_count += 1

        if is_stable and has_done_marker and not already_manifested:
            ready_units.append((unit_key, unit))

    deferred_ready_units_count = max(0, len(ready_units) - max_ready_units_per_tick)
    if deferred_ready_units_count > 0:
        ready_units = ready_units[:max_ready_units_per_tick]

    if not ready_units:
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload(next_units, scan_offset=next_scan_offset),
                ensure_ascii=False,
            )
        )
        reason = (
            "복사 안정화 대기 중: "
            f"units={len(units)} (scan_window={len(units_to_scan)}/{len(discovered_units)}), "
            f"criteria=cycles>={stable_cycles_required}, age>={stable_age_sec}s"
        )
        if waiting_done_marker_count > 0:
            reason += f", done_marker_waiting={waiting_done_marker_count}, marker={done_marker_name}"
        if deferred_ready_units_count > 0:
            reason += f", deferred_ready={deferred_ready_units_count}"
        if scan_elapsed_sec >= 1:
            reason += f", scan_elapsed={scan_elapsed_sec:.1f}s"
        return SkipReason(reason)

    created_manifests: list[str] = []
    deferred_manifest_budget_count = 0
    for index, (unit_key, unit) in enumerate(ready_units, start=1):
        signature = str(unit["signature"])
        unit_files = list(unit["files"])
        unit_file_chunks = _chunk_files(unit_files, max_files_per_manifest)
        chunk_count = len(unit_file_chunks)
        if len(created_manifests) + chunk_count > max_new_manifests_per_tick:
            deferred_manifest_budget_count += 1
            continue
        unit_manifest_failed = False

        for chunk_index, chunk_files in enumerate(unit_file_chunks, start=1):
            now = datetime.now()
            manifest_id = (
                f"auto_bootstrap_{now:%Y%m%d_%H%M%S_%f}_"
                f"{index:03d}_{chunk_index:03d}"
            )
            manifest_filename = f"{manifest_id}.json"
            source_unit_dispatch_key = (
                f"{unit['unit_path']}#chunk:{chunk_index:04d}/{chunk_count:04d}"
            )

            manifest = {
                "manifest_id": manifest_id,
                "generated_at": now.isoformat(),
                "source_dir": str(incoming_dir),
                "source_unit_type": unit["unit_type"],
                "source_unit_path": unit["unit_path"],
                "source_unit_name": unit["unit_name"],
                "source_unit_dispatch_key": source_unit_dispatch_key,
                "source_unit_total_file_count": len(unit_files),
                "source_unit_chunk_index": chunk_index,
                "source_unit_chunk_count": chunk_count,
                "stable_signature": signature,
                "transfer_tool": "auto_bootstrap_sensor",
                "file_count": len(chunk_files),
                "files": [
                    {
                        "path": row["path"],
                        "size": row["size"],
                        "rel_path": row.get("rel_path", Path(row["path"]).name),
                    }
                    for row in chunk_files
                ],
            }

            try:
                (pending_dir / manifest_filename).write_text(
                    json.dumps(manifest, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"manifest 저장 실패: {manifest_filename}: {exc}")
                unit_manifest_failed = True
                break

            created_manifests.append(manifest_filename)
            context.log.info(
                "auto_bootstrap: manifest 생성 완료 — "
                f"{manifest_filename} ({len(chunk_files)} files, "
                f"chunk={chunk_index}/{chunk_count}, unit={unit['unit_path']})"
            )

        if not unit_manifest_failed:
            next_units[unit_key]["manifested_signature"] = signature

    context.update_cursor(
        json.dumps(
            _build_auto_bootstrap_cursor_payload(next_units, scan_offset=next_scan_offset),
            ensure_ascii=False,
        )
    )

    if not created_manifests:
        if deferred_manifest_budget_count > 0:
            return SkipReason(
                "manifest 생성 보류: tick당 생성 예산 초과 "
                f"(limit={max_new_manifests_per_tick}, deferred_units={deferred_manifest_budget_count})"
            )
        return SkipReason("manifest 저장 실패: 생성된 파일 없음")

    suffix = f", deferred_ready={deferred_ready_units_count}" if deferred_ready_units_count > 0 else ""
    if deferred_manifest_budget_count > 0:
        suffix += (
            f", deferred_manifest_budget={deferred_manifest_budget_count}, "
            f"max_new_manifests_per_tick={max_new_manifests_per_tick}"
        )
    return SkipReason(
        f"manifest 생성 완료: {len(created_manifests)}개 "
        f"(안정화 기준 cycles>={stable_cycles_required}, age>={stable_age_sec}s, "
        f"scan_window={len(units_to_scan)}/{len(discovered_units)}{suffix})"
    )
