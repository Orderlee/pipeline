"""INGEST sensor 공통 헬퍼 — cursor 파싱, run_key 생성 등.

sensor_incoming, sensor_stuck_guard, sensor_bootstrap에서 공유.
"""

from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path

from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

INGEST_MANIFEST_JOB_NAMES = {
    "ingest_job",
    "mvp_stage_job",
}


def parse_cursor(raw_cursor: str | None) -> dict[str, int]:
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


def build_source_unit_run_key(
    source_unit_path: str,
    stable_signature: str,
    source_unit_dispatch_key: str = "",
    manifest_id: str = "",
) -> str:
    """manifest 단위 중복 방지용 run_key 생성.

    같은 source unit을 반복 테스트할 때도 manifest_id가 달라지면 새 run이 생성되어야 한다.
    manifest_id가 비어 있는 legacy 상황만 source unit + signature 조합으로 fallback 한다.
    """
    manifest_value = str(manifest_id or "").strip()
    if manifest_value:
        base = manifest_value
    else:
        source_value = source_unit_dispatch_key or source_unit_path or "<unknown_source_unit>"
        signature_value = stable_signature or "<unknown_signature>"
        base = f"{source_value}|{signature_value}"
    source_hash = sha1(base.encode("utf-8")).hexdigest()[:20]
    return f"incoming-unit-{source_hash}"


def read_manifest_payload(manifest_path: Path, context) -> dict:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"manifest JSON 파싱 실패: {manifest_path}: {exc}")
        return {}

    if not isinstance(payload, dict):
        context.log.warning(f"manifest 형식 오류(객체 아님): {manifest_path}")
        return {}
    return payload


def load_pending_manifest_entries(manifests: list[Path], context) -> list[dict]:
    entries: list[dict] = []
    for manifest_path in manifests:
        payload = read_manifest_payload(manifest_path, context)
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


def source_unit_group_key(entry: dict) -> str:
    source_unit_dispatch_key = str(entry.get("source_unit_dispatch_key", "")).strip()
    if source_unit_dispatch_key:
        return source_unit_dispatch_key
    return f"manifest_path:{entry['path']}"


def select_latest_per_source_unit(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for entry in entries:
        grouped.setdefault(source_unit_group_key(entry), []).append(entry)

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


def resolve_superseded_path(processed_dir: Path, manifest_path: Path) -> Path:
    base = processed_dir / f"{manifest_path.stem}.superseded.json"
    if not base.exists():
        return base
    index = 2
    while True:
        candidate = processed_dir / f"{manifest_path.stem}.superseded__{index}.json"
        if not candidate.exists():
            return candidate
        index += 1


def move_superseded_manifests(entries: list[dict], processed_dir: Path, context) -> int:
    moved = 0
    for entry in entries:
        manifest_path = entry["path"]
        if not manifest_path.exists():
            continue
        destination = resolve_superseded_path(processed_dir, manifest_path)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.rename(destination)
            moved += 1
            context.log.info(
                f"중복 pending manifest 정리(superseded): {manifest_path.name} -> {destination.name}"
            )
        except OSError as exc:
            context.log.warning(
                f"superseded manifest 이동 실패: {manifest_path} -> {destination}: {exc}"
            )
    return moved


def collect_in_flight_runs(context) -> list:
    try:
        runs = context.instance.get_runs(
            filters=RunsFilter(
                statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED],
            ),
            limit=200,
        )
        return [
            run
            for run in runs
            if str(getattr(run, "job_name", "") or "") in INGEST_MANIFEST_JOB_NAMES
        ]
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"in-flight run 조회 실패(백프레셔 약화): {exc}")
        return []


def collect_in_flight_source_units(context, runs: list | None = None) -> set[str]:
    if runs is None:
        runs = collect_in_flight_runs(context)
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


def manifest_retry_state(context, manifest_id: str) -> tuple[bool, int, str]:
    """manifest_id 기준 최근 실행 상태를 바탕으로 재시도 여부를 계산."""
    normalized_id = str(manifest_id or "").strip()
    if not normalized_id:
        return False, 0, "NONE"

    try:
        recent_runs = context.instance.get_runs(
            filters=RunsFilter(tags={"manifest_id": normalized_id}),
            limit=50,
        )
        recent_runs = [
            run
            for run in recent_runs
            if str(getattr(run, "job_name", "") or "") in INGEST_MANIFEST_JOB_NAMES
        ]
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
