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


@sensor(
    job_name="mvp_stage_job",
    minimum_interval_seconds=120,
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

    max_in_flight_runs = max(1, int(os.getenv("INCOMING_SENSOR_MAX_IN_FLIGHT_RUNS", "2")))
    in_flight_runs = _collect_in_flight_runs(context)
    in_flight_run_count = len(in_flight_runs)

    for entry in selected_entries:
        manifest_path = entry["path"]
        mtime_ns = int(entry["mtime_ns"])
        key = str(manifest_path)

        prev_mtime = previous_state.get(key)
        is_new = key not in previous_state
        is_modified = prev_mtime != mtime_ns

        if is_new or is_modified:
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

    for entry in new_entries:
        manifest_path = entry["path"]
        manifest_payload = entry.get("payload") or _read_manifest_payload(manifest_path, context)
        source_unit_path = str(manifest_payload.get("source_unit_path", "")).strip()
        source_unit_dispatch_key = (
            str(manifest_payload.get("source_unit_dispatch_key", "")).strip() or source_unit_path
        )
        stable_signature = str(manifest_payload.get("stable_signature", "")).strip()
        manifest_id = str(
            manifest_payload.get("manifest_id", "") or manifest_path.stem
        ).strip()

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

        try:
            run_key = _build_source_unit_run_key(
                source_unit_path,
                stable_signature,
                source_unit_dispatch_key=source_unit_dispatch_key,
            )
            run_requests.append(
                RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                            "dedup_results": {"config": {"limit": 200, "threshold": 5}},
                            "processed_clips": {"config": {"limit": 1000}},
                        }
                    },
                    tags={
                        "trigger": "incoming_manifest_sensor",
                        "manifest_path": str(manifest_path),
                        "manifest_name": manifest_path.name,
                        "source_unit_path": source_unit_path,
                        "source_unit_dispatch_key": source_unit_dispatch_key,
                        "stable_signature": stable_signature,
                        "manifest_id": manifest_id,
                    },
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
                "mtime_ns": mtime_ns,
            }
        )
    return entries


def _source_unit_group_key(entry: dict) -> str:
    source_unit_dispatch_key = str(entry.get("source_unit_dispatch_key", "")).strip()
    if source_unit_dispatch_key:
        return source_unit_dispatch_key
    source_unit_path = str(entry.get("source_unit_path", "")).strip()
    if source_unit_path:
        return source_unit_path
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
                statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED],
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


def _parse_auto_bootstrap_cursor(raw_cursor: str | None) -> dict[str, dict]:
    """auto_bootstrap_manifest_sensor cursor 파싱.

    스키마:
      {"version": 2, "units": {"directory:/nas/incoming/a": {"signature": "...", "stable_cycles": 2, "manifested_signature": "..."}}}
    """
    if not raw_cursor:
        return {}
    try:
        data = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}

    units = data.get("units")
    if not isinstance(units, dict):
        return {}

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
    return parsed


def _scan_incoming_media_files(incoming_dir: Path) -> list[dict]:
    """incoming 전체를 스캔하여 허용 확장자 파일 목록을 반환."""
    files: list[dict] = []
    allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
    manifest_root = incoming_dir / ".manifests"

    for root, dirs, names in os.walk(incoming_dir):
        root_path = Path(root)
        # .manifests 하위는 bootstrap 대상에서 제외
        if root_path == manifest_root:
            dirs[:] = []
            continue
        if manifest_root in root_path.parents:
            dirs[:] = []
            continue
        # 다운로드 중 임시 폴더(.partial__*)는 스캔 대상에서 제외
        dirs[:] = [name for name in dirs if not name.startswith(".partial__")]

        for name in names:
            path_obj = root_path / name
            if path_obj.suffix.lower() not in allowed_exts:
                continue
            try:
                stat = path_obj.stat()
            except OSError:
                continue
            files.append(
                {
                    "path": str(path_obj),
                    "size": int(stat.st_size),
                    "mtime_ns": int(stat.st_mtime_ns),
                }
            )

    files.sort(key=lambda row: row["path"])
    return files


def _group_files_by_source_unit(files: list[dict], incoming_dir: Path) -> dict[str, dict]:
    """incoming 파일을 source unit(최상위 폴더/단일 파일) 기준으로 그룹핑."""
    units: dict[str, dict] = {}

    for item in files:
        path_obj = Path(item["path"])
        size = int(item.get("size", 0))
        mtime_ns = int(item.get("mtime_ns", 0))

        try:
            rel = path_obj.relative_to(incoming_dir)
        except Exception:
            rel = Path(path_obj.name)

        if len(rel.parts) >= 2:
            unit_type = "directory"
            top_dir = rel.parts[0]
            if top_dir == "gcp" and len(rel.parts) >= 4:
                # gcp/<bucket>/<date-folder>/... -> unit_name = gcp/<bucket>/<date-folder>
                unit_name = str(Path(rel.parts[0]) / rel.parts[1] / rel.parts[2])
                unit_path = incoming_dir / rel.parts[0] / rel.parts[1] / rel.parts[2]
                rel_in_unit = Path(*rel.parts[3:])
            elif top_dir == "gcp" and len(rel.parts) >= 3:
                # 예외 케이스: gcp/<bucket>/<file>
                unit_name = str(Path(rel.parts[0]) / rel.parts[1])
                unit_path = incoming_dir / rel.parts[0] / rel.parts[1]
                rel_in_unit = Path(*rel.parts[2:])
            elif len(rel.parts) >= 3:
                # 2단계 이상 중첩 폴더: AX지원사업/260213_MCC_Construction/file.mp4
                # -> unit_name = AX지원사업/260213_MCC_Construction
                unit_name = str(Path(rel.parts[0]) / rel.parts[1])
                unit_path = incoming_dir / rel.parts[0] / rel.parts[1]
                rel_in_unit = Path(*rel.parts[2:])
            else:
                # 1단계 폴더: folder/file.mp4 -> unit_name = folder
                unit_name = top_dir
                unit_path = incoming_dir / unit_name
                rel_in_unit = Path(*rel.parts[1:])
        else:
            unit_type = "file"
            unit_name = path_obj.name
            unit_path = path_obj
            rel_in_unit = Path(path_obj.name)

        unit_key = f"{unit_type}:{unit_path}"
        unit = units.setdefault(
            unit_key,
            {
                "unit_type": unit_type,
                "unit_name": unit_name,
                "unit_path": str(unit_path),
                "files": [],
                "total_size": 0,
                "max_mtime_ns": 0,
            },
        )

        unit["files"].append(
            {
                "path": str(path_obj),
                "size": size,
                "mtime_ns": mtime_ns,
                "rel_path": str(rel_in_unit),
            }
        )
        unit["total_size"] += size
        unit["max_mtime_ns"] = max(unit["max_mtime_ns"], mtime_ns)

    for unit in units.values():
        unit["files"].sort(key=lambda row: row["path"])
        file_count = len(unit["files"])
        unit["file_count"] = file_count
        unit["signature"] = f"{file_count}:{unit['total_size']}:{unit['max_mtime_ns']}"

    return units


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
    minimum_interval_seconds=60,
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

    stable_cycles_required = max(1, int(os.getenv("AUTO_BOOTSTRAP_STABLE_CYCLES", "2")))
    stable_age_sec = max(0, int(os.getenv("AUTO_BOOTSTRAP_STABLE_AGE_SEC", "120")))
    stable_age_ns = stable_age_sec * 1_000_000_000
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

    if not incoming_dir.exists():
        context.update_cursor(json.dumps({"version": 2, "units": {}}, ensure_ascii=False))
        return SkipReason(f"incoming 디렉토리 없음: {incoming_dir}")

    try:
        files = _scan_incoming_media_files(incoming_dir)
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"incoming 스캔 실패: {exc}")

    if not files:
        context.update_cursor(json.dumps({"version": 2, "units": {}}, ensure_ascii=False))
        return SkipReason("incoming에 새 미디어 파일 없음")

    units = _group_files_by_source_unit(files, incoming_dir)
    if not units:
        context.update_cursor(json.dumps({"version": 2, "units": {}}, ensure_ascii=False))
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

    previous_units = _parse_auto_bootstrap_cursor(context.cursor)
    now_ns = time.time_ns()

    next_units: dict[str, dict] = {}
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

    if not ready_units:
        context.update_cursor(json.dumps({"version": 2, "units": next_units}, ensure_ascii=False))
        reason = (
            "복사 안정화 대기 중: "
            f"units={len(units)}, criteria=cycles>={stable_cycles_required}, age>={stable_age_sec}s"
        )
        if waiting_done_marker_count > 0:
            reason += f", done_marker_waiting={waiting_done_marker_count}, marker={done_marker_name}"
        return SkipReason(reason)

    created_manifests: list[str] = []
    for index, (unit_key, unit) in enumerate(ready_units, start=1):
        signature = str(unit["signature"])
        unit_files = list(unit["files"])
        unit_file_chunks = _chunk_files(unit_files, max_files_per_manifest)
        chunk_count = len(unit_file_chunks)
        unit_manifest_failed = False

        for chunk_index, chunk_files in enumerate(unit_file_chunks, start=1):
            manifest_id = (
                f"auto_bootstrap_{datetime.now():%Y%m%d_%H%M%S_%f}_"
                f"{index:03d}_{chunk_index:03d}"
            )
            manifest_filename = f"{manifest_id}.json"
            source_unit_dispatch_key = (
                f"{unit['unit_path']}#chunk:{chunk_index:04d}/{chunk_count:04d}"
            )

            manifest = {
                "manifest_id": manifest_id,
                "generated_at": datetime.now().isoformat(),
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

    context.update_cursor(json.dumps({"version": 2, "units": next_units}, ensure_ascii=False))

    if not created_manifests:
        return SkipReason("manifest 저장 실패: 생성된 파일 없음")

    return SkipReason(
        f"manifest 생성 완료: {len(created_manifests)}개 "
        f"(안정화 기준 cycles>={stable_cycles_required}, age>={stable_age_sec}s)"
    )
