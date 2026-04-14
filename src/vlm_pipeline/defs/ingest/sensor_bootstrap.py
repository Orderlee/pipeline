"""auto_bootstrap_manifest_sensor — incoming 미디어 파일 유입 시 안정화 후 pending manifest 자동 생성."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

from dagster import DefaultSensorStatus, SkipReason, sensor

from vlm_pipeline.lib.env_utils import bool_env, int_env
from vlm_pipeline.lib.runtime_profile import resolve_runtime_profile
from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS
from vlm_pipeline.resources.config import PipelineConfig

from .runtime_policy import (
    auto_bootstrap_manifest_archive_requested,
    auto_bootstrap_unit_allowed,
)

def _build_auto_bootstrap_cursor_payload(
    units: dict[str, dict],
    scan_offset: int = 0,
    discovery_offset: int = 0,
) -> dict:
    return {
        "version": 4,
        "scan_offset": max(scan_offset, 0),
        "discovery_offset": max(discovery_offset, 0),
        "units": units,
    }


def _parse_auto_bootstrap_cursor(
    raw_cursor: str | None,
) -> tuple[dict[str, dict], int, int]:
    """Returns (previous_units, scan_offset, discovery_offset)."""
    if not raw_cursor:
        return {}, 0, 0
    try:
        data = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return {}, 0, 0
    if not isinstance(data, dict):
        return {}, 0, 0

    try:
        scan_offset = int(data.get("scan_offset", 0))
    except (TypeError, ValueError):
        scan_offset = 0
    try:
        discovery_offset = int(data.get("discovery_offset", 0))
    except (TypeError, ValueError):
        discovery_offset = 0

    units = data.get("units")
    if not isinstance(units, dict):
        return {}, max(scan_offset, 0), max(discovery_offset, 0)

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
    return parsed, max(scan_offset, 0), max(discovery_offset, 0)


def _iter_sorted_dir_entries(path: Path) -> list[Path]:
    try:
        entries = list(path.iterdir())
    except OSError:
        return []
    return sorted(entries, key=lambda row: row.name)


def _load_dispatch_requested_folders(dispatch_pending_dir: Path) -> set[str]:
    requested_folders: set[str] = set()
    if not dispatch_pending_dir.exists():
        return requested_folders

    for request_path in sorted(dispatch_pending_dir.glob("*.json")):
        try:
            payload = json.loads(request_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        folder_name = str(payload.get("folder_name", "")).strip()
        if folder_name:
            requested_folders.add(folder_name)
    return requested_folders


def _has_allowed_direct_file(dir_path: Path, allowed_exts: set[str]) -> bool:
    for entry in _iter_sorted_dir_entries(dir_path):
        if entry.name.startswith("."):
            continue
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        if entry.suffix.lower() in allowed_exts:
            return True
    return False


def _discover_source_units(
    incoming_dir: Path,
    allowed_exts: set[str],
    budget_sec: int,
    discovery_start_index: int = 0,
    max_top_entries_per_tick: int = 0,
    excluded_top_level_names: set[str] | None = None,
) -> tuple[list[dict], int]:
    """Discovery를 틱당 상한으로 나누어 NAS 지연 시 gRPC 타임아웃을 피한다.

    max_top_entries_per_tick > 0 이면 상위 디렉터리에서 매 틱 최대 N개만 처리하고
    다음 틱에서 이어서 처리한다. 0이면 기존처럼 예산(시간) 안에서 전부 처리한다.

    Returns:
        (discovered_units, next_discovery_offset)
    """
    discovered: list[dict] = []
    seen_unit_keys: set[str] = set()
    started_at = time.perf_counter()

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

    top_entries = _iter_sorted_dir_entries(incoming_dir)
    total_top = len(top_entries)
    if max_top_entries_per_tick > 0 and total_top > 0:
        end = min(discovery_start_index + max_top_entries_per_tick, total_top)
        entries_to_process = top_entries[discovery_start_index:end]
        next_discovery_offset = end if end < total_top else 0
    else:
        entries_to_process = top_entries
        next_discovery_offset = 0

    processed_in_slice = 0
    for top_entry in entries_to_process:
        if time.perf_counter() - started_at > budget_sec:
            if max_top_entries_per_tick > 0:
                next_discovery_offset = discovery_start_index + processed_in_slice
            break
        processed_in_slice += 1

        top_name = top_entry.name
        if top_name.startswith("."):
            continue
        if excluded_top_level_names and top_name in excluded_top_level_names:
            continue

        try:
            is_dir = top_entry.is_dir()
            is_file = top_entry.is_file()
        except OSError:
            continue

        if is_file:
            if top_entry.suffix.lower() in allowed_exts:
                _add_unit(unit_type="file", unit_path=top_entry, unit_name=top_name, scan_recursive=False)
            continue

        if not is_dir:
            continue

        if top_name == "gcp":
            for bucket_entry in _iter_sorted_dir_entries(top_entry):
                if bucket_entry.name.startswith("."):
                    continue
                try:
                    if not bucket_entry.is_dir():
                        continue
                except OSError:
                    continue

                bucket_unit_name = str(Path("gcp") / bucket_entry.name)
                if _has_allowed_direct_file(bucket_entry, allowed_exts):
                    _add_unit(
                        unit_type="directory", unit_path=bucket_entry,
                        unit_name=bucket_unit_name, scan_recursive=False,
                    )

                for date_entry in _iter_sorted_dir_entries(bucket_entry):
                    if date_entry.name.startswith("."):
                        continue
                    try:
                        if not date_entry.is_dir():
                            continue
                    except OSError:
                        continue
                    _add_unit(
                        unit_type="directory", unit_path=date_entry,
                        unit_name=str(Path(bucket_unit_name) / date_entry.name),
                        scan_recursive=True,
                    )
            continue

        if _has_allowed_direct_file(top_entry, allowed_exts):
            _add_unit(unit_type="directory", unit_path=top_entry, unit_name=top_name, scan_recursive=False)

        for child_entry in _iter_sorted_dir_entries(top_entry):
            if child_entry.name.startswith("."):
                continue
            try:
                if not child_entry.is_dir():
                    continue
            except OSError:
                continue
            _add_unit(
                unit_type="directory", unit_path=child_entry,
                unit_name=str(Path(top_name) / child_entry.name),
                scan_recursive=True,
            )

    return sorted(discovered, key=_source_unit_sort_key), next_discovery_offset


def _source_unit_sort_key(unit: dict) -> tuple[int, str]:
    unit_type = str(unit.get("unit_type", ""))
    scan_recursive = bool(unit.get("scan_recursive", True))
    if unit_type == "file":
        priority = 0
    elif not scan_recursive:
        priority = 1
    else:
        priority = 2
    return priority, str(unit.get("unit_key", ""))


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


def _scan_discovered_units(
    units_to_scan: list[dict],
    allowed_exts: set[str],
    scan_budget_sec: int,
) -> tuple[dict[str, dict], set[str], int, float, bool]:
    units: dict[str, dict] = {}
    scanned_unit_keys: set[str] = set()
    scanned_unit_count = 0
    scan_started = time.perf_counter()

    for unit in units_to_scan:
        # The sensor gRPC call has a hard deadline. Stop between units so the
        # next tick can resume from the updated cursor instead of timing out.
        if scanned_unit_count > 0 and (time.perf_counter() - scan_started) >= scan_budget_sec:
            break

        unit_files = _scan_unit_media_files(unit, allowed_exts)
        scanned_unit_count += 1
        scanned_unit_keys.add(str(unit["unit_key"]))
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

    elapsed_sec = time.perf_counter() - scan_started
    scan_completed_window = scanned_unit_count >= len(units_to_scan)
    return units, scanned_unit_keys, scanned_unit_count, elapsed_sec, scan_completed_window


def _effective_auto_bootstrap_scan_budget_sec() -> int:
    """gRPC 타임아웃(기본 180초) 내에서 스캔 예산. NAS 지연 시 DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS=300 등으로 상향."""
    configured_timeout_sec = int_env("AUTO_BOOTSTRAP_UNIT_SCAN_TIMEOUT_SEC", 120, 15)
    grpc_timeout_sec = int_env("DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS", 180, 60)
    timeout_margin_sec = int_env("AUTO_BOOTSTRAP_SENSOR_TIMEOUT_MARGIN_SEC", 110, 5)
    safe_upper_bound = max(5, grpc_timeout_sec - timeout_margin_sec)
    return max(5, min(configured_timeout_sec, safe_upper_bound))


def _effective_auto_bootstrap_max_units_per_tick() -> int:
    configured_limit = int_env("AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK", 5, 1)
    safe_limit = int_env("AUTO_BOOTSTRAP_SAFE_MAX_UNITS_PER_TICK", 10, 1)
    return max(1, min(configured_limit, safe_limit))


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


def _has_done_marker(unit_path: str, marker_name: str) -> bool:
    return (Path(unit_path) / marker_name).exists()


def _is_gcp_unit_path(unit_path: str, incoming_dir: Path) -> bool:
    gcp_root = (incoming_dir / "gcp").resolve()
    try:
        Path(unit_path).resolve().relative_to(gcp_root)
        return True
    except Exception:
        return False


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
    minimum_interval_seconds=int_env("AUTO_BOOTSTRAP_SENSOR_INTERVAL_SEC", 180, 30),
    default_status=DefaultSensorStatus.RUNNING,
    description="incoming 미디어 파일 유입 시 안정화 후 pending manifest 자동 생성",
)
def auto_bootstrap_manifest_sensor(context):
    """incoming 파일을 감지해 pending manifest를 자동 생성한다.

    조건:
      1) 동일 signature가 stable_cycles 이상 연속 관측
      2) 마지막 수정 시각이 stable_age_sec 이상 경과

    test profile: **incoming/gcp** 트리만 스캔 (트리거 JSON 없이 GCS 경로만).

    production: auto_bootstrap도 **incoming/gcp/** 만 스캔한다.
    `incoming/tmp_data_2` 같은 직접 드롭 폴더는 `.dispatch/pending` 트리거 JSON 없이는 처리하지 않는다.
    """
    config = PipelineConfig()
    runtime_profile = resolve_runtime_profile()
    incoming_dir = Path(config.incoming_dir)
    pending_dir = Path(config.manifest_dir) / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    max_pending_manifests = int_env("AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS", 200, 1)
    max_new_manifests_per_tick = int_env("AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK", 20, 1)

    pending_manifests = sorted(pending_dir.glob("*.json"), key=lambda p: str(p))
    pending_count = len(pending_manifests)
    if pending_count >= max_pending_manifests:
        return SkipReason(
            f"pending backlog 보호: auto bootstrap 중단 "
            f"(pending={pending_count}, limit={max_pending_manifests})"
        )

    stable_cycles_required = int_env("AUTO_BOOTSTRAP_STABLE_CYCLES", 2, 1)
    stable_age_sec = int_env("AUTO_BOOTSTRAP_STABLE_AGE_SEC", 120, 0)
    stable_age_ns = stable_age_sec * 1_000_000_000
    max_units_per_tick = _effective_auto_bootstrap_max_units_per_tick()
    max_ready_units_per_tick = int_env("AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK", 5, 1)
    scan_budget_sec = _effective_auto_bootstrap_scan_budget_sec()
    skip_on_partial_scan = bool_env("AUTO_BOOTSTRAP_SKIP_ON_PARTIAL_SCAN", True)
    require_done_marker = bool_env("AUTO_BOOTSTRAP_REQUIRE_DONE_MARKER", True)
    done_marker_gcp_only = bool_env("AUTO_BOOTSTRAP_DONE_MARKER_GCP_ONLY", True)
    done_marker_name = os.getenv("AUTO_BOOTSTRAP_DONE_MARKER_NAME", "_DONE").strip() or "_DONE"
    max_files_per_manifest = int_env("AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST", 100, 1)
    allowed_exts = {ext.lower() for ext in ALLOWED_EXTENSIONS}
    previous_units, previous_scan_offset, previous_discovery_offset = _parse_auto_bootstrap_cursor(
        context.cursor
    )
    dispatch_requested_folders = _load_dispatch_requested_folders(
        incoming_dir / ".dispatch" / "pending"
    )
    # NAS 지연 시: 0=무제한(기존), 15~30 권장. discovery를 틱당 N개 상위 entry로 나누어 gRPC 타임아웃 방지
    discovery_max_top_entries = int_env("AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES", 0, 0)

    if not incoming_dir.exists():
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload({}, scan_offset=0, discovery_offset=0),
                ensure_ascii=False,
            )
        )
        return SkipReason(f"incoming 디렉토리 없음: {incoming_dir}")

    discovery_elapsed_sec = 0.0
    scan_elapsed_sec = 0.0
    scanned_unit_count = 0
    scan_completed_window = True
    try:
        discovery_started = time.perf_counter()
        # Discovery(디렉토리 스캔) 시간 제한. NAS 지연 시 60초 상한으로 gRPC 타임아웃 여유 확보
        discovery_budget_sec = min(60, max(10, int(scan_budget_sec * 0.5)))
        discovered_units, next_discovery_offset = _discover_source_units(
            incoming_dir,
            allowed_exts,
            discovery_budget_sec,
            discovery_start_index=previous_discovery_offset,
            max_top_entries_per_tick=discovery_max_top_entries,
            excluded_top_level_names=dispatch_requested_folders,
        )
        discovered_units = [
            u
            for u in discovered_units
            if auto_bootstrap_unit_allowed(
                str(u.get("unit_path", "")),
                incoming_dir=incoming_dir,
                config=config,
                runtime_profile=runtime_profile,
            )
        ]
        discovery_elapsed_sec = time.perf_counter() - discovery_started
        
        # 파일 메타 정보 읽기 스캔에는 남은 예산을 사용합니다
        remaining_budget = max(5, scan_budget_sec - int(discovery_elapsed_sec))
        
        units_to_scan, _ = _select_units_for_tick(
            discovered_units, scan_offset=previous_scan_offset, max_units_per_tick=max_units_per_tick,
        )
        units, scanned_unit_keys, scanned_unit_count, scan_elapsed_sec, scan_completed_window = _scan_discovered_units(
            units_to_scan,
            allowed_exts,
            scan_budget_sec=remaining_budget,
        )
        total_discovered_units = len(discovered_units)
        if total_discovered_units > 0:
            next_scan_offset = (previous_scan_offset + scanned_unit_count) % total_discovered_units
        else:
            next_scan_offset = 0
    except Exception as exc:  # noqa: BLE001
        return SkipReason(f"incoming 스캔 실패: {exc}")

    if not discovered_units:
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload(
                    {}, scan_offset=0, discovery_offset=next_discovery_offset
                ),
                ensure_ascii=False,
            )
        )
        return SkipReason("incoming에 처리 가능한 미디어 unit 없음")

    pending_unit_signatures: set[tuple[str, str]] = set()
    pending_unit_paths: set[str] = set()
    for pending_manifest in pending_manifests:
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
            f"auto_bootstrap scan 통계: discovery_elapsed={discovery_elapsed_sec:.2f}s, "
            f"scan_elapsed={scan_elapsed_sec:.2f}s, "
            f"discovered_units={len(discovered_units)}, requested_scan_window={len(units_to_scan)}, "
            f"processed_units={scanned_unit_count}, media_units={len(units)}, "
            f"scanned_files={scanned_file_count}, budget={scan_budget_sec}s, "
            f"partial_scan={'true' if not scan_completed_window else 'false'}"
        )

    now_ns = time.time_ns()
    active_unit_keys = {str(unit["unit_key"]) for unit in discovered_units}
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
        has_marker = _has_done_marker(str(unit["unit_path"]), done_marker_name) if needs_done_marker else True

        next_units[unit_key] = {
            "signature": signature,
            "stable_cycles": stable_cycles,
            "manifested_signature": manifested_signature,
        }

        if is_stable and not has_marker:
            waiting_done_marker_count += 1

        if is_stable and has_marker and not already_manifested:
            ready_units.append((unit_key, unit))

    deferred_ready_units_count = max(0, len(ready_units) - max_ready_units_per_tick)
    if deferred_ready_units_count > 0:
        ready_units = ready_units[:max_ready_units_per_tick]

    if not ready_units:
        context.update_cursor(
            json.dumps(
                _build_auto_bootstrap_cursor_payload(
                    next_units,
                    scan_offset=next_scan_offset,
                    discovery_offset=next_discovery_offset,
                ),
                ensure_ascii=False,
            )
        )
        reason = (
            f"복사 안정화 대기 중: units={len(units)} "
            f"(scan_window={scanned_unit_count}/{len(units_to_scan)}/{len(discovered_units)}), "
            f"criteria=cycles>={stable_cycles_required}, age>={stable_age_sec}s"
        )
        if waiting_done_marker_count > 0:
            reason += f", done_marker_waiting={waiting_done_marker_count}, marker={done_marker_name}"
        if deferred_ready_units_count > 0:
            reason += f", deferred_ready={deferred_ready_units_count}"
        if not scan_completed_window:
            reason += f", partial_scan={len(units_to_scan) - scanned_unit_count}, scan_budget={scan_budget_sec}s"
            if skip_on_partial_scan:
                reason += ", cursor_resumes_next_tick=true"
        if scan_elapsed_sec >= 1:
            reason += f", scan_elapsed={scan_elapsed_sec:.1f}s"
        return SkipReason(reason)

    created_manifests: list[str] = []
    deferred_manifest_budget_count = 0
    for index, (unit_key, unit) in enumerate(ready_units, start=1):
        signature = str(unit["signature"])
        unit_files = list(unit["files"])
        manifest_unit_type = str(unit["unit_type"])
        manifest_unit_name = str(unit["unit_name"])
        manifest_unit_path = str(unit["unit_path"])

        unit_file_chunks = _chunk_files(unit_files, max_files_per_manifest)
        chunk_count = len(unit_file_chunks)
        if len(created_manifests) + chunk_count > max_new_manifests_per_tick:
            deferred_manifest_budget_count += 1
            continue
        unit_manifest_failed = False

        for chunk_index, chunk_files in enumerate(unit_file_chunks, start=1):
            now = datetime.now()
            manifest_id = f"auto_bootstrap_{now:%Y%m%d_%H%M%S_%f}_{index:03d}_{chunk_index:03d}"
            manifest_filename = f"{manifest_id}.json"
            source_unit_dispatch_key = (
                f"{manifest_unit_path}#chunk:{chunk_index:04d}/{chunk_count:04d}"
            )

            manifest = {
                "manifest_id": manifest_id,
                "generated_at": now.isoformat(),
                "source_dir": str(incoming_dir),
                "source_unit_type": manifest_unit_type,
                "source_unit_path": manifest_unit_path,
                "source_unit_name": manifest_unit_name,
                "source_unit_dispatch_key": source_unit_dispatch_key,
                "source_unit_total_file_count": len(unit_files),
                "source_unit_chunk_index": chunk_index,
                "source_unit_chunk_count": chunk_count,
                "stable_signature": signature,
                "transfer_tool": "auto_bootstrap_sensor",
                "archive_requested": auto_bootstrap_manifest_archive_requested(
                    {"source_unit_path": manifest_unit_path},
                    config=config,
                    runtime_profile=runtime_profile,
                ),
                "file_count": len(chunk_files),
                "files": [
                    {
                        "path": str(row["path"]),
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
                f"auto_bootstrap: manifest 생성 완료 — {manifest_filename} "
                f"({len(chunk_files)} files, chunk={chunk_index}/{chunk_count}, unit={manifest_unit_path})"
            )

        if not unit_manifest_failed:
            next_units[unit_key]["manifested_signature"] = signature

    context.update_cursor(
        json.dumps(
            _build_auto_bootstrap_cursor_payload(
                    next_units,
                    scan_offset=next_scan_offset,
                    discovery_offset=next_discovery_offset,
                ),
            ensure_ascii=False,
        )
    )

    if not created_manifests:
        if deferred_manifest_budget_count > 0:
            return SkipReason(
                f"manifest 생성 보류: tick당 생성 예산 초과 "
                f"(limit={max_new_manifests_per_tick}, deferred_units={deferred_manifest_budget_count})"
            )
        return SkipReason("manifest 저장 실패: 생성된 파일 없음")

    suffix = f", deferred_ready={deferred_ready_units_count}" if deferred_ready_units_count > 0 else ""
    if deferred_manifest_budget_count > 0:
        suffix += (
            f", deferred_manifest_budget={deferred_manifest_budget_count}, "
            f"max_new_manifests_per_tick={max_new_manifests_per_tick}"
        )
    if not scan_completed_window:
        suffix += (
            f", partial_scan={len(units_to_scan) - scanned_unit_count}, "
            f"scan_budget={scan_budget_sec}s"
        )
    return SkipReason(
        f"manifest 생성 완료: {len(created_manifests)}개 "
        f"(안정화 기준 cycles>={stable_cycles_required}, age>={stable_age_sec}s, "
        f"scan_window={scanned_unit_count}/{len(units_to_scan)}/{len(discovered_units)}{suffix})"
    )
