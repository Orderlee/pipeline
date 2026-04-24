"""auto_bootstrap sensor — unit 파일 스캔, timeout budget, tick 선택."""

from __future__ import annotations

import os
import time
from pathlib import Path

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.validator import is_macos_metadata_file

from .sensor_bootstrap_helpers import _iter_sorted_dir_entries


def _scan_unit_media_files(unit: dict, allowed_exts: set[str]) -> list[dict]:
    unit_type = str(unit.get("unit_type", ""))
    unit_path = Path(str(unit.get("unit_path", "")))
    files: list[dict] = []

    if unit_type == "file":
        try:
            if not unit_path.is_file():
                return []
            if is_macos_metadata_file(unit_path.name):
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
            dirs[:] = sorted(
                [
                    name
                    for name in dirs
                    if not name.startswith(".partial__")
                    and not name.endswith(".tmp")
                    and not is_macos_metadata_file(name)
                ]
            )
            root_path = Path(root)
            for name in sorted(names):
                if is_macos_metadata_file(name):
                    continue
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
            if is_macos_metadata_file(entry.name):
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
