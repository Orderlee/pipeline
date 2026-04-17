"""auto_bootstrap sensor — incoming 디렉토리에서 source unit 발견.

NAS 지연 시 gRPC 타임아웃을 피하기 위해 discovery를 틱당 상한으로 나눈다.
"""

from __future__ import annotations

import time
from pathlib import Path

from .sensor_bootstrap_helpers import (
    _has_allowed_direct_file,
    _iter_sorted_dir_entries,
    _source_unit_sort_key,
)


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
