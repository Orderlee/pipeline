"""auto_bootstrap sensor cursor v4 직렬화 / 파싱.

⚠️ cursor 포맷(version=4)은 Dagster 상태에 저장되므로 필드명·version 숫자가 변경되면
기존 sensor가 파싱에 실패한다. 이 모듈의 두 함수는 바이트 단위 그대로 유지해야 한다.
"""

from __future__ import annotations

import json


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
