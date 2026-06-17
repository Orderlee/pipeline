"""auto_bootstrap sensor — env-driven policy dataclass and unit-readiness helpers.

BootstrapPolicy centralises every int_env / bool_env call that previously
lived inline in auto_bootstrap_manifest_sensor, making the policy independently
testable without a live sensor context.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from vlm_pipeline.lib.env_utils import bool_env, int_env
from vlm_pipeline.lib.validator import ALLOWED_EXTENSIONS

from .sensor_bootstrap_helpers import _has_done_marker, _is_gcp_unit_path
from .sensor_bootstrap_scan import (
    _effective_auto_bootstrap_max_units_per_tick,
    _effective_auto_bootstrap_scan_budget_sec,
)

__all__ = [
    "BootstrapPolicy",
    "decide_unit_ready",
    "decide_eligible_units",
]


@dataclass(frozen=True)
class BootstrapPolicy:
    """Env-driven configuration for auto_bootstrap_manifest_sensor.

    All fields are resolved once from environment variables at construction
    time so that sensor logic only consults this object — no inline int_env
    calls in the hot path.
    """

    max_pending_manifests: int
    max_new_manifests_per_tick: int
    stable_cycles_required: int
    stable_age_sec: int
    stable_age_ns: int
    max_units_per_tick: int
    max_ready_units_per_tick: int
    scan_budget_sec: int
    skip_on_partial_scan: bool
    require_done_marker: bool
    done_marker_gcp_only: bool
    done_marker_name: str
    max_files_per_manifest: int
    discovery_max_top_entries: int
    allowed_exts: frozenset[str]

    @classmethod
    def from_env(cls) -> "BootstrapPolicy":
        stable_age_sec = int_env("AUTO_BOOTSTRAP_STABLE_AGE_SEC", 120, 0)
        done_marker_name = os.getenv("AUTO_BOOTSTRAP_DONE_MARKER_NAME", "_DONE").strip() or "_DONE"
        return cls(
            max_pending_manifests=int_env("AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS", 200, 1),
            max_new_manifests_per_tick=int_env("AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK", 20, 1),
            stable_cycles_required=int_env("AUTO_BOOTSTRAP_STABLE_CYCLES", 2, 1),
            stable_age_sec=stable_age_sec,
            stable_age_ns=stable_age_sec * 1_000_000_000,
            max_units_per_tick=_effective_auto_bootstrap_max_units_per_tick(),
            max_ready_units_per_tick=int_env("AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK", 5, 1),
            scan_budget_sec=_effective_auto_bootstrap_scan_budget_sec(),
            skip_on_partial_scan=bool_env("AUTO_BOOTSTRAP_SKIP_ON_PARTIAL_SCAN", True),
            require_done_marker=bool_env("AUTO_BOOTSTRAP_REQUIRE_DONE_MARKER", True),
            done_marker_gcp_only=bool_env("AUTO_BOOTSTRAP_DONE_MARKER_GCP_ONLY", True),
            done_marker_name=done_marker_name,
            max_files_per_manifest=int_env("AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST", 100, 1),
            discovery_max_top_entries=int_env("AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES", 0, 0),
            allowed_exts=frozenset(ext.lower() for ext in ALLOWED_EXTENSIONS),
        )


def decide_unit_ready(
    unit: dict,
    previous: dict,
    now_ns: int,
    policy: BootstrapPolicy,
    incoming_dir: Path,
) -> tuple[bool, bool, int, str]:
    """Determine whether a scanned unit is ready for manifest generation.

    Pure function — no I/O beyond the already-scanned unit dict.

    Args:
        unit: scanned unit dict from _scan_discovered_units
              (keys: unit_type, unit_path, signature, max_mtime_ns, files)
        previous: cursor entry for this unit_key (signature, stable_cycles, manifested_signature)
        now_ns: current epoch nanoseconds (caller supplies for consistency)
        policy: resolved BootstrapPolicy
        incoming_dir: Path to incoming directory (used for GCP path check)

    Returns:
        (is_stable, has_marker, stable_cycles, new_signature)
        Caller decides whether to append to ready_units based on
        (is_stable and has_marker and not already_manifested).
    """
    signature = str(unit["signature"])
    previous_signature = str(previous.get("signature", ""))
    previous_cycles = int(previous.get("stable_cycles", 0))

    stable_cycles = previous_cycles + 1 if previous_signature == signature else 1
    max_mtime_ns = int(unit.get("max_mtime_ns", 0))
    age_ns = (now_ns - max_mtime_ns) if max_mtime_ns > 0 else policy.stable_age_ns
    is_stable = stable_cycles >= policy.stable_cycles_required and age_ns >= policy.stable_age_ns

    unit_type = str(unit.get("unit_type", ""))
    unit_path = str(unit["unit_path"])
    needs_done_marker = (
        policy.require_done_marker
        and unit_type == "directory"
        and (not policy.done_marker_gcp_only or _is_gcp_unit_path(unit_path, incoming_dir))
    )
    has_marker = _has_done_marker(unit_path, policy.done_marker_name) if needs_done_marker else True

    return is_stable, has_marker, stable_cycles, signature


def decide_eligible_units(
    units: dict[str, dict],
    previous_units: dict[str, dict],
    pending_unit_paths: set[str],
    pending_unit_signatures: set[tuple[str, str]],
    now_ns: int,
    policy: BootstrapPolicy,
    incoming_dir: Path,
) -> tuple[list[tuple[str, dict]], dict[str, dict], int]:
    """Classify scanned units into ready and updated cursor state.

    Args:
        units: mapping from unit_key to scanned unit dict
        previous_units: cursor units from last tick
        pending_unit_paths: unit paths that already have a pending manifest
        pending_unit_signatures: (unit_path, signature) pairs with pending manifests
        now_ns: current epoch nanoseconds
        policy: resolved BootstrapPolicy
        incoming_dir: Path to incoming directory

    Returns:
        (ready_units, next_units, waiting_done_marker_count)
        ready_units: list of (unit_key, unit) pairs ordered by unit_key
        next_units: updated cursor dict for all processed units
        waiting_done_marker_count: how many stable units are still waiting for done marker
    """
    next_units: dict[str, dict] = {}
    ready_units: list[tuple[str, dict]] = []
    waiting_done_marker_count = 0

    for unit_key, unit in sorted(units.items(), key=lambda row: row[0]):
        previous = previous_units.get(unit_key, {})
        manifested_signature = str(previous.get("manifested_signature", ""))

        is_stable, has_marker, stable_cycles, signature = decide_unit_ready(
            unit, previous, now_ns, policy, incoming_dir
        )

        next_units[unit_key] = {
            "signature": signature,
            "stable_cycles": stable_cycles,
            "manifested_signature": manifested_signature,
        }

        if is_stable and not has_marker:
            waiting_done_marker_count += 1

        unit_path = str(unit["unit_path"])
        has_pending_manifest = (unit_path in pending_unit_paths) or ((unit_path, signature) in pending_unit_signatures)
        already_manifested = manifested_signature == signature or has_pending_manifest

        if is_stable and has_marker and not already_manifested:
            ready_units.append((unit_key, unit))

    return ready_units, next_units, waiting_done_marker_count
