"""Unit tests for BootstrapPolicy dataclass and decide_unit_ready / decide_eligible_units.

All tests are pure Python — no filesystem I/O, no Dagster context, no env side-effects.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from vlm_pipeline.defs.ingest.sensor_bootstrap_policy import (
    BootstrapPolicy,
    decide_eligible_units,
    decide_unit_ready,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────


def _make_policy(**overrides) -> BootstrapPolicy:
    defaults = dict(
        max_pending_manifests=200,
        max_new_manifests_per_tick=20,
        stable_cycles_required=2,
        stable_age_sec=120,
        stable_age_ns=120 * 1_000_000_000,
        max_units_per_tick=5,
        max_ready_units_per_tick=5,
        scan_budget_sec=60,
        skip_on_partial_scan=True,
        require_done_marker=False,
        done_marker_gcp_only=True,
        done_marker_name="_DONE",
        max_files_per_manifest=100,
        discovery_max_top_entries=0,
        allowed_exts=frozenset({".mp4", ".jpg"}),
    )
    defaults.update(overrides)
    return BootstrapPolicy(**defaults)


def _make_unit(
    unit_type: str = "directory",
    unit_path: str = "/nas/incoming/folder_a",
    signature: str = "5:1000:100000",
    max_mtime_ns: int = 0,
) -> dict:
    return {
        "unit_type": unit_type,
        "unit_name": Path(unit_path).name,
        "unit_path": unit_path,
        "signature": signature,
        "max_mtime_ns": max_mtime_ns,
        "files": [],
        "file_count": 5,
        "total_size": 1000,
    }


# ─── BootstrapPolicy.from_env ────────────────────────────────────────────────


def test_from_env_defaults() -> None:
    env_patch = {
        "AUTO_BOOTSTRAP_MAX_PENDING_MANIFESTS": "200",
        "AUTO_BOOTSTRAP_MAX_NEW_MANIFESTS_PER_TICK": "20",
        "AUTO_BOOTSTRAP_STABLE_CYCLES": "2",
        "AUTO_BOOTSTRAP_STABLE_AGE_SEC": "120",
        "AUTO_BOOTSTRAP_MAX_UNITS_PER_TICK": "5",
        "AUTO_BOOTSTRAP_SAFE_MAX_UNITS_PER_TICK": "10",
        "AUTO_BOOTSTRAP_MAX_READY_UNITS_PER_TICK": "5",
        "AUTO_BOOTSTRAP_UNIT_SCAN_TIMEOUT_SEC": "120",
        "DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS": "180",
        "AUTO_BOOTSTRAP_SENSOR_TIMEOUT_MARGIN_SEC": "110",
        "AUTO_BOOTSTRAP_SKIP_ON_PARTIAL_SCAN": "true",
        "AUTO_BOOTSTRAP_REQUIRE_DONE_MARKER": "true",
        "AUTO_BOOTSTRAP_DONE_MARKER_GCP_ONLY": "true",
        "AUTO_BOOTSTRAP_MAX_FILES_PER_MANIFEST": "100",
        "AUTO_BOOTSTRAP_DISCOVERY_MAX_TOP_ENTRIES": "0",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        policy = BootstrapPolicy.from_env()

    assert policy.stable_cycles_required == 2
    assert policy.stable_age_sec == 120
    assert policy.stable_age_ns == 120 * 1_000_000_000
    assert policy.require_done_marker is True
    assert policy.done_marker_gcp_only is True
    assert policy.done_marker_name == "_DONE"
    assert policy.max_files_per_manifest == 100
    assert isinstance(policy.allowed_exts, frozenset)


def test_from_env_custom_done_marker_name() -> None:
    with patch.dict(os.environ, {"AUTO_BOOTSTRAP_DONE_MARKER_NAME": "READY"}, clear=False):
        policy = BootstrapPolicy.from_env()
    assert policy.done_marker_name == "READY"


def test_from_env_empty_done_marker_name_falls_back_to_default() -> None:
    with patch.dict(os.environ, {"AUTO_BOOTSTRAP_DONE_MARKER_NAME": "   "}, clear=False):
        policy = BootstrapPolicy.from_env()
    assert policy.done_marker_name == "_DONE"


def test_policy_is_frozen() -> None:
    policy = _make_policy()
    with pytest.raises((AttributeError, TypeError)):
        policy.stable_age_sec = 999  # type: ignore[misc]


# ─── decide_unit_ready ───────────────────────────────────────────────────────


def test_unit_not_stable_first_tick() -> None:
    policy = _make_policy(stable_cycles_required=2, stable_age_sec=0)
    now_ns = time.time_ns()
    unit = _make_unit(signature="5:1000:100")
    previous = {}  # no history
    is_stable, has_marker, stable_cycles, signature = decide_unit_ready(
        unit, previous, now_ns, policy, Path("/nas/incoming")
    )
    assert is_stable is False
    assert stable_cycles == 1


def test_unit_stable_after_required_cycles() -> None:
    policy = _make_policy(stable_cycles_required=2, stable_age_sec=0, stable_age_ns=0)
    now_ns = time.time_ns()
    unit = _make_unit(signature="5:1000:100")
    previous = {"signature": "5:1000:100", "stable_cycles": 1, "manifested_signature": ""}
    is_stable, has_marker, stable_cycles, signature = decide_unit_ready(
        unit, previous, now_ns, policy, Path("/nas/incoming")
    )
    assert is_stable is True
    assert stable_cycles == 2


def test_unit_signature_change_resets_cycles() -> None:
    policy = _make_policy(stable_cycles_required=2, stable_age_sec=0, stable_age_ns=0)
    now_ns = time.time_ns()
    unit = _make_unit(signature="6:2000:200")
    previous = {"signature": "5:1000:100", "stable_cycles": 5, "manifested_signature": ""}
    is_stable, _, stable_cycles, _ = decide_unit_ready(unit, previous, now_ns, policy, Path("/nas/incoming"))
    assert stable_cycles == 1
    assert is_stable is False


def test_unit_age_check_blocks_when_too_recent() -> None:
    policy = _make_policy(
        stable_cycles_required=1,
        stable_age_sec=300,
        stable_age_ns=300 * 1_000_000_000,
    )
    now_ns = time.time_ns()
    recent_mtime_ns = now_ns - 10 * 1_000_000_000  # only 10 seconds old
    unit = _make_unit(signature="5:1000:100", max_mtime_ns=recent_mtime_ns)
    previous = {"signature": "5:1000:100", "stable_cycles": 3, "manifested_signature": ""}
    is_stable, _, _, _ = decide_unit_ready(unit, previous, now_ns, policy, Path("/nas/incoming"))
    assert is_stable is False


def test_unit_age_check_passes_when_old_enough() -> None:
    policy = _make_policy(
        stable_cycles_required=1,
        stable_age_sec=120,
        stable_age_ns=120 * 1_000_000_000,
    )
    now_ns = time.time_ns()
    old_mtime_ns = now_ns - 300 * 1_000_000_000  # 300 seconds old
    unit = _make_unit(signature="5:1000:100", max_mtime_ns=old_mtime_ns)
    previous = {"signature": "5:1000:100", "stable_cycles": 1, "manifested_signature": ""}
    is_stable, _, _, _ = decide_unit_ready(unit, previous, now_ns, policy, Path("/nas/incoming"))
    assert is_stable is True


def test_done_marker_not_required_when_flag_off() -> None:
    policy = _make_policy(
        stable_cycles_required=1,
        stable_age_sec=0,
        stable_age_ns=0,
        require_done_marker=False,
    )
    now_ns = time.time_ns()
    unit = _make_unit(unit_type="directory", signature="5:1000:100")
    previous = {"signature": "5:1000:100", "stable_cycles": 1, "manifested_signature": ""}
    _, has_marker, _, _ = decide_unit_ready(unit, previous, now_ns, policy, Path("/nas/incoming"))
    assert has_marker is True


# ─── decide_eligible_units ───────────────────────────────────────────────────


def test_decide_eligible_units_empty_returns_empty() -> None:
    policy = _make_policy()
    ready, next_units, waiting = decide_eligible_units(
        {}, {}, set(), set(), time.time_ns(), policy, Path("/nas/incoming")
    )
    assert ready == []
    assert next_units == {}
    assert waiting == 0


def test_decide_eligible_units_ready_unit_included() -> None:
    policy = _make_policy(
        stable_cycles_required=2,
        stable_age_sec=0,
        stable_age_ns=0,
        require_done_marker=False,
    )
    now_ns = time.time_ns()
    unit_key = "directory:/nas/incoming/folder_a"
    unit = _make_unit(unit_path="/nas/incoming/folder_a", signature="5:1000:100")
    units = {unit_key: unit}
    previous_units = {unit_key: {"signature": "5:1000:100", "stable_cycles": 1, "manifested_signature": ""}}

    ready, next_units, waiting = decide_eligible_units(
        units, previous_units, set(), set(), now_ns, policy, Path("/nas/incoming")
    )
    assert len(ready) == 1
    assert ready[0][0] == unit_key
    assert waiting == 0


def test_decide_eligible_units_already_manifested_excluded() -> None:
    policy = _make_policy(
        stable_cycles_required=1,
        stable_age_sec=0,
        stable_age_ns=0,
        require_done_marker=False,
    )
    now_ns = time.time_ns()
    unit_key = "directory:/nas/incoming/folder_a"
    unit = _make_unit(unit_path="/nas/incoming/folder_a", signature="5:1000:100")
    units = {unit_key: unit}
    previous_units = {unit_key: {"signature": "5:1000:100", "stable_cycles": 2, "manifested_signature": "5:1000:100"}}

    ready, _, _ = decide_eligible_units(units, previous_units, set(), set(), now_ns, policy, Path("/nas/incoming"))
    assert ready == []


def test_decide_eligible_units_pending_manifest_excludes_unit() -> None:
    policy = _make_policy(
        stable_cycles_required=1,
        stable_age_sec=0,
        stable_age_ns=0,
        require_done_marker=False,
    )
    now_ns = time.time_ns()
    unit_key = "directory:/nas/incoming/folder_a"
    unit = _make_unit(unit_path="/nas/incoming/folder_a", signature="5:1000:100")
    units = {unit_key: unit}
    previous_units = {unit_key: {"signature": "5:1000:100", "stable_cycles": 1, "manifested_signature": ""}}
    pending_paths = {"/nas/incoming/folder_a"}

    ready, _, _ = decide_eligible_units(
        units, previous_units, pending_paths, set(), now_ns, policy, Path("/nas/incoming")
    )
    assert ready == []


def test_decide_eligible_units_next_units_always_includes_scanned() -> None:
    policy = _make_policy(
        stable_cycles_required=5,
        stable_age_sec=0,
        stable_age_ns=0,
        require_done_marker=False,
    )
    now_ns = time.time_ns()
    unit_key = "directory:/nas/incoming/folder_a"
    unit = _make_unit(unit_path="/nas/incoming/folder_a", signature="5:1000:100")
    units = {unit_key: unit}
    previous_units = {}

    _, next_units, _ = decide_eligible_units(units, previous_units, set(), set(), now_ns, policy, Path("/nas/incoming"))
    assert unit_key in next_units
    assert next_units[unit_key]["stable_cycles"] == 1
