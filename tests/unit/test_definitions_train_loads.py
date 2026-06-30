"""build_trainset asset is wired into the Dagster Definitions and loads cleanly."""

from __future__ import annotations

import pytest

pytest.importorskip("dagster")


def test_build_trainset_in_definitions():
    from vlm_pipeline.definitions import defs

    # Dagster 1.13.x: no get_all_asset_specs/get_asset_graph on Definitions — resolve keys directly.
    keys = {k.to_user_string() for k in defs.resolve_all_asset_keys()}
    assert "build_trainset" in keys
