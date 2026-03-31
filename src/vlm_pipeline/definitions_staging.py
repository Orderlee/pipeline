"""Dagster Definitions — Staging entrypoint."""

from __future__ import annotations

from vlm_pipeline.definitions_profiles import build_definitions_for_profile

defs = build_definitions_for_profile("staging")
