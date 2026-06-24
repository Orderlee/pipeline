"""Shared MotherDuck sync policy constants."""

from __future__ import annotations

COLD_MOTHERDUCK_SYNC_TABLES: tuple[str, ...] = (
    "image_metadata",
    "datasets",
    "dataset_clips",
    "image_labels",
)

__all__ = ["COLD_MOTHERDUCK_SYNC_TABLES"]
