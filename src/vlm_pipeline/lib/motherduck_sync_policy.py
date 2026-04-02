"""Shared MotherDuck sync policy constants and normalization helpers."""

from __future__ import annotations

HOT_MOTHERDUCK_SYNC_TABLES: tuple[str, ...] = (
    "raw_files",
    "video_metadata",
    "labels",
    "processed_clips",
)

COLD_MOTHERDUCK_SYNC_TABLES: tuple[str, ...] = (
    "image_metadata",
    "datasets",
    "dataset_clips",
    "image_labels",
)

ALL_MOTHERDUCK_SYNC_TABLES: tuple[str, ...] = (
    *HOT_MOTHERDUCK_SYNC_TABLES,
    *COLD_MOTHERDUCK_SYNC_TABLES,
)

VALID_MOTHERDUCK_ATTACH_MODES = frozenset({"single", "workspace"})
VALID_MOTHERDUCK_FRESHNESS_MODES = frozenset({"auto", "share", "snapshot", "both"})


def normalize_motherduck_attach_mode(raw_value: object, *, default: str = "single") -> str:
    value = str(raw_value or default).strip().lower()
    if value not in VALID_MOTHERDUCK_ATTACH_MODES:
        return default
    return value


def normalize_motherduck_freshness_mode(raw_value: object, *, default: str = "auto") -> str:
    value = str(raw_value or default).strip().lower()
    if value not in VALID_MOTHERDUCK_FRESHNESS_MODES:
        return default
    return value


__all__ = [
    "ALL_MOTHERDUCK_SYNC_TABLES",
    "COLD_MOTHERDUCK_SYNC_TABLES",
    "HOT_MOTHERDUCK_SYNC_TABLES",
    "VALID_MOTHERDUCK_ATTACH_MODES",
    "VALID_MOTHERDUCK_FRESHNESS_MODES",
    "normalize_motherduck_attach_mode",
    "normalize_motherduck_freshness_mode",
]
