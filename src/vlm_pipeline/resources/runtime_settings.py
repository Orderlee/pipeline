"""Runtime env-backed settings helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass

from vlm_pipeline.lib.env_utils import bool_env, int_env


@dataclass(frozen=True)
class IngestFeatureSettings:
    defer_video_env_classification: bool
    premove_archive_enabled: bool


@dataclass(frozen=True)
class StuckRunGuardSettings:
    enabled: bool
    timeout_sec: int
    orphan_timeout_sec: int
    max_cancels: int
    auto_requeue_enabled: bool
    max_requeues: int
    target_jobs: set[str]
    orphan_only_jobs: set[str]


@dataclass(frozen=True)
class MotherDuckSensorSettings:
    interval_sec: int
    skip_during_duckdb_writer: bool
    watched_tables: tuple[str, ...]


@dataclass(frozen=True)
class StagingAgentPollingSettings:
    enabled: bool
    base_url: str
    poll_limit: int
    connect_timeout_sec: int
    read_timeout_sec: int
    interval_sec: int


@dataclass(frozen=True)
class ProductionAgentPollingSettings:
    enabled: bool
    base_url: str
    poll_limit: int
    connect_timeout_sec: int
    read_timeout_sec: int
    interval_sec: int


def _split_csv(raw_value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw_value.split(",") if item.strip())


def load_ingest_feature_settings() -> IngestFeatureSettings:
    return IngestFeatureSettings(
        defer_video_env_classification=bool_env("INGEST_DEFER_VIDEO_ENV_CLASSIFICATION", False),
        premove_archive_enabled=bool_env("INGEST_PREMOVE_ARCHIVE", False),
    )


def load_stuck_run_guard_settings() -> StuckRunGuardSettings:
    target_jobs_raw = os.getenv(
        "STUCK_RUN_GUARD_TARGET_JOBS",
        "mvp_stage_job,ingest_job,dispatch_stage_job,motherduck_sync_job",
    )
    orphan_only_jobs_raw = os.getenv(
        "STUCK_RUN_GUARD_ORPHAN_ONLY_JOBS",
        "",
    )
    return StuckRunGuardSettings(
        enabled=bool_env("STUCK_RUN_GUARD_ENABLED", True),
        timeout_sec=int_env("STUCK_RUN_GUARD_TIMEOUT_SEC", 3 * 60 * 60, 60),
        orphan_timeout_sec=int_env("STUCK_RUN_GUARD_ORPHANED_RUN_TIMEOUT_SEC", 15 * 60, 60),
        max_cancels=int_env("STUCK_RUN_GUARD_MAX_CANCELS_PER_TICK", 1, 1),
        auto_requeue_enabled=bool_env("STUCK_RUN_GUARD_AUTO_REQUEUE_ENABLED", True),
        max_requeues=int_env("STUCK_RUN_GUARD_MAX_REQUEUES_PER_TICK", 1, 1),
        target_jobs={
            item.strip()
            for item in target_jobs_raw.split(",")
            if item.strip()
        },
        orphan_only_jobs={
            item.strip()
            for item in orphan_only_jobs_raw.split(",")
            if item.strip()
        },
    )


def load_motherduck_sensor_settings() -> MotherDuckSensorSettings:
    watched_tables_raw = os.getenv(
        "MOTHERDUCK_SENSOR_WATCHED_TABLES",
        "raw_files,video_metadata,labels,processed_clips",
    )
    watched_tables = _split_csv(watched_tables_raw)
    if not watched_tables:
        watched_tables = ("raw_files", "video_metadata", "labels", "processed_clips")
    return MotherDuckSensorSettings(
        interval_sec=int_env("MOTHERDUCK_SENSOR_INTERVAL_SEC", 300, 30),
        skip_during_duckdb_writer=bool_env("MOTHERDUCK_SENSOR_SKIP_DURING_DUCKDB_WRITER", True),
        watched_tables=watched_tables,
    )


def load_staging_agent_polling_settings() -> StagingAgentPollingSettings:
    base_url = (os.getenv("STAGING_AGENT_BASE_URL") or "http://host.docker.internal:8081").strip().rstrip("/")
    return StagingAgentPollingSettings(
        enabled=bool_env("STAGING_AGENT_POLLING_ENABLED", False),
        base_url=base_url or "http://host.docker.internal:8081",
        poll_limit=int_env("STAGING_AGENT_POLL_LIMIT", 20, 1),
        connect_timeout_sec=int_env("STAGING_AGENT_CONNECT_TIMEOUT_SEC", 3, 1),
        read_timeout_sec=int_env("STAGING_AGENT_READ_TIMEOUT_SEC", 10, 1),
        interval_sec=int_env("STAGING_AGENT_SENSOR_INTERVAL_SEC", 30, 5),
    )


def load_production_agent_polling_settings() -> ProductionAgentPollingSettings:
    base_url = (os.getenv("PROD_AGENT_BASE_URL") or "http://host.docker.internal:8080").strip().rstrip("/")
    return ProductionAgentPollingSettings(
        enabled=bool_env("PROD_AGENT_POLLING_ENABLED", False),
        base_url=base_url or "http://host.docker.internal:8080",
        poll_limit=int_env("PROD_AGENT_POLL_LIMIT", 20, 1),
        connect_timeout_sec=int_env("PROD_AGENT_CONNECT_TIMEOUT_SEC", 3, 1),
        read_timeout_sec=int_env("PROD_AGENT_READ_TIMEOUT_SEC", 10, 1),
        interval_sec=int_env("PROD_AGENT_SENSOR_INTERVAL_SEC", 30, 5),
    )
