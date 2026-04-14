"""Runtime env-backed settings helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass

from vlm_pipeline.lib.env_utils import bool_env, int_env, normalize_dispatch_method_token
from vlm_pipeline.lib.runtime_profile import resolve_runtime_profile


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
class DispatchAgentPollingSettings:
    enabled: bool
    base_url: str
    pending_path: str
    ack_path: str
    poll_limit: int
    connect_timeout_sec: int
    read_timeout_sec: int
    interval_sec: int


@dataclass(frozen=True)
class ProductionAgentPollingSettings(DispatchAgentPollingSettings):
    pass


@dataclass(frozen=True)
class TestAgentPollingSettings(DispatchAgentPollingSettings):
    pass


@dataclass(frozen=True)
class StagingAgentPollingSettings(TestAgentPollingSettings):
    """Backward-compatible alias for legacy staging-named callers."""


def _split_csv(raw_value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw_value.split(",") if item.strip())


def _get_env_value(*names: str) -> str | None:
    for name in names:
        if name and name in os.environ:
            return os.environ[name]
    return None


def _bool_env_with_alias(default: bool, *names: str) -> bool:
    raw = _get_env_value(*names)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _int_env_with_alias(default: int, minimum: int, *names: str) -> int:
    raw = _get_env_value(*names)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def load_ingest_feature_settings() -> IngestFeatureSettings:
    return IngestFeatureSettings(
        defer_video_env_classification=bool_env("INGEST_DEFER_VIDEO_ENV_CLASSIFICATION", False),
        premove_archive_enabled=bool_env("INGEST_PREMOVE_ARCHIVE", False),
    )


def load_dispatch_ingest_only_labeling_methods() -> set[str]:
    raw = os.getenv("DISPATCH_INGEST_ONLY_LABELING_METHODS", "")
    methods: set[str] = set()
    for item in raw.split(","):
        normalized = normalize_dispatch_method_token(item)
        if normalized:
            methods.add(normalized)
    return methods


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


def _load_agent_polling_settings(
    *,
    enabled_env: str,
    base_url_env: str,
    pending_path_env: str,
    ack_path_env: str,
    poll_limit_env: str,
    connect_timeout_env: str,
    read_timeout_env: str,
    interval_env: str,
    enabled_alias_envs: tuple[str, ...] = (),
    base_url_alias_envs: tuple[str, ...] = (),
    pending_path_alias_envs: tuple[str, ...] = (),
    ack_path_alias_envs: tuple[str, ...] = (),
    poll_limit_alias_envs: tuple[str, ...] = (),
    connect_timeout_alias_envs: tuple[str, ...] = (),
    read_timeout_alias_envs: tuple[str, ...] = (),
    interval_alias_envs: tuple[str, ...] = (),
    default_base_url: str,
    default_pending_path: str,
    default_ack_path: str,
) -> DispatchAgentPollingSettings:
    base_url = str(
        _get_env_value(base_url_env, *base_url_alias_envs) or default_base_url
    ).strip().rstrip("/")
    pending_path = str(
        _get_env_value(pending_path_env, *pending_path_alias_envs) or default_pending_path
    ).strip()
    ack_path = str(
        _get_env_value(ack_path_env, *ack_path_alias_envs) or default_ack_path
    ).strip()
    return DispatchAgentPollingSettings(
        enabled=_bool_env_with_alias(False, enabled_env, *enabled_alias_envs),
        base_url=base_url or default_base_url,
        pending_path=pending_path if pending_path.startswith("/") else f"/{pending_path}",
        ack_path=ack_path if ack_path.startswith("/") else f"/{ack_path}",
        poll_limit=_int_env_with_alias(20, 1, poll_limit_env, *poll_limit_alias_envs),
        connect_timeout_sec=_int_env_with_alias(
            3,
            1,
            connect_timeout_env,
            *connect_timeout_alias_envs,
        ),
        read_timeout_sec=_int_env_with_alias(
            10,
            1,
            read_timeout_env,
            *read_timeout_alias_envs,
        ),
        interval_sec=_int_env_with_alias(30, 5, interval_env, *interval_alias_envs),
    )


def load_dispatch_agent_polling_settings() -> DispatchAgentPollingSettings:
    profile = resolve_runtime_profile()
    if profile.is_test:
        default_base_url = "http://host.docker.internal:8081"
        default_pending_path = "/api/staging/dispatch/pending"
        default_ack_path = "/api/staging/dispatch/ack"
    else:
        default_base_url = "http://host.docker.internal:8080"
        default_pending_path = "/api/production/dispatch/pending"
        default_ack_path = "/api/production/dispatch/ack"

    return _load_agent_polling_settings(
        enabled_env="DISPATCH_AGENT_POLLING_ENABLED",
        base_url_env="DISPATCH_AGENT_BASE_URL",
        pending_path_env="DISPATCH_AGENT_PENDING_PATH",
        ack_path_env="DISPATCH_AGENT_ACK_PATH",
        poll_limit_env="DISPATCH_AGENT_POLL_LIMIT",
        connect_timeout_env="DISPATCH_AGENT_CONNECT_TIMEOUT_SEC",
        read_timeout_env="DISPATCH_AGENT_READ_TIMEOUT_SEC",
        interval_env="DISPATCH_AGENT_SENSOR_INTERVAL_SEC",
        enabled_alias_envs=(
            "TEST_AGENT_POLLING_ENABLED",
            "STAGING_AGENT_POLLING_ENABLED",
        ) if profile.is_test else ("PROD_AGENT_POLLING_ENABLED",),
        base_url_alias_envs=(
            "TEST_AGENT_BASE_URL",
            "STAGING_AGENT_BASE_URL",
        ) if profile.is_test else ("PROD_AGENT_BASE_URL",),
        pending_path_alias_envs=(
            "TEST_AGENT_PENDING_PATH",
            "STAGING_AGENT_PENDING_PATH",
        ) if profile.is_test else ("PROD_AGENT_PENDING_PATH",),
        ack_path_alias_envs=(
            "TEST_AGENT_ACK_PATH",
            "STAGING_AGENT_ACK_PATH",
        ) if profile.is_test else ("PROD_AGENT_ACK_PATH",),
        poll_limit_alias_envs=(
            "TEST_AGENT_POLL_LIMIT",
            "STAGING_AGENT_POLL_LIMIT",
        ) if profile.is_test else ("PROD_AGENT_POLL_LIMIT",),
        connect_timeout_alias_envs=(
            "TEST_AGENT_CONNECT_TIMEOUT_SEC",
            "STAGING_AGENT_CONNECT_TIMEOUT_SEC",
        ) if profile.is_test else ("PROD_AGENT_CONNECT_TIMEOUT_SEC",),
        read_timeout_alias_envs=(
            "TEST_AGENT_READ_TIMEOUT_SEC",
            "STAGING_AGENT_READ_TIMEOUT_SEC",
        ) if profile.is_test else ("PROD_AGENT_READ_TIMEOUT_SEC",),
        interval_alias_envs=(
            "TEST_AGENT_SENSOR_INTERVAL_SEC",
            "STAGING_AGENT_SENSOR_INTERVAL_SEC",
        ) if profile.is_test else ("PROD_AGENT_SENSOR_INTERVAL_SEC",),
        default_base_url=default_base_url,
        default_pending_path=default_pending_path,
        default_ack_path=default_ack_path,
    )


def load_test_agent_polling_settings() -> TestAgentPollingSettings:
    settings = _load_agent_polling_settings(
        enabled_env="TEST_AGENT_POLLING_ENABLED",
        base_url_env="TEST_AGENT_BASE_URL",
        pending_path_env="TEST_AGENT_PENDING_PATH",
        ack_path_env="TEST_AGENT_ACK_PATH",
        poll_limit_env="TEST_AGENT_POLL_LIMIT",
        connect_timeout_env="TEST_AGENT_CONNECT_TIMEOUT_SEC",
        read_timeout_env="TEST_AGENT_READ_TIMEOUT_SEC",
        interval_env="TEST_AGENT_SENSOR_INTERVAL_SEC",
        enabled_alias_envs=("STAGING_AGENT_POLLING_ENABLED",),
        base_url_alias_envs=("STAGING_AGENT_BASE_URL",),
        pending_path_alias_envs=("STAGING_AGENT_PENDING_PATH",),
        ack_path_alias_envs=("STAGING_AGENT_ACK_PATH",),
        poll_limit_alias_envs=("STAGING_AGENT_POLL_LIMIT",),
        connect_timeout_alias_envs=("STAGING_AGENT_CONNECT_TIMEOUT_SEC",),
        read_timeout_alias_envs=("STAGING_AGENT_READ_TIMEOUT_SEC",),
        interval_alias_envs=("STAGING_AGENT_SENSOR_INTERVAL_SEC",),
        default_base_url="http://host.docker.internal:8081",
        default_pending_path="/api/staging/dispatch/pending",
        default_ack_path="/api/staging/dispatch/ack",
    )
    return TestAgentPollingSettings(**settings.__dict__)


def load_staging_agent_polling_settings() -> StagingAgentPollingSettings:
    settings = load_test_agent_polling_settings()
    return StagingAgentPollingSettings(**settings.__dict__)


def load_production_agent_polling_settings() -> ProductionAgentPollingSettings:
    settings = _load_agent_polling_settings(
        enabled_env="PROD_AGENT_POLLING_ENABLED",
        base_url_env="PROD_AGENT_BASE_URL",
        pending_path_env="PROD_AGENT_PENDING_PATH",
        ack_path_env="PROD_AGENT_ACK_PATH",
        poll_limit_env="PROD_AGENT_POLL_LIMIT",
        connect_timeout_env="PROD_AGENT_CONNECT_TIMEOUT_SEC",
        read_timeout_env="PROD_AGENT_READ_TIMEOUT_SEC",
        interval_env="PROD_AGENT_SENSOR_INTERVAL_SEC",
        default_base_url="http://host.docker.internal:8080",
        default_pending_path="/api/production/dispatch/pending",
        default_ack_path="/api/production/dispatch/ack",
    )
    return ProductionAgentPollingSettings(**settings.__dict__)
