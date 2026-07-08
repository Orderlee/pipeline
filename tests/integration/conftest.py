"""tests/integration/ shared fixtures.

Phase 4-B (#12): tests/conftest.py 는 git 미추적 (host pydantic skew allowlist 정책)
이라 CI checkout 에 포함 안 됨. 따라서 integration test 는 자체 conftest 를 두고
필요한 PG fixture 를 self-contained 로 정의한다.

자세한 사항:
  - per-test 임시 DB 생성 → ensure_schema() (마이그레이션 001~004 자동 적용)
    → 테스트 → DB drop. 오버헤드 ~0.3-1.0s/test.
  - ``DATAOPS_TEST_POSTGRES_DSN`` 미설정/unreachable 이면 자동 skip.
  - vlm_pipeline.resources.* mixin 은 dagster 의존 없음 → host pydantic skew 면역.
  - sys.modules clear 패턴 (Phase 3-C/D + 4-A 와 동일) 으로 stale editable install 회피.
"""

from __future__ import annotations

import importlib
import os
import socket
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import pytest


# ─── workspace src 로 우선순위 보장 + stale 모듈 캐시 제거 ──────────────
_WORKSPACE_SRC = Path(__file__).resolve().parents[2] / "src"
_src_str = str(_WORKSPACE_SRC)
if _src_str not in sys.path:
    sys.path.insert(0, _src_str)
for _mod in list(sys.modules):
    if _mod == "vlm_pipeline" or _mod.startswith("vlm_pipeline."):
        del sys.modules[_mod]


PostgresBaseMixin = importlib.import_module("vlm_pipeline.resources.postgres_base").PostgresBaseMixin
PostgresDedupMixin = importlib.import_module("vlm_pipeline.resources.postgres_dedup").PostgresDedupMixin
PostgresIngestMixin = importlib.import_module("vlm_pipeline.resources.postgres_ingest").PostgresIngestMixin
PostgresLabelingMixin = importlib.import_module("vlm_pipeline.resources.postgres_labeling").PostgresLabelingMixin
PostgresMaintenanceMixin = importlib.import_module(
    "vlm_pipeline.resources.postgres_maintenance"
).PostgresMaintenanceMixin
PostgresSpecMixin = importlib.import_module("vlm_pipeline.resources.postgres_spec").PostgresSpecMixin


class _PgIntegrationResource(
    PostgresBaseMixin,
    PostgresMaintenanceMixin,
    PostgresIngestMixin,
    PostgresDedupMixin,
    PostgresLabelingMixin,
    PostgresSpecMixin,
):
    def __init__(self, dsn: str, *, pool_min: int = 1, pool_max: int = 2) -> None:
        self.dsn = dsn
        self.pool_min = pool_min
        self.pool_max = pool_max


def _admin_dsn() -> str | None:
    return os.getenv("DATAOPS_TEST_POSTGRES_DSN") or os.getenv("DATAOPS_POSTGRES_DSN") or None


def _is_pg_reachable(dsn: str, timeout_sec: float = 1.0) -> bool:
    parsed = urlparse(dsn)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except (OSError, ValueError):
        return False


def _replace_dsn_dbname(dsn: str, new_dbname: str) -> str:
    parsed = urlparse(dsn)
    return urlunparse(parsed._replace(path=f"/{new_dbname}"))


@pytest.fixture(scope="session")
def integration_admin_dsn() -> str:
    dsn = _admin_dsn()
    if not dsn:
        pytest.skip(
            "Phase 4-B integration tests skipped — DATAOPS_TEST_POSTGRES_DSN 미설정. "
            "예: postgresql://postgres:test@localhost:5432/postgres"
        )
    if not _is_pg_reachable(dsn):
        pytest.skip(f"Phase 4-B integration tests skipped — {dsn} unreachable")
    return dsn


@pytest.fixture
def pg_resource(integration_admin_dsn) -> Any:
    """per-test 임시 DB + ensure_schema. tests/conftest.py 의 postgres_resource 와 동일 의미.

    teardown: 활성 connection 강제 종료 + DROP DATABASE.
    """
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    test_db_name = f"vlm_pg_int_{uuid.uuid4().hex[:12]}"
    admin_dsn = integration_admin_dsn

    admin_conn = psycopg2.connect(admin_dsn)
    admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with admin_conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{test_db_name}"')
    finally:
        admin_conn.close()

    test_dsn = _replace_dsn_dbname(admin_dsn, test_db_name)
    resource = _PgIntegrationResource(test_dsn)
    try:
        resource.ensure_schema()
        yield resource
    finally:
        try:
            PostgresBaseMixin._close_all_pools()
        except Exception:  # noqa: BLE001
            pass
        admin_conn = psycopg2.connect(admin_dsn)
        admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with admin_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                    """,
                    (test_db_name,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{test_db_name}"')
        finally:
            admin_conn.close()
