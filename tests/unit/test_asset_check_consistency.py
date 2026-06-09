"""Phase 3-C @asset_check resource-layer unit tests.

CI 환경 노트: self-hosted runner 가 anaconda Python + 영구 _work 디렉토리 라
``pip install -e ".[dev]"`` 가 가끔 새 method 를 안 잡고 직전 install 의 동일
모듈 객체를 재사용함 → ``AttributeError`` (실 method 가 source 엔 있어도). 회피:
``importlib.util.spec_from_file_location()`` 으로 source 파일을 명시 경로에서
fresh import → method 존재 보장. 로컬에선 정상 import 도 동작.

cross_table_inconsistencies 와 같은 mock 패턴 — CI conftest 비의존.
"""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock


def _load_postgres_ingest_audit_module():
    """Force-load postgres_ingest_audit.py from the workspace source (not site-packages cache).

    CI runner 의 stale editable install 회피 — 워크스페이스의 실제 파일을 spec_from_file
    로 직접 import. 로컬 환경에서도 동일 경로라 부작용 없음.
    """
    workspace_root = Path(__file__).resolve().parents[2]
    source_path = workspace_root / "src" / "vlm_pipeline" / "resources" / "postgres_ingest_audit.py"
    spec = importlib.util.spec_from_file_location(
        "vlm_pipeline_resources_postgres_ingest_audit_fresh_c",
        source_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to build spec for {source_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_fresh_module = _load_postgres_ingest_audit_module()
PostgresIngestAuditMixin = _fresh_module.PostgresIngestAuditMixin


class _StubConnection:
    def __init__(self, fetchone_results: list[Any]) -> None:
        self._results: list[Any] = list(fetchone_results)
        self.executed_sql: list[str] = []

    @contextmanager
    def cursor(self):
        cur = MagicMock()

        def _execute(sql: str, *_params: Any) -> None:
            self.executed_sql.append(sql)

        def _fetchone():
            return self._results.pop(0) if self._results else None

        cur.execute.side_effect = _execute
        cur.fetchone.side_effect = _fetchone
        yield cur


class _Harness(PostgresIngestAuditMixin):
    def __init__(self, fetchone_results: list[Any]) -> None:
        self.stub_conn = _StubConnection(fetchone_results)

    @contextmanager
    def connect(self):
        yield self.stub_conn


def test_count_completed_raw_files_without_archive_zero() -> None:
    harness = _Harness(fetchone_results=[(0,)])
    assert harness.count_completed_raw_files_without_archive() == 0


def test_count_completed_raw_files_without_archive_positive() -> None:
    harness = _Harness(fetchone_results=[(13,)])
    assert harness.count_completed_raw_files_without_archive() == 13


def test_count_completed_raw_files_without_archive_handles_none_row() -> None:
    """SELECT COUNT 은 항상 row 1개 반환이지만 None 안전망."""
    harness = _Harness(fetchone_results=[None])
    assert harness.count_completed_raw_files_without_archive() == 0


def test_count_completed_raw_files_without_archive_sql_shape() -> None:
    """SQL 이 ingest_status='completed' AND archive_path IS NULL 패턴 유지."""
    harness = _Harness(fetchone_results=[(0,)])
    harness.count_completed_raw_files_without_archive()

    assert len(harness.stub_conn.executed_sql) == 1
    sql = harness.stub_conn.executed_sql[0]
    assert "raw_files" in sql
    assert "ingest_status = 'completed'" in sql
    assert "archive_path IS NULL" in sql
