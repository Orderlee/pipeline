"""Phase 3-B inline DQ #5 — cross-table consistency unit tests.

Verify `PostgresIngestRawMixin.count_cross_table_inconsistencies()` issues the
expected SQL and aggregates cursor results into the documented dict shape.

CI 컨벤션: ``tests/conftest.py`` 가 git untracked (host pydantic skew 회피 정책) 이라
``db_resource`` 픽스처를 CI 에서 사용 불가 → 본 파일은 cursor 만 mock 해서
self-contained 로 검증한다. 실제 PG 회귀는 운영자가 로컬에서
``DATAOPS_TEST_POSTGRES_DSN`` 셋업 + tests/conftest 로 별도 검증 (커밋 안 됨).
"""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock


def _load_postgres_ingest_audit_module():
    """Force-load source file (avoid CI runner stale editable-install cache).

    Phase 3-C 회귀에서 발견 — self-hosted runner 의 ``pip install -e ".[dev]"`` 가
    가끔 stale 된 site-packages 모듈을 재사용해 새 method 가 ``AttributeError`` 로
    뜸. ``spec_from_file_location`` 으로 직접 로드해 회피.
    """
    workspace_root = Path(__file__).resolve().parents[2]
    source_path = workspace_root / "src" / "vlm_pipeline" / "resources" / "postgres_ingest_audit.py"
    spec = importlib.util.spec_from_file_location(
        "vlm_pipeline_resources_postgres_ingest_audit_fresh",
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
    """psycopg2 connection mock — count_cross_table_inconsistencies 가 부르는
    ``with conn.cursor() as cur:`` 패턴을 흉내. fetchone 반환값을 사전 주입.
    """

    def __init__(self, fetchone_results: list[Any]) -> None:
        # 안전: tuple 외 None 도 그대로 수용 (실 driver 도 row 없으면 None).
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


class _MixinHarness(PostgresIngestAuditMixin):
    """Mixin 단독 테스트용 minimum wrapper — ``connect()`` 만 stub."""

    def __init__(self, fetchone_results: list[Any]) -> None:
        self.stub_conn = _StubConnection(fetchone_results)

    @contextmanager
    def connect(self):
        yield self.stub_conn


def test_count_returns_documented_dict_shape() -> None:
    """반환 dict 가 두 키를 모두 포함해야 한다."""
    harness = _MixinHarness(fetchone_results=[(0,), (0,)])
    out = harness.count_cross_table_inconsistencies()
    assert set(out.keys()) == {
        "ingest_completed_no_metadata",
        "timestamp_completed_no_label_key",
    }


def test_count_all_zero_when_clean() -> None:
    harness = _MixinHarness(fetchone_results=[(0,), (0,)])
    out = harness.count_cross_table_inconsistencies()
    assert out["ingest_completed_no_metadata"] == 0
    assert out["timestamp_completed_no_label_key"] == 0


def test_count_ingest_only_inconsistency() -> None:
    """첫 쿼리 양수, 두번째 0 → 첫 카운트만 채워진다."""
    harness = _MixinHarness(fetchone_results=[(7,), (0,)])
    out = harness.count_cross_table_inconsistencies()
    assert out["ingest_completed_no_metadata"] == 7
    assert out["timestamp_completed_no_label_key"] == 0


def test_count_timestamp_only_inconsistency() -> None:
    """첫 쿼리 0, 두번째 양수 → 두번째 카운트만 채워진다."""
    harness = _MixinHarness(fetchone_results=[(0,), (3,)])
    out = harness.count_cross_table_inconsistencies()
    assert out["ingest_completed_no_metadata"] == 0
    assert out["timestamp_completed_no_label_key"] == 3


def test_count_both_inconsistencies() -> None:
    """두 쿼리 다 양수 → 각각 dict 키에 매핑."""
    harness = _MixinHarness(fetchone_results=[(2,), (5,)])
    out = harness.count_cross_table_inconsistencies()
    assert out["ingest_completed_no_metadata"] == 2
    assert out["timestamp_completed_no_label_key"] == 5


def test_count_executes_two_queries_with_expected_shape() -> None:
    """첫 쿼리 = raw_files (video) ↔ NOT EXISTS video_metadata,
    두번째 = video_metadata.timestamp_status='completed' AND timestamp_label_key IS NULL."""
    harness = _MixinHarness(fetchone_results=[(0,), (0,)])
    harness.count_cross_table_inconsistencies()

    assert len(harness.stub_conn.executed_sql) == 2
    first, second = harness.stub_conn.executed_sql

    # 첫 쿼리 shape
    assert "raw_files" in first
    assert "ingest_status = 'completed'" in first
    assert "media_type = 'video'" in first
    assert "NOT EXISTS" in first
    assert "video_metadata" in first

    # 두번째 쿼리 shape
    assert "video_metadata" in second
    assert "timestamp_status = 'completed'" in second
    assert "timestamp_label_key IS NULL" in second


def test_count_handles_none_fetchone_safely() -> None:
    """fetchone 이 None 이라도 (이론상 SELECT COUNT 결과는 항상 row 1개라 사실상 안 일어남)
    KeyError/TypeError 안 나고 0 으로 처리되어야 한다."""
    harness = _MixinHarness(fetchone_results=[None, None])
    out = harness.count_cross_table_inconsistencies()
    assert out == {
        "ingest_completed_no_metadata": 0,
        "timestamp_completed_no_label_key": 0,
    }
