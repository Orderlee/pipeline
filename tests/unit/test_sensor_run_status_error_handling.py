from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from vlm_pipeline.defs.dispatch import sensor_run_status


def _make_context(job_name: str = "dispatch_stage_job") -> MagicMock:
    ctx = MagicMock()
    ctx.dagster_run.run_id = "test-run"
    ctx.dagster_run.job_name = job_name
    ctx.dagster_run.tags = {"dispatch_request_id": "req_test"}
    ctx.log = MagicMock()
    return ctx


def test_finalizer_logs_and_returns_on_close_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_context()

    fake_db = MagicMock()
    fake_db.close_dispatch_request.side_effect = RuntimeError("duckdb locked")
    monkeypatch.setattr(sensor_run_status, "_build_runtime_db_resource", lambda: fake_db)

    # Should NOT raise — sensor must keep running even when DB write fails.
    sensor_run_status._finalize_dispatch_request(ctx, status="canceled")

    ctx.log.exception.assert_called_once()
    # abort_in_progress_raw_files must not be called when close failed
    fake_db.abort_in_progress_raw_files_for_dispatch.assert_not_called()


def test_finalizer_logs_on_abort_raw_failure_but_close_still_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_context()

    fake_db = MagicMock()
    fake_db.close_dispatch_request.return_value = None
    fake_db.abort_in_progress_raw_files_for_dispatch.side_effect = RuntimeError("write lock")
    monkeypatch.setattr(sensor_run_status, "_build_runtime_db_resource", lambda: fake_db)

    sensor_run_status._finalize_dispatch_request(ctx, status="failed")

    fake_db.close_dispatch_request.assert_called_once()
    fake_db.abort_in_progress_raw_files_for_dispatch.assert_called_once()
    ctx.log.exception.assert_called_once()
    # close 는 성공했으므로 최종 info 요약 로그는 남아야 함 (aborted_raw_files=0)
    ctx.log.info.assert_called_once()


def test_finalizer_happy_path_logs_info(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_context()

    fake_db = MagicMock()
    fake_db.close_dispatch_request.return_value = None
    fake_db.abort_in_progress_raw_files_for_dispatch.return_value = 5
    monkeypatch.setattr(sensor_run_status, "_build_runtime_db_resource", lambda: fake_db)

    sensor_run_status._finalize_dispatch_request(ctx, status="canceled")

    ctx.log.info.assert_called_once()
    ctx.log.exception.assert_not_called()


def test_finalizer_skips_when_no_request_id(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_context()
    ctx.dagster_run.tags = {}

    fake_db = MagicMock()
    monkeypatch.setattr(sensor_run_status, "_build_runtime_db_resource", lambda: fake_db)

    sensor_run_status._finalize_dispatch_request(ctx, status="completed")

    fake_db.close_dispatch_request.assert_not_called()
