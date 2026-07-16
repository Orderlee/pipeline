"""docker/genai/db/pg.py recompute_batch_status 파생 로직 스모크 테스트.

genai 컨테이너 코드는 패키지가 아니므로 파일 경로로 직접 로드한다.
DB 는 fake cursor 로 대체 — 검증 대상은 (n_done, n_failed, n_total) → status 파생과
terminal 여부 파라미터(completed_at 보존/초기화)뿐이다.
"""

import importlib.util
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg2")

_PG_PATH = Path(__file__).resolve().parents[2] / "docker" / "genai" / "db" / "pg.py"
_spec = importlib.util.spec_from_file_location("genai_pg", _PG_PATH)
pg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pg)


def _run(monkeypatch, n_done, n_failed, n_total):
    cur = MagicMock()
    cur.fetchone.return_value = (n_done, n_failed, n_total)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    monkeypatch.setattr(pg, "connect", lambda: nullcontext(conn))
    status = pg.recompute_batch_status("b1")
    return status, cur


@pytest.mark.parametrize(
    "n_done,n_failed,n_total,expected,terminal",
    [
        (3, 0, 3, "succeeded", True),
        (0, 3, 3, "failed", True),
        (2, 1, 3, "partial_success", True),
        (1, 0, 3, "running", False),
    ],
)
def test_derived_status(monkeypatch, n_done, n_failed, n_total, expected, terminal):
    status, cur = _run(monkeypatch, n_done, n_failed, n_total)
    assert status == expected
    update_params = cur.execute.call_args_list[-1][0][1]
    assert update_params[0] == expected
    # terminal 여부가 completed_at CASE 파라미터로 전달되는지 (보존 vs NULL 초기화)
    assert update_params[3] is terminal


def test_zero_jobs_no_update(monkeypatch):
    status, cur = _run(monkeypatch, 0, 0, 0)
    assert status == "pending"
    # advisory lock + SELECT 만 실행, batch UPDATE 없음
    assert not any("UPDATE genai_batches" in c[0][0] for c in cur.execute.call_args_list)


def test_cancel_batch_zero_jobs_terminates_batch(monkeypatch):
    # 0-job batch: recompute 가 'pending' 반환(no-op) → cancel 이 직접 failed 로 종료해야 함
    cur = MagicMock()
    cur.rowcount = 0
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    monkeypatch.setattr(pg, "connect", lambda: nullcontext(conn))
    monkeypatch.setattr(pg, "recompute_batch_status", lambda b: "pending")
    assert pg.cancel_batch("b1", "op cancel") == 0
    assert any("UPDATE genai_batches" in c[0][0] and "'failed'" in c[0][0]
               for c in cur.execute.call_args_list)
