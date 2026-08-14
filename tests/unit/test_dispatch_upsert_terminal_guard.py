"""upsert_failed_dispatch_request 가 라이브/터미널 행을 클로버하지 않는지 (감사 DISPATCH-5 Case A).

duplicate-reject 경로(service.py)가 이미 running/completed 인 dispatch_requests 행을
'failed' 로 되돌리면 라이브 run 추적이 끊기거나 이력이 오염된다 — ON CONFLICT 절의
status 가드가 그 회귀를 막는다. PG 없이 SQL 문자열 계약만 검증한다.
"""

from unittest.mock import MagicMock

from vlm_pipeline.resources.postgres_ingest_dispatch import PostgresIngestDispatchMixin


def _db_with_captured_cursor():
    cur = MagicMock()
    conn_cm = MagicMock()
    conn = conn_cm.__enter__.return_value
    conn.cursor.return_value.__enter__.return_value = cur
    db = PostgresIngestDispatchMixin.__new__(PostgresIngestDispatchMixin)
    db.connect = lambda: conn_cm
    return db, cur


def test_upsert_failed_guards_live_and_terminal_rows():
    db, cur = _db_with_captured_cursor()
    db.upsert_failed_dispatch_request({"request_id": "req-1", "error_message": "dup"})

    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT (request_id) DO UPDATE" in sql
    guard = sql.split("DO UPDATE", 1)[1]
    assert "WHERE dispatch_requests.status NOT IN" in guard
    for status in ("'running'", "'archive_moved'", "'completed'", "'canceled'"):
        assert status in guard, f"{status} missing from clobber guard"
