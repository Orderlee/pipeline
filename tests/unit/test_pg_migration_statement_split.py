"""마이그레이션 러너의 문장 분리 — CONCURRENTLY 파일만 문장 단위로 보낸다.

배경: AUTOCOMMIT 커넥션이어도 문장 2개 이상을 한 번에 보내면 PG 가 그 simple query 를
암시적 트랜잭션으로 감싸므로 ``CREATE INDEX CONCURRENTLY`` 가 ActiveSqlTransaction 으로
죽는다. 2026-09-03 배포가 이 이유로 실패했고(024, 문장 3개), 그 전까지는 CONCURRENTLY 파일이
021 하나뿐이고 문장이 하나여서 우연히 통과하고 있었다.

여기서 지키는 것:
  - CONCURRENTLY 없는 파일은 **통째 1건** (DO $$ 블록·BEGIN/COMMIT 파일의 기존 동작 불변)
  - CONCURRENTLY 파일은 문장 수만큼 분리
  - 분리기가 문자열/달러인용/주석 안의 ``;`` 을 경계로 착각하지 않음
  - 주석만 남은 꼬리로 빈 쿼리를 실행하지 않음 (psycopg2 는 빈 쿼리를 오류로 던진다)
"""

from __future__ import annotations

from unittest.mock import MagicMock

from vlm_pipeline.resources.postgres_migration import (
    PostgresMigrationMixin,
    _split_sql_statements,
)


def test_plain_file_is_sent_whole() -> None:
    sql = "BEGIN;\nCREATE TABLE t (id INT);\nCOMMIT;\n"
    assert PostgresMigrationMixin._statements_for(sql) == [sql]


def test_do_block_file_is_sent_whole() -> None:
    sql = "DO $$ BEGIN IF TRUE THEN RAISE NOTICE 'x'; END IF; END $$;"
    assert PostgresMigrationMixin._statements_for(sql) == [sql]


def test_concurrently_file_is_split_per_statement() -> None:
    sql = (
        "-- 헤더 주석\n"
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS a_idx ON a (x);\n"
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS b_idx ON b (y)\n"
        "    WHERE y = 'done';\n"
        "-- @ASSERT_AFTER: SELECT 1\n"
    )
    stmts = PostgresMigrationMixin._statements_for(sql)
    assert len(stmts) == 2
    assert stmts[0].endswith("ON a (x);")
    assert "b_idx" in stmts[1] and stmts[1].endswith("'done';")
    # 꼬리 주석만으로 세 번째(빈) 문장이 생기면 psycopg2 가 빈 쿼리로 죽는다
    assert all(s.strip() for s in stmts)


def test_concurrently_with_explicit_begin_keeps_original_text() -> None:
    """파일이 스스로 BEGIN 을 열었으면 쪼개서 숨기지 않고 원문 그대로 → 오류가 드러나게."""
    sql = "BEGIN;\nCREATE INDEX CONCURRENTLY x_idx ON t (c);\nCOMMIT;\n"
    assert PostgresMigrationMixin._statements_for(sql) == [sql]


def test_splitter_ignores_semicolons_inside_literals_and_comments() -> None:
    sql = (
        "INSERT INTO t (v) VALUES ('a;b');\n"
        "/* 블록 주석; 안의 세미콜론 */\n"
        "DO $tag$ BEGIN PERFORM 1; END $tag$;\n"
        "SELECT 1;\n"
    )
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 3
    assert stmts[0].endswith("('a;b');")
    assert "$tag$" in stmts[1] and stmts[1].endswith("$tag$;")
    assert stmts[2].endswith("SELECT 1;")


def test_escaped_quote_inside_literal_does_not_end_string() -> None:
    stmts = _split_sql_statements("SELECT 'it''s; fine'; SELECT 2;")
    assert len(stmts) == 2
    assert stmts[0] == "SELECT 'it''s; fine';"


def test_apply_one_executes_each_statement_then_records(tmp_path) -> None:
    path = tmp_path / "024_x.sql"
    path.write_text(
        "CREATE INDEX CONCURRENTLY a_idx ON a (x);\nCREATE INDEX CONCURRENTLY b_idx ON b (y);\n",
        encoding="utf-8",
    )
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    PostgresMigrationMixin._apply_one(conn, path)

    executed = [c.args[0] for c in cur.execute.call_args_list]
    assert len(executed) == 3  # 문장 2개 + _pg_migrations 기록
    assert executed[0].count("CREATE INDEX") == 1
    assert executed[1].count("CREATE INDEX") == 1
    assert "INSERT INTO _pg_migrations" in executed[2]
    assert cur.execute.call_args_list[-1].args[1] == ("024_x.sql",)
