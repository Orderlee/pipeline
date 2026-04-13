#!/usr/bin/env python3
"""로컬 pipeline.duckdb를 빠르게 조회하는 간단 CLI."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb


def _default_db_path() -> str:
    env_path = os.getenv("DATAOPS_DUCKDB_PATH") or os.getenv("DUCKDB_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    repo_default = Path(__file__).resolve().parents[1] / "docker" / "data" / "pipeline.duckdb"
    if repo_default.exists():
        return str(repo_default)

    return env_path or str(repo_default)


def _execute_sql(db_path: Path, sql: str) -> tuple[list[str], list[tuple]]:
    con = None
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        result = con.execute(sql)
        columns = [desc[0] for desc in (result.description or [])]
        rows = result.fetchall()
        return columns, rows
    finally:
        if con is not None:
            con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="로컬 DuckDB 조회")
    parser.add_argument("--db", default=_default_db_path(), help="DuckDB 파일 경로")
    parser.add_argument(
        "--sql",
        default=(
            "SELECT table_name "
            "FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "ORDER BY table_name"
        ),
        help="실행할 SQL",
    )
    parser.add_argument(
        "--no-lock-fallback",
        action="store_true",
        help="DuckDB lock 충돌 시 임시 복사본 조회 fallback 비활성화",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] DB 파일이 없습니다: {db_path}", file=sys.stderr)
        return 1

    try:
        columns, rows = _execute_sql(db_path, args.sql)
    except Exception as exc:  # noqa: BLE001
        try:
            is_lock_error = "Could not set lock on file" in str(exc)
        except Exception:  # noqa: BLE001
            is_lock_error = False

        if not args.no_lock_fallback and is_lock_error:
            try:
                with tempfile.TemporaryDirectory(prefix="duckdb_ro_") as tmpdir:
                    snapshot_path = Path(tmpdir) / db_path.name
                    shutil.copy2(db_path, snapshot_path)
                    columns, rows = _execute_sql(snapshot_path, args.sql)
                print(
                    f"[WARN] lock 충돌 감지: 임시 복사본으로 조회했습니다 ({db_path}).",
                    file=sys.stderr,
                )
            except Exception as fallback_exc:  # noqa: BLE001
                print(f"[ERROR] 쿼리 실패: {exc}", file=sys.stderr)
                print(f"[ERROR] fallback 실패: {fallback_exc}", file=sys.stderr)
                return 1
        else:
            print(f"[ERROR] 쿼리 실패: {exc}", file=sys.stderr)
            return 1

    if columns:
        print("\t".join(columns))
    if not rows:
        print("(0 rows)")
        return 0

    for row in rows:
        print("\t".join("NULL" if value is None else str(value) for value in row))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
