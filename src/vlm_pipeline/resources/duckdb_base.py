"""DuckDB 기반 코드 — connect(), ensure_schema(), lock 처리 등."""

from __future__ import annotations

import os
import re
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb

from vlm_pipeline.lib.env_utils import is_duckdb_lock_conflict
from vlm_pipeline.resources.duckdb_migration import DuckDBMigrationMixin
from vlm_pipeline.resources.duckdb_phash import DuckDBPhashMixin


class DuckDBBaseMixin(DuckDBPhashMixin, DuckDBMigrationMixin):
    """DuckDB 커넥션 관리 기반 mixin.

    pHash 유틸리티는 ``DuckDBPhashMixin``, 스키마 마이그레이션은
    ``DuckDBMigrationMixin`` 에서 상속받는다.
    """

    db_path: str

    @classmethod
    def _load_schema_ddl(cls, *, include_image_labels: bool) -> str:
        schema_path = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
        if not schema_path.exists():
            raise FileNotFoundError(f"schema.sql not found: {schema_path}")

        ddl = schema_path.read_text(encoding="utf-8")
        if include_image_labels:
            return ddl
        return re.sub(
            r"CREATE TABLE IF NOT EXISTS image_labels \([\s\S]*?\);\n",
            "",
            ddl,
            count=1,
        )

    @staticmethod
    def _is_lock_conflict(exc: Exception) -> bool:
        return is_duckdb_lock_conflict(exc)

    @contextmanager
    def connect(self):
        """DuckDB 커넥션 컨텍스트 매니저."""
        retry_count = max(0, int(os.getenv("DUCKDB_LOCK_RETRY_COUNT", "20")))
        base_delay_ms = max(10, int(os.getenv("DUCKDB_LOCK_RETRY_DELAY_MS", "100")))
        max_delay_ms = max(
            base_delay_ms,
            int(os.getenv("DUCKDB_LOCK_RETRY_MAX_DELAY_MS", "2000")),
        )

        conn = None
        for attempt in range(retry_count + 1):
            try:
                conn = duckdb.connect(self.db_path)
                break
            except Exception as exc:  # noqa: BLE001
                if not self._is_lock_conflict(exc) or attempt >= retry_count:
                    raise
                delay_ms = min(max_delay_ms, base_delay_ms * (2 ** attempt))
                time.sleep(delay_ms / 1000.0)

        if conn is None:
            raise RuntimeError("DuckDB connection failed unexpectedly")

        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0] > 0)

    @classmethod
    def _table_columns(cls, conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
        if not cls._table_exists(conn, table_name):
            return set()
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchall()
        return {str(row[0]) for row in rows}
