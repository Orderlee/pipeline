"""PostgreSQL 스키마 마이그레이션 mixin — file-based forward-only runner.

DuckDB era의 ``DuckDBMigrationMixin`` 은 schema.sql + 17개 ``_ensure_*_columns()``
헬퍼로 이뤄진 ad-hoc upgrade 코드였다. PG에서는 다음과 같이 정돈한다:

  - sql/migrations/postgres/NNN_*.sql 를 정렬해 순회
  - _pg_migrations 메타 테이블에 적용 기록
  - 미적용 파일을 트랜잭션으로 실행
  - schema.sql 본체 import 같은 통째 실행은 없다 (각 파일이 자체적으로 BEGIN/COMMIT)

운영 규칙은 ``src/vlm_pipeline/sql/migrations/postgres/README.md`` 참조.

DuckDB era와 호환을 위해 동일한 entry-point 이름을 유지한다:
  - ``ensure_schema()``    : 모든 미적용 마이그레이션 적용 (배포 단계용)
  - ``ensure_runtime_schema()`` : 같은 동작 + 프로세스당 1회 가드 (hot path용)

PG는 마이그레이션이 멱등하고 빠르므로 두 메서드의 구현은 거의 동일하다.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar

import psycopg2
import psycopg2.extensions

# 메타 테이블 — 적용 이력 추적. 마이그레이션 시스템 자체가 의존하므로 별도 파일 없이
# 코드 안에 둔다 (chicken-and-egg 해결).
_PG_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS _pg_migrations (
    name        TEXT PRIMARY KEY,
    applied_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checksum    TEXT
);
"""


class PostgresMigrationMixin:
    """forward-only file-based migration runner."""

    _runtime_schema_ensured: ClassVar[bool] = False
    _runtime_schema_lock: ClassVar[threading.Lock] = threading.Lock()

    # ── Public entry-points ──

    def ensure_schema(self) -> None:
        """모든 미적용 마이그레이션을 적용한다 (배포/부팅 시).

        멱등하므로 여러 번 호출해도 안전. 이미 적용된 파일은 skip.

        구현 노트: 마이그레이션 파일이 자체적으로 ``BEGIN/COMMIT`` 을 포함하므로
        ``connect()`` ctxmgr 의 outer transaction 과 충돌하지 않도록 dedicated
        AUTOCOMMIT 커넥션을 직접 열어 사용한다 (pool 우회).
        """
        with self._autocommit_conn() as conn:
            self._ensure_migrations_table(conn)
            applied = self._fetch_applied(conn)
            for path in self._discover_migrations():
                if path.name in applied:
                    continue
                self._apply_one(conn, path)

    def ensure_runtime_schema(self) -> None:
        """런타임 hot path 가드 — 프로세스당 1회만 실행.

        Dagster code-server 부팅 시 한 번만 마이그레이션을 적용하기 위함.
        ``ensure_schema()`` 와 동작은 같지만 ClassVar 플래그로 후속 호출을 skip한다.
        """
        if PostgresMigrationMixin._runtime_schema_ensured:
            return
        with PostgresMigrationMixin._runtime_schema_lock:
            if PostgresMigrationMixin._runtime_schema_ensured:
                return
            self.ensure_schema()
            PostgresMigrationMixin._runtime_schema_ensured = True

    # ── Internals ──

    @contextmanager
    def _autocommit_conn(self):
        """마이그레이션 전용 dedicated 커넥션 (pool 우회, AUTOCOMMIT).

        파일 내부 ``BEGIN/COMMIT`` 이 outer transaction 과 충돌하지 않도록
        autocommit 으로 시작. ``set_isolation_level`` 을 활성 트랜잭션 도중에
        호출하면 implicit ROLLBACK 으로 직전 DDL 이 사라지는 이슈를 회피.
        """
        conn = psycopg2.connect(self.dsn)
        try:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @classmethod
    def _migrations_dir(cls) -> Path:
        # resources/postgres_migration.py → ../sql/migrations/postgres/
        return Path(__file__).resolve().parents[1] / "sql" / "migrations" / "postgres"

    @classmethod
    def _discover_migrations(cls) -> list[Path]:
        directory = cls._migrations_dir()
        if not directory.exists():
            raise FileNotFoundError(f"PG migrations dir missing: {directory}")
        files = sorted(p for p in directory.glob("*.sql") if p.is_file())
        return files

    @staticmethod
    def _ensure_migrations_table(conn: psycopg2.extensions.connection) -> None:
        with conn.cursor() as cur:
            cur.execute(_PG_MIGRATIONS_TABLE_DDL)

    @staticmethod
    def _fetch_applied(conn: psycopg2.extensions.connection) -> set[str]:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM _pg_migrations")
            return {str(row[0]) for row in cur.fetchall()}

    @classmethod
    def _apply_one(
        cls,
        conn: psycopg2.extensions.connection,
        path: Path,
    ) -> None:
        """단일 마이그레이션 파일 적용.

        호출자(``ensure_schema``)가 AUTOCOMMIT 커넥션을 제공한다. 파일 내부의
        ``BEGIN/COMMIT`` 은 서버 측에서 트랜잭션 경계를 형성하고, 그 바깥에서
        진행되는 ``INSERT INTO _pg_migrations`` 는 autocommit 으로 즉시 반영된다.
        """
        sql_text = path.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql_text)
            cur.execute(
                "INSERT INTO _pg_migrations (name) VALUES (%s) "
                "ON CONFLICT (name) DO NOTHING",
                (path.name,),
            )
