"""PostgreSQL 기반 코드 — connect() / pool 관리 / transient-error retry.

DuckDB era의 ``DuckDBBaseMixin`` 과 동일한 인터페이스를 제공하기 위한 PG 버전.
달라지는 것:
  - 단일 파일 write lock → connection pool (ThreadedConnectionPool)
  - lock conflict 재시도 → OperationalError(network 끊김, 서버 재시작) 재시도
  - 인트로스펙션 쿼리는 information_schema 대신 PG 표준 사용

마이그레이션 mixin / 도메인 CRUD mixin 은 별도 모듈에 둔다 (postgres_migration.py 등).
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import ClassVar, Optional

from vlm_pipeline.lib.env_utils import int_env

import psycopg2
import psycopg2.extensions
import psycopg2.pool

from vlm_pipeline.resources.postgres_migration import PostgresMigrationMixin
from vlm_pipeline.resources.postgres_phash import PostgresPhashMixin


def _is_transient_pg_error(exc: BaseException) -> bool:
    """일시적(재시도 가치 있는) PG 에러 판정.

    - OperationalError: 네트워크 끊김, 서버 재시작 등
    - InterfaceError: pool에서 빌려온 커넥션이 이미 끊어진 경우
    - PoolError("connection pool exhausted"): 동시성이 pool_max 를 넘어 일시적으로
      빈 커넥션이 없는 경우 — 다른 스레드가 putconn 하면 해소되므로 backoff 후
      재시도 가치가 있다. 단 "pool is closed"(풀 자체 종료)는 재시도해도 무의미.
    SQL 에러(IntegrityError 등)는 재시도해도 동일하므로 즉시 raise.
    """
    if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True
    if isinstance(exc, psycopg2.pool.PoolError):
        return "closed" not in str(exc).lower()
    return False


class PostgresBaseMixin(PostgresPhashMixin, PostgresMigrationMixin):
    """psycopg2 기반 커넥션 / pool 관리 mixin.

    하위 도메인 mixin들이 ``self.connect()`` 컨텍스트 매니저로 커넥션을 빌려 쓴다.
    schema migration helper는 ``PostgresMigrationMixin`` 에서 제공.

    Required attributes (set by ``PostgresResource``):
        - ``dsn`` : ``postgresql://user:pass@host:port/dbname``
        - ``pool_min`` : 최소 풀 크기 (default 2)
        - ``pool_max`` : 최대 풀 크기 (default 10)
    """

    dsn: str
    pool_min: int = 2
    pool_max: int = 10

    # 프로세스 단위로 풀 1개 공유 (DSN 별로 분기). DuckDB와 달리 풀은 무거우므로
    # ConfigurableResource 인스턴스가 새로 생성되어도 동일 DSN이면 같은 풀을 재사용.
    _pool_registry: ClassVar[dict[str, psycopg2.pool.ThreadedConnectionPool]] = {}
    _pool_registry_lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def _get_pool(
        cls,
        dsn: str,
        *,
        minconn: int,
        maxconn: int,
    ) -> psycopg2.pool.ThreadedConnectionPool:
        with cls._pool_registry_lock:
            pool = cls._pool_registry.get(dsn)
            if pool is None:
                pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    dsn=dsn,
                )
                cls._pool_registry[dsn] = pool
            return pool

    @classmethod
    def _close_all_pools(cls) -> None:
        """테스트 / shutdown 용. 일반 운영 코드에서는 호출하지 않는다."""
        with cls._pool_registry_lock:
            for pool in cls._pool_registry.values():
                try:
                    pool.closeall()
                except Exception:
                    pass
            cls._pool_registry.clear()

    @staticmethod
    def _is_transient_error(exc: BaseException) -> bool:
        return _is_transient_pg_error(exc)

    @staticmethod
    def _rows_to_dicts(rows, columns: list[str]) -> list[dict]:
        """[(v1, v2, ...), ...] + columns 리스트 → [{c1: v1, c2: v2}, ...]."""
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _row_to_dict(row, columns: list[str]) -> dict | None:
        """단일 row tuple → dict, None passthrough."""
        if row is None:
            return None
        return dict(zip(columns, row))

    @staticmethod
    def _cursor_to_dicts(cur) -> list[dict]:
        """cursor.description 기반 동적 컬럼 — SELECT * 류에 사용."""
        if cur.description is None:
            return []
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    @staticmethod
    def _cursor_to_dict(cur) -> dict | None:
        """단일 row + 동적 컬럼."""
        if cur.description is None:
            return None
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(columns, row))

    @staticmethod
    def _norm_str(value, *, default: str = "") -> str:
        """str(value or "").strip(). value None/empty → default.

        str ID/이름 정규화 통합 헬퍼 — resources/ 의 `normalized_X = str(X or "").strip()` 패턴 통합.
        """
        if value is None:
            return default
        s = str(value).strip()
        return s if s else default

    @staticmethod
    def _folder_filter(folder_name: str | None, *, column: str = "r.raw_key") -> tuple[str, list]:
        """raw_key LIKE 'folder_name/%' SQL fragment 와 param list 생성.

        Args:
            folder_name: 폴더명. None 이면 빈 fragment + 빈 list 반환.
            column: 컬럼 prefix (기본 "r.raw_key"). 다른 alias 가 필요한 곳에서 override.

        Returns:
            (clause, params) — clause 는 "AND {column} LIKE %s" 또는 "" (folder_name 없음).
                              params 는 ["{folder_name}/%"] 또는 [].
        """
        if not folder_name:
            return "", []
        return f"AND {column} LIKE %s", [f"{folder_name}/%"]

    @contextmanager
    def connect(self):
        """psycopg2 커넥션 컨텍스트 매니저.

        풀에서 커넥션을 빌려와 yield. 블록 종료 시 자동 commit (예외 시 rollback)
        후 풀에 반납. transient error 발생 시 exponential backoff 재시도.

        DuckDB ctxmgr와 인터페이스가 (yield 1개) 동일하므로 도메인 mixin은
        그대로 작동한다.
        """
        retry_count = int_env("POSTGRES_TRANSIENT_RETRY_COUNT", 5, minimum=0)
        base_delay_ms = int_env("POSTGRES_TRANSIENT_RETRY_DELAY_MS", 100, minimum=10)
        max_delay_ms = max(
            base_delay_ms,
            int_env("POSTGRES_TRANSIENT_RETRY_MAX_DELAY_MS", 2000, minimum=10),
        )

        pool = self._get_pool(self.dsn, minconn=self.pool_min, maxconn=self.pool_max)

        conn: Optional[psycopg2.extensions.connection] = None
        for attempt in range(retry_count + 1):
            try:
                # 풀 고갈 시 getconn() 은 None 이 아니라 psycopg2.pool.PoolError 를
                # raise 한다 → _is_transient_pg_error 가 이를 transient 로 잡아 backoff 재시도.
                conn = pool.getconn()
                # 끊어진 커넥션을 빌려왔는지 가벼운 확인
                if conn.closed:
                    pool.putconn(conn, close=True)
                    raise psycopg2.InterfaceError("borrowed a closed connection from pool")
                break
            except Exception as exc:  # noqa: BLE001
                if not self._is_transient_error(exc) or attempt >= retry_count:
                    raise
                delay_ms = min(max_delay_ms, base_delay_ms * (2**attempt))
                time.sleep(delay_ms / 1000.0)

        assert conn is not None  # for type checker
        try:
            yield conn
            if not conn.closed:
                conn.commit()
        except Exception:
            try:
                if not conn.closed:
                    conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                # closed 여부와 무관하게 pool에 반납해야 슬롯이 회수된다.
                pool.putconn(conn, close=conn.closed)
            except Exception:
                pass

    # ── Introspection helpers (information_schema 호환은 같지만 schema='public') ──

    @staticmethod
    def _table_exists(conn: psycopg2.extensions.connection, table_name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (table_name,),
            )
            row = cur.fetchone()
        return bool(row and row[0] > 0)

    @classmethod
    def _table_columns(
        cls,
        conn: psycopg2.extensions.connection,
        table_name: str,
    ) -> set[str]:
        if not cls._table_exists(conn, table_name):
            return set()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (table_name,),
            )
            rows = cur.fetchall()
        return {str(row[0]) for row in rows}
