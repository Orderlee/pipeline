"""Sensor read-only DB connection helper.

운영 모드 (``DATAOPS_DB_BACKEND``) 와 무관하게 sensor 들이 동일한 DuckDB SQL
인터페이스로 read-only 쿼리를 수행할 수 있게 만드는 thin facade.

동작:
  - ``DATAOPS_POSTGRES_DSN`` 설정 시: ``duckdb.connect(":memory:")`` + ``LOAD postgres``
    + ``ATTACH ... TYPE postgres, READ_ONLY`` + ``USE pg.public``.
    sensor 의 SQL (``FROM raw_files``, ``FROM dispatch_requests`` 등) 그대로 동작.
  - 미설정 시: legacy ``duckdb.connect(default_duckdb_path(), read_only=True)``.
    DuckDB single 모드 (마이그레이션 이전) 호환.

write 는 절대 안 됨 — 모든 write 는 ``db`` resource (``DualDBResource`` /
``PostgresResource``) 통해서만. 이 facade 는 read-only 보장 (`ATTACH READ_ONLY`).

성능:
  - ``LOAD postgres`` 는 extension 이미 설치되어 있으면 빠름. Dockerfile 빌드시
    사전 install 권장 (``RUN python3 -c "import duckdb; duckdb.connect(':memory:').execute('INSTALL postgres')"``).
  - sensor 매 tick connect 비용은 ATTACH lazy connect 라 미미. 실제 PG query
    는 sensor 의 작은 ``count(*)`` / index lookup 수준이라 PG 부하 무시할만함.
"""

from __future__ import annotations

import duckdb

from .env_utils import default_duckdb_path, default_postgres_dsn


def open_sensor_read_connection() -> duckdb.DuckDBPyConnection:
    """Sensor 용 read-only DuckDB connection.

    PG primary 모드면 in-memory DuckDB 에 PG 를 ATTACH 하여 반환.
    DuckDB single 모드면 기존 file 직접 read.

    호출자는 사용 후 반드시 ``conn.close()`` 또는 ``with`` block 사용.
    """
    dsn = default_postgres_dsn()
    if dsn:
        con = duckdb.connect(":memory:")
        con.execute("LOAD postgres")
        # ATTACH 는 parameter binding 미지원 → SQL string 임베드.
        # DSN 은 trusted env (DATAOPS_POSTGRES_DSN), 그래도 SQL standard 식
        # single-quote escape 적용 (`'` → `''`) 으로 정형 안전 가드.
        escaped = dsn.replace("'", "''")
        con.execute(f"ATTACH '{escaped}' AS pg (TYPE postgres, READ_ONLY)")
        con.execute("USE pg.public")
        return con
    return duckdb.connect(default_duckdb_path(), read_only=True)
