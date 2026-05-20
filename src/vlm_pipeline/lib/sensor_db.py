"""Sensor read-only DB connection helper — Postgres direct (post-DuckDB-deprecation).

Sensors 가 ``%s`` 바인딩 + cursor 패턴으로 read-only 쿼리. 모든 sensor 는
이 facade 호출 후 ``with conn.cursor() as cur:`` 로 cursor 열고 사용.

이전 (2026-05-19 이전) 에는 DuckDB-as-PG-bridge (``duckdb.connect(":memory:")`` +
``ATTACH ... TYPE postgres, READ_ONLY``) 였으나, ``DATAOPS_DB_BACKEND=postgres``
단독 모드가 영구화되면서 DuckDB extension 경유 비용/복잡도 제거.

write 는 절대 안 됨 — 모든 write 는 ``db`` resource (``PostgresResource``) 통해서만.
"""

from __future__ import annotations

import psycopg2

from .env_utils import default_postgres_dsn


def open_sensor_read_connection() -> psycopg2.extensions.connection:
    """Sensor 용 PostgreSQL connection.

    호출자는 사용 후 반드시 ``conn.close()`` 또는 ``with`` block 사용.
    cursor 작업은 ``with conn.cursor() as cur:`` 패턴 권장.
    """
    dsn = default_postgres_dsn()
    if not dsn:
        raise RuntimeError(
            "DATAOPS_POSTGRES_DSN 미설정 — sensor 가 read 할 PG 없음. "
            "DuckDB single 모드는 deprecated (2026-05-19 PG-only cutover)."
        )
    return psycopg2.connect(dsn)
