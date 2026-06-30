"""015_embedding_active_model 적용 검증 (real-PG fixture, pgvector-gated _OPTIONAL).

DATAOPS_TEST_POSTGRES_DSN 미설정 시 자동 skip. pgvector 미가용 PG 에서는 적용되지 않으므로
(테이블 부재) 표는 conditional — table 존재 시에만 seed/CHECK 를 단언한다.
"""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)",
        (name,),
    )
    return bool(cur.fetchone()[0])


def test_015_active_model_table_and_seed(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        with conn.cursor() as cur:
            if not _table_exists(cur, "embedding_active_model"):
                pytest.skip("embedding_active_model absent — pgvector not available on this PG")
            # seed row present + defaults to stock model_name
            cur.execute(
                "SELECT model_name FROM embedding_active_model WHERE scope = 'frame_search'"
            )
            row = cur.fetchone()
            assert row is not None, "frame_search seed row missing"
            assert row[0] == "facebook/PE-Core-L14-336"
            # CHECK rejects unknown scope
            with pytest.raises(Exception):
                cur.execute(
                    "INSERT INTO embedding_active_model (scope, model_name) VALUES ('bogus', 'x')"
                )
