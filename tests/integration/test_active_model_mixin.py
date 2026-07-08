"""PostgresEmbeddingMixin active-model pointer + partial HNSW (real-PG; pgvector-gated)."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def _has_table(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables " "WHERE table_schema='public' AND table_name=%s)",
            (name,),
        )
        return bool(cur.fetchone()[0])


def test_get_active_falls_back_to_stock_when_table_absent(postgres_resource) -> None:
    # On a non-pgvector test PG the table won't exist → must return stock, not raise.
    name = postgres_resource.get_active_embedding_model()
    assert name == "facebook/PE-Core-L14-336"


def test_set_then_get_roundtrip(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        if not _has_table(conn, "embedding_active_model"):
            pytest.skip("embedding_active_model absent — pgvector not available")
    postgres_resource.set_active_embedding_model("facebook/PE-Core-L14-336@ft-test", updated_by="pytest")
    assert postgres_resource.get_active_embedding_model() == "facebook/PE-Core-L14-336@ft-test"
    # restore stock so the test is idempotent across reruns
    postgres_resource.set_active_embedding_model("facebook/PE-Core-L14-336", updated_by="pytest")


def test_create_model_partial_hnsw_idempotent(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        if not _has_table(conn, "image_embeddings"):
            pytest.skip("image_embeddings absent — pgvector not available")
    names1 = postgres_resource.create_model_partial_hnsw("facebook/PE-Core-L14-336@ft-test")
    names2 = postgres_resource.create_model_partial_hnsw("facebook/PE-Core-L14-336@ft-test")
    assert names1 == names2 and len(names1) == 2  # frame + caption
    # cleanup
    with postgres_resource.connect() as conn, conn.cursor() as cur:
        for n in names1:
            cur.execute(f"DROP INDEX IF EXISTS {n}")
