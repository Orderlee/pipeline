"""find_reembed_targets — incumbent 커버 집합만 새 model_name 대상으로 (real-PG, pgvector-gated)."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def _has(conn, name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables " "WHERE table_schema='public' AND table_name=%s)",
            (name,),
        )
        return bool(cur.fetchone()[0])


def test_reembed_targets_only_incumbent_covered_frames(postgres_resource) -> None:
    with postgres_resource.connect() as conn:
        if not (_has(conn, "image_embeddings") and _has(conn, "image_metadata")):
            pytest.skip("pgvector tables absent")
        with conn.cursor() as cur:
            # raw_files parent (source_asset_id FK + NOT NULL source_path)
            cur.execute(
                "INSERT INTO raw_files (asset_id, source_path) VALUES ('rt-a1','/x') "
                "ON CONFLICT (asset_id) DO NOTHING"
            )
            # two frames in image_metadata; only one has an incumbent embedding
            cur.execute(
                "INSERT INTO image_metadata (image_id, source_asset_id, image_bucket, image_key, image_role) "
                "VALUES ('rt-f1','rt-a1','b','k1','source_image'),('rt-f2','rt-a1','b','k2','source_image') "
                "ON CONFLICT (image_id) DO NOTHING"
            )
            cur.execute(
                "INSERT INTO image_embeddings "
                "(embedding_id, entity_type, entity_id, image_id, model_name, dim, embedding) "
                "VALUES ('frame|rt-f1|inc','frame','rt-f1','rt-f1','inc-model',1024, "
                "  ('[' || array_to_string(array_fill(0.0::float8, ARRAY[1024]), ',') || ']')::vector) "
                "ON CONFLICT (entity_type, entity_id, model_name) DO NOTHING"
            )
    targets = postgres_resource.find_reembed_targets(
        incumbent_model_name="inc-model",
        new_model_name="inc-model@ft-x",
        entity_type="frame",
        limit=100,
    )
    ids = {t["image_id"] for t in targets}
    assert "rt-f1" in ids  # incumbent-covered → re-embed target
    assert "rt-f2" not in ids  # never embedded → NOT a re-embed target (coverage discipline)
