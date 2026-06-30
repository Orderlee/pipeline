"""lib.pgvector_index — model_name 별 partial HNSW 인덱스 SQL 빌더 (순수, no PG).

design §8.1-(C): 파인튠 model_name 은 동적('@ft-<ver>')이라 정적 마이그레이션으로 열거 불가 →
승격 시점에 이 빌더로 인덱스 생성 SQL 을 만들어 실행한다 (Task H4).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "pgvector_index.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_pgvector_index_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_m = _load()
model_index_name = _m.model_index_name
build_partial_hnsw_sql = _m.build_partial_hnsw_sql
build_drop_partial_hnsw_sql = _m.build_drop_partial_hnsw_sql


def test_index_name_deterministic_and_bounded() -> None:
    name = "facebook/PE-Core-L14-336@ft-2026.06.29-lora-001"
    a = model_index_name(name)
    b = model_index_name(name)
    assert a == b
    assert len(a) <= 63  # PG identifier limit
    assert a.startswith("image_embeddings_hnsw_frame_")


def test_index_name_distinct_per_model_and_entity() -> None:
    n1 = model_index_name("facebook/PE-Core-L14-336@ft-001", "frame")
    n2 = model_index_name("facebook/PE-Core-L14-336@ft-002", "frame")
    n3 = model_index_name("facebook/PE-Core-L14-336@ft-001", "caption")
    assert n1 != n2
    assert n1 != n3


def test_build_partial_hnsw_sql_shape() -> None:
    sql = build_partial_hnsw_sql("facebook/PE-Core-L14-336@ft-001", "frame")
    assert "CREATE INDEX IF NOT EXISTS" in sql
    assert "USING hnsw (embedding vector_cosine_ops)" in sql
    assert "WHERE entity_type = 'frame'" in sql
    assert "model_name = 'facebook/PE-Core-L14-336@ft-001'" in sql


def test_build_partial_hnsw_sql_escapes_quotes() -> None:
    # model_name with an embedded single quote must be doubled (no injection).
    sql = build_partial_hnsw_sql("base@ft-o'brien", "frame")
    assert "model_name = 'base@ft-o''brien'" in sql


def test_build_drop_sql_matches_create_name() -> None:
    name = "facebook/PE-Core-L14-336@ft-001"
    create = build_partial_hnsw_sql(name, "frame")
    drop = build_drop_partial_hnsw_sql(name, "frame")
    idx = model_index_name(name, "frame")
    assert idx in create and idx in drop
    assert drop.startswith("DROP INDEX IF EXISTS")
