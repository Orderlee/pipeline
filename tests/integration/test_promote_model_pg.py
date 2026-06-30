"""promote_model 트랜잭션 SQL 을 real-PG fixture 에 적용 검증 (Section A model_registry 필요).

pg_resource = _PgIntegrationResource(PostgresBaseMixin) → `.connect()` 컨텍스트 사용
(직접 .cursor()/.commit() 아님). model_version_id 는 TEXT PK(기본값 없음)라 명시.
DATAOPS_TEST_POSTGRES_DSN 미설정 시 fixture 가 자동 skip.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "promote_model", str(pathlib.Path("scripts/promote_model.py").resolve())
)
promote_model = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(promote_model)

_INSERT = (
    "INSERT INTO model_registry (model_version_id, model, version, checkpoint_key, "
    "artifact_checksum, status, incumbent_source, created_at, promoted_at, promoted_env) VALUES "
    "('mv-itv1','pe_core','itv1','_models/pe_core/itv1/merged.pt','c1','archived','stock_base',"
    " now() - interval '2 day', now() - interval '2 day','prod'),"
    "('mv-itv2','pe_core','itv2','_models/pe_core/itv2/merged.pt','c2','promotable','promoted',"
    " now(), NULL, NULL)"
)


def test_promote_then_rollback_roundtrip(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.model_registry')")
            if cur.fetchone()[0] is None:
                pytest.skip("model_registry not migrated (Section A) — integration deferred")
            cur.execute("DELETE FROM model_registry WHERE model='pe_core' AND version IN ('itv1','itv2')")
            cur.execute(_INSERT)
        conn.commit()

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            row = promote_model.select_promotable_row(cur, model="pe_core", model_version_id=None)
            assert row["version"] == "itv2"
            promote_model.promote_transition(cur, row=row, env="prod")
        conn.commit()

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv2'")
            assert cur.fetchone()[0] == "promoted"
            cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv1'")
            assert cur.fetchone()[0] == "archived"
            # rollback target = itv1 (archived + promoted_at not null)
            restore = promote_model.select_rollback_target(cur, model="pe_core")
            assert restore["version"] == "itv1"
            promote_model.rollback_transition(
                cur, restore_row=restore,
                current_promoted_id=row["model_version_id"], env="prod",
            )
        conn.commit()

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv2'")
            assert cur.fetchone()[0] == "rolled_back"
            cur.execute("SELECT status FROM model_registry WHERE model='pe_core' AND version='itv1'")
            assert cur.fetchone()[0] == "promoted"
            cur.execute("DELETE FROM model_registry WHERE model='pe_core' AND version IN ('itv1','itv2')")
        conn.commit()
