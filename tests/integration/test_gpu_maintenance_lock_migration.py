"""014_gpu_maintenance_lock 적용 검증 (real-PG fixture)."""
from __future__ import annotations

import pytest

pytest.importorskip("psycopg2")


def test_gpu_maintenance_lock_table_and_constraint(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'gpu_maintenance_lock_target_chk' "
                "AND conrelid = 'gpu_maintenance_lock'::regclass)"
            )
            assert cur.fetchone()[0] is True
            # CHECK rejects unknown target
            cur.execute("INSERT INTO gpu_maintenance_lock (target) VALUES ('sam3')")
            with pytest.raises(Exception):
                cur.execute("INSERT INTO gpu_maintenance_lock (target) VALUES ('bogus')")
