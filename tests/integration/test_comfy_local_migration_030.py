"""030_comfy_local schema + GPU0 lease semantics on a real PostgreSQL fixture.

Skipped wholesale when ``DATAOPS_TEST_POSTGRES_DSN`` is unset or unreachable —
see tests/integration/conftest.py, which builds a throwaway database per test.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


def test_comfy_local_engine_constraints_and_tables(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_get_constraintdef(oid)
                  FROM pg_constraint
                 WHERE conname='genai_batches_engine_check'
                   AND conrelid='genai_batches'::regclass
                """
            )
            assert "comfy_local" in cur.fetchone()[0]
            cur.execute(
                """
                SELECT pg_get_constraintdef(oid)
                  FROM pg_constraint
                 WHERE conname='chk_raw_files_genai_engine'
                   AND conrelid='raw_files'::regclass
                """
            )
            assert "comfy_local" in cur.fetchone()[0]
            cur.execute(
                """
                SELECT to_regclass('public.genai_job_provenance'),
                       to_regclass('public.generation_gpu_leases')
                """
            )
            assert cur.fetchone() == ("genai_job_provenance", "generation_gpu_leases")


def test_comfy_gpu_lease_resource_constraint(pg_resource):
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO genai_batches (
                    batch_id, engine, output_media, prompt, status, n_total
                ) VALUES ('comfy-test', 'comfy_local', 'image', 'test', 'running', 1)
                """
            )
            cur.execute(
                """
                INSERT INTO genai_jobs (job_id, batch_id, seq_in_batch)
                VALUES ('comfy-test-001', 'comfy-test', 1)
                """
            )
            cur.execute(
                """
                INSERT INTO generation_gpu_leases (
                    resource, owner_job_id, lease_token, expires_at
                ) VALUES ('gpu0_comfy', 'comfy-test-001', 'token', now() + interval '1 minute')
                """
            )
            cur.execute("SELECT owner_job_id FROM generation_gpu_leases WHERE resource='gpu0_comfy'")
            assert cur.fetchone()[0] == "comfy-test-001"


# ---------------------------------------------------------------------------
# GPU0 lease semantics, driven through the real docker/genai/db/pg.py helpers.
#
# Until now the only coverage was tests/unit/test_comfy_local_adapter.py, which
# monkeypatches acquire_generation_gpu_lease to return "lease" or None — so the
# `ON CONFLICT (resource) DO UPDATE ... WHERE` that decides who owns GPU0 had
# never actually executed in a test.  These run it against real PostgreSQL.
# ---------------------------------------------------------------------------


_GENAI_ROOT = Path(__file__).resolve().parents[2] / "docker" / "genai"
_WRONG_TOKEN = "0" * 48


@pytest.fixture
def genai_pg(pg_resource, monkeypatch):
    """``docker/genai/db/pg.py`` bound to this test's throwaway database.

    The lease helpers themselves are the unit under test, so the test drives the
    real module rather than re-typing its SQL: an assertion against a copy of the
    statement would prove nothing about the statement that runs in prod.
    """
    monkeypatch.syspath_prepend(str(_GENAI_ROOT))
    monkeypatch.setenv("DATAOPS_POSTGRES_DSN", pg_resource.dsn)
    monkeypatch.delenv("PIPELINE_DB_DSN", raising=False)

    # tests/unit/test_comfy_local_adapter.py imports the same top-level `db`
    # package (via adapters.comfy_local) and its module-global pool sticks to
    # whatever DSN was live then.  Swap the module out for the duration of this
    # test and put the original objects back afterwards, so suite order — unit
    # first or integration first — cannot change the outcome.
    saved = {name: mod for name, mod in sys.modules.items() if name == "db" or name.startswith("db.")}
    for name in saved:
        del sys.modules[name]
    pg = importlib.import_module("db.pg")
    try:
        yield pg
    finally:
        if getattr(pg, "_pool", None) is not None:
            pg._pool.closeall()
            pg._pool = None
        for name in [n for n in sys.modules if n == "db" or n.startswith("db.")]:
            del sys.modules[name]
        sys.modules.update(saved)


@pytest.fixture
def lease_jobs(pg_resource):
    """Two genai_jobs rows — generation_gpu_leases.owner_job_id is an FK to them."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO genai_batches (batch_id, engine, output_media, prompt, status, n_total)
                VALUES ('lease-batch', 'comfy_local', 'image', 'p', 'running', 2)
                """
            )
            cur.executemany(
                "INSERT INTO genai_jobs (job_id, batch_id, seq_in_batch) VALUES (%s, 'lease-batch', %s)",
                [("job-a", 1), ("job-b", 2)],
            )
    return "job-a", "job-b"


def _lease_row(pg_resource) -> tuple:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT owner_job_id, lease_token, state, release_reason
                  FROM generation_gpu_leases WHERE resource='gpu0_comfy'
                """
            )
            return cur.fetchone()


def _expire_lease(pg_resource) -> None:
    """Age the lease past its TTL without sleeping through a 20-minute default."""
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE generation_gpu_leases
                   SET expires_at = now() - interval '1 second'
                 WHERE resource='gpu0_comfy'
                """
            )
            assert cur.rowcount == 1


def test_second_acquire_is_refused_while_the_lease_is_live(genai_pg, pg_resource, lease_jobs):
    job_a, job_b = lease_jobs
    token_a = genai_pg.acquire_generation_gpu_lease(job_a, 600)
    assert token_a

    assert genai_pg.acquire_generation_gpu_lease(job_b, 600) is None

    owner, token, state, _reason = _lease_row(pg_resource)
    assert (owner, token, state) == (job_a, token_a, "active")


def test_expired_lease_is_stolen_and_dispossesses_the_old_owner(genai_pg, pg_resource, lease_jobs):
    job_a, job_b = lease_jobs
    token_a = genai_pg.acquire_generation_gpu_lease(job_a, 600)
    _expire_lease(pg_resource)

    token_b = genai_pg.acquire_generation_gpu_lease(job_b, 600)
    assert token_b and token_b != token_a
    owner, token, state, _reason = _lease_row(pg_resource)
    assert (owner, token, state) == (job_b, token_b, "active")

    # The evicted owner may not extend or end a lease that is no longer its own.
    assert genai_pg.heartbeat_generation_gpu_lease(job_a, 600, token_a) is False
    assert genai_pg.release_generation_gpu_lease(job_a, "late release", token_a) is False
    assert _lease_row(pg_resource)[:3] == (job_b, token_b, "active")


def test_heartbeat_and_release_reject_a_stale_token(genai_pg, pg_resource, lease_jobs):
    job_a, job_b = lease_jobs
    token_a = genai_pg.acquire_generation_gpu_lease(job_a, 600)

    assert genai_pg.heartbeat_generation_gpu_lease(job_a, 600, _WRONG_TOKEN) is False
    assert genai_pg.release_generation_gpu_lease(job_a, "stale token", _WRONG_TOKEN) is False
    # Wrong owner with the right token is refused too — both halves must match.
    assert genai_pg.heartbeat_generation_gpu_lease(job_b, 600, token_a) is False
    assert genai_pg.release_generation_gpu_lease(job_b, "wrong owner", token_a) is False
    assert _lease_row(pg_resource)[:3] == (job_a, token_a, "active")

    # The real holder still gets through.
    assert genai_pg.heartbeat_generation_gpu_lease(job_a, 600, token_a) is True
    assert genai_pg.release_generation_gpu_lease(job_a, "completed", token_a) is True


def test_a_retrying_job_is_fenced_from_its_own_stale_attempt(genai_pg, pg_resource, lease_jobs):
    """The bug the token closes: matching on owner_job_id alone is not enough.

    Attempt #1 of job-a hangs past its TTL; the retry re-acquires under the *same*
    job id.  When attempt #1 finally unwinds and releases, owner-only matching would
    free GPU0 while the retry is mid-generation.  Its token no longer matches, so it
    cannot.
    """
    job_a, _job_b = lease_jobs
    first = genai_pg.acquire_generation_gpu_lease(job_a, 600)
    _expire_lease(pg_resource)
    second = genai_pg.acquire_generation_gpu_lease(job_a, 600)
    assert second and second != first

    assert genai_pg.release_generation_gpu_lease(job_a, "submit_error", first) is False
    assert genai_pg.heartbeat_generation_gpu_lease(job_a, 600, first) is False
    assert _lease_row(pg_resource)[:3] == (job_a, second, "active")

    assert genai_pg.release_generation_gpu_lease(job_a, "completed", second) is True


def test_tokenless_callers_still_match_on_owner_alone(genai_pg, pg_resource, lease_jobs):
    """Compatibility boundary, pinned so it cannot widen or vanish unnoticed.

    poll()/cancel()/download_result() run off a fresh adapter and hold no token;
    they keep the pre-token owner match.  What protects them is not the token but
    the prompt-scoped provenance lookup that resolves their owner in the first place.
    """
    job_a, _job_b = lease_jobs
    genai_pg.acquire_generation_gpu_lease(job_a, 600)

    assert genai_pg.heartbeat_generation_gpu_lease(job_a, 600) is True
    assert genai_pg.release_generation_gpu_lease(job_a, "tokenless caller") is True
    assert _lease_row(pg_resource)[2] == "released"


def test_release_hands_the_lease_to_the_next_job(genai_pg, pg_resource, lease_jobs):
    job_a, job_b = lease_jobs
    token_a = genai_pg.acquire_generation_gpu_lease(job_a, 600)

    assert genai_pg.release_generation_gpu_lease(job_a, "completed", token_a) is True
    owner, _token, state, reason = _lease_row(pg_resource)
    assert (owner, state, reason) == (job_a, "released", "completed")
    # A released lease is nobody's: releasing twice is a no-op, not a second free.
    assert genai_pg.release_generation_gpu_lease(job_a, "completed again", token_a) is False

    token_b = genai_pg.acquire_generation_gpu_lease(job_b, 600)
    assert token_b and token_b != token_a
    assert _lease_row(pg_resource)[:3] == (job_b, token_b, "active")


def test_a_refused_release_cannot_pin_gpu0_past_the_ttl(genai_pg, pg_resource, lease_jobs):
    """Guard for the 2026-09-18 outage class: token checks must not create a lease
    that nothing can free.  Recovery may never depend on presenting a token, so the
    TTL steal in acquire_generation_gpu_lease deliberately ignores lease_token.
    """
    job_a, job_b = lease_jobs
    token_a = genai_pg.acquire_generation_gpu_lease(job_a, 600)

    # Every token-bearing way out is refused — the lease stays 'active' and held.
    assert genai_pg.release_generation_gpu_lease(job_a, "wrong token", _WRONG_TOKEN) is False
    assert genai_pg.release_generation_gpu_lease(job_b, "wrong owner", token_a) is False
    assert genai_pg.acquire_generation_gpu_lease(job_b, 600) is None
    assert _lease_row(pg_resource)[2] == "active"

    # Expiry alone still reclaims GPU0, with no token in hand anywhere.
    _expire_lease(pg_resource)
    stolen = genai_pg.acquire_generation_gpu_lease(job_b, 600)
    assert stolen and stolen != token_a
    assert _lease_row(pg_resource)[:3] == (job_b, stolen, "active")
