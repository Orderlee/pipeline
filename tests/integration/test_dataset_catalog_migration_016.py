"""016_dataset_catalog.sql — real-PG assertions (tables / UNIQUE / status CHECK / FKs).

pg_resource applies ALL migrations (ensure_schema) on a throwaway DB, so this also
exercises the 016 -> 013 ALTER ordering. Auto-skips without DATAOPS_TEST_POSTGRES_DSN;
a skip is NOT a pass — provision a throwaway postgres:15 (see J1.2) to actually run it.
"""

from __future__ import annotations

import uuid

import psycopg2


def _exists(cur, sql: str) -> bool:
    cur.execute(sql)
    return bool(cur.fetchone()[0])


def test_016_creates_three_catalog_tables(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            for tbl in ("dataset_catalog", "dataset_catalog_aliases", "dataset_catalog_pin_events"):
                assert _exists(cur, f"SELECT to_regclass('{tbl}') IS NOT NULL"), f"{tbl} missing"


def test_016_catalog_core_columns(pg_resource) -> None:
    expected = {
        "dataset_catalog_id",
        "task",
        "dataset_name",
        "status",
        "data_repo_id",
        "data_repo_url",
        "git_rev",
        "git_short_rev",
        "git_ref",
        "git_tag",
        "commit_subject",
        "commit_message",
        "commit_author_name",
        "commit_author_email",
        "committed_at",
        "ingested_at",
        "dvc_file_path",
        "dvc_out_path",
        "dvc_md5",
        "dvc_size_bytes",
        "dvc_nfiles",
        "dvc_remote_name",
        "dvc_remote_url",
        "train_dataset_version_id",
        "content_checksum",
        "mlflow_run_id",
    }
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset_catalog'")
            got = {r[0] for r in cur.fetchall()}
    missing = expected - got
    assert not missing, f"dataset_catalog missing columns: {sorted(missing)}"


def test_016_unique_on_repo_rev_dvc(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'dataset_catalog_repo_rev_dvc_unique' "
                "AND conrelid = 'dataset_catalog'::regclass AND contype = 'u')",
            ), "UNIQUE(data_repo_id, git_rev, dvc_file_path, dvc_out_path) missing"


def test_016_status_check_rejects_bad_status(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'dataset_catalog_status_check' "
                "AND conrelid = 'dataset_catalog'::regclass AND contype = 'c')",
            ), "status CHECK missing"
            raised = False
            try:
                cur.execute(
                    "INSERT INTO dataset_catalog "
                    "(dataset_catalog_id, task, dataset_name, status, data_repo_id, git_rev, "
                    " dvc_file_path, dvc_out_path) "
                    "VALUES (%s,'sam3_detection','d','BOGUS','repo','abc','a.dvc','data/a')",
                    (str(uuid.uuid4()),),
                )
            except psycopg2.errors.CheckViolation:
                raised = True
            assert raised, "bad status was not rejected by CHECK"


def test_016_alters_train_dataset_versions_with_fk(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_name = 'train_dataset_versions' AND column_name = 'dataset_catalog_id')",
            ), "train_dataset_versions.dataset_catalog_id column missing"
            assert _exists(
                cur,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conrelid = 'train_dataset_versions'::regclass "
                "AND confrelid = 'dataset_catalog'::regclass AND contype = 'f')",
            ), "train_dataset_versions -> dataset_catalog FK missing"


def test_016_alias_unique_one_per_task(pg_resource) -> None:
    """PK(task, alias) — one 'current' alias per task can exist."""
    cid = str(uuid.uuid4())
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dataset_catalog "
                "(dataset_catalog_id, task, dataset_name, status, data_repo_id, git_rev, "
                " dvc_file_path, dvc_out_path) "
                "VALUES (%s,'sam3_detection','d','available','repo','rev1','a.dvc','data/a')",
                (cid,),
            )
            cur.execute(
                "INSERT INTO dataset_catalog_aliases (task, alias, dataset_catalog_id, pinned_by) "
                "VALUES ('sam3_detection','current',%s,'tester')",
                (cid,),
            )
            raised = False
            try:
                cur.execute(
                    "INSERT INTO dataset_catalog_aliases (task, alias, dataset_catalog_id, pinned_by) "
                    "VALUES ('sam3_detection','current',%s,'tester')",
                    (cid,),
                )
            except psycopg2.errors.UniqueViolation:
                raised = True
            assert raised, "second 'current' alias for same task was not rejected"
