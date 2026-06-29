"""013_mlops_finetune migration — applies on real-PG fixture, asserts tables + constraints.

Mirrors tests/integration/test_migration_sequence.py patterns. Requires
DATAOPS_TEST_POSTGRES_DSN (else auto-skipped by the pg_resource fixture chain).
"""

from __future__ import annotations


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        )
        """,
        (table_name, column_name),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _constraint_exists(cur, conname: str, conrelid: str, contype: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = %s AND conrelid = %s::regclass AND contype = %s
        )
        """,
        (conname, conrelid, contype),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def test_013_creates_train_dataset_versions(pg_resource) -> None:
    expected_cols = [
        "train_dataset_version_id", "created_at", "task", "source_spec", "class_map",
        "group_key_field", "split_assignment_key", "split_ratios", "manifest_key",
        "content_checksum", "ls_count", "al_confirmed_count", "per_class_counts",
        "total_count", "seed", "upstream_dataset_id",
    ]
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _table_exists(cur, "train_dataset_versions"), "train_dataset_versions missing"
            for col in expected_cols:
                assert _column_exists(cur, "train_dataset_versions", col), f"train_dataset_versions.{col} missing"


def test_013_creates_model_registry(pg_resource) -> None:
    expected_cols = [
        "model_version_id", "model", "version", "train_dataset_version_id", "train_method",
        "git_sha", "training_image_digest", "training_config", "env_lock_key", "eval_config",
        "metrics", "incumbent_metrics", "incumbent_source", "checkpoint_key", "artifact_checksum",
        "status", "created_at", "promoted_at", "promoted_env",
    ]
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _table_exists(cur, "model_registry"), "model_registry missing"
            for col in expected_cols:
                assert _column_exists(cur, "model_registry", col), f"model_registry.{col} missing"


def test_013_task_checksum_unique_constraint(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            assert _constraint_exists(
                cur, "train_dataset_versions_task_checksum_unique", "train_dataset_versions", "u"
            ), "UNIQUE(task, content_checksum) constraint missing"


def test_013_model_registry_fk_to_train_dataset_versions(pg_resource) -> None:
    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'model_registry'::regclass
                      AND confrelid = 'train_dataset_versions'::regclass
                      AND contype = 'f'
                )
                """
            )
            assert bool(cur.fetchone()[0]), "model_registry → train_dataset_versions FK missing"


def test_013_unique_constraint_enforced(pg_resource) -> None:
    """Inserting two rows with same (task, content_checksum) must raise UniqueViolation."""
    import psycopg2.errors

    with pg_resource.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO train_dataset_versions
                    (train_dataset_version_id, task, content_checksum, seed)
                VALUES ('tdv-a', 'sam3_detection', 'deadbeef', 42)
                """
            )
            raised = False
            try:
                cur.execute(
                    """
                    INSERT INTO train_dataset_versions
                        (train_dataset_version_id, task, content_checksum, seed)
                    VALUES ('tdv-b', 'sam3_detection', 'deadbeef', 7)
                    """
                )
            except psycopg2.errors.UniqueViolation:
                raised = True
            assert raised, "duplicate (task, content_checksum) was not rejected"
