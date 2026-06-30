"""insert_candidate_model_version builds the correct INSERT and returns the id (no real DB)."""
from __future__ import annotations


class _DummyCursor:
    def __init__(self):
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return ("mv-test-1",)


class _DummyConn:
    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return self._cur


class _DummyDB:
    """Mirrors the real connect()/cursor() idiom of insert_train_dataset_version."""

    def __init__(self):
        self.cursor_obj = _DummyCursor()

    def connect(self):
        return _DummyConn(self.cursor_obj)


def test_insert_candidate_builds_insert_with_status_candidate():
    from vlm_pipeline.resources.postgres_train import insert_candidate_model_version

    db = _DummyDB()
    mv_id = insert_candidate_model_version(
        db, model="sam3", version="sam3-2026.06.29-lora-001",
        train_dataset_version_id="tdv-1", train_method="lora",
        checkpoint_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/checkpoint.pt",
        artifact_checksum="abc123", git_sha="deadbee", training_image_digest="sha256:xyz",
        training_config={"seed": 7}, env_lock_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/env_lock.json",
    )
    sql = db.cursor_obj.sql.lower()
    assert "insert into model_registry" in sql
    assert "'candidate'" in sql  # status hard-set to candidate
    assert db.cursor_obj.params is not None
    assert mv_id == "mv-test-1"
