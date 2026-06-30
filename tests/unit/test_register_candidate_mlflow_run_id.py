"""insert_candidate_model_version threads mlflow_run_id into the INSERT (I3 reconciliation)."""
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
    """Mirrors the real connect()/cursor() idiom of insert_candidate_model_version."""

    def __init__(self):
        self.cursor_obj = _DummyCursor()

    def connect(self):
        return _DummyConn(self.cursor_obj)


_COMMON = dict(
    model="sam3",
    version="sam3-2026.06.29-lora-001",
    train_dataset_version_id="tdv-1",
    train_method="lora",
    checkpoint_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/checkpoint.pt",
    artifact_checksum="abc123",
    git_sha="deadbee",
    training_image_digest="sha256:xyz",
    training_config={"seed": 7},
    env_lock_key="vlm-dataset/_models/sam3/sam3-2026.06.29-lora-001/env_lock.json",
)


def _params_as_text(params) -> str:
    return str(params)


def test_mlflow_run_id_written_when_provided() -> None:
    from vlm_pipeline.resources.postgres_train import insert_candidate_model_version

    db = _DummyDB()
    insert_candidate_model_version(db, mlflow_run_id="run-xyz", **_COMMON)
    sql = db.cursor_obj.sql.lower()
    assert "mlflow_run_id" in sql, "INSERT must name the mlflow_run_id column"
    assert "run-xyz" in _params_as_text(db.cursor_obj.params), "run_id must be bound as a param"


def test_mlflow_run_id_defaults_to_none() -> None:
    from vlm_pipeline.resources.postgres_train import insert_candidate_model_version

    db = _DummyDB()
    mv_id = insert_candidate_model_version(db, **_COMMON)  # no mlflow_run_id
    assert mv_id == "mv-test-1"
    text = _params_as_text(db.cursor_obj.params)
    assert "run-" not in text  # no stray run id leaked in
