"""mlflow_logging DVC lineage fields (J8) — dvc_* keys logged as params + ds.* tags.

A fake mlflow module is injected so NO real mlflow/network is touched.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
import types


def _load():
    mod_path = pathlib.Path(__file__).resolve().parents[2] / "docker" / "trainer" / "mlflow_logging.py"
    spec = importlib.util.spec_from_file_location("mlflow_logging", mod_path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class _Rec:
    def __init__(self):
        self.params = {}
        self.tags = {}
        self.inputs = []


def _fake_mlflow(rec):
    m = types.ModuleType("mlflow")
    m.set_tracking_uri = lambda uri: None
    m.set_experiment = lambda name: None

    class _Run:
        class info:
            run_id = "run-xyz"

    class _Ctx:
        def __enter__(self):
            return _Run()

        def __exit__(self, *a):
            return False

    m.start_run = lambda run_name=None: _Ctx()
    m.log_params = lambda d: rec.params.update(d)
    m.log_metrics = lambda d: None
    m.set_tags = lambda d: rec.tags.update(d)
    m.log_artifact = lambda p: None
    data_mod = types.ModuleType("mlflow.data")
    data_mod.from_dict = lambda d, name=None: {"name": name}
    m.data = data_mod
    m.log_input = lambda ds: rec.inputs.append(ds)
    return m


_DATASET = {
    "train_dataset_version_id": "tdv-1",
    "content_checksum": "csum-abc",
    "dvc_catalog_id": "cid-7",
    "dvc_git_rev": "9f3c2a1b",
    "dvc_commit_subject": "curate: fire v3",
    "dvc_md5": "deadbeef.dir",
}


def test_dvc_fields_logged_as_params_and_tags(monkeypatch):
    rec = _Rec()
    fake = _fake_mlflow(rec)
    monkeypatch.setitem(sys.modules, "mlflow", fake)
    monkeypatch.setitem(sys.modules, "mlflow.data", fake.data)
    m = _load()

    run_id = m.log_training_run(
        hparams={"lr": 0.001}, dataset=_DATASET, metrics={}, artifact_paths=[], experiment="sam3-ft",
    )
    assert run_id == "run-xyz"
    for key in ("dvc_catalog_id", "dvc_git_rev", "dvc_commit_subject", "dvc_md5", "content_checksum"):
        assert key in rec.params, f"DVC lineage key {key} missing from params"
        assert f"ds.{key}" in rec.tags, f"DVC lineage key {key} missing from ds.* tags"
    assert rec.params["dvc_git_rev"] == "9f3c2a1b"
