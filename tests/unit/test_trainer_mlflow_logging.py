"""mlflow_logging.log_training_run — fail-soft trainer MLflow wrapper.

A fake `mlflow` module records calls. Asserts (a) params + dataset lineage + metrics
+ artifacts are logged inside an active run, (b) the run_id is returned, and
(c) ANY mlflow error is swallowed (fail-soft) → returns None, training continues.
No real mlflow, no network.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

_MOD = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "mlflow_logging.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("mlflow_logging", _MOD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Recorder:
    def __init__(self) -> None:
        self.params: dict = {}
        self.metrics: dict = {}
        self.tags: dict = {}
        self.artifacts: list = []
        self.inputs: list = []
        self.set_uri: str | None = None
        self.experiment: str | None = None


def _fake_mlflow(rec: _Recorder, *, run_id: str = "run-abc123", explode: bool = False) -> types.ModuleType:
    m = types.ModuleType("mlflow")

    class _Run:
        info = types.SimpleNamespace(run_id=run_id)

    class _ActiveRun:
        def __enter__(self):
            if explode:
                raise RuntimeError("mlflow tracking server unreachable")
            return _Run()

        def __exit__(self, *a):
            return False

    def start_run(run_name=None):  # noqa: ARG001
        return _ActiveRun()

    m.start_run = start_run
    m.set_tracking_uri = lambda uri: setattr(rec, "set_uri", uri)
    m.set_experiment = lambda name: setattr(rec, "experiment", name)
    m.log_params = lambda d: rec.params.update(d)
    m.log_metrics = lambda d: rec.metrics.update(d)
    m.set_tags = lambda d: rec.tags.update(d)
    m.log_artifact = lambda p: rec.artifacts.append(p)
    # log_input is best-effort (older mlflow lacks it) — provide it so the happy path uses it.
    data_mod = types.ModuleType("mlflow.data")

    def from_dict(d, name=None):  # noqa: ARG001
        return {"_dataset": d, "_name": name}

    data_mod.from_dict = from_dict
    m.data = data_mod
    m.log_input = lambda ds: rec.inputs.append(ds)
    return m


_DATASET = {
    "train_dataset_version_id": "tdv-7",
    "content_checksum": "deadbeef",
    "manifest_key": "vlm-dataset/_trainsets/tdv-7/manifest.json",
    "ls_count": 288,
    "al_confirmed_count": 0,
    "per_class_counts": {"fire": 120, "smoke": 168},
    "split_ratios": {"train": 0.8, "val": 0.1, "test": 0.1},
}
_HPARAMS = {"train_method": "lora", "lr": 1e-4, "epochs": 5, "lora_rank": 16, "seed": 123}
_METRICS = {"box_map": 0.41, "box_map_fire": 0.38}


def test_logs_params_dataset_metrics_and_returns_run_id(tmp_path, monkeypatch) -> None:
    rec = _Recorder()
    monkeypatch.setitem(sys.modules, "mlflow", _fake_mlflow(rec, run_id="run-xyz"))
    monkeypatch.setitem(sys.modules, "mlflow.data", _fake_mlflow(rec).data)
    art = tmp_path / "training_summary.json"
    art.write_text("{}", encoding="utf-8")
    mod = _load_module()

    run_id = mod.log_training_run(
        hparams=_HPARAMS, dataset=_DATASET, metrics=_METRICS,
        artifact_paths=[str(art)], experiment="sam3_detection", run_name="sam3-2026.06.29-lora-001",
        tracking_uri="http://mlflow:5000",
    )

    assert run_id == "run-xyz"
    assert rec.set_uri == "http://mlflow:5000"
    assert rec.experiment == "sam3_detection"
    # hparams logged as params:
    assert rec.params["train_method"] == "lora" and rec.params["lora_rank"] == 16
    # dataset lineage logged (params or tags) — version id + checksum must be present somewhere:
    flat = {**rec.params, **rec.tags}
    assert flat.get("train_dataset_version_id") == "tdv-7"
    assert flat.get("content_checksum") == "deadbeef"
    assert str(flat.get("al_confirmed_count")) == "0"  # honest zero (spec §7.2)
    # metrics logged:
    assert rec.metrics["box_map"] == 0.41
    # artifact logged:
    assert str(art) in rec.artifacts
    # dataset object logged via log_input (best-effort):
    assert rec.inputs, "expected mlflow.log_input to receive the dataset lineage object"


def test_failsoft_returns_none_when_mlflow_raises(tmp_path, monkeypatch) -> None:
    rec = _Recorder()
    monkeypatch.setitem(sys.modules, "mlflow", _fake_mlflow(rec, explode=True))
    mod = _load_module()
    # MUST NOT raise — fail-soft. Returns None so the trainer keeps going.
    run_id = mod.log_training_run(
        hparams=_HPARAMS, dataset=_DATASET, metrics=_METRICS,
        artifact_paths=[], experiment="sam3_detection",
    )
    assert run_id is None


def test_failsoft_returns_none_when_mlflow_not_installed(monkeypatch) -> None:
    # Simulate `import mlflow` ImportError (mlflow client absent in some build).
    monkeypatch.setitem(sys.modules, "mlflow", None)  # forces ImportError on `import mlflow`
    mod = _load_module()
    run_id = mod.log_training_run(
        hparams=_HPARAMS, dataset=_DATASET, metrics=_METRICS, artifact_paths=[], experiment="x",
    )
    assert run_id is None
