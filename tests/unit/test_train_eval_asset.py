"""train_eval_gate asset wiring — scorers monkeypatched (no GPU, no pycocotools, no PG)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


def _load_eval_module():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "defs" / "train" / "eval.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_defs_train_eval_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_ev = _load_eval_module()


class _DummyLog:
    def info(self, *a: Any, **k: Any) -> None: ...
    def warning(self, *a: Any, **k: Any) -> None: ...
    def error(self, *a: Any, **k: Any) -> None: ...


class _DummyContext:
    def __init__(self, op_config: dict[str, Any]) -> None:
        self.op_config = op_config
        self.log = _DummyLog()


class _DummyDB:
    """Captures the registry row read + the gate write-back."""

    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row
        self.written: dict[str, Any] | None = None

    def get_model_registry_row(self, model_version_id: str) -> dict[str, Any]:
        assert model_version_id == self._row["model_version_id"]
        return dict(self._row)

    def update_model_registry_eval(self, model_version_id: str, **fields: Any) -> None:
        self.written = {"model_version_id": model_version_id, **fields}


class _DummyMinIO:
    pass


def _base_row(**over: Any) -> dict[str, Any]:
    row = {
        "model_version_id": "mv-001",
        "model": "sam3",
        "version": "sam3-2026.06.29-lora-001",
        "train_dataset_version_id": "tdv-001",
        "eval_config": None,
        "status": "candidate",
    }
    row.update(over)
    return row


def test_promotable_path_writes_status_and_metrics(monkeypatch) -> None:
    db = _DummyDB(_base_row())
    minio = _DummyMinIO()
    ctx = _DummyContext({"model_version_id": "mv-001"})

    monkeypatch.setattr(
        _ev, "_score_candidate",
        lambda context, db_, minio_, row: {"map": 0.50, "per_class_ap": {"fire": 0.50}},
    )
    monkeypatch.setattr(
        _ev, "_score_incumbent",
        lambda context, db_, minio_, row: (
            {"map": 0.30, "per_class_ap": {"fire": 0.30}}, "stock_base",
        ),
    )

    out = _ev._run_train_eval_gate(ctx, db, minio)
    assert out["promotable"] is True
    assert db.written["status"] == "promotable"
    assert db.written["incumbent_source"] == "stock_base"
    assert db.written["metrics"]["map"] == 0.50
    assert db.written["incumbent_metrics"]["map"] == 0.30


def test_not_promotable_keeps_candidate_status(monkeypatch) -> None:
    db = _DummyDB(_base_row())
    ctx = _DummyContext({"model_version_id": "mv-001"})
    monkeypatch.setattr(
        _ev, "_score_candidate",
        lambda *a: {"map": 0.305, "per_class_ap": {"fire": 0.305}},
    )
    monkeypatch.setattr(
        _ev, "_score_incumbent",
        lambda *a: ({"map": 0.30, "per_class_ap": {"fire": 0.30}}, "promoted"),
    )
    out = _ev._run_train_eval_gate(ctx, db, _DummyMinIO())
    assert out["promotable"] is False
    assert db.written["status"] == "candidate"


def test_eval_config_override_from_op_config(monkeypatch) -> None:
    db = _DummyDB(_base_row())
    # huge margin -> even a big win fails
    ctx = _DummyContext({"model_version_id": "mv-001", "eval_config": {
        "primary_metric": "map", "primary_margin": 0.99,
        "per_class_field": "per_class_ap", "per_class_floor": -0.02, "advisory": False,
    }})
    monkeypatch.setattr(_ev, "_score_candidate", lambda *a: {"map": 0.9, "per_class_ap": {"fire": 0.9}})
    monkeypatch.setattr(_ev, "_score_incumbent", lambda *a: ({"map": 0.1, "per_class_ap": {"fire": 0.1}}, "promoted"))
    out = _ev._run_train_eval_gate(ctx, db, _DummyMinIO())
    assert out["promotable"] is False
    assert db.written["eval_config"]["primary_margin"] == 0.99
