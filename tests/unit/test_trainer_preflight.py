"""trainer_lib.preflight_secrets() fail-fast + format_train_log_line() JSONL.

preflight: must raise listing ALL missing secrets (HF_TOKEN/MINIO_ACCESS_KEY/
MINIO_SECRET_KEY) so the operator fixes .env in one shot, not whack-a-mole.
format_train_log_line: compact one-line JSON for _models/<ver>/train_log.jsonl.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
_spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
trainer_lib = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trainer_lib)


def test_preflight_passes_when_all_present() -> None:
    trainer_lib.preflight_secrets({"HF_TOKEN": "x", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "s"})  # no raise


def test_preflight_lists_every_missing_secret() -> None:
    with pytest.raises(RuntimeError) as exc:
        trainer_lib.preflight_secrets({"MINIO_ACCESS_KEY": "a"})
    msg = str(exc.value)
    assert "HF_TOKEN" in msg
    assert "MINIO_SECRET_KEY" in msg


def test_preflight_treats_empty_string_as_missing() -> None:
    with pytest.raises(RuntimeError) as exc:
        trainer_lib.preflight_secrets({"HF_TOKEN": "", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "s"})
    assert "HF_TOKEN" in str(exc.value)


def test_train_log_line_is_compact_json_with_newline() -> None:
    line = trainer_lib.format_train_log_line(step=5, loss=0.25, lr=1e-4, throughput=12.5)
    assert line.endswith("\n")
    obj = json.loads(line)
    assert obj == {"step": 5, "loss": 0.25, "lr": 1e-4, "throughput": 12.5}


def test_load_and_record_mlflow_run_id_roundtrip(tmp_path) -> None:
    p = tmp_path / "training_summary.json"
    p.write_text('{"hparams": {"lr": 0.001}, "dataset": {"train_dataset_version_id": "tdv-1"}}', encoding="utf-8")
    loaded = trainer_lib.load_training_summary(str(p))
    assert loaded["hparams"]["lr"] == 0.001
    trainer_lib.record_mlflow_run_id(str(p), "run-xyz")
    assert trainer_lib.load_training_summary(str(p))["mlflow_run_id"] == "run-xyz"
    # None run_id recorded faithfully (registry remains SoT)
    trainer_lib.record_mlflow_run_id(str(p), None)
    assert trainer_lib.load_training_summary(str(p))["mlflow_run_id"] is None
