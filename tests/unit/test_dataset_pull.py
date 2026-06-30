"""scripts/dataset_pull.py + lib.dvc_pull — resolve via catalog, dvc get mocked, verify md5.

Dry-run default → no dvc invoked, no bytes moved (scaffolding only).
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.lib.dvc_pull import build_dvc_get_argv, verify_pulled_md5  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "dataset_pull", str((pathlib.Path(__file__).resolve().parents[2] / "scripts" / "dataset_pull.py"))
)
dataset_pull = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(dataset_pull)


def test_build_dvc_get_argv():
    argv = build_dvc_get_argv("/srv/data-repos/dvc-datasets.git", "fire_v3", "abc123", "/tmp/out")
    assert argv == ["dvc", "get", "/srv/data-repos/dvc-datasets.git", "fire_v3", "--rev", "abc123", "-o", "/tmp/out"]


def test_verify_pulled_md5():
    assert verify_pulled_md5(None, "anything") is True       # nothing to verify
    assert verify_pulled_md5("deadbeef", "deadbeef") is True
    assert verify_pulled_md5("deadbeef", "cafef00d") is False


def test_dry_run_resolves_but_does_not_invoke_dvc(monkeypatch, capsys):
    invoked = {"dvc": False}

    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            assert task == "sam3_detection"
            return {
                "dataset_catalog_id": "cid-1", "git_rev": "abc123", "dvc_out_path": "fire_v3",
                "dvc_md5": "deadbeef", "status": "pinned", "commit_subject": "curate: fire v3",
            }

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    monkeypatch.setattr(
        dataset_pull, "_run_dvc_get",
        lambda *a, **k: invoked.__setitem__("dvc", True),
    )

    rc = dataset_pull.main(["--task", "sam3_detection", "--alias", "current", "--dest", "/tmp/x"])
    assert rc == 0
    assert invoked["dvc"] is False, "dry-run must NOT invoke dvc get"
    out = capsys.readouterr().out
    assert "abc123" in out and "fire_v3" in out and "DRY-RUN" in out


def test_no_pin_returns_nonzero(monkeypatch):
    class _DB:
        def get_catalog_by_alias(self, task, alias="current"):
            return None

    monkeypatch.setattr(dataset_pull, "_open_db", lambda: _DB())
    monkeypatch.setattr(dataset_pull, "_repo_path", lambda: "/srv/data-repos/dvc-datasets.git")
    rc = dataset_pull.main(["--task", "sam3_detection", "--dest", "/tmp/x"])
    assert rc == 2
