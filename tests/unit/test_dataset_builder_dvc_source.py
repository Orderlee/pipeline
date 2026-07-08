"""Section B builder DVC-source hook (J6) — pinned alias → dvc get → back-link FK.

dvc get is an injected callable (never the real binary). No-alias → returns None
(builder keeps its existing LS/AL candidate path — DVC source is opt-in).
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))

from vlm_pipeline.defs.train.dataset import _materialize_pinned_dvc_source  # noqa: E402


class _DB:
    def __init__(self, row):
        self._row = row
        self.asked = None

    def get_catalog_by_alias(self, task, alias="current"):
        self.asked = (task, alias)
        return self._row


def test_materialize_returns_none_when_no_alias(tmp_path):
    db = _DB(None)
    out = _materialize_pinned_dvc_source(
        db, task="sam3_detection", alias="current", dest_root=str(tmp_path), dvc_get=lambda argv: None
    )
    assert out is None
    assert db.asked == ("sam3_detection", "current")


def test_materialize_invokes_dvc_get_and_returns_backlink(tmp_path):
    calls = []
    db = _DB({"dataset_catalog_id": "cid-9", "git_rev": "abc123", "dvc_out_path": "fire_v3", "dvc_md5": "deadbeef"})
    out = _materialize_pinned_dvc_source(
        db,
        task="sam3_detection",
        alias="current",
        dest_root=str(tmp_path),
        dvc_get=lambda argv: calls.append(argv),
    )
    assert out["dataset_catalog_id"] == "cid-9"
    assert out["git_rev"] == "abc123"
    assert out["dvc_out_path"] == "fire_v3"
    assert out["local_path"] == str(tmp_path / "fire_v3")
    # dvc get was invoked once with the canonical argv (mocked):
    assert len(calls) == 1
    assert calls[0][:2] == ["dvc", "get"]
    assert "--rev" in calls[0] and "abc123" in calls[0]
