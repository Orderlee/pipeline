"""scripts/promote_pe_core.py — 포인터 전환/롤백 + 재임베딩 선결 검증 (스텁, dry-run 기본)."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "promote_pe_core", str(pathlib.Path("scripts/promote_pe_core.py").resolve())
)
promote_pe_core = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(promote_pe_core)


class _FakeCursor:
    def __init__(self, fetch_results):
        self._results = list(fetch_results)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._results.pop(0) if self._results else None


def test_assert_reembed_complete_ok():
    # count(new_model_name) > 0 → coverage present
    cur = _FakeCursor([(187994,)])
    n = promote_pe_core.assert_reembed_complete(cur, new_model_name="facebook/PE-Core-L14-336@ft-x")
    assert n == 187994
    assert "model_name" in cur.executed[0][0].lower()


def test_assert_reembed_complete_zero_raises():
    cur = _FakeCursor([(0,)])
    with pytest.raises(promote_pe_core.PromotionError):
        promote_pe_core.assert_reembed_complete(cur, new_model_name="facebook/PE-Core-L14-336@ft-x")


def test_assert_reembed_complete_partial_vs_incumbent_raises():
    # Codex BUG4: new coverage < incumbent coverage → partial re-embed → reject the flip.
    cur = _FakeCursor([(100,), (200,)])  # new=100, incumbent=200
    with pytest.raises(promote_pe_core.PromotionError):
        promote_pe_core.assert_reembed_complete(
            cur, new_model_name="facebook/PE-Core-L14-336@ft-x", incumbent_model_name="facebook/PE-Core-L14-336"
        )


def test_assert_reembed_complete_full_vs_incumbent_ok():
    cur = _FakeCursor([(200,), (200,)])  # new fully covers incumbent
    n = promote_pe_core.assert_reembed_complete(
        cur, new_model_name="facebook/PE-Core-L14-336@ft-x", incumbent_model_name="facebook/PE-Core-L14-336"
    )
    assert n == 200


def test_flip_pointer_dry_run_no_write():
    cur = _FakeCursor([])
    promote_pe_core.flip_pointer(
        cur, new_model_name="facebook/PE-Core-L14-336@ft-x", scope="frame_search", dry_run=True
    )
    assert cur.executed == []  # dry-run = no UPDATE


def test_flip_pointer_apply_updates_pointer():
    cur = _FakeCursor([])
    promote_pe_core.flip_pointer(
        cur, new_model_name="facebook/PE-Core-L14-336@ft-x", scope="frame_search", dry_run=False
    )
    sql = " ".join(s.lower() for s, _ in cur.executed)
    assert "embedding_active_model" in sql
    assert "model_name" in sql  # UPSERT/UPDATE of the pointer


def test_rollback_pointer_restores_prior():
    cur = _FakeCursor([])
    promote_pe_core.rollback_pointer(
        cur, prior_model_name="facebook/PE-Core-L14-336", scope="frame_search", dry_run=False
    )
    sql = " ".join(s.lower() for s, _ in cur.executed)
    assert "facebook/pe-core-l14-336" in (cur.executed[0][1] or {}).get("model_name", "").lower() \
        or "embedding_active_model" in sql
