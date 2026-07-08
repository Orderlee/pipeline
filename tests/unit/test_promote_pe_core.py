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
    assert (
        "facebook/pe-core-l14-336" in (cur.executed[0][1] or {}).get("model_name", "").lower()
        or "embedding_active_model" in sql
    )


# ── C-2: main() 배선 검증 (이전엔 --apply 도 no-op 이었음 → 이제 실제 함수 호출) ──


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur
        self.autocommit = None
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        cur = self._cur

        class _Ctx:
            def __enter__(self_inner):
                return cur

            def __exit__(self_inner, *a):
                return False

        return _Ctx()

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def test_model_name_from_row_empty_version_raises():
    with pytest.raises(promote_pe_core.PromotionError):
        promote_pe_core._model_name_from_row({"model_version_id": "mv-1", "version": ""})


def test_model_name_from_row_builds_versioned_when_no_override():
    from vlm_pipeline.lib.embedding_model_name import build_versioned_model_name

    assert promote_pe_core._model_name_from_row({"version": "ft-001"}) == build_versioned_model_name("ft-001")


def test_model_name_from_row_override_mismatch_raises():
    with pytest.raises(promote_pe_core.PromotionError):
        promote_pe_core._model_name_from_row({"version": "ft-001"}, override="facebook/PE-Core-L14-336@ft-999")


def test_model_name_from_row_override_match_passes_through():
    from vlm_pipeline.lib.embedding_model_name import build_versioned_model_name

    name = build_versioned_model_name("ft-001")
    assert promote_pe_core._model_name_from_row({"version": "ft-001"}, override=name) == name


def test_main_no_dsn_returns_2_loud(monkeypatch):
    monkeypatch.delenv("DATAOPS_POSTGRES_DSN", raising=False)
    assert promote_pe_core.main(["--model-version-id", "mv-1"]) == 2


def test_main_rollback_combo_rejected(monkeypatch):
    monkeypatch.setenv("DATAOPS_POSTGRES_DSN", "postgresql://x")
    assert promote_pe_core.main(["--rollback-to", "m@ft-1", "--model-version-id", "mv-1"]) == 2


def test_main_dry_run_calls_real_functions_not_noop(monkeypatch):
    """C-2 회귀: dry-run 이 select_promotable_row + assert_reembed + flip_pointer(dry) 를 실제 호출하고
    commit 없이 rollback 후 0 반환 — 이전 no-op(무조건 return 0, 아무것도 안 함)이면 이 assert 들이 실패."""
    cur = _FakeCursor([(100,), (100,)])  # assert_reembed: new count, incumbent count
    conn = _FakeConn(cur)
    monkeypatch.setattr(
        promote_pe_core,
        "select_promotable_row",
        lambda c, *, model, model_version_id: {
            "model_version_id": "mv-1",
            "version": "ft-001",
            "checkpoint_key": "k",
            "artifact_checksum": "s",
        },
    )

    class _FakeDB:
        def get_active_embedding_model(self, *, scope):
            return "facebook/PE-Core-L14-336@ft-000"

    monkeypatch.setattr(promote_pe_core, "_make_pe_resource", lambda dsn: _FakeDB())
    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda dsn: conn)

    rc = promote_pe_core.main(["--model-version-id", "mv-1", "--dsn", "postgresql://x"])
    assert rc == 0
    assert conn.rolled_back is True and conn.committed is False  # dry-run: no DB write
    assert any("model_name" in (s or "").lower() for s, _ in cur.executed)  # assert_reembed ran (not no-op)
