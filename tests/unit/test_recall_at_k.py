"""Cross-modal recall@k for PE-Core text->image holdout (advisory metric). numpy only."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "recall_at_k.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_recall_at_k_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_r = _load()
cross_modal_recall_at_k = _r.cross_modal_recall_at_k
bootstrap_ci = _r.bootstrap_ci


def test_recall_all_hit_at_1() -> None:
    # 2 queries, gt image is rank-0 for both
    rows = [[True, False, False], [True, False, False]]
    out = cross_modal_recall_at_k(rows, ks=(1, 3))
    assert out[1] == 1.0
    assert out[3] == 1.0


def test_recall_partial() -> None:
    # q0: gt at rank0 (hit@1). q1: gt at rank2 (miss@1, hit@3)
    rows = [[True, False, False], [False, False, True]]
    out = cross_modal_recall_at_k(rows, ks=(1, 3))
    assert out[1] == 0.5
    assert out[3] == 1.0


def test_recall_empty_returns_zero() -> None:
    out = cross_modal_recall_at_k([], ks=(1, 5))
    assert out == {1: 0.0, 5: 0.0}


def test_bootstrap_ci_all_hits() -> None:
    mean, lo, hi = bootstrap_ci([True, True, True, True], iterations=500, seed=0)
    assert mean == 1.0
    assert lo == 1.0 and hi == 1.0


def test_bootstrap_ci_is_deterministic_with_seed() -> None:
    hits = [True, False, True, False, True, False]
    a = bootstrap_ci(hits, iterations=500, seed=7)
    b = bootstrap_ci(hits, iterations=500, seed=7)
    assert a == b
    assert a[1] <= a[0] <= a[2]  # lo <= mean <= hi


def test_bootstrap_ci_empty() -> None:
    assert bootstrap_ci([], iterations=10, seed=0) == (0.0, 0.0, 0.0)
