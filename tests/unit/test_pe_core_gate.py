"""PE-Core gate: 사람-검수 GT < N_min 이면 abstain (승격 불가). design §7.4 / §8.1-D."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "pe_core_gate.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_pe_core_gate_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_g = _load()
pe_core_gate_decision = _g.pe_core_gate_decision
DEFAULT_PE_CORE_MIN_GT = _g.DEFAULT_PE_CORE_MIN_GT

_CFG = {
    "primary_metric": "recall_at_5",
    "primary_margin": 0.02,
    "per_class_field": "per_class_recall",
    "per_class_floor": -0.05,
    "advisory": False,            # gate hard so the abstain path is the only thing relaxing it
    "pe_core_min_gt": DEFAULT_PE_CORE_MIN_GT,
}


def test_abstains_when_gt_below_min() -> None:
    cand = {"recall_at_5": 0.9, "per_class_recall": {"fire": 0.9}}
    inc = {"recall_at_5": 0.1, "per_class_recall": {"fire": 0.1}}  # candidate would crush incumbent
    d = pe_core_gate_decision(cand, inc, _CFG, gt_count=10, incumbent_source="stock_base")
    assert d.promotable is False
    assert any("gt_below_min" in r for r in d.reasons)


def test_evaluates_normally_when_gt_sufficient() -> None:
    cand = {"recall_at_5": 0.9, "per_class_recall": {"fire": 0.9}}
    inc = {"recall_at_5": 0.1, "per_class_recall": {"fire": 0.1}}
    d = pe_core_gate_decision(cand, inc, _CFG, gt_count=200, incumbent_source="stock_base")
    assert d.promotable is True


def test_gt_exactly_min_is_sufficient() -> None:
    cand = {"recall_at_5": 0.9, "per_class_recall": {"fire": 0.9}}
    inc = {"recall_at_5": 0.1, "per_class_recall": {"fire": 0.1}}
    d = pe_core_gate_decision(cand, inc, _CFG, gt_count=DEFAULT_PE_CORE_MIN_GT, incumbent_source="stock_base")
    assert d.promotable is True


def test_default_min_gt_is_committed_value() -> None:
    assert DEFAULT_PE_CORE_MIN_GT == 50
