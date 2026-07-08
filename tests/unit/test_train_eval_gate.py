"""Promotion gate decision logic — per-metric margin AND per-class non-regression floor.

Pure dicts, no GPU, no PG. stdlib only.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parents[2]
    src = root / "src" / "vlm_pipeline" / "lib" / "train_eval_gate.py"
    spec = importlib.util.spec_from_file_location("vlm_pipeline_lib_train_eval_gate_fresh", src)
    assert spec is not None and spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = m  # register so @dataclass module lookup resolves (not None)
    spec.loader.exec_module(m)
    return m


_g = _load()
evaluate_gate = _g.evaluate_gate
DEFAULT_EVAL_CONFIG = _g.DEFAULT_EVAL_CONFIG
PE_CORE_EVAL_CONFIG = _g.PE_CORE_EVAL_CONFIG

_CFG = {
    "primary_metric": "map",
    "primary_margin": 0.01,
    "per_class_field": "per_class_ap",
    "per_class_floor": -0.02,
    "advisory": False,
}


def test_candidate_beats_incumbent_by_margin_promotable() -> None:
    cand = {"map": 0.50, "per_class_ap": {"fire": 0.48, "smoke": 0.52}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is True
    assert d.per_metric["map"]["delta"] == 0.10
    assert d.per_metric["map"]["passed"] is True


def test_candidate_below_margin_not_promotable() -> None:
    cand = {"map": 0.405, "per_class_ap": {"fire": 0.41, "smoke": 0.40}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is False
    assert d.per_metric["map"]["passed"] is False
    assert any("margin" in r for r in d.reasons)


def test_default_eval_config_has_required_keys() -> None:
    for key in ("primary_metric", "primary_margin", "per_class_field", "per_class_floor"):
        assert key in DEFAULT_EVAL_CONFIG


def test_per_class_regression_vetoes_even_when_primary_passes() -> None:
    # primary mAP improves by 0.05 (passes margin) but 'smoke' collapses -0.10 (< -0.02 floor)
    cand = {"map": 0.45, "per_class_ap": {"fire": 0.60, "smoke": 0.30}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is False
    assert d.per_metric["map"]["passed"] is True  # primary alone would pass
    assert d.per_class["smoke"]["passed"] is False
    assert any("smoke" in r and "regression" in r for r in d.reasons)


def test_per_class_within_floor_does_not_veto() -> None:
    # smoke drops only 0.01 (>= -0.02 floor) -> allowed
    cand = {"map": 0.45, "per_class_ap": {"fire": 0.50, "smoke": 0.39}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40, "smoke": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is True
    assert d.per_class["smoke"]["passed"] is True


def test_stock_base_cold_start_still_requires_margin() -> None:
    # First run: incumbent is stock model scored on same sealed split.
    # Gate is NOT relaxed for cold-start; candidate must beat stock by margin.
    cand = {"map": 0.50, "per_class_ap": {"fire": 0.50}}
    inc = {"map": 0.30, "per_class_ap": {"fire": 0.30}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="stock_base")
    assert d.promotable is True
    assert "stock_base" in d.reasons[0] if d.reasons else True


def test_stock_base_candidate_worse_than_stock_not_promotable() -> None:
    cand = {"map": 0.25, "per_class_ap": {"fire": 0.25}}
    inc = {"map": 0.30, "per_class_ap": {"fire": 0.30}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="stock_base")
    assert d.promotable is False


def test_tie_is_veto() -> None:
    # exact tie -> delta 0.0 < margin 0.01 -> not promotable
    cand = {"map": 0.40, "per_class_ap": {"fire": 0.40}}
    inc = {"map": 0.40, "per_class_ap": {"fire": 0.40}}
    d = evaluate_gate(cand, inc, _CFG, incumbent_source="promoted")
    assert d.promotable is False
    assert d.per_metric["map"]["delta"] == 0.0


def test_advisory_metric_never_vetoes() -> None:
    # PE-Core recall regresses but advisory=True -> promotable stays True, reason logged
    cand = {"recall_at_5": 0.10, "per_class_recall": {"fire": 0.10}}
    inc = {"recall_at_5": 0.50, "per_class_recall": {"fire": 0.50}}
    d = evaluate_gate(cand, inc, PE_CORE_EVAL_CONFIG, incumbent_source="stock_base")
    assert d.promotable is True
    assert any("advisory" in r for r in d.reasons)
