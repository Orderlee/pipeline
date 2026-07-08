"""Promotion gate: candidate vs incumbent metrics -> promotable? (pure decision logic).

Gate = (primary metric beats incumbent by >= primary_margin)  AND
       (no per-class regression below per_class_floor).
Tie (delta == 0) is a VETO (margin is strict >=, and 0 < margin>0).
Cold-start: first run has incumbent_source='stock_base' (stock model scored on the
SAME sealed split by the caller). Margins are applied identically — we do NOT relax
the gate for cold-start; a fine-tune must still beat the stock baseline by margin.
advisory=True: regressions are reported in reasons but NEVER veto (used for PE-Core
cross-modal recall on the small finalized holdout, §7.4).

Layer: L1-2 pure lib. stdlib only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Concrete defaults (spec §11 Q2 was open; these are the committed starting values,
# overridable per-run via model_registry.eval_config). SAM3 detection track.
DEFAULT_EVAL_CONFIG: dict[str, Any] = {
    "primary_metric": "map",
    "primary_margin": 0.01,  # absolute mAP@0.5 improvement required
    "per_class_field": "per_class_ap",
    "per_class_floor": -0.02,  # each class may drop at most 0.02 absolute AP
    "advisory": False,
}

# PE-Core cross-modal recall config — advisory until image_label_annotations backfill
# (small-N holdout -> high variance, see §7.4 / §10).
PE_CORE_EVAL_CONFIG: dict[str, Any] = {
    "primary_metric": "recall_at_5",
    "primary_margin": 0.02,
    "per_class_field": "per_class_recall",
    "per_class_floor": -0.05,
    "advisory": True,
}


@dataclass
class GateDecision:
    promotable: bool
    reasons: list[str] = field(default_factory=list)
    per_metric: dict[str, dict[str, Any]] = field(default_factory=dict)
    per_class: dict[str, dict[str, Any]] = field(default_factory=dict)


def _round(value: float) -> float:
    return round(float(value), 6)


def evaluate_gate(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
    eval_config: dict[str, Any],
    *,
    incumbent_source: str,
) -> GateDecision:
    primary = eval_config["primary_metric"]
    margin = float(eval_config["primary_margin"])
    pc_field = eval_config["per_class_field"]
    floor = float(eval_config["per_class_floor"])
    advisory = bool(eval_config.get("advisory", False))

    reasons: list[str] = []
    cand_primary = float(candidate_metrics.get(primary, 0.0))
    inc_primary = float(incumbent_metrics.get(primary, 0.0))
    delta = _round(cand_primary - inc_primary)
    margin_passed = delta >= margin  # strict: tie (delta=0) fails when margin>0
    per_metric = {
        primary: {
            "candidate": _round(cand_primary),
            "incumbent": _round(inc_primary),
            "delta": delta,
            "margin": margin,
            "passed": margin_passed,
        }
    }
    if not margin_passed:
        reasons.append(
            f"primary metric '{primary}' delta {delta} < required margin {margin} "
            f"(candidate={_round(cand_primary)}, incumbent={_round(inc_primary)}, "
            f"incumbent_source={incumbent_source})"
        )

    cand_pc = dict(candidate_metrics.get(pc_field, {}) or {})
    inc_pc = dict(incumbent_metrics.get(pc_field, {}) or {})
    per_class: dict[str, dict[str, Any]] = {}
    class_regression = False
    for cls in sorted(set(cand_pc) | set(inc_pc)):
        c_val = float(cand_pc.get(cls, 0.0))
        i_val = float(inc_pc.get(cls, 0.0))
        c_delta = _round(c_val - i_val)
        ok = c_delta >= floor
        per_class[cls] = {
            "candidate": _round(c_val),
            "incumbent": _round(i_val),
            "delta": c_delta,
            "floor": floor,
            "passed": ok,
        }
        if not ok:
            class_regression = True
            reasons.append(f"per-class regression: '{cls}' delta {c_delta} < floor {floor}")

    hard_pass = margin_passed and not class_regression
    if advisory:
        # Advisory: report reasons but never veto on this metric track.
        if not hard_pass:
            reasons.append("advisory metric: regressions reported, not vetoing")
        promotable = True
    else:
        promotable = hard_pass

    return GateDecision(
        promotable=promotable,
        reasons=reasons,
        per_metric=per_metric,
        per_class=per_class,
    )
