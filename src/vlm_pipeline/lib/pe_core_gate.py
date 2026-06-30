"""PE-Core 승격 게이트 — GT-abstain 래퍼 (L1 lib, 순수).

design §7.4 / §8.1-D: PE 게이트 메트릭 = 사람-검수 GT 에 대한 cross-modal recall@k.
현재 쿼리가능 GT≈0 → GT count < eval_config['pe_core_min_gt'] 이면 abstain(승격 불가).
GT 가 §12 백필/리뷰로 쌓이기 전까지 PE 승격은 실질 비활성 (메커니즘만 구축).
GT 충분하면 lib.train_eval_gate.evaluate_gate 로 위임 (margin + per-class floor).

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

from typing import Any

from vlm_pipeline.lib.train_eval_gate import GateDecision, evaluate_gate

DEFAULT_PE_CORE_MIN_GT = 50  # spec §15-Q3 N_min 시작값 (eval_config 로 override 가능)


def pe_core_gate_decision(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
    eval_config: dict[str, Any],
    *,
    gt_count: int,
    incumbent_source: str,
) -> GateDecision:
    """GT < pe_core_min_gt 면 abstain(promotable=False). 아니면 evaluate_gate 위임."""
    min_gt = int(eval_config.get("pe_core_min_gt", DEFAULT_PE_CORE_MIN_GT))
    if int(gt_count) < min_gt:
        return GateDecision(
            promotable=False,
            reasons=[
                f"gt_below_min: human-reviewed GT count {gt_count} < pe_core_min_gt {min_gt} "
                f"(abstain — PE 승격은 GT 축적 전까지 비활성, design §7.4/§8.1-D)"
            ],
            per_metric={},
            per_class={},
        )
    return evaluate_gate(
        candidate_metrics, incumbent_metrics, eval_config, incumbent_source=incumbent_source
    )
