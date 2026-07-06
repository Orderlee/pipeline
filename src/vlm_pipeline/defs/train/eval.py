"""Eval + promotion gate asset (MLOps fine-tuning scaffolding, §7.4).

GT-anchored scoring on the SEALED test split:
- SAM3   : box mAP. Prefers sam3.eval.coco_eval (pycocotools, GPU/sam3 box only);
           falls back to lib.box_map otherwise. CI never imports the sam3 path.
- PE-Core: cross-modal recall@k (advisory, small-N holdout, CI-reported).
First run: incumbent = stock model scored on the SAME sealed split, incumbent_source='stock_base'.
Gate: lib.train_eval_gate.evaluate_gate -> status='promotable' when it passes.

Layer L4: thin @asset -> _run_train_eval_gate -> pure lib. GPU scoring is gated and
never executed in CI (monkeypatched _score_* in unit tests).
"""

from __future__ import annotations

from typing import Any

from dagster import Field, asset

from vlm_pipeline.lib.box_map import mean_average_precision
from vlm_pipeline.lib.recall_at_k import bootstrap_ci, cross_modal_recall_at_k
from vlm_pipeline.lib.train_eval_gate import (
    DEFAULT_EVAL_CONFIG,
    PE_CORE_EVAL_CONFIG,
    evaluate_gate,
)
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource


@asset(
    name="train_eval_gate",
    description="Score candidate vs incumbent on sealed test split; set model_registry.status=promotable on pass.",
    group_name="train",
    config_schema={
        "model_version_id": Field(str),
        "eval_config": Field(dict, is_required=False),
    },
)
def train_eval_gate(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict[str, Any]:
    return _run_train_eval_gate(context, db, minio)


def _resolve_eval_config(row: dict[str, Any], op_config: dict[str, Any]) -> dict[str, Any]:
    if op_config.get("eval_config"):
        return dict(op_config["eval_config"])
    if row.get("eval_config"):
        return dict(row["eval_config"])
    return dict(PE_CORE_EVAL_CONFIG if row.get("model") == "pe_core" else DEFAULT_EVAL_CONFIG)


def _score_sam3_predictions(
    per_image_gt: list[dict[str, list[list[float]]]],
    per_image_pred: list[dict[str, list[tuple[list[float], float]]]],
) -> dict[str, Any]:
    """SAM3 box mAP. Uses official sam3.eval.coco_eval if importable, else lib.box_map.

    NOTE: sam3.eval is only present inside docker-sam3-1 (GPU box). CI/dagster image
    has no pycocotools -> ImportError -> lib.box_map fallback. See SPIKE (D8) for the
    CocoEvaluator wiring on the GPU path.
    """
    try:
        from sam3.eval.coco_eval import CocoEvaluator  # noqa: F401  (GPU/sam3 box only)
    except ImportError:
        return mean_average_precision(per_image_gt, per_image_pred, iou_thresholds=(0.5,))
    # GPU path: wired in D8 follow-up using CocoEvaluator; falls through to the pure
    # scorer until that branch is filled (kept fallback to avoid GPU dependency here).
    return mean_average_precision(per_image_gt, per_image_pred, iou_thresholds=(0.5,))


def _score_pe_core_recall(ranked_gt_hits: list[list[bool]]) -> dict[str, Any]:
    recalls = cross_modal_recall_at_k(ranked_gt_hits, ks=(1, 5, 10))
    hits_at_5 = [bool(any(row[:5])) for row in ranked_gt_hits]
    mean, lo, hi = bootstrap_ci(hits_at_5, iterations=1000, seed=0)
    return {
        "recall_at_1": recalls.get(1, 0.0),
        "recall_at_5": recalls.get(5, 0.0),
        "recall_at_10": recalls.get(10, 0.0),
        "recall_at_5_ci": [lo, hi],
        "n_queries": len(ranked_gt_hits),
        "per_class_recall": {},
    }


def _score_candidate(context, db: PostgresResource, minio: MinIOResource, row: dict[str, Any]) -> dict[str, Any]:
    """Run the candidate checkpoint over the sealed test split. GPU-only; stubbed in CI.

    NOTE: real inference runs on the prod GPU box during a controlled window
    (ENABLE_TRAINING / manual). Resolves row['train_dataset_version_id'] -> sealed
    'test' split keys under vlm-dataset/_trainsets/<tdv>/splits/, runs the candidate
    checkpoint (row['checkpoint_key']), and dispatches to _score_sam3_predictions /
    _score_pe_core_recall. Not executed in CI (monkeypatched).
    """
    raise NotImplementedError("GPU candidate scoring runs on prod box; monkeypatched in CI")


def _score_incumbent(
    context, db: PostgresResource, minio: MinIOResource, row: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    """Score the incumbent on the SAME sealed split. Returns (metrics, incumbent_source).

    If a 'promoted' model_registry row exists for this model -> score it ('promoted').
    Else cold-start: score the stock/base model ('stock_base', §7.4 H3). GPU-only;
    monkeypatched in CI.
    """
    raise NotImplementedError("GPU incumbent scoring runs on prod box; monkeypatched in CI")


def _run_train_eval_gate(context, db: PostgresResource, minio: MinIOResource) -> dict[str, Any]:
    model_version_id = str(context.op_config["model_version_id"])
    row = db.get_model_registry_row(model_version_id)
    cfg = _resolve_eval_config(row, context.op_config)

    candidate_metrics = _score_candidate(context, db, minio, row)
    incumbent_metrics, incumbent_source = _score_incumbent(context, db, minio, row)

    decision = evaluate_gate(
        candidate_metrics,
        incumbent_metrics,
        cfg,
        incumbent_source=incumbent_source,
    )
    new_status = "promotable" if decision.promotable else "candidate"
    for reason in decision.reasons:
        context.log.info("train_eval_gate[%s]: %s", model_version_id, reason)

    db.update_model_registry_eval(
        model_version_id,
        metrics=candidate_metrics,
        incumbent_metrics=incumbent_metrics,
        incumbent_source=incumbent_source,
        eval_config=cfg,
        status=new_status,
    )
    return {
        "model_version_id": model_version_id,
        "promotable": decision.promotable,
        "status": new_status,
        "reasons": decision.reasons,
        "per_metric": decision.per_metric,
        "per_class": decision.per_class,
        "incumbent_source": incumbent_source,
    }
