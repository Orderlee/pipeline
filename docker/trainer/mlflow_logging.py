"""Fail-soft MLflow logging for the vlm-trainer (spec §7.5).

MLflow = human tracking/compare UI; model_registry(PG) = promotion source of truth.
This logger is BEST-EFFORT: any MLflow error (server unreachable, client absent,
auth) is swallowed with a warning and log_training_run returns None — training and
registry writes (the SoT) proceed regardless.

Importable by file path in CI tests (docker/trainer is NOT a vlm_pipeline package);
`import mlflow` is deferred into the try so CI never needs the mlflow client.
"""
from __future__ import annotations

import os
from typing import Any

# Dataset-lineage fields mirrored from train_dataset_versions (design §5.1). Logged as
# MLflow params/tags so a run page shows EXACTLY which frozen, content-checksummed
# snapshot — and which DVC curation version (spec §7.6 step 6) — trained the model
# (end-to-end lineage; the manifest holds per-object checksums).
_LINEAGE_FIELDS = (
    "train_dataset_version_id",
    "content_checksum",
    "manifest_key",
    "ls_count",
    "al_confirmed_count",
    "per_class_counts",
    "split_ratios",
    # TODO(mlops-audit H-2): 아래 dvc_* 매핑은 구현됨이나 생산자 없음 — 아무 코드도
    # training_summary["dataset"] 에 이 4키를 안 채운다. M-1(register 단계) 완료 후, freeze/register 가
    # train_dataset_versions.dataset_catalog_id → dataset_catalog(git_rev/commit_subject/dvc_md5) 해석해
    # 채우도록 배선 + DVC-sourced 학습으로 검증. docs/pipeline-flow-audit-2026-07-01.md §추후작업 H-2.
    "dvc_catalog_id",       # J8: which dataset_catalog row (DVC version) sourced this trainset
    "dvc_git_rev",          # J8: the data-repo commit
    "dvc_commit_subject",   # J8: human-readable curation intent
    "dvc_md5",              # J8: DVC out md5 (byte-identity of the curation dataset)
)


def _coerce(value: Any) -> Any:
    """MLflow params must be scalars/strings; stringify dict/list lineage values."""
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, sort_keys=True)
    return value


def _build_run(
    *,
    hparams: dict[str, Any],
    dataset: dict[str, Any],
    metrics: dict[str, Any],
    artifact_paths: list[str],
    experiment: str,
    run_name: str | None,
    tracking_uri: str | None,
) -> str | None:
    import mlflow  # deferred: fail-soft if the client is absent

    uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI")
    if uri:
        mlflow.set_tracking_uri(uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=run_name) as run:
        # 1) hyperparameters (lr, epochs, batch, LoRA rank/alpha, seed, train_method, base model…)
        mlflow.log_params({k: _coerce(v) for k, v in hparams.items()})

        # 2) dataset lineage — WHICH frozen snapshot trained this model.
        lineage = {f: _coerce(dataset[f]) for f in _LINEAGE_FIELDS if f in dataset}
        mlflow.log_params(lineage)
        # Tags too, so lineage is filterable in the compare UI even if a param key collides.
        try:
            mlflow.set_tags({f"ds.{k}": v for k, v in lineage.items()})
        except Exception:  # noqa: BLE001 — tags are nice-to-have, never fatal
            pass
        # MLflow Dataset object (best-effort; older clients lack mlflow.data/log_input).
        try:
            ds_obj = mlflow.data.from_dict(
                {k: dataset.get(k) for k in _LINEAGE_FIELDS},
                name=dataset.get("train_dataset_version_id", "trainset"),
            )
            mlflow.log_input(ds_obj)
        except Exception:  # noqa: BLE001
            pass

        # 3) eval metrics (SAM box mAP / PE recall@k, per-class).
        if metrics:
            mlflow.log_metrics({k: float(v) for k, v in metrics.items()})

        # 4) artifacts already written to _models/<ver>/ (env_lock, train_log, summary).
        for path in artifact_paths:
            if path and os.path.exists(path):
                mlflow.log_artifact(path)

        return run.info.run_id


def log_training_run(
    *,
    hparams: dict[str, Any],
    dataset: dict[str, Any],
    metrics: dict[str, Any],
    artifact_paths: list[str],
    experiment: str,
    run_name: str | None = None,
    tracking_uri: str | None = None,
) -> str | None:
    """Log one training run to MLflow. FAIL-SOFT: returns run_id, or None on ANY error.

    The returned run_id is meant to be stored in model_registry.mlflow_run_id (R1/I3)
    as a cross-link; a None just means "no MLflow run" — registry remains the SoT.
    """
    try:
        return _build_run(
            hparams=hparams,
            dataset=dataset,
            metrics=metrics,
            artifact_paths=artifact_paths,
            experiment=experiment,
            run_name=run_name,
            tracking_uri=tracking_uri,
        )
    except Exception as exc:  # noqa: BLE001 — tracking must never kill training
        print(f"[trainer][mlflow] logging failed (fail-soft, continuing): {exc!r}", flush=True)
        return None
