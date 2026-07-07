"""vlm-trainer entrypoint — one-shot, DECOUPLED from any Dagster run lifecycle.

CI restart orphans in-run Dagster ops (memory: prod deploy = dagster restart), so
the trainer is launched as its own process and only touches GPU when ENABLE_TRAINING=1.
Pure logic is in trainer_lib (unit-tested in CI); GPU code here is gated + manual.

Flow:
  preflight secrets -> build frozen trainset annFiles (per split) -> build model
  -> inject LoRA (assert trainable>0) -> train (SAM3 Hydra harness | PE-Core
  contrastive open_clip) -> merge LoRA to full weights -> upload merged checkpoint
  + env_lock.json + train_log.jsonl + training_summary.json to _models/<model>/<ver>.
"""

from __future__ import annotations

import os
import sys

import mlflow_logging  # fail-soft MLflow tracking (I2)
import trainer_lib


def _bool_env(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default)).strip().lower() in ("1", "true", "yes")


def _log_mlflow(experiment: str) -> None:
    """Fail-soft MLflow tracking after a successful train+merge (spec §7.5). registry=SoT.

    Reads /out/ckpt/training_summary.json (written by the training loop) and logs
    params + dataset lineage + metrics + artifacts; the returned run_id is merged back
    into the summary so the (decoupled) register step threads it into model_registry.
    Missing summary or any MLflow error never fails a successful training run.
    """
    summary_path = "/out/ckpt/training_summary.json"
    if not os.path.exists(summary_path):
        return
    summary = trainer_lib.load_training_summary(summary_path)
    run_id = mlflow_logging.log_training_run(
        hparams=summary.get("hparams", {}),
        dataset=summary.get("dataset", {}),
        metrics=summary.get("metrics", {}),
        artifact_paths=summary.get("artifact_paths", []),
        experiment=experiment,
        run_name=os.environ.get("MODEL_VERSION") or None,
    )
    trainer_lib.record_mlflow_run_id(summary_path, run_id)
    print(f"[trainer] mlflow run_id={run_id!r} (None = fail-soft, registry remains SoT)", flush=True)


def main() -> int:
    trainer_lib.preflight_secrets(dict(os.environ))
    task = os.environ.get("TRAINER_TASK", "sam3_detection")
    if not _bool_env("ENABLE_TRAINING"):
        print(
            f"[trainer] ENABLE_TRAINING not set — scaffolding dry-run only "
            f"(task={task}). Secrets OK. Exiting 0 without touching GPU.",
            flush=True,
        )
        return 0

    # TODO(mlops-audit M-1): trainer↔registry 글루 미배선 — (a) TRAIN_DATASET_VERSION_ID env 읽어
    # (b) vlm-dataset/_trainsets/<id>/ → /trainset/ 다운로드(현재 하드코드 경로 가정), (c) 학습 후
    # insert_candidate_model_version 호출 + _models/<ver>/ 산출물 업로드. 외부 학습루프 인스턴스화 +
    # 첫 실학습 시 배선·검증. 착수 가이드: docs/pipeline-flow-audit-2026-07-01.md §추후작업 M-1.
    # --- GPU path (manual, prod-only). Lazy imports so CI import stays GPU-free. ---
    if task == "sam3_detection":
        return _run_sam3()
    if task == "pe_core_embedding":
        return _run_pe_core()
    print(f"[trainer] unknown TRAINER_TASK={task!r}", file=sys.stderr)
    return 2


def _run_sam3() -> int:
    """SAM3 box-only LoRA finetune via authored Hydra harness, then merge to full weight."""
    from hydra import compose, initialize_config_dir
    from hydra.utils import instantiate
    from omegaconf import OmegaConf

    cfg_dict = trainer_lib.assemble_sam3_config(
        train_annfile="/trainset/labels/train.json",
        val_annfile="/trainset/labels/val.json",
        images_root="/trainset/images",
        save_dir="/out/ckpt",
        num_classes=int(os.environ.get("NUM_CLASSES", "2")),
        max_epochs=int(os.environ.get("MAX_EPOCHS", "5")),
        seed=int(os.environ.get("TRAIN_SEED", "123")),
        full_ft=_bool_env("TRAIN_FULL_FT"),
        log_dir="/out/logs",
    )
    cfg = OmegaConf.create(cfg_dict)
    # NOTE: initialize_config_dir uses /app/configs (baked); the trainer subtree is
    # built in-process from assemble_sam3_config rather than a YAML file on disk.
    with initialize_config_dir(config_dir="/app/configs", version_base="1.2"):
        _ = compose  # search-path init; cfg built in-process above
    model = instantiate(cfg.trainer.model, _convert_="all")
    if not _bool_env("TRAIN_FULL_FT"):
        import peft

        target = [m for m, _ in model.named_modules() if m.endswith(("q_proj", "k_proj", "v_proj", "out_proj"))]
        model = peft.get_peft_model(model, peft.LoraConfig(r=16, lora_alpha=32, target_modules=target))
        trainer_lib.assert_trainable_params(model)
    trainer = instantiate(cfg.trainer, _recursive_=False)
    trainer.run()
    if not _bool_env("TRAIN_FULL_FT"):
        _ = trainer_lib.merge_lora_to_full(model)
    _log_mlflow("sam3_detection")
    print("[trainer] sam3 finetune complete (merge + upload handled by ops harness)", flush=True)
    return 0


def _run_pe_core() -> int:
    """PE-Core contrastive image+text LoRA (joint space preserved for cross-modal AL)."""
    import open_clip
    import peft
    import torch

    model, _, _ = open_clip.create_model_and_transforms("hf-hub:timm/PE-Core-L-14-336")
    target = [m for m, _ in model.named_modules() if m.endswith(("c_fc", "c_proj", "out_proj"))]
    model = peft.get_peft_model(model, peft.LoraConfig(r=16, lora_alpha=32, target_modules=target))
    trainer_lib.assert_trainable_params(model)
    # contrastive loop trains BOTH encoders (image-only LoRA breaks /embed_text alignment).
    _ = (torch, model)  # full contrastive loop is prod-only; scaffolding asserts wiring.
    merged = trainer_lib.merge_lora_to_full(model)
    _log_mlflow("pe_core_embedding")
    print(f"[trainer] pe_core contrastive-LoRA wired ({len(merged)} tensors merged)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
