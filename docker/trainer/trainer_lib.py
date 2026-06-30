"""vlm-trainer pure logic (no torch/sam3/hydra import at module top).

Importable by file path in CI tests (this dir is NOT a vlm_pipeline package).
GPU-touching code lives in entrypoint.py and is gated by ENABLE_TRAINING.

Sections:
  - build_sam3_coco_annfile : frozen-trainset COCO -> per-split sam3 annFile.
  - assemble_sam3_config    : authored Hydra OmegaConf tree (box-only loss).
  - merge_lora_to_full      : LoRA adapter -> merged full-weight state_dict.
"""

from __future__ import annotations

import json
from typing import Any


def build_sam3_coco_annfile(coco: dict[str, Any], *, split_image_ids: list[int]) -> dict[str, Any]:
    """Filter a frozen-trainset COCO dict to one split's images.

    - Keeps only images whose id is in ``split_image_ids``, ordered by id ascending.
    - Keeps only annotations whose image_id is in the kept set.
    - Preserves ALL categories unchanged (sam3 COCO_FROM_JSON emits one text-query
      per category; dropping unused categories would shrink the query space).
    - Boxes are left as absolute COCO xywh — sam3's loader normalizes by width/height.
    """
    keep = set(int(i) for i in split_image_ids)
    images = sorted(
        (img for img in coco.get("images", []) if int(img["id"]) in keep),
        key=lambda img: int(img["id"]),
    )
    kept_ids = {int(img["id"]) for img in images}
    annotations = [a for a in coco.get("annotations", []) if int(a["image_id"]) in kept_ids]
    return {
        "images": images,
        "annotations": annotations,
        "categories": list(coco.get("categories", [])),
    }


def assemble_sam3_config(
    *,
    train_annfile: str,
    val_annfile: str,
    images_root: str,
    save_dir: str,
    num_classes: int,
    max_epochs: int,
    seed: int,
    full_ft: bool,
    log_dir: str,
) -> dict[str, Any]:
    """Author the Hydra/OmegaConf source dict for a BOX-ONLY SAM3 detection finetune.

    Box-only: loss_fns_find = [IABCEMdetr (cls+IoU-aware BCE), Boxes (l1+giou)],
    NO Masks loss, loss_fn_semantic_seg=None; dataset load_segmentation=False so
    no mask GT is required (spike-confirmed in sam3.train.{loss,data}).

    ``full_ft`` selects param-group scope downstream (LoRA targeting happens in
    entrypoint after model build, not in config); it only flips grad-checkpoint here.
    """
    dataset = {
        # sam3.train.data.sam3_image_dataset.CustomCocoDetectionAPI (spike-confirmed)
        "_target_": "sam3.train.data.sam3_image_dataset.CustomCocoDetectionAPI",
        "root": images_root,
        "annFile": train_annfile,
        "load_segmentation": False,  # box-only — no mask GT
    }
    val_dataset = dict(dataset, annFile=val_annfile)
    loss = {
        "default": {
            # sam3.train.loss.sam3_loss.Sam3LossWrapper (spike-confirmed)
            "_target_": "sam3.train.loss.sam3_loss.Sam3LossWrapper",
            "loss_fn_semantic_seg": None,  # semantic seg OFF
            "loss_fns_find": [
                {
                    # sam3.train.loss.loss_fns.IABCEMdetr (spike-confirmed)
                    "_target_": "sam3.train.loss.loss_fns.IABCEMdetr",
                    "weight_dict": {"loss_ce": 1.0},
                },
                {
                    # sam3.train.loss.loss_fns.Boxes (spike-confirmed)
                    "_target_": "sam3.train.loss.loss_fns.Boxes",
                    "weight_dict": {"loss_bbox": 5.0, "loss_giou": 2.0},
                },
            ],
            # sam3.train.matcher.HungarianMatcher (spike-confirmed)
            "matcher": {"_target_": "sam3.train.matcher.HungarianMatcher"},
        }
    }
    model = {
        # sam3.model_builder.build_sam3_image_model (returns nn.Module, spike-confirmed)
        "_target_": "sam3.model_builder.build_sam3_image_model",
        "device": "cuda",
        "load_from_HF": True,
    }
    return {
        "launcher": {"num_nodes": 1, "gpus_per_node": 1, "experiment_log_dir": log_dir},
        "submitit": {"use_cluster": False, "port_range": [10000, 10100]},
        "trainer": {
            "_target_": "sam3.train.trainer.Trainer",
            "max_epochs": max_epochs,
            "seed_value": seed,
            "accelerator": "cuda",
            "data": {"train": dataset, "val": val_dataset},
            "model": model,
            "loss": loss,
            "logging": {"log_dir": log_dir, "log_freq": 10},
            "checkpoint": {"save_dir": save_dir, "save_freq": 1},
            "cuda": {"cudnn_deterministic": True, "cudnn_benchmark": False},
            "optim": {
                "optimizer": {"_target_": "torch.optim.AdamW", "lr": 1e-4},
                "amp": {"enabled": True, "amp_dtype": "bfloat16"},
                "gradient_accumulation_steps": 1,
            },
            "gradient_accumulation_steps": (1 if full_ft else 1),
        },
    }


def assert_trainable_params(model: Any) -> int:
    """Count trainable params; raise ValueError if zero.

    Guards the classic PEFT footgun: a target_modules regex that matched nothing
    injects an adapter with 0 trainable params and trains noise. Call right after
    get_peft_model(), before the optimizer is built.
    """
    n = sum(int(p.numel()) for p in model.parameters() if p.requires_grad)
    if n <= 0:
        raise ValueError(
            "no trainable parameters after adapter injection — "
            "target_modules matched nothing (check named_modules())"
        )
    return n


def merge_lora_to_full(peft_model: Any) -> Any:
    """Merge LoRA adapters into the base weights and return a base-shaped state_dict.

    Serving (sam3 build_sam3_image_model / open_clip create_model_and_transforms)
    loads FULL weights only — it has no PEFT loader. So we merge_and_unload() and
    strip any residual 'base_model.model.' / wrapper prefixes so the result drops
    straight into the stock serving module via load_state_dict.
    """
    from collections import OrderedDict

    merged = peft_model.merge_and_unload()
    raw = merged.state_dict()
    out: "OrderedDict[str, Any]" = OrderedDict()
    for key, tensor in raw.items():
        clean = key
        for prefix in ("base_model.model.", "base_model.", "_orig_mod."):
            if clean.startswith(prefix):
                clean = clean[len(prefix):]
        out[clean] = tensor
    return out


REQUIRED_SECRETS = ("HF_TOKEN", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")


def preflight_secrets(env: dict[str, str]) -> None:
    """Fail-fast if any required secret is missing/empty. Reports ALL at once.

    embedding-service never needed a runtime HF_TOKEN, so prod .env may lack it —
    the trainer pulls gated weights and MUST have it. Run before any GPU/network work.
    """
    missing = [name for name in REQUIRED_SECRETS if not str(env.get(name, "")).strip()]
    if missing:
        raise RuntimeError(
            "trainer preflight failed — missing/empty required env: " + ", ".join(missing)
        )


def format_train_log_line(*, step: int, loss: float, lr: float, throughput: float) -> str:
    """One compact JSON line for stdout + _models/<ver>/train_log.jsonl."""
    return json.dumps(
        {"step": int(step), "loss": float(loss), "lr": float(lr), "throughput": float(throughput)}
    ) + "\n"


def load_training_summary(path: str) -> dict:
    """Read the trainer's training_summary.json ({hparams, dataset, metrics, artifact_paths})."""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def record_mlflow_run_id(path: str, run_id: str | None) -> None:
    """Merge the MLflow run_id back into training_summary.json (None = no run; registry is SoT)."""
    summary = load_training_summary(path)
    summary["mlflow_run_id"] = run_id
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh)
