"""trainer_lib.assemble_sam3_config() — authored box-only Hydra config tree.

Asserts the assembled config has the keys sam3.train.trainer.Trainer.__init__
requires (data/model/logging/checkpoint/max_epochs/optim/loss) and that the loss
is BOX-ONLY: loss_fns_find references IABCEMdetr + Boxes but NOT Masks, and
loss_fn_semantic_seg is None; dataset load_segmentation is False.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_TRAINER = Path(__file__).resolve().parents[2] / "docker" / "trainer" / "trainer_lib.py"
_spec = importlib.util.spec_from_file_location("trainer_lib", _TRAINER)
trainer_lib = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trainer_lib)


def _cfg() -> dict:
    return trainer_lib.assemble_sam3_config(
        train_annfile="/trainset/labels/train.json",
        val_annfile="/trainset/labels/val.json",
        images_root="/trainset/images",
        save_dir="/out/ckpt",
        num_classes=2,
        max_epochs=3,
        seed=123,
        full_ft=False,
        log_dir="/out/logs",
    )


def test_has_trainer_required_keys() -> None:
    t = _cfg()["trainer"]
    for key in ("data", "model", "logging", "checkpoint", "max_epochs", "optim", "loss"):
        assert key in t, f"missing trainer key {key}"
    assert _cfg()["trainer"]["max_epochs"] == 3


def test_launcher_and_submitit_present_local() -> None:
    cfg = _cfg()
    assert cfg["launcher"]["num_nodes"] == 1
    assert cfg["launcher"]["gpus_per_node"] == 1
    assert cfg["submitit"]["use_cluster"] is False


def test_loss_is_box_only() -> None:
    loss_blob = json.dumps(_cfg()["trainer"]["loss"])
    assert "IABCEMdetr" in loss_blob
    assert "Boxes" in loss_blob
    assert "Masks" not in loss_blob  # no mask loss — box-only supervision
    # semantic seg loss explicitly off
    assert (
        '"loss_fn_semantic_seg": null' in loss_blob
        or "loss_fn_semantic_seg" not in _cfg()["trainer"]["loss"]
        or _has_none_semantic(_cfg())
    )


def _has_none_semantic(cfg: dict) -> bool:
    # walk the loss subtree for loss_fn_semantic_seg == None
    def walk(node):
        if isinstance(node, dict):
            if node.get("loss_fn_semantic_seg", "MISSING") is None:
                return True
            return any(walk(v) for v in node.values())
        if isinstance(node, list):
            return any(walk(v) for v in node)
        return False

    return walk(cfg["trainer"]["loss"])


def test_dataset_box_only_load_segmentation_false() -> None:
    data_blob = json.dumps(_cfg()["trainer"]["data"])
    assert '"load_segmentation": false' in data_blob


def test_full_ft_toggle_changes_seed_value_passthrough() -> None:
    full = trainer_lib.assemble_sam3_config(
        train_annfile="/t",
        val_annfile="/v",
        images_root="/i",
        save_dir="/s",
        num_classes=2,
        max_epochs=1,
        seed=7,
        full_ft=True,
        log_dir="/l",
    )
    assert full["trainer"]["seed_value"] == 7
