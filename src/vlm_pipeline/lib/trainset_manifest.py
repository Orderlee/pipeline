"""Frozen-trainset manifest + content checksum + COCO->YOLO bridge.

Layer 1: pure Python. The content_checksum feeds the UNIQUE(task, checksum)
idempotency gate on train_dataset_versions (design §7.2 H6). COCO->YOLO math is
NOT reinvented — it delegates to split_dataset/split_dataset.py's
`_coco_bbox_to_yolo`, loaded by file path because split_dataset/ is not an
installed package.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from vlm_pipeline.lib.checksum import sha256_bytes

# repo_root/split_dataset/split_dataset.py — lib is src/vlm_pipeline/lib/, so
# parents[3] == repo root.
_SPLIT_DATASET_PY = Path(__file__).resolve().parents[3] / "split_dataset" / "split_dataset.py"


def _load_coco_bridge():
    spec = importlib.util.spec_from_file_location("_split_dataset_bridge", _SPLIT_DATASET_PY)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load split_dataset bridge from {_SPLIT_DATASET_PY}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_BRIDGE = None


def coco_bbox_to_yolo(bbox, img_w: int, img_h: int):
    """COCO [x,y,w,h] px -> YOLO (xc,yc,w,h) normalized. Delegates to split_dataset."""
    global _BRIDGE
    if _BRIDGE is None:
        _BRIDGE = _load_coco_bridge()
    return _BRIDGE._coco_bbox_to_yolo(bbox, img_w, img_h)


def build_manifest(objects: list[tuple[str, str]]) -> dict:
    """objects = [(object_key, per_object_sha256), ...] -> sorted manifest dict."""
    items = sorted(((str(k), str(c)) for k, c in objects), key=lambda x: x[0])
    return {
        "objects": [{"key": k, "checksum": c} for k, c in items],
        "count": len(items),
    }


def content_checksum(
    task: str,
    manifest: dict,
    class_map: dict,
    split_assignment: dict,
    seed: int,
) -> str:
    """Canonical-JSON sha256 over (task, sorted manifest, class_map, split, seed)."""
    canonical = json.dumps(
        {
            "task": str(task),
            "manifest": manifest,
            "class_map": class_map,
            "split_assignment": split_assignment,
            "seed": int(seed),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return sha256_bytes(canonical.encode("utf-8"))
