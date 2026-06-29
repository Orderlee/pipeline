"""Group-aware deterministic 3-way dataset splitter.

Layer 1: pure Python, no Dagster / DB / MinIO import. Used by the frozen
snapshot builder (defs/train/dataset.py). Distinct from split_dataset's
`_split_records` (per-record 2-way shuffle) — this one splits by GROUP key so
all frames of one source video land in exactly one of train/val/test, with a
stable pre-sort that makes assignment independent of input row order.
"""

from __future__ import annotations

import random
from collections import OrderedDict
from typing import Callable

SPLIT_NAMES = ("train", "val", "test")


def _normalize_ratios(ratios: dict) -> tuple[float, float, float]:
    """{'train','val','test'} → (t,v,te). Must sum to 1.0 (±1e-6) and each > 0."""
    try:
        t = float(ratios["train"])
        v = float(ratios["val"])
        te = float(ratios["test"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"ratios must have numeric train/val/test: {ratios!r}") from exc
    if min(t, v, te) <= 0:
        raise ValueError(f"all split ratios must be > 0: {ratios!r}")
    if abs((t + v + te) - 1.0) > 1e-6:
        raise ValueError(f"split ratios must sum to 1.0: {ratios!r} -> {t + v + te}")
    return t, v, te


def _split_groups(
    records: list[dict],
    key_fn: Callable[[dict], str],
    ratios: dict,
    seed: int,
) -> dict[str, list[dict]]:
    """Group-aware deterministic 3-way split.

    1. Group records by key_fn(record).
    2. STABLE pre-sort the unique group keys (kills input-order nondeterminism).
    3. Seeded-shuffle the sorted group-key list.
    4. Cut by cumulative ratio into train/val/test buckets of GROUP KEYS.
    5. Expand groups back to records, preserving each group's original record order.

    Guarantees: a group key is assigned to exactly one split; deterministic for
    fixed (group-key set, seed).
    """
    t_ratio, v_ratio, _te_ratio = _normalize_ratios(ratios)

    grouped: "OrderedDict[str, list[dict]]" = OrderedDict()
    for rec in records:
        grouped.setdefault(str(key_fn(rec)), []).append(rec)

    keys = sorted(grouped.keys())  # stable pre-sort
    rng = random.Random(seed)
    rng.shuffle(keys)

    n = len(keys)
    out: dict[str, list[dict]] = {name: [] for name in SPLIT_NAMES}
    if n == 0:
        return out

    n_train = int(n * t_ratio)
    n_val = int(n * v_ratio)
    # guarantee non-empty val/test when there are >= 3 groups
    if n >= 3:
        n_train = min(n_train, n - 2)
        n_train = max(1, n_train)
        n_val = max(1, min(n_val, n - n_train - 1))
    train_keys = keys[:n_train]
    val_keys = keys[n_train : n_train + n_val]
    test_keys = keys[n_train + n_val :]

    for k in train_keys:
        out["train"].extend(grouped[k])
    for k in val_keys:
        out["val"].extend(grouped[k])
    for k in test_keys:
        out["test"].extend(grouped[k])
    return out


def _per_class_floor_ok(
    splits: dict[str, list[dict]],
    class_fn: Callable[[dict], str],
    min_per_split: int,
) -> tuple[bool, dict]:
    """Stratify floor check. Returns (ok, per_class_counts).

    per_class_counts = {class: {"train": n, "val": n, "test": n, "total": n}}.
    ok=False if any observed class has < min_per_split in any split that is
    expected to be populated (i.e. the class exists overall and that split is
    non-empty in the dataset). The builder raises honestly when ok is False.
    """
    classes: set[str] = set()
    for rows in splits.values():
        for r in rows:
            classes.add(str(class_fn(r)))

    counts: dict = {}
    for cls in sorted(classes):
        per = {name: 0 for name in SPLIT_NAMES}
        for name in SPLIT_NAMES:
            per[name] = sum(1 for r in splits.get(name, []) if str(class_fn(r)) == cls)
        per["total"] = sum(per[name] for name in SPLIT_NAMES)
        counts[cls] = per

    ok = True
    for cls, per in counts.items():
        for name in SPLIT_NAMES:
            # only enforce floor on splits that have any rows at all
            if splits.get(name) and per[name] < min_per_split:
                ok = False
    return ok, counts
