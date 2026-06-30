"""Cross-modal recall@k + bootstrap CI for PE-Core eval (advisory, small-N holdout).

`ranked_gt_hits[i]` is a per-text-query boolean list: position j is True iff the
ground-truth image for query i is the j-th ranked image (descending cosine).
recall@k = fraction of queries whose gt image appears within the top-k ranks.

Layer: L1-2 pure lib. numpy only.
"""

from __future__ import annotations

import numpy as np


def cross_modal_recall_at_k(
    ranked_gt_hits: list[list[bool]],
    ks: tuple[int, ...] = (1, 5, 10),
) -> dict[int, float]:
    if not ranked_gt_hits:
        return {int(k): 0.0 for k in ks}
    n = len(ranked_gt_hits)
    out: dict[int, float] = {}
    for k in ks:
        hits = sum(1 for row in ranked_gt_hits if any(row[: int(k)]))
        out[int(k)] = round(hits / n, 6)
    return out


def bootstrap_ci(
    per_query_hits: list[bool],
    iterations: int = 1000,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple[float, float, float]:
    """Percentile bootstrap CI of mean(per_query_hits). Returns (mean, lo, hi)."""
    if not per_query_hits:
        return (0.0, 0.0, 0.0)
    arr = np.asarray([1.0 if h else 0.0 for h in per_query_hits], dtype=np.float64)
    rng = np.random.default_rng(seed)
    n = arr.size
    means = np.empty(iterations, dtype=np.float64)
    for i in range(iterations):
        sample = arr[rng.integers(0, n, size=n)]
        means[i] = sample.mean()
    lo = float(np.percentile(means, 100.0 * (alpha / 2.0)))
    hi = float(np.percentile(means, 100.0 * (1.0 - alpha / 2.0)))
    return (round(float(arr.mean()), 6), round(lo, 6), round(hi, 6))
