#!/usr/bin/env python3
"""§4 트레이드오프 재계산 — 합본/클래스수입 시뮬레이션(결정적) + 매칭카운트 팩토리얼(10 seeds).

두 산출물(`geometry/factorial.json`, 보고서 §4 의 합본표)은 원래 애드혹으로 만들어져
파이프라인 스테이지가 없다. GT 가 바뀌면 되살릴 방법이 없으므로 여기에 고정한다.
`prompt_geometry.py` 의 load_all/class_sims 를 그대로 재사용해 채점 규칙 일치를 보장한다.
"""
from __future__ import annotations

import json
import sys

import numpy as np

sys.path.insert(0, "/workspace")
from prompt_geometry import (  # noqa: E402
    CLASS_NAMES,
    GEO,
    VERSIONS,
    class_sims,
    load_all,
)

SEEDS = 10
CLASSES = sorted(CLASS_NAMES)


def score(per_class_best: dict[int, np.ndarray], gt: np.ndarray) -> dict:
    """클래스별 최고 코사인 → argmax → micro/macro/per-class recall."""
    stacked = np.stack([per_class_best[c] for c in CLASSES], axis=1)
    pred = np.array(CLASSES)[stacked.argmax(axis=1)]
    per = {CLASS_NAMES[c]: float((pred[gt == c] == c).mean()) for c in CLASSES}
    return {"micro": float((pred == gt).mean()),
            "macro": float(sum(per.values()) / len(per)),
            "per_class": per}


def main() -> None:
    keys, X, gt, src, banks = load_all()
    sims = {v: class_sims(X, banks[v]) for v in VERSIONS}   # {ver: {cls: [N, n_c]}}
    V0, V4 = VERSIONS
    best = {v: {c: sims[v][c].max(axis=1) for c in CLASSES} for v in VERSIONS}
    out = {"n_frames": len(keys), "gt_dist": {CLASS_NAMES[c]: int((gt == c).sum()) for c in CLASSES}}

    # ── 1) 합본 / 클래스 단위 수입 (결정적: 두 뱅크 문장을 합치고 max) ──
    def merged(import_classes: set[int]) -> dict:
        """v084 기본 + import_classes 는 v080 문장을 함께 얹는다(합집합)."""
        return score({c: (np.maximum(best[V4][c], best[V0][c]) if c in import_classes
                          else best[V4][c]) for c in CLASSES}, gt)

    out["merge"] = {
        V0: score(best[V0], gt),
        V4: score(best[V4], gt),
        "all_merged": merged(set(CLASSES)),
        **{f"import_{CLASS_NAMES[c]}": merged({c}) for c in CLASSES},
    }

    # ── 2) 매칭 카운트 팩토리얼: 클래스별 개수를 min 으로 고정, 소스만 2^4 전환 ──
    sizes = {c: min(int((banks[v]["cls"] == c).sum()) for v in VERSIONS) for c in CLASSES}
    out["matched_sizes"] = {CLASS_NAMES[c]: sizes[c] for c in CLASSES}
    combos = {}
    for mask in range(16):
        src_of = {c: (V4 if mask >> i & 1 else V0) for i, c in enumerate(CLASSES)}
        runs = []
        for seed in range(SEEDS):
            rng = np.random.default_rng(1000 + seed)
            pcb = {}
            for c in CLASSES:
                S = sims[src_of[c]][c]
                pick = rng.choice(S.shape[1], size=sizes[c], replace=False)
                pcb[c] = S[:, pick].max(axis=1)
            runs.append(score(pcb, gt))
        name = "".join("4" if src_of[c] == V4 else "N" for c in CLASSES)
        combos[name] = {
            "micro": float(np.mean([r["micro"] for r in runs])),
            "micro_std": float(np.std([r["micro"] for r in runs])),
            "per_class": {CLASS_NAMES[c]: float(np.mean([r["per_class"][CLASS_NAMES[c]] for r in runs]))
                          for c in CLASSES},
        }
    out["factorial"] = combos

    # 주효과 = 해당 클래스 소스가 v084 인 8조합 평균 − v080 인 8조합 평균
    main_eff = {}
    for i, c in enumerate(CLASSES):
        hi = [v for k, v in combos.items() if k[i] == "4"]
        lo = [v for k, v in combos.items() if k[i] == "N"]
        m = lambda rs, f: float(np.mean([f(r) for r in rs]))  # noqa: E731
        main_eff[CLASS_NAMES[c]] = {
            "micro_delta": m(hi, lambda r: r["micro"]) - m(lo, lambda r: r["micro"]),
            "self_recall_delta": (m(hi, lambda r: r["per_class"][CLASS_NAMES[c]])
                                  - m(lo, lambda r: r["per_class"][CLASS_NAMES[c]])),
            "other_recall_delta": {CLASS_NAMES[o]: (m(hi, lambda r: r["per_class"][CLASS_NAMES[o]])
                                                   - m(lo, lambda r: r["per_class"][CLASS_NAMES[o]]))
                                   for o in CLASSES if o != c},
        }
    out["main_effects"] = main_eff

    with open(f"{GEO}/factorial.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    print(f"프레임 {out['n_frames']} / GT {out['gt_dist']}")
    print(f"matched sizes {out['matched_sizes']}")
    print("\n[합본·수입 시뮬레이션]  micro / macro / fire / smoke / normal / falldown")
    for k, r in out["merge"].items():
        p = r["per_class"]
        print(f"  {k:20s} {r['micro']:6.1%} {r['macro']:6.1%} "
              f"{p['fire']:6.1%} {p['smoke']:6.1%} {p['normal']:6.1%} {p['falldown']:6.1%}")
    print("\n[팩토리얼 코너]")
    for k in ("NNNN", "4444"):
        print(f"  {k} micro {combos[k]['micro']:.2%} ± {combos[k]['micro_std']:.2%}")
    print("\n[주효과: 한 클래스 소스만 v080→v084]")
    for c, e in main_eff.items():
        oth = ", ".join(f"{k} {v:+.1%}" for k, v in e["other_recall_delta"].items())
        print(f"  {c:9s} micro {e['micro_delta']:+.1%} | 자기 recall {e['self_recall_delta']:+.1%} | {oth}")


def demo() -> None:
    """score() 의 argmax 규칙 — 클래스 최고 코사인이 가장 큰 클래스를 예측."""
    b = {0: np.array([0.9, 0.1]), 1: np.array([0.1, 0.9]), 2: np.array([0.0, 0.0]),
         3: np.array([0.0, 0.0])}
    r = score(b, np.array([0, 1]))
    assert r["micro"] == 1.0, r
    r2 = score(b, np.array([1, 0]))
    assert r2["micro"] == 0.0, r2
    print("demo ok")


if __name__ == "__main__":
    demo() if "--selftest" in sys.argv else main()
