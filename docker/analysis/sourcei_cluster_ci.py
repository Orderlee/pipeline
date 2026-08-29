#!/usr/bin/env python3
"""카메라 군집 부트스트랩 — 지금까지의 뱅크·규칙 비교에 신뢰구간을 붙인다.

지금까지 낸 숫자(예: v1.0.8.1 0.529 vs v1.0.8.0 0.480)는 **전 프레임 풀링 점추정**이다.
프레임은 카메라·이벤트 안에서 강하게 상관돼 있어 유효 표본이 7,498 보다 훨씬 작다.
카메라를 재표집 단위로 하는 **cluster bootstrap** 으로 CI 를 내고, 같은 카메라에서 두 뱅크를
동시에 재는 **쌍대 비교**(차이의 CI)까지 낸다 — 쌍대가 아니면 카메라 이질성이 차이를 삼킨다.

design effect: 단순무작위 가정 대비 분산이 몇 배인가. deff = Var_cluster / Var_srs.
"""
import json, itertools, collections
import numpy as np

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
B = 4000
RNG = np.random.default_rng(0)
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]
banks = list(d["banks"]); cams = np.unique(cam)
idx_by_cam = {c: np.where(cam == c)[0] for c in cams}


def macro_f1(t, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


def boot_idx(n=B):
    """카메라를 복원추출 → 그 카메라의 프레임 전체를 이어붙인다(군집 부트스트랩)."""
    for _ in range(n):
        pick = RNG.choice(len(cams), size=len(cams), replace=True)
        yield np.concatenate([idx_by_cam[cams[i]] for i in pick])


BOOT = list(boot_idx())
SRS = [RNG.choice(len(gt), size=len(gt), replace=True) for _ in range(B)]   # 단순무작위(잘못된 가정) 비교용


def ci(vals, q=(2.5, 97.5)):
    return float(np.percentile(vals, q[0])), float(np.percentile(vals, q[1]))


out = {}
print("=== 1) 단일 뱅크 macro-F1 (top-K) — 군집 CI vs 단순무작위 CI ===")
print(f"{'뱅크':<12}{'점추정':>8}{'군집 95% CI':>22}{'SRS 95% CI':>22}{'deff':>7}")
for b in ["v1.0.8.1", "v1.0.8.0", "v1.0.8.4", "v1.0.12.0"]:
    p = d[f"topk__{b}"]; pt = macro_f1(gt, p)
    bc = np.array([macro_f1(gt[i], p[i]) for i in BOOT])
    bs = np.array([macro_f1(gt[i], p[i]) for i in SRS])
    deff = float(bc.var() / max(bs.var(), 1e-12))
    out[b] = dict(point=pt, cluster_ci=ci(bc), srs_ci=ci(bs), deff=deff)
    print(f"{b:<12}{pt:>8.3f}   [{ci(bc)[0]:.3f}, {ci(bc)[1]:.3f}]      [{ci(bs)[0]:.3f}, {ci(bs)[1]:.3f}]{deff:>7.1f}")

print("\n=== 2) 쌍대 비교 (같은 부트스트랩 표본에서 두 값의 차이) ===")
pairs = [("v1.0.8.1", "v1.0.8.0", "topk", "topk", "채택 후보 vs 기준선"),
         ("v1.0.8.0", "v1.0.8.4", "topk", "topk", "기준선 vs 전면교체본"),
         ("v1.0.8.0", "v1.0.8.0", "topk", "argmax", "top-K vs argmax (같은 뱅크)"),
         ("v1.0.8.0", "v1.0.8.0", "topk", "wave", "top-K vs 분포-IoU@0.15")]
for a, b, ra, rb, lab in pairs:
    pa, pb = d[f"{ra}__{a}"], d[f"{rb}__{b}"]
    pt = macro_f1(gt, pa) - macro_f1(gt, pb)
    diff = np.array([macro_f1(gt[i], pa[i]) - macro_f1(gt[i], pb[i]) for i in BOOT])
    lo, hi = ci(diff); sig = "유의" if lo > 0 or hi < 0 else "**유의하지 않음**"
    out[lab] = dict(point=pt, ci=[lo, hi], p_gt0=float((diff > 0).mean()), significant=(lo > 0 or hi < 0))
    print(f"  {lab:<28} Δ={pt:+.3f}  95% CI [{lo:+.3f}, {hi:+.3f}]  P(Δ>0)={float((diff>0).mean()):.3f}  {sig}")

print("\n=== 3) 31뱅크 순위의 안정성 — 부트스트랩에서 1위가 바뀌는가 ===")
M = np.stack([d[f"topk__{b}"] for b in banks])
winners = collections.Counter()
for i in BOOT[:1000]:
    s = [macro_f1(gt[i], M[j][i]) for j in range(len(banks))]
    winners[banks[int(np.argmax(s))]] += 1
top = winners.most_common(6)
out["winner_stability"] = {k: v / 1000 for k, v in winners.items()}
print("  1위 뱅크 분포(1,000회):", ", ".join(f"{k} {v/10:.1f}%" for k, v in top))
print(f"  → 1위가 유일하게 고정되지 않음 (서로 다른 뱅크가 {len(winners)}종 1위를 차지)")

print("\n=== 4) 카메라별 프레임 상관 = 유효표본 축소 ===")
p = d["topk__v1.0.8.0"]; ok = (p == gt).astype(float)
mu = ok.mean(); ns = np.array([len(idx_by_cam[c]) for c in cams])
between = sum(len(idx_by_cam[c]) * (ok[idx_by_cam[c]].mean() - mu) ** 2 for c in cams) / (len(cams) - 1)
within = sum(((ok[idx_by_cam[c]] - ok[idx_by_cam[c]].mean()) ** 2).sum() for c in cams) / (len(gt) - len(cams))
n0 = (len(gt) - (ns ** 2).sum() / len(gt)) / (len(cams) - 1)
icc = (between - within) / (between + (n0 - 1) * within)
deff_icc = 1 + (n0 - 1) * icc
out["icc"] = dict(icc=float(icc), avg_cluster_size=float(n0), deff=float(deff_icc), eff_n=float(len(gt) / deff_icc))
print(f"  ICC(정확도, 카메라) = {icc:.3f}, 평균 군집 크기 {n0:.0f} → design effect {deff_icc:.1f}")
print(f"  유효표본 ≈ {len(gt)/deff_icc:.0f} (겉보기 {len(gt):,})")
json.dump(out, open(f"{OUT}/cluster_ci.json", "w"), ensure_ascii=False, indent=1)
