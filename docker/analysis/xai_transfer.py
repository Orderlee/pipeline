#!/usr/bin/env python3
"""0.900 중 얼마가 '장소 암기'인가 — 전이 가능한 신호만 남겨 다시 재고, 실용 트리아지를 만든다."""
import sys
import numpy as np
sys.path.insert(0, "/workspace")
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import GroupKFold
import xai_features as XF

OUT = "/workspace/_xai"
d = np.load(f"{OUT}/features.npz", allow_pickle=True)
F = {k: d[k] for k in d.files if not k.startswith("meta_")}
gt = np.asarray([str(x) for x in d["meta_gt"]]); pred = np.asarray([str(x) for x in d["meta_pred"]])
cam = np.asarray([str(x) for x in d["meta_cam"]]); ev = np.asarray([str(x) for x in d["meta_ev"]])
y = (pred != gt).astype(int)
G = {f["name"]: f["group"] for f in XF.FEATURES}
GBM = lambda: HistGradientBoostingClassifier(max_depth=3, max_iter=250, learning_rate=.06,
                                             min_samples_leaf=40, random_state=0)

# 장소·클립 정체성을 직접 담는 피처 — 새 현장에 전이되지 않는다
IDENTITY = ["pc1", "pc2", "pc3", "pc4", "clip_size", "event_size", "camera_dev",
            "knn_same_camera", "cos_to_camera", "cos_to_global"]

def score(sub, groups):
    M, _ = XF.design_matrix(F, sub)
    gkf = GroupKFold(n_splits=5); oof = np.full(len(y), np.nan)
    for tr, te in gkf.split(M, y, groups):
        if len(set(y[tr].tolist())) < 2: continue
        m = GBM(); m.fit(M[tr], y[tr]); oof[te] = m.predict_proba(M[te])[:, 1]
    ok = ~np.isnan(oof)
    if len(set(y[ok].tolist())) < 2: return float("nan"), float("nan"), oof
    return roc_auc_score(y[ok], oof[ok]), average_precision_score(y[ok], oof[ok]), oof

ALL = [f["name"] for f in XF.FEATURES if f["leak"] == "free"]
TRANSFER = [x for x in ALL if x not in IDENTITY]
sets = [("전체 32피처", ALL), (f"정체성 제외 {len(TRANSFER)}피처", TRANSFER),
        ("A 프롬프트점수만 11", [x for x in ALL if G[x].startswith("A_")]),
        ("normal_lead 하나만", ["normal_lead"])]
print(f"오답 {y.sum():,}/{len(y):,} ({y.mean()*100:.1f}%) · PR 기준선 {y.mean():.3f}\n")
print(f"{'피처 집합':26s} {'이벤트 AUC':>10s} {'이벤트 PR':>10s} {'카메라 AUC':>10s} {'카메라 PR':>10s}")
oofs = {}
for nm, sub in sets:
    a1, p1, o1 = score(sub, ev); a2, p2, _ = score(sub, cam)
    oofs[nm] = o1
    print(f"  {nm:24s} {a1:>10.3f} {p1:>10.3f} {a2:>10.3f} {p2:>10.3f}")

print("\n=== normal_lead 구간별 실제 오답률 (부분의존 대신 실측 분위) ===")
nl = np.asarray(F["normal_lead"], dtype=float)
qs = np.quantile(nl, np.linspace(0, 1, 11))
print(f"  {'구간':28s} {'n':>6s} {'오답률':>7s} {'누적 오답 비중':>12s}")
tot = y.sum(); cum = 0
rows = []
for i in range(10):
    lo, hi = qs[i], qs[i + 1]
    m = (nl >= lo) & (nl <= hi if i == 9 else nl < hi)
    if m.sum() == 0: continue
    cum += y[m].sum()
    rows.append((lo, hi, int(m.sum()), float(y[m].mean()), cum / tot))
for lo, hi, n_, r_, c_ in rows:
    print(f"  [{lo:+.4f}, {hi:+.4f}) {n_:>6,} {r_*100:>6.1f}% {c_*100:>11.1f}%")

print("\n=== 실용 트리아지: 상위 위험 X% 를 사람이 보면 오답 몇 %를 잡나 ===")
o = oofs["정체성 제외 " + str(len(TRANSFER)) + "피처"]
ok = ~np.isnan(o); oo = o[ok]; yy = y[ok]
order = np.argsort(-oo)
print(f"  {'검토 비율':>8s} {'검토 장수':>8s} {'잡은 오답':>9s} {'회수율':>7s} {'정밀도':>7s}")
for frac in (.05, .10, .20, .30, .50):
    k = int(len(oo) * frac); sel = order[:k]
    print(f"  {frac*100:>7.0f}% {k:>8,} {int(yy[sel].sum()):>9,} {yy[sel].sum()/yy.sum()*100:>6.1f}% {yy[sel].mean()*100:>6.1f}%")

print("\n=== 클래스별 normal_lead 중앙값 (양수 = normal 이 이벤트를 앞선다) ===")
for c in ("normal", "falldown", "smoke", "fire"):
    m = gt == c
    print(f"  {c:9s} n {int(m.sum()):>5,}  중앙값 {np.median(nl[m]):+.4f}  "
          f"양수 비율 {float((nl[m] > 0).mean())*100:5.1f}%  오답률 {y[m].mean()*100:5.1f}%")
np.save(f"{OUT}/oof_transfer.npy", o)
