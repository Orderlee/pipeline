#!/usr/bin/env python3
"""프롬프트 규칙의 오답을 설명한다 — 해석 가능한 모델 + 순열 중요도 + 그룹 절제.

`xai_features.py` 가 만든 표를 읽어 두 질문에 답한다.
  Q1 어떤 피처가 '이 프레임에서 규칙이 틀릴 것'을 예측하는가 (오답 예측 = 신뢰도 추정)
  Q2 프롬프트를 안 쓰고 라벨 없는 피처만으로 클래스를 얼마나 맞히는가 (상한 비교)

검증은 **GroupKFold** 만 쓴다. 그룹은 이벤트와 카메라 두 벌로 각각 돌린다.
무작위 분할은 이 데이터에서 성적을 부풀린다(같은 이벤트 프레임이 사실상 같은 그림).
기준선은 다수클래스(81.5%)이므로 정확도가 아니라 **ROC-AUC / PR-AUC** 로 읽는다.
"""
from __future__ import annotations

import json
import os
import sys

import numpy as np
sys.path.insert(0, "/workspace")
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import xai_features as XF

OUT = "/workspace/_xai"
d = np.load(f"{OUT}/features.npz", allow_pickle=True)
F = {k: d[k] for k in d.files if not k.startswith("meta_")}
gt = np.asarray([str(x) for x in d["meta_gt"]])
pred = np.asarray([str(x) for x in d["meta_pred"]])
cam = np.asarray([str(x) for x in d["meta_cam"]])
ev = np.asarray([str(x) for x in d["meta_ev"]])
y_wrong = (pred != gt).astype(int)
M, names = XF.design_matrix(F)
print(f"표본 {len(gt):,} · free 피처 {len(names)} · 오답 {y_wrong.sum():,} ({y_wrong.mean()*100:.1f}%)")
print(f"그룹: 이벤트 {len(set(ev.tolist())):,} · 카메라 {len(set(cam.tolist()))}\n")


def cv_scores(M, y, groups, model, n_splits=5):
    gkf = GroupKFold(n_splits=min(n_splits, len(set(groups.tolist()))))
    oof = np.full(len(y), np.nan)
    for tr, te in gkf.split(M, y, groups):
        if len(set(y[tr].tolist())) < 2:
            continue
        m = model()
        m.fit(M[tr], y[tr])
        oof[te] = m.predict_proba(M[te])[:, 1]
    ok = ~np.isnan(oof)
    return (roc_auc_score(y[ok], oof[ok]), average_precision_score(y[ok], oof[ok]), oof)


LOGIT = lambda: make_pipeline(StandardScaler(), LogisticRegression(max_iter=3000, C=1.0))  # noqa: E731
GBM = lambda: HistGradientBoostingClassifier(max_depth=3, max_iter=250, learning_rate=.06,  # noqa: E731
                                             min_samples_leaf=40, random_state=0)

print("=== Q1. 오답을 예측할 수 있는가 (양성 = 규칙이 틀림) ===")
print(f"  {'분할':14s} {'모델':10s} {'ROC-AUC':>8s} {'PR-AUC':>8s}   (PR 기준선 {y_wrong.mean():.3f})")
res = {}
for gname, groups in (("이벤트", ev), ("카메라", cam)):
    for mname, mk in (("로지스틱", LOGIT), ("GBM", GBM)):
        auc, ap, oof = cv_scores(M, y_wrong, groups, mk)
        res[(gname, mname)] = (auc, ap, oof)
        print(f"  {gname:14s} {mname:10s} {auc:>8.3f} {ap:>8.3f}")

# ── 순열 중요도 (이벤트 분할 1폴드 홀드아웃) ──
# ⚠️ 폴드를 첫 번째로 고정하면 안 된다 — GroupKFold 의 첫 테스트 폴드에 오답이 한 클래스만
# 들어가 ROC-AUC 가 정의되지 않고 순열 중요도가 전량 NaN 이 된다(실측). 양쪽 클래스가
# 충분히 든 폴드 중 오답 비율이 전체와 가장 비슷한 폴드를 고른다.
gkf = GroupKFold(n_splits=5)
folds = list(gkf.split(M, y_wrong, ev))
cand = [(abs(y_wrong[te].mean() - y_wrong.mean()), k) for k, (tr, te) in enumerate(folds)
        if 20 <= y_wrong[te].sum() <= len(te) - 20]
if not cand:
    raise SystemExit("양쪽 클래스가 충분한 폴드가 없다 — 순열 중요도 계산 불가")
tr, te = folds[min(cand)[1]]
print(f"\n순열 중요도 폴드: 학습 {len(tr):,} / 홀드아웃 {len(te):,} "
      f"(홀드아웃 오답률 {y_wrong[te].mean()*100:.1f}%, 전체 {y_wrong.mean()*100:.1f}%)")
m = GBM(); m.fit(M[tr], y_wrong[tr])
pi = permutation_importance(m, M[te], y_wrong[te], n_repeats=15, random_state=0,
                            scoring="roc_auc", n_jobs=2)
ordi = np.argsort(-pi.importances_mean)
print("\n=== 순열 중요도 상위 15 (ROC-AUC 하락폭, 이벤트 홀드아웃) ===")
G = {f["name"]: f["group"] for f in XF.FEATURES}
for i in ordi[:15]:
    print(f"  {names[i]:22s} {G[names[i]]:16s} {pi.importances_mean[i]:+.4f} ± {pi.importances_std[i]:.4f}")

# ── 로지스틱 계수 (표준화, 부호가 곧 방향) ──
lm = LOGIT(); lm.fit(M, y_wrong)
coef = lm[-1].coef_[0]
o2 = np.argsort(-np.abs(coef))
print("\n=== 표준화 로지스틱 계수 상위 12 (양수 = 클수록 틀림) ===")
for i in o2[:12]:
    print(f"  {names[i]:22s} {G[names[i]]:16s} {coef[i]:+.3f}")

# ── 피처 묶음 절제 ──
print("\n=== 묶음 절제 (이벤트 분할, GBM, ROC-AUC) ===")
groups_of = {}
for f in XF.FEATURES:
    if f["leak"] == "free":
        groups_of.setdefault(f["group"], []).append(f["name"])
full = res[("이벤트", "GBM")][0]
print(f"  {'전체 32피처':28s} {full:.3f}")
for gk, gv in sorted(groups_of.items()):
    sub = [x for x in names if x in gv]
    Ms, _ = XF.design_matrix(F, sub)
    a, _, _ = cv_scores(Ms, y_wrong, ev, GBM)
    rest = [x for x in names if x not in gv]
    Mr, _ = XF.design_matrix(F, rest)
    ar, _, _ = cv_scores(Mr, y_wrong, ev, GBM)
    print(f"  {gk:28s} 단독 {a:.3f}   그 묶음만 제거 {ar:.3f} ({ar-full:+.3f})  [{len(gv)}개]")

# ── Q2. 프롬프트를 안 쓰고 클래스를 맞히면? ──
print("\n=== Q2. 프롬프트 점수 없이(B/C/D 묶음만) 클래스 맞히기 ===")
nonprompt = [x for x in names if not G[x].startswith("A_")]
Mn, _ = XF.design_matrix(F, nonprompt)
from sklearn.metrics import accuracy_score, f1_score
for gname, groups in (("이벤트", ev), ("카메라", cam)):
    gkf2 = GroupKFold(n_splits=5); oof = np.empty(len(gt), dtype=object)
    for tri, tei in gkf2.split(Mn, gt, groups):
        mm = HistGradientBoostingClassifier(max_depth=3, max_iter=250, learning_rate=.06,
                                            min_samples_leaf=40, random_state=0)
        mm.fit(Mn[tri], gt[tri]); oof[tei] = mm.predict(Mn[tei])
    oof = np.asarray([str(x) for x in oof])
    print(f"  {gname} 분할: 정확도 {accuracy_score(gt,oof)*100:5.1f}% · macroF1 {f1_score(gt,oof,average='macro')*100:5.1f}%"
          f"   (프롬프트 규칙 {(pred==gt).mean()*100:.1f}% / macroF1 {f1_score(gt,pred,average='macro')*100:.1f}%)")

json.dump({"auc": {f"{k[0]}_{k[1]}": [v[0], v[1]] for k, v in res.items()},
           "perm_importance": {names[i]: [float(pi.importances_mean[i]), float(pi.importances_std[i])]
                               for i in ordi},
           "logit_coef": {names[i]: float(coef[i]) for i in o2}},
          open(f"{OUT}/xai_results.json", "w"), ensure_ascii=False, indent=1)
np.save(f"{OUT}/oof_wrong_event.npy", res[("이벤트", "GBM")][2])
print(f"\n→ {OUT}/xai_results.json")
