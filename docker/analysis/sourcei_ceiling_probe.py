#!/usr/bin/env python3
"""천장 실측 — 이 임베딩으로 **어떤 방법이든** 얼마나 올릴 수 있나.

프롬프트 뱅크(무학습)의 최고가 top-K macro-F1 0.529 였다. 그 위에 뭐가 있는지 모르면
"프롬프트를 더 다듬을까 / 라벨을 더 모을까 / 인코더를 바꿀까" 를 못 고른다. 세 가지를 같은
카메라 홀드아웃에서 잰다:
  ① zero-shot 프롬프트 (기준: 뱅크별 최고)
  ② k-NN (k=10, 코사인) — 임베딩 국소 구조가 GT 를 얼마나 담고 있나
  ③ 선형 프로브 (multinomial logistic) — 임베딩 위 선형 분리 상한
  ④ 프로브 학습량 곡선 — 클래스당 라벨 4·8·16·32·64·전량 (프롬프트 천장 ≈ 라벨 4장 주장의 실측)

**카메라 그룹 홀드아웃**이 핵심이다. 무작위 분할은 같은 카메라·같은 이벤트 프레임이 학습/평가에
같이 들어가 천장을 크게 부풀린다(누수). GT 자체가 영상 윈도우 라벨이라 절대값이 아니라
'프롬프트 대비 얼마나 위인가'로만 읽는다.
"""
import os, json, collections
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "6")
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import GroupKFold

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
RNG = np.random.default_rng(0)

import fiftyone as fo
ds = fo.load_dataset("sourcei")
ids, emb, gt, cam = ds.values(["id", "embedding", "ground_truth.label", "camera"])
X = np.asarray(emb, dtype=np.float32); X /= np.linalg.norm(X, axis=1, keepdims=True)
y = np.array([CLASSES.index(g) for g in gt]); cam = np.array(cam)
print(f"{len(y):,} 프레임, 카메라 {len(set(cam))}, GT {dict(collections.Counter(gt))}")


def macro_f1(t, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


# 프롬프트 예측(무학습)도 **같은 테스트 폴드에서** 재야 비교가 성립한다.
# 프롬프트를 전량에서, 프로브를 홀드아웃에서 재고 나란히 놓으면 프롬프트가 유리해진다.
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
assert list(d["ids"]) == list(ids), "preds.npz 와 프레임 순서 불일치"
PB = {b: d[f"topk__{b}"] for b in d["banks"]}
gkf = GroupKFold(n_splits=5)
folds = list(gkf.split(X, y, groups=cam))
res = collections.defaultdict(list)
for k, (tr, te) in enumerate(folds):
    if len(set(y[te])) < 2: continue
    knn = KNeighborsClassifier(n_neighbors=10, metric="cosine", weights="distance").fit(X[tr], y[tr])
    res["kNN(k=10)"].append((macro_f1(y[te], knn.predict(X[te])), (knn.predict(X[te]) == y[te]).mean()))
    lr = LogisticRegression(max_iter=2000, C=1.0, class_weight="balanced").fit(X[tr], y[tr])
    res["선형 프로브(전량)"].append((macro_f1(y[te], lr.predict(X[te])), (lr.predict(X[te]) == y[te]).mean()))
    # 학습량 곡선 — 클래스당 n 장만
    for n in (4, 8, 16, 32, 64, 128):
        idx = []
        for c in range(4):
            pool = tr[y[tr] == c]
            if len(pool) == 0: continue
            idx.extend(RNG.choice(pool, size=min(n, len(pool)), replace=False))
        idx = np.array(idx)
        if len(set(y[idx])) < 2: continue
        m = LogisticRegression(max_iter=2000, C=1.0, class_weight="balanced").fit(X[idx], y[idx])
        res[f"프로브 {n}장/클래스"].append((macro_f1(y[te], m.predict(X[te])), (m.predict(X[te]) == y[te]).mean()))
    pf = {b: macro_f1(y[te], PB[b][te]) for b in PB}
    bb = max(pf, key=pf.get)
    res["zero-shot 프롬프트(폴드 최고)"].append((pf[bb], (PB[bb][te] == y[te]).mean()))
    res["zero-shot 프롬프트(v1.0.8.1)"].append((pf["v1.0.8.1"], (PB["v1.0.8.1"][te] == y[te]).mean()))
    res["zero-shot 프롬프트(v1.0.8.0)"].append((pf["v1.0.8.0"], (PB["v1.0.8.0"][te] == y[te]).mean()))
    print(f"  fold{k} 최고뱅크 {bb} {pf[bb]:.3f} | 완료 (학습 {len(tr):,} / 평가 {len(te):,}, 평가 카메라 {len(set(cam[te]))})")

summary = {}
print(f"\n{'방법':<22}{'macro-F1(이벤트3)':>18}{'정확도':>10}   (카메라 5-fold 그룹 홀드아웃 평균±표준편차)")
for k, v in res.items():
    f1 = np.array([x[0] for x in v]); ac = np.array([x[1] for x in v])
    summary[k] = dict(macro_f1=float(f1.mean()), macro_f1_sd=float(f1.std()), acc=float(ac.mean()), n_folds=len(v))
    print(f"{k:<22}{f1.mean():>12.3f} ±{f1.std():.3f}{ac.mean():>10.3f}")
# 프롬프트 기준선 (같은 데이터, 전량 평가 — 홀드아웃 개념이 없는 무학습이라 비교는 참고용)
m = json.load(open(f"{OUT}/metrics.json"))
best = max(m["banks"].items(), key=lambda kv: kv[1]["rules"]["topk"]["macro_f1_ev"])
summary["zero-shot 프롬프트 최고"] = dict(bank=best[0], macro_f1=best[1]["rules"]["topk"]["macro_f1_ev"], acc=best[1]["rules"]["topk"]["acc"])
print(f"\n[참고] zero-shot 프롬프트 최고 {best[0]}: macro-F1 {best[1]['rules']['topk']['macro_f1_ev']:.3f} / acc {best[1]['rules']['topk']['acc']:.3f} (학습 없음 = 전량 평가)")
json.dump(summary, open(f"{OUT}/ceiling_probe.json", "w"), ensure_ascii=False, indent=1)
