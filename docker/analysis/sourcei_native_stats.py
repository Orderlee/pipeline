#!/usr/bin/env python3
"""sourcei 를 pgvector 에 연결하면 **실제로 좋아지는가** — DB 쓰기 없이 선검정.

27-4 는 "배경통계 모수(frames 22프로젝트)와 평가 모수(sourcei 15카메라)가 카메라를 하나도
공유하지 않는다"를 지적했다. 자연스러운 처방은 "sourcei 를 pgvector 에 넣고 배경통계를
sourcei 위에서 다시 계산하라"다. 그런데 그게 **정말 신호를 개선하는지**는 별개 문제다.

반대 가설이 있다. 27-4 가 인용한 실측은 "군집은 이벤트보다 장소를 4배 강하게 담는다"였다.
sourcei 는 카메라가 15대뿐이라, 그 안에서 군집을 내면 군집이 사실상 **카메라 식별자**가 될 수
있다. 그러면 `sd`(군집 특이도)는 "sourcei 카메라를 얼마나 잘 가르는가"가 되어 이벤트 탐지와
**더** 멀어진다. 연결이 문제를 악화시킬 수도 있다는 뜻이다.

그래서 세 가지를 잰다 — 전부 FiftyOne 임베딩만 쓰므로 운영 DB 를 건드리지 않는다.
  ① 군집이 무엇을 담는가: NMI(군집, 카메라) vs NMI(군집, 클래스). frames 쪽 0.586/0.149 와 비교.
  ② 두 배경통계의 유용성: sd_frames vs sd_sourcei 가 **문장의 실제 판별력**과 얼마나 상관하는가.
     판별력 y = 그 문장이 선언한 클래스 프레임에서의 평균 코사인 − 나머지 클래스 평균 코사인
     (sourcei GT 기준 직접 측정. §D4 와 같은 정의).
  ③ 카메라 교락 통제: y 를 카메라 내부에서 중심화한 뒤 같은 상관을 다시 본다.
     (장소 효과를 빼도 신호가 남는지 — 이게 진짜 질문이다.)
"""
from __future__ import annotations
import os, sys, json, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import normalized_mutual_info_score as nmi
from scipy import stats as sps
from prompt_cos_db import load_sentence_vectors

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
AB = f"{OUT}/filter_ab"
CLASSES = ["normal", "falldown", "fire", "smoke"]
K = int(os.environ.get("HN_K", "64"))
POOL_PER_CLS = 3000

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt = np.array(d["gt"]); cam = np.array([str(x) for x in d["camera"]]); ids = list(d["ids"])
hy = fo.load_dataset("sourcei"); hid, hemb = hy.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
print(f"sourcei 프레임 {len(gt):,} · 카메라 {len(set(cam))} · k={K}")

# ── ① sourcei 자체 군집이 무엇을 담는가 ────────────────────────────
km = MiniBatchKMeans(n_clusters=K, random_state=0, n_init=10, batch_size=1024).fit(FH)
cl = km.labels_
n_cam, n_cls = nmi(cl, cam), nmi(cl, gt)
print(f"\n① sourcei 군집(k={K}) 이 담는 것")
print(f"   NMI(군집, 카메라) = {n_cam:.3f}")
print(f"   NMI(군집, 클래스) = {n_cls:.3f}   → 장소/이벤트 비 {n_cam/max(n_cls,1e-9):.2f}배")
print(f"   (frames 모수 실측은 장소 0.586 vs 이벤트 0.149 = 3.93배)")
pure_cam = np.mean([collections.Counter(cam[cl == k]).most_common(1)[0][1] / max((cl == k).sum(), 1)
                    for k in range(K) if (cl == k).any()])
print(f"   군집의 카메라 순도 평균 {pure_cam:.1%}  ← 1.0 에 가까우면 군집 = 카메라")

# ── 후보 문장 풀 (§23 과 같은 규약) ────────────────────────────────
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes = collections.defaultdict(dict)
for h, c, n_ in cur: votes[h][c] = n_
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes.items()}
st = np.load(f"{OUT}/sent_stats_byhash.npz", allow_pickle=True)
pos = {h: i for i, h in enumerate(st["hashes"])}
row = np.array([pos.get(h, -1) for h in hashes], np.int64); ok = row >= 0
sd_fr = np.full(len(hashes), np.nan, np.float32); sd_fr[ok] = st["sd"][row[ok]]
ms_fr = np.full(len(hashes), np.nan, np.float32); ms_fr[ok] = st["m_s_mean"][row[ok]]

sel, lab = [], []
for ci, c in enumerate(CLASSES):
    ii = np.array([j for j, h in enumerate(hashes) if maj.get(h) == c and ok[j]])
    keep = ii[np.argsort(-sd_fr[ii])[:POOL_PER_CLS]]      # frames 기준 상위 (현행 규약)
    sel.extend(keep.tolist()); lab.extend([ci] * len(keep))
sel = np.array(sel); lab = np.array(lab)
P = SENT[sel].astype(np.float32); P /= np.linalg.norm(P, axis=1, keepdims=True)
print(f"\n후보 문장 {len(sel):,} " + str(dict(collections.Counter(CLASSES[i] for i in lab))))

# ── ② sourcei 위에서 배경통계 재계산 ──────────────────────────────
S = FH @ P.T                                        # [7498, NP]
ms_hy = S.mean(0)
Akp = np.stack([S[cl == k].mean(0) for k in range(K) if (cl == k).any()], 1)
sd_hy = (Akp - Akp.mean(1, keepdims=True)).std(1)
print(f"② sourcei 배경통계 — m_s {ms_hy.mean():.4f} (frames {ms_fr[sel].mean():.4f}) · "
      f"sd {sd_hy.mean():.5f} (frames {sd_fr[sel].mean():.5f})")
print(f"   sd 두 모수 상관 = {np.corrcoef(sd_fr[sel], sd_hy)[0,1]:+.3f}")

# ── 문장의 실제 판별력 y (sourcei GT 직접) ────────────────────────
def contrast(Smat, center_by_camera=False):
    X = Smat.copy()
    if center_by_camera:                              # 카메라 평균 제거 = 장소 효과 통제
        for c in np.unique(cam):
            m = cam == c
            X[m] -= X[m].mean(0, keepdims=True)
    y = np.empty(X.shape[1], np.float32)
    for j in range(X.shape[1]):
        own = gt == lab[j]
        y[j] = X[own, j].mean() - X[~own, j].mean()
    return y
y_raw = contrast(S)
y_cam = contrast(S, center_by_camera=True)

def rep(name, x, y):
    m = np.isfinite(x) & np.isfinite(y)
    r = sps.pearsonr(x[m], y[m]); s = sps.spearmanr(x[m], y[m])
    return f"{name:22} pearson {r.statistic:+.3f} (p={r.pvalue:.1e}) · spearman {s.statistic:+.3f}"

print("\n③ 배경통계가 '문장의 실제 판별력' 을 얼마나 예측하는가")
print("   [원본 y — 장소 효과 포함]")
print("   " + rep("sd_frames (현행)", sd_fr[sel], y_raw))
print("   " + rep("sd_sourcei (연결 후)", sd_hy, y_raw))
print("   " + rep("m_s_frames (현행)", ms_fr[sel], y_raw))
print("   " + rep("m_s_sourcei (연결 후)", ms_hy, y_raw))
print("   [카메라 중심화 y — 장소 효과 통제]")
print("   " + rep("sd_frames (현행)", sd_fr[sel], y_cam))
print("   " + rep("sd_sourcei (연결 후)", sd_hy, y_cam))
print("   " + rep("m_s_frames (현행)", ms_fr[sel], y_cam))
print("   " + rep("m_s_sourcei (연결 후)", ms_hy, y_cam))

# 클래스별로도 (전체 상관은 클래스 간 차이에 끌릴 수 있다)
print("\n   클래스별 spearman (sd_frames → sd_sourcei), 카메라 중심화 y 기준")
for ci, c in enumerate(CLASSES):
    m = lab == ci
    a = sps.spearmanr(sd_fr[sel][m], y_cam[m]).statistic
    b = sps.spearmanr(sd_hy[m], y_cam[m]).statistic
    print(f"     {c:9} {a:+.3f} → {b:+.3f}   ({'개선' if abs(b)>abs(a) else '악화 또는 동일'})")

json.dump(dict(k=K, nmi_camera=float(n_cam), nmi_class=float(n_cls),
               cluster_camera_purity=float(pure_cam),
               sd_corr_between_populations=float(np.corrcoef(sd_fr[sel], sd_hy)[0, 1]),
               corr={
                 "raw": {"sd_frames": float(sps.spearmanr(sd_fr[sel], y_raw).statistic),
                          "sd_sourcei": float(sps.spearmanr(sd_hy, y_raw).statistic),
                          "ms_frames": float(sps.spearmanr(ms_fr[sel], y_raw).statistic),
                          "ms_sourcei": float(sps.spearmanr(ms_hy, y_raw).statistic)},
                 "camera_centered": {"sd_frames": float(sps.spearmanr(sd_fr[sel], y_cam).statistic),
                          "sd_sourcei": float(sps.spearmanr(sd_hy, y_cam).statistic),
                          "ms_frames": float(sps.spearmanr(ms_fr[sel], y_cam).statistic),
                          "ms_sourcei": float(sps.spearmanr(ms_hy, y_cam).statistic)}},
               n_sentences=int(len(sel))),
          open(f"{AB}/sourcei_native.json", "w"), ensure_ascii=False, indent=1)
print(f"\n→ {AB}/sourcei_native.json")
