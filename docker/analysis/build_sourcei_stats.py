#!/usr/bin/env python3
"""sourcei **자체** 배경통계를 해시 기준으로 캐시한다 (`sent_stats_sourcei.npz`).

현행 통계는 `frames`(22 프로젝트, 90,084장) 위에서 계산된다. sourcei 와 카메라를 하나도
공유하지 않는 모수다(§27-4). 같은 정의를 **sourcei 7,498장 + sourcei 자체 64군집** 위에서
다시 계산해, 두 모수를 같은 하네스에서 A/B 할 수 있게 만든다.

정의는 `rebuild_sent_stats.py` 와 동일하다 — 모수만 바꾼다:
  m_s_mean = 프레임 전량 평균 코사인 · m_s_max = 군집평균 중 최대 · sd = 군집평균의 표준편차
저장 규약도 동일: **해시 배열 동봉**(행 인덱스로 키를 잡지 않는다).
"""
import os, sys, time
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo
from sklearn.cluster import MiniBatchKMeans

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
K = int(os.environ.get("HN_K", "64"))
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
cur.execute("SELECT entity_id, embedding::text FROM image_embeddings "
            "WHERE entity_type='prompt' ORDER BY entity_id")
hs, vs = [], []
for eid, vt in cur:
    hs.append(eid); vs.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32))
P = np.vstack(vs); P /= np.linalg.norm(P, axis=1, keepdims=True); del vs
log(f"문장 {P.shape[0]:,}")

hy = fo.load_dataset("sourcei")
FH = np.asarray(hy.values("embedding"), dtype=np.float32)
FH /= np.linalg.norm(FH, axis=1, keepdims=True)
cl = MiniBatchKMeans(n_clusters=K, random_state=0, n_init=10, batch_size=1024).fit(FH).labels_
log(f"sourcei 프레임 {len(FH):,} · 군집 {K}")

ms = np.empty(P.shape[0], np.float32)
Ak = np.empty((P.shape[0], K), np.float32)
ks = [k for k in range(K) if (cl == k).any()]
for s0 in range(0, P.shape[0], 4000):
    S = FH @ P[s0:s0 + 4000].T
    ms[s0:s0 + 4000] = S.mean(0)
    for i, k in enumerate(ks): Ak[s0:s0 + 4000, i] = S[cl == k].mean(0)
    del S
Ak = Ak[:, :len(ks)]
sd = (Ak - Ak.mean(1, keepdims=True)).std(1)
np.savez_compressed(f"{OUT}/sent_stats_sourcei.npz", hashes=np.array(hs),
                    m_s_mean=ms, m_s_max=Ak.max(1), sd=sd, Ak=Ak,
                    n_frames=len(FH), k=len(ks))
log(f"→ sent_stats_sourcei.npz · m_s {ms.mean():.4f} · sd {sd.mean():.5f} · 군집 {len(ks)}")
print("DONE")
