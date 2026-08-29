#!/usr/bin/env python3
"""문장 배경통계(`m_s`·`Ak`)를 **해시 기준**으로 재구축한다.

왜: `prompt_cos_db.load_sentence_vectors` 에 `ORDER BY` 가 없다. 행 순서가 Postgres 계획에
달려 있어, 2026-08-28 에 vOPT/vGEN 2,500 문장을 등록한 뒤 순서가 바뀌었다. 그 결과 **행
인덱스로 저장된 `m_s_bg90k.npy`·`Ak_kmeans64.npy` 가 조용히 무효**가 됐다
(검증: 24,792프레임 근사 m_s 와 저장값 피어슨 0.33 — 정렬돼 있으면 0.95+).

그래서 이번엔 **content_hash 배열을 함께 저장**한다. 소비자는 해시로 조회하므로 DB 에 문장이
더 들어와도 다시 깨지지 않는다.

⚠️ 같은 이유로 다음 배열들도 현재 순서와 어긋나 있을 수 있다 — 이 스크립트는 손대지 않는다:
   `cluster_specificity_z.npy`, `percls_*.npy`. 소비 전에 검증할 것.
"""
import os, sys, time, json
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CHUNK = int(os.environ.get("RS_CHUNK", "1000"))
NK = 64
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
cur = conn.cursor()
# ⚠️ ORDER BY 를 **명시**한다 — 이게 이 스크립트의 존재 이유다.
cur.execute("SELECT entity_id, embedding::text FROM image_embeddings "
            "WHERE entity_type='prompt' ORDER BY entity_id")
hs, vs = [], []
for eid, vt in cur:
    hs.append(eid); vs.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32))
P = np.vstack(vs); P /= np.linalg.norm(P, axis=1, keepdims=True); del vs
NP = P.shape[0]
log(f"문장 {NP:,} (해시 정렬)")

cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
e2k = dict(cur.fetchall())
log(f"군집 배정 {len(e2k):,}")

msum = np.zeros(NP, np.float64)
Akp = np.zeros((NP, NK), np.float64)
cnt = np.zeros(NK, np.int64)
ntot = 0
buf_v, buf_k = [], []

def flush():
    global ntot
    if not buf_v: return
    X = np.vstack(buf_v); X /= np.linalg.norm(X, axis=1, keepdims=True)
    S = (X @ P.T).astype(np.float32)
    msum[:] += S.sum(0, dtype=np.float64)
    kk = np.asarray(buf_k); ntot += len(kk)
    for k0 in np.unique(kk):
        m = kk == k0
        Akp[:, k0] += S[m].sum(0, dtype=np.float64); cnt[k0] += int(m.sum())
    buf_v.clear(); buf_k.clear()
    if ntot % 10000 < CHUNK: log(f"  프레임 {ntot:,}")

with conn.cursor(name="fr_rs") as c2:
    c2.itersize = CHUNK
    c2.execute("SELECT entity_id, embedding::text FROM image_embeddings WHERE entity_type='frame'")
    for eid, vt in c2:
        if e2k.get(eid) is None: continue
        buf_v.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32))
        buf_k.append(e2k[eid])
        if len(buf_v) >= CHUNK: flush()
flush()
log(f"프레임 총 {ntot:,} (기존 실행은 90,084)")

m_s = (msum / max(ntot, 1)).astype(np.float32)
Ak = (Akp / np.maximum(cnt, 1)).astype(np.float32)
sd = (Ak - Ak.mean(1, keepdims=True)).std(1)
np.savez_compressed(f"{OUT}/sent_stats_byhash.npz",
                    hashes=np.array(hs), m_s_mean=m_s, m_s_max=Ak.max(1).astype(np.float32),
                    sd=sd, Ak=Ak, n_frames=ntot)
log(f"→ {OUT}/sent_stats_byhash.npz · m_s MEAN {m_s.mean():.4f} / MAX {Ak.max(1).mean():.4f} "
    f"· 특이도 {sd.mean():.5f} · 두 집계 상관 {np.corrcoef(m_s, Ak.max(1))[0,1]:+.3f}")
json.dump(dict(n_sentences=NP, n_frames=ntot, order="entity_id ASC",
               note="소비자는 hashes 로 조회할 것 — 행 인덱스 금지"),
          open(f"{OUT}/sent_stats_byhash.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
