#!/usr/bin/env python3
"""kmeans64 군집 커버리지 감사 — 배경통계(m_s·sd)의 모수 편향 측정 (읽기 전용).

배경: 라벨-free 필터의 배경통계는 `analysis.frame_cluster`(method='kmeans64') 에 배정된
90,084 프레임 위에서 계산되는데, `image_embeddings`(entity_type='frame') 는 188,190 행이다.
빠진 절반이 무엇이고(프로젝트별), 그 누락이 m_s 를 실제로 왜곡하는지 측정한다.

측정 3종:
  1) 커버리지 표 — 누락 프레임의 프로젝트 귀속 (FiftyOne `frames`.project 조인,
     군집 포함분은 frame_cluster.project 자체 컬럼 사용)
  2) 분포 편향 — 군집 포함/누락 두 집합의 중심 간 코사인 + 집합 내부 평균 코사인(응집도).
     · 정확값: 정규화 벡터 합으로 닫힌형 계산 (E[cos] = (||Σx||²−n)/(n(n−1)))
     · 요청된 3,000장 표본판도 병기 (사전 추출한 표본 id 로 같은 스트림에서 수집)
  3) 실질 영향 — 문장 2,000개 표본에 대해 현행 m_s(90,084 모수, npz 정답본) vs
     전량 m_s(188,190 모수, 이번 실측) 의 상관·순위 변화

메모리 규율: 프레임 벡터는 서버사이드 커서로 청크 스트리밍(전량 미적재).
상주 대형 객체 = 문장행렬 2,000×1024(8MB) + 표본벡터 6,000×1024(24MB) + id 집합들.

실행: docker exec docker-analysis-1 sh -c "cd /workspace && COS_THREADS=2 nice -n 19 python3 cluster_coverage_audit.py"
"""
import os, sys, time, json, datetime

THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ[_v] = THR

import numpy as np
import psycopg2

DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
NPZ = "/data/fiftyone/frames_bank/report/sourcei_gt/sent_stats_byhash.npz"
OUT_DIR = "/data/fiftyone/frames_bank/report/sourcei_gt/filter_ab"
OUT_JSON = os.path.join(OUT_DIR, "cluster_coverage.json")
CHUNK = int(os.environ.get("CC_CHUNK", "1000"))
N_SENT = 2000     # 문장 표본 수
N_SAMP = 3000     # 집합별 프레임 표본 수 (요청 사양)
SEED = 20260829

T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

rng = np.random.default_rng(SEED)
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True)   # 계약: 이 스크립트는 DB 를 절대 쓰지 않는다
cur = conn.cursor()

# ── 0) 군집 배정 로드 (entity_id → project) ─────────────────────────────
cur.execute("SELECT entity_id, project FROM analysis.frame_cluster WHERE method='kmeans64'")
clus_proj = dict(cur.fetchall())
C = set(clus_proj)
log(f"군집 배정 {len(C):,} (kmeans64)")

# ── 1) 프레임 임베딩 id 전량 (id 만 — 벡터는 아직 안 읽음) ────────────────
cur.execute("SELECT entity_id FROM image_embeddings WHERE entity_type='frame'")
all_ids = [r[0] for r in cur.fetchall()]
n_total = len(all_ids)
in_ids = [e for e in all_ids if e in C]
miss_ids = [e for e in all_ids if e not in C]
log(f"임베딩 프레임 {n_total:,} = 군집포함 {len(in_ids):,} + 누락 {len(miss_ids):,} "
    f"(커버리지 {len(in_ids)/n_total*100:.1f}%)")

# ── 2) FiftyOne frames 에서 entity_id → project 매핑 (문자열만 — values OOM 패턴 아님) ──
import fiftyone as fo  # noqa: E402  (스레드 env 설정 뒤 import)
ds = fo.load_dataset("frames")
fo_eids = ds.values("entity_id")
fo_projs = ds.values("project")
fo_proj = dict(zip(fo_eids, fo_projs))
log(f"FiftyOne frames 매핑 {len(fo_proj):,} 샘플")
del fo_eids, fo_projs

def project_of(eid: str, covered: bool) -> str:
    # 군집 포함분은 frame_cluster 자체가 project 를 갖고 있어 그것이 1차 정본,
    # 누락분은 FiftyOne frames 로만 귀속 가능. 둘 다 없으면 명시적 미상 라벨.
    if covered and eid in clus_proj:
        return clus_proj[eid]
    p = fo_proj.get(eid)
    return p if p else "(FiftyOne frames 밖)"

# ── 3) 프로젝트별 커버리지 표 ──────────────────────────────────────────
cov = {}  # project → [총, 포함]
for eid in all_ids:
    covered = eid in C
    p = project_of(eid, covered)
    row = cov.setdefault(p, [0, 0])
    row[0] += 1
    row[1] += covered
cov_table = sorted(
    ({"project": p, "frames_with_embedding": t, "in_cluster": i,
      "missing": t - i, "coverage_pct": round(i / t * 100, 1)}
     for p, (t, i) in cov.items()),
    key=lambda r: -r["missing"])
log("프로젝트별 커버리지 표 완성")
for r in cov_table[:25]:
    log(f"  {r['project']:<40s} 총 {r['frames_with_embedding']:>7,} · 포함 {r['in_cluster']:>7,} "
        f"· 누락 {r['missing']:>7,} · {r['coverage_pct']:5.1f}%")

# FiftyOne 소속 교차검증 (배경의 99.8% 주장 재실측)
n_clus_in_fo = sum(1 for e in C if e in fo_proj)
n_miss_in_fo = sum(1 for e in miss_ids if e in fo_proj)
log(f"군집 90k 중 FiftyOne frames 소속 {n_clus_in_fo:,} / 누락분 중 소속 {n_miss_in_fo:,}")

# ── 4) 문장 표본 2,000개 — npz 해시에서 무작위 추출 후 DB 에서 벡터 로드 ────
z = np.load(NPZ, allow_pickle=False)
hashes = z["hashes"]
sent_idx = np.sort(rng.choice(len(hashes), size=N_SENT, replace=False))
sent_hashes = [str(h) for h in hashes[sent_idx]]
ms_npz = z["m_s_mean"][sent_idx].astype(np.float64)   # 현행(90,084 모수) 정답본
cur.execute("SELECT entity_id, embedding::text FROM image_embeddings "
            "WHERE entity_type='prompt' AND entity_id = ANY(%s)", (sent_hashes,))
svec = {}
for eid, vt in cur:
    svec[eid] = np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32)
missing_s = [h for h in sent_hashes if h not in svec]
if missing_s:
    log(f"⚠️ 문장 벡터 미발견 {len(missing_s)} — 해당 문장 제외")
    keep = [i for i, h in enumerate(sent_hashes) if h in svec]
    sent_hashes = [sent_hashes[i] for i in keep]
    ms_npz = ms_npz[keep]
P = np.vstack([svec[h] for h in sent_hashes])
P /= np.linalg.norm(P, axis=1, keepdims=True)
NS = P.shape[0]
del svec
log(f"문장 표본 {NS:,} 벡터 로드")

# ── 5) 프레임 표본 3,000 id 사전 추출 (스트림 중 벡터 수집용) ───────────────
samp_in = set(rng.choice(np.asarray(in_ids, dtype=object), size=min(N_SAMP, len(in_ids)), replace=False))
samp_miss = set(rng.choice(np.asarray(miss_ids, dtype=object), size=min(N_SAMP, len(miss_ids)), replace=False))
in_set = C  # 멤버십 판정용
del all_ids, in_ids  # miss_ids 는 표본추출 끝났으니 해제
del miss_ids

# ── 6) 단일 스트리밍 패스 — 코사인 합·정규화벡터 합·표본벡터 수집 ────────────
msum_in = np.zeros(NS, np.float64);  msum_miss = np.zeros(NS, np.float64)
vsum_in = None;  vsum_miss = None    # 정규화 벡터 합 (차원은 첫 청크에서 확정)
n_in = 0; n_miss = 0
cap_in = []; cap_miss = []           # 표본 벡터 (각 ≤3,000)
buf_v, buf_m, buf_e = [], [], []     # 벡터 / in-cluster 마스크 / entity_id

def flush():
    global n_in, n_miss, vsum_in, vsum_miss
    if not buf_v:
        return
    X = np.vstack(buf_v)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    if vsum_in is None:
        vsum_in = np.zeros(X.shape[1], np.float64)
        vsum_miss = np.zeros(X.shape[1], np.float64)
    S = X @ P.T                       # (chunk, NS)
    m = np.asarray(buf_m)
    if m.any():
        msum_in[:] += S[m].sum(0, dtype=np.float64)
        vsum_in += X[m].sum(0, dtype=np.float64)
        n_in += int(m.sum())
    if (~m).any():
        msum_miss[:] += S[~m].sum(0, dtype=np.float64)
        vsum_miss += X[~m].sum(0, dtype=np.float64)
        n_miss += int((~m).sum())
    for i, e in enumerate(buf_e):     # 표본 벡터 캡처 (id 사전추출이라 무편향)
        if e in samp_in:
            cap_in.append(X[i].astype(np.float32))
        elif e in samp_miss:
            cap_miss.append(X[i].astype(np.float32))
    buf_v.clear(); buf_m.clear(); buf_e.clear()
    if (n_in + n_miss) % 20000 < CHUNK:
        log(f"  프레임 {n_in + n_miss:,}")

with conn.cursor(name="cc_stream") as c2:
    c2.itersize = CHUNK
    c2.execute("SELECT entity_id, embedding::text FROM image_embeddings WHERE entity_type='frame'")
    for eid, vt in c2:
        buf_v.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32))
        buf_m.append(eid in in_set)
        buf_e.append(eid)
        if len(buf_v) >= CHUNK:
            flush()
flush()
log(f"스트림 종료: 포함 {n_in:,} / 누락 {n_miss:,}")

# ── 7) 편향 지표 ──────────────────────────────────────────────────────
def unit(v):
    return v / np.linalg.norm(v)

def cohesion_exact(vsum, n):
    # 정규화 벡터의 평균 쌍별 코사인: (||Σx||² − n) / (n(n−1))
    return float((np.dot(vsum, vsum) - n) / (n * (n - 1)))

centroid_cos_exact = float(np.dot(unit(vsum_in), unit(vsum_miss)))
coh_in_exact = cohesion_exact(vsum_in, n_in)
coh_miss_exact = cohesion_exact(vsum_miss, n_miss)

Xi = np.vstack(cap_in); Xm = np.vstack(cap_miss)
centroid_cos_samp = float(np.dot(unit(Xi.mean(0)), unit(Xm.mean(0))))
def cohesion_samp(X):
    s = X.sum(0).astype(np.float64); n = X.shape[0]
    return float((np.dot(s, s) - n) / (n * (n - 1)))
coh_in_samp = cohesion_samp(Xi)
coh_miss_samp = cohesion_samp(Xm)
cross_cos_samp = float((Xi.astype(np.float64) @ Xm.astype(np.float64).T).mean())
log(f"중심 간 코사인 정확 {centroid_cos_exact:.4f} / 표본 {centroid_cos_samp:.4f}")
log(f"응집도(내부 평균 cos) 포함 {coh_in_exact:.4f} vs 누락 {coh_miss_exact:.4f} · 교차 평균 {cross_cos_samp:.4f}")

# ── 8) m_s 비교 — 현행(npz, 90,084) vs 전량(188,190 실측) ────────────────
ms_in = msum_in / max(n_in, 1)                       # 이번 실측 재계산 (npz 검증용)
ms_full = (msum_in + msum_miss) / max(n_in + n_miss, 1)
ms_miss = msum_miss / max(n_miss, 1)

def pearson(a, b):
    return float(np.corrcoef(a, b)[0, 1])

def rankvec(a):
    r = np.empty(len(a), np.int64)
    r[np.argsort(-a)] = np.arange(len(a))
    return r

def spearman(a, b):
    return pearson(rankvec(a).astype(np.float64), rankvec(b).astype(np.float64))

valid_r = pearson(ms_npz, ms_in)      # npz 정답본 vs 같은 모수 재계산 → ~1.0 이어야
r_full = pearson(ms_npz, ms_full)
rho_full = spearman(ms_npz, ms_full)
rk_cur = rankvec(ms_npz); rk_full = rankvec(ms_full)
dr = np.abs(rk_cur - rk_full)
top_k = max(1, NS // 10)              # 상위 10% (200개)
top_cur = set(np.argsort(-ms_npz)[:top_k].tolist())
top_full = set(np.argsort(-ms_full)[:top_k].tolist())
top_overlap = len(top_cur & top_full) / top_k
log(f"검증(현행 npz vs 재계산 동일모수) r={valid_r:.6f}")
log(f"현행 vs 전량: Pearson {r_full:.4f} · Spearman {rho_full:.4f} · "
    f"|Δrank| 평균 {dr.mean():.1f}/중앙 {np.median(dr):.0f}/p95 {np.percentile(dr,95):.0f}/최대 {dr.max()} (모수 {NS}) · "
    f"상위10% 겹침 {top_overlap*100:.1f}%")
log(f"m_s 수준: 포함 {ms_in.mean():.4f} vs 누락 {ms_miss.mean():.4f} vs 전량 {ms_full.mean():.4f} · "
    f"포함↔누락 문장별 상관 {pearson(ms_in, ms_miss):.4f}")

# ── 9) JSON 저장 ─────────────────────────────────────────────────────
result = {
    "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
    "script": "docker/analysis/cluster_coverage_audit.py",
    "params": {"n_sentence_sample": NS, "n_frame_sample_per_group": N_SAMP,
               "seed": SEED, "chunk": CHUNK, "npz": NPZ},
    "population": {
        "image_embeddings_frame_rows": n_total,
        "frame_cluster_kmeans64": len(C),
        "in_cluster_with_embedding": n_in,
        "missing_from_cluster": n_miss,
        "coverage_pct": round(n_in / n_total * 100, 2),
        "cluster_ids_in_fiftyone_frames": n_clus_in_fo,
        "missing_ids_in_fiftyone_frames": n_miss_in_fo,
    },
    "coverage_by_project": cov_table,
    "bias_metrics": {
        "centroid_cosine_exact": round(centroid_cos_exact, 6),
        "centroid_cosine_sample3000": round(centroid_cos_samp, 6),
        "cohesion_in_cluster_exact": round(coh_in_exact, 6),
        "cohesion_missing_exact": round(coh_miss_exact, 6),
        "cohesion_in_cluster_sample3000": round(coh_in_samp, 6),
        "cohesion_missing_sample3000": round(coh_miss_samp, 6),
        "cross_mean_cosine_sample3000": round(cross_cos_samp, 6),
        "note": "정확값=정규화벡터 합 닫힌형(전수), 표본값=집합별 3,000장 사전추출",
    },
    "m_s_impact": {
        "validation_r_npz_vs_recomputed_same_pop": round(valid_r, 6),
        "pearson_current_vs_full": round(r_full, 6),
        "spearman_current_vs_full": round(rho_full, 6),
        "abs_rank_shift": {"mean": round(float(dr.mean()), 2),
                           "median": int(np.median(dr)),
                           "p95": int(np.percentile(dr, 95)),
                           "max": int(dr.max()),
                           "n": NS},
        "top_decile_overlap_pct": round(top_overlap * 100, 2),
        "mean_m_s": {"in_cluster": round(float(ms_in.mean()), 6),
                     "missing": round(float(ms_miss.mean()), 6),
                     "full": round(float(ms_full.mean()), 6)},
        "pearson_ms_in_vs_ms_miss": round(pearson(ms_in, ms_miss), 6),
    },
}
os.makedirs(OUT_DIR, exist_ok=True)
with open(OUT_JSON, "w") as f:
    json.dump(result, f, ensure_ascii=False, indent=1)
log(f"→ {OUT_JSON}")
print("DONE")
