#!/usr/bin/env python3
"""C1 — 군집 × (이벤트 vs 정상) 대비(contrast) 친화도, C2 — 군집설명분 vs 프로젝트 잔차.

## 왜 이걸 만드는가 (기존 결과에 대한 반증에서 출발)

`analysis.sentence_affinity`(군집 평균 코사인)로 뽑은 "특이(specific) 문장"은 **장소 문장**이었다.
창고 군집의 상위 문장은 "fire near storage shelves in a warehouse" 와 "a warehouse aisle" 이
나란히 올라왔고, 그 기준으로 만든 시설입지(facility-location) 뱅크는 GT 정확도 0.340 으로 붕괴했다.
빠진 성분은 **군집 안에서 이벤트 프레임 vs 정상 프레임의 대비**다. 군집 평균은 그 군집의
'장소'를 기술하는 문장에 최댓값을 주지만, 장소 문장은 이벤트 프레임과 정상 프레임에 **똑같이**
붙으므로 대비를 취하면 0 으로 소거된다. 이벤트 문장만 살아남는다.

라벨은 쓰지 않는다. 약참조(weak reference)는 SAM3 검출 클래스(`frames.normalized_class`):
  · 이벤트 = fire / smoke
  · 정상측(normal_ref) = none / person
  · fall / patient 는 'other' 로 분리(대비에 쓰지 않음) — 관측치는 기록한다.
sourcei GT 는 **사후 평가에만** 쓴다.

C1 = A[s,k,r] (군집×참조클래스 평균 코사인) → C_e[s,k] = A[s,k,e] − A[s,k,normal_ref].
C2 = A_k(군집 평균) 로 프로젝트 친화도를 예측하고(Â[s,p] = Σ_k w[p,k]A_k[s,k]) 잔차
     R[s,p] = A_p[s,p] − Â[s,p] 를 본다. 잔차가 작은 문장 = 시각 군집 구성만으로 설명되는
     = **이식성 있는** 문장. 자기포함(project p 의 프레임이 A_k 에 들어간다) 편향은 명시하고
     |R| 상위 200 문장에 LOPO(leave-one-project-out) 재계산으로 크기를 실측한다.

계약(어기면 조용한 오답):
  · 문장 벡터 조인 = bank_sentences.content_hash → image_embeddings.entity_id(entity_type='prompt')
  · class_label 은 뱅크 속성이라 문장 클래스는 **뱅크 간 다수결** + 합의도(class_agreement) 병기
  · 프레임 모집단 = analysis.frame_cluster(method='kmeans64') 90,084 행 그대로. 임베딩 결손 0 실측.
  · 셀 최소 표본 = 20 프레임. 미달 셀은 계산하지 않는다(NaN).

출력: csv/30_contrast_ledger.csv, csv/30b_cluster_top_contrast.csv,
      csv/30c_contrast_vs_specificity.csv, csv/30d_contrast_bank_eval.csv,
      csv/30e_project_residual.csv, fig/f34_contrast_vs_specificity.png,
      fig/f35_contrast_bank_eval.png, fig/f35b_project_residual.png,
      contrast_affinity_summary.json
"""
import collections
import csv
import glob
import json
import os
import sys
import time

os.environ.setdefault("COS_THREADS", "6")
sys.path.insert(0, "/workspace")
# prompt_cos_db 는 numpy import 전에 BLAS 스레드를 캡한다 — 반드시 numpy 보다 앞에서 import.
from prompt_cos_db import load_sentence_vectors, topk_vote, RULE_K, _topk_selfcheck  # noqa: E402

import warnings  # noqa: E402
import numpy as np  # noqa: E402

warnings.filterwarnings('ignore', category=RuntimeWarning)
import psycopg2  # noqa: E402
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.font_manager as fm  # noqa: E402
from scipy.stats import spearmanr  # noqa: E402

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSVD, FIGD = f"{OUT}/csv", f"{OUT}/fig"
DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
CLASSES = ["normal", "falldown", "fire", "smoke"]
CLS_COL = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
BG = "#fcfcfb"
EVENTS = ["fire", "smoke"]
REFS = ["normal_ref", "fire", "smoke", "other", "unknown"]
REF_MAP = {"fire": "fire", "smoke": "smoke", "none": "normal_ref", "person": "normal_ref",
           "fall": "other", "patient": "other"}
MIN_CELL = 20
CHUNK = 2000
NBOOT = 2000
BASE = "v1.0.8.0"
AGREE_MIN = 0.7
BANK_PER_CLASS = 40
TOP50 = 50
A2_ACC, A2_MF1 = 0.340, 0.426     # A2 시설입지 초안(재실행하지 않음, 숫자만 참조)
T0 = time.time()


def log(m):
    print(f"[{time.strftime('%H:%M:%S')} +{time.time() - T0:6.0f}s] {m}", flush=True)


def wcsv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    log(f"→ {path} {len(rows):,}행")


def setup_font():
    for f in glob.glob("/workspace/.fonts/*.tt[fc]"):
        fm.fontManager.addfont(f)
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.facecolor"] = BG
    plt.rcParams["axes.facecolor"] = BG


def despine(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def rank_pct(v):
    """작은 값이 0, 큰 값이 1 인 순위 백분위 (NaN 은 NaN)."""
    out = np.full(len(v), np.nan)
    ok = np.isfinite(v)
    o = np.argsort(v[ok], kind="stable")
    r = np.empty(ok.sum())
    r[o] = np.arange(ok.sum())
    out[ok] = r / max(ok.sum() - 1, 1)
    return out


# ═════════════════════════════ 1) 적재 ═════════════════════════════
setup_font()
os.makedirs(CSVD, exist_ok=True)
os.makedirs(FIGD, exist_ok=True)
_topk_selfcheck()
log("[SELF-CHECK] ✅ topk_vote 브루트포스 대조 통과")

conn = psycopg2.connect(DSN)
cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
NS = SENT.shape[0]
hashes = [None] * NS
for h, i in h2c.items():
    hashes[i] = h
log(f"문장 벡터 {SENT.shape}, L2 오차 max {abs(np.linalg.norm(SENT, axis=1) - 1).max():.2e}")

cur.execute("""
  WITH c AS (SELECT content_hash, class_label, count(*) n,
                    row_number() OVER (PARTITION BY content_hash
                                       ORDER BY count(*) DESC, class_label) rn,
                    sum(count(*)) OVER (PARTITION BY content_hash) tot
             FROM bank_sentences GROUP BY 1,2)
  SELECT c.content_hash, c.class_label, c.n::float8/c.tot, t.txt
  FROM c JOIN (SELECT content_hash, MIN(text) txt FROM bank_sentences GROUP BY 1) t
       USING(content_hash)
  WHERE c.rn = 1""")
maj_cls, agree, text = {}, {}, {}
for h, c, a, t in cur:
    maj_cls[h], agree[h], text[h] = c, float(a), t
assert set(maj_cls) == set(h2c), "bank_sentences 해시 집합 ≠ prompt 임베딩 해시 집합"
SCLS = np.array([maj_cls[h] for h in hashes])
SAGR = np.array([agree[h] for h in hashes], np.float32)
log(f"문장 클래스 다수결 {len(maj_cls):,}, 분포 {dict(collections.Counter(SCLS.tolist()))}")
log(f"GT 4클래스 문장 {int(np.isin(SCLS, CLASSES).sum()):,} / 합의도 ≥{AGREE_MIN} "
    f"{int((SAGR >= AGREE_MIN).sum()):,}")

# SMOKE 모드 — 문장 축소로 전 코드경로를 싸게 검증한다. 프레임 모집단·셀·자기검사는 전부 실물.
SMOKE = int(os.environ.get("SMOKE_SENT", "0"))
if SMOKE:
    _r0 = np.random.default_rng(7)
    _keep = []
    for _c in CLASSES:
        _cand = np.where((SCLS == _c) & (SAGR >= AGREE_MIN))[0]
        _keep.append(_r0.choice(_cand, min(max(SMOKE // 4, 120), len(_cand)), replace=False))
    _keep = np.unique(np.concatenate(_keep))
    SENT = SENT[_keep]
    hashes = [hashes[i] for i in _keep]
    SCLS, SAGR = SCLS[_keep], SAGR[_keep]
    NS = len(_keep)
    log(f"[SMOKE] ⚠️ 문장 {NS:,} 로 축소 — 코드경로 검증용, 수치를 결과로 읽지 말 것")

# ── 군집 (kmeans64) ──
cur.execute("SELECT entity_id, cluster_id, project FROM analysis.frame_cluster "
            "WHERE method='kmeans64'")
fc = {}
for eid, k, p in cur:
    fc[eid] = (int(k), p)
N_FC = len(fc)
assert N_FC == 90084, f"frame_cluster kmeans64 행수 {N_FC} ≠ 90,084"
log(f"[SELF-CHECK] ✅ frame_cluster kmeans64 {N_FC:,} 행 (기대 90,084)")
projects = sorted({p for _k, p in fc.values()})
pidx = {p: i for i, p in enumerate(projects)}
NP, NK = len(projects), 64
assert max(k for k, _p in fc.values()) == NK - 1
log(f"프로젝트 {NP}, 군집 {NK}")

# ── SAM3 약참조 (FiftyOne frames.normalized_class) ──
log("FiftyOne frames 약참조 적재…")
import fiftyone as fo  # noqa: E402
from fiftyone import ViewField as FF  # noqa: E402

_fr = fo.load_dataset("frames").match(FF("modality") == "frame")
_ent, _ncls = _fr.values(["entity_id", "normalized_class"])
sam3 = {}
for e, c in zip(_ent, _ncls):
    if e is not None:
        sam3[e] = (c or "none")
raw_dist = collections.Counter(sam3.get(e, "__missing__") for e in fc)
n_found = N_FC - raw_dist["__missing__"]
sam3_share = n_found / N_FC
log(f"SAM3 약참조 확보 {n_found:,}/{N_FC:,} = {sam3_share:.4f}; "
    f"원시 분포 {dict(raw_dist)}")
assert sam3_share >= 0.95, f"SAM3 클래스 확보율 {sam3_share:.4f} < 0.95"
log(f"[SELF-CHECK] ✅ SAM3 약참조 확보율 {sam3_share:.2%} ≥ 95%")
del _fr, _ent, _ncls

# ── 셀 인덱스 ──
ridx = {r: i for i, r in enumerate(REFS)}
NR = len(REFS)
pk_pairs = sorted({(p, k) for (k, p) in fc.values()}, key=lambda x: (pidx[x[0]], x[1]))
pkidx = {t: i for i, t in enumerate(pk_pairs)}
NPK = len(pk_pairs)
log(f"비어있지 않은 (프로젝트×군집) 셀 {NPK}")

CRS = np.zeros((NK * NR, NS), np.float64)     # 군집×참조클래스 합
CRN = np.zeros(NK * NR, np.int64)
PKS = np.zeros((NPK, NS), np.float64)         # 프로젝트×군집 합 (A_k / A_p / LOPO 전부 여기서)
PKN = np.zeros(NPK, np.int64)
GLOB = np.zeros(NS, np.float64)               # 전역 배경합 (독립 경로 — m_s 교차검증용)
log(f"누산기 CRS {CRS.nbytes / 2**20:.0f}MB + PKS {PKS.nbytes / 2**20:.0f}MB")


# ═════════════════════════════ 2) 단일 스트리밍 패스 ═════════════════════════════
def frame_stream(conn_, chunk):
    """군집 배정된 프레임만 흘린다 (서버사이드 커서, itersize 5000)."""
    with conn_.cursor(name="fr_emb") as c2:
        c2.itersize = 5000
        c2.execute("""
          SELECT ie.entity_id, ie.embedding::text
          FROM image_embeddings ie
          JOIN analysis.frame_cluster fc
            ON fc.entity_id = ie.entity_id AND fc.method = 'kmeans64'
          WHERE ie.entity_type = 'frame'
        """)
        bcell, bpk, bv = [], [], []
        for eid, vtxt in c2:
            hit = fc.get(eid)
            if hit is None:
                continue
            k, p = hit
            r = REF_MAP.get(sam3.get(eid, "__none__"), "unknown")
            bcell.append(k * NR + ridx[r])
            bpk.append(pkidx[(p, k)])
            bv.append(np.fromstring(vtxt.strip("[]"), sep=",", dtype=np.float32))
            if len(bv) >= chunk:
                yield np.asarray(bcell), np.asarray(bpk), np.vstack(bv)
                bcell, bpk, bv = [], [], []
        if bv:
            yield np.asarray(bcell), np.asarray(bpk), np.vstack(bv)


log("스트리밍 패스 시작 (프레임 × 문장 gemm)…")
n_seen = 0
for bcell, bpk, Fb in frame_stream(conn, CHUNK):
    Fb /= np.linalg.norm(Fb, axis=1, keepdims=True)
    S = Fb @ SENT.T                                       # (chunk, 121614) f32
    GLOB += S.sum(axis=0, dtype=np.float64)
    # 셀 합은 지시행렬 gemm 으로 — 부울 마스크 fancy indexing 은 청크 사본(최대 ~1GB)을 만든다.
    for grp, SUMS, CNTS in ((bcell, CRS, CRN), (bpk, PKS, PKN)):
        uq, inv = np.unique(grp, return_inverse=True)
        G = np.zeros((len(bcell), len(uq)), np.float32)
        G[np.arange(len(bcell)), inv] = 1.0
        SUMS[uq] += (G.T @ S).astype(np.float64)
        CNTS[uq] += np.bincount(inv, minlength=len(uq))
        del G
    n_seen += len(bcell)
    del S
    if n_seen % 20000 < CHUNK:
        log(f"  {n_seen:,}/{N_FC:,}")
log(f"패스 완료 {n_seen:,} 프레임")
assert n_seen == N_FC, f"패스 프레임수 {n_seen} ≠ {N_FC}"
assert CRN.sum() == N_FC and PKN.sum() == N_FC
log(f"[SELF-CHECK] ✅ 모든 군집 프레임이 임베딩과 셀을 가졌다 ({n_seen:,})")

# m_s 교차검증 (독립 누산 경로 3개)
m_s = GLOB / N_FC
m_s_cr = CRS.sum(axis=0) / N_FC
m_s_pk = PKS.sum(axis=0) / N_FC
_rng = np.random.default_rng(0)
probe = _rng.choice(NS, 5, replace=False)
d_cr = float(np.abs(m_s[probe] - m_s_cr[probe]).max())
d_pk = float(np.abs(m_s[probe] - m_s_pk[probe]).max())
log(f"[SELF-CHECK] m_s 표본 5 (문장 {probe.tolist()}): 직접 {np.round(m_s[probe], 6).tolist()}, "
    f"셀합 재구성 최대차 {d_cr:.2e}, 프로젝트×군집합 최대차 {d_pk:.2e}")
assert d_cr < 1e-4 and d_pk < 1e-4
log("[SELF-CHECK] ✅ m_s 재구성 오차 < 1e-4")

# ═════════════════════════════ 3) 셀 평균 / A_k / A_p ═════════════════════════════
A_cell = np.full((NK * NR, NS), np.nan, np.float32)
ok_cell = CRN >= MIN_CELL
A_cell[ok_cell] = (CRS[ok_cell] / CRN[ok_cell, None]).astype(np.float32)
del CRS

CKN = np.zeros(NK, np.int64)
CKS = np.zeros((NK, NS), np.float64)
PN = np.zeros(NP, np.int64)
PS = np.zeros((NP, NS), np.float64)
for (p, k), i in pkidx.items():
    CKN[k] += PKN[i]
    CKS[k] += PKS[i]
    PN[pidx[p]] += PKN[i]
    PS[pidx[p]] += PKS[i]
A_k = (CKS / CKN[:, None]).astype(np.float32).T            # (NS, NK)
A_p = (PS / PN[:, None]).astype(np.float32).T              # (NS, NP)
del CKS, PS
log(f"A_k {A_k.shape} 군집 프레임수 {CKN.min():,}~{CKN.max():,}; A_p {A_p.shape}")

# 특이도 z (기존 기준): R = A_k − rowmean, 군집별로 문장 방향 표준화
R_spec = A_k - A_k.mean(axis=1, keepdims=True)
SPEC_Z = (R_spec - R_spec.mean(axis=0)) / (R_spec.std(axis=0) + 1e-9)
SPEC_RAW = A_k - m_s[:, None].astype(np.float32)           # falldown 대체 기준
del R_spec

# 셀 표본표
cell_rows = []
for k in range(NK):
    for r in REFS:
        c = k * NR + ridx[r]
        if CRN[c]:
            cell_rows.append((k, r, int(CRN[c])))
ref_tot = {r: int(sum(CRN[k * NR + ridx[r]] for k in range(NK))) for r in REFS}
log(f"참조클래스 총 프레임 {ref_tot}")

# ═════════════════════════════ 4) C1 대비 ═════════════════════════════
qual = {e: [] for e in EVENTS}
for k in range(NK):
    n_nr = int(CRN[k * NR + ridx["normal_ref"]])
    for e in EVENTS:
        n_e = int(CRN[k * NR + ridx[e]])
        if n_e >= MIN_CELL and n_nr >= MIN_CELL:
            qual[e].append((k, n_e, n_nr))
clu_proj = {}
for k in range(NK):
    per = {p: int(PKN[i]) for (p, kk), i in pkidx.items() if kk == k}
    tot = sum(per.values())
    dom = max(per, key=per.get)
    clu_proj[k] = dict(n=tot, dominant_project=dom,
                       dominant_share=round(per[dom] / tot, 4), n_projects=len(per))
for e in EVENTS:
    log(f"대비 자격 군집 [{e}] {len(qual[e])}: "
        + ", ".join(f"k{k}(n_{e}={ne},n_norm={nn})" for k, ne, nn in qual[e]))
    log(f"  └ 자격 군집의 프로젝트 구성: "
        + ", ".join(f"k{k}→{clu_proj[k]['dominant_project']} "
                    f"{clu_proj[k]['dominant_share']:.1%}" for k, _ne, _nn in qual[e]))
_qp = {clu_proj[k]["dominant_project"] for e in EVENTS for k, _a, _b in qual[e]}
log(f"⚠️ 대비 신호의 출처 프로젝트 = {sorted(_qp)} — 이 밖의 현장에는 대비 근거가 없다")

CON = {}   # e -> (NS, n_qual) float32
for e in EVENTS:
    if not qual[e]:
        CON[e] = np.zeros((NS, 0), np.float32)
        continue
    cols = []
    for k, _ne, _nn in qual[e]:
        cols.append(A_cell[k * NR + ridx[e]] - A_cell[k * NR + ridx["normal_ref"]])
    CON[e] = np.stack(cols, axis=1)
mean_con = {e: (CON[e].mean(axis=1) if CON[e].shape[1] else np.full(NS, np.nan, np.float32))
            for e in EVENTS}
max_con = {e: (CON[e].max(axis=1) if CON[e].shape[1] else np.full(NS, np.nan, np.float32))
           for e in EVENTS}
nq = {e: CON[e].shape[1] for e in EVENTS}

# ── 4a) 군집×이벤트별 상위 10 (두 기준) + 상위 50 이벤트클래스 비중 + Spearman ──
rows_top, rows_cmp = [], []
share_c, share_s = {e: [] for e in EVENTS}, {e: [] for e in EVENTS}
for e in EVENTS:
    for j, (k, ne, nn) in enumerate(qual[e]):
        cv = CON[e][:, j]
        sv = SPEC_Z[:, k]
        rho = float(spearmanr(cv, sv).statistic)
        oc = np.argsort(-cv)
        os_ = np.argsort(-sv)
        sc = float((SCLS[oc[:TOP50]] == e).mean())
        ss = float((SCLS[os_[:TOP50]] == e).mean())
        share_c[e].append(sc)
        share_s[e].append(ss)
        for crit, order, val in (("contrast", oc, cv), ("specificity", os_, sv)):
            for rk, i in enumerate(order[:10]):
                rows_top.append((k, e, crit, rk + 1, round(float(cv[i]), 5),
                                 round(float(sv[i]), 3), SCLS[i], round(float(SAGR[i]), 3),
                                 round(float(m_s[i]), 4), text[hashes[i]]))
        top10c = SCLS[oc[:10]]
        top10s = SCLS[os_[:10]]
        rows_cmp.append((k, e, ne, nn, round(rho, 4), round(sc, 3), round(ss, 3),
                         int((top10c == e).sum()), int((top10c == "normal").sum()),
                         int((top10c == [x for x in EVENTS if x != e][0]).sum()),
                         int((top10c == "falldown").sum()),
                         int((top10s == e).sum()), int((top10s == "normal").sum()),
                         int((top10s == [x for x in EVENTS if x != e][0]).sum()),
                         int((top10s == "falldown").sum())))
for e in EVENTS:
    if share_c[e]:
        log(f"[핵심] {e}: top-50 이벤트클래스 비중 대비 {np.mean(share_c[e]):.3f} "
            f"({min(share_c[e]):.2f}~{max(share_c[e]):.2f}) vs 특이도 {np.mean(share_s[e]):.3f} "
            f"({min(share_s[e]):.2f}~{max(share_s[e]):.2f})")
rho_all = [r[4] for r in rows_cmp]
log(f"Spearman(대비, 특이도) 군집×이벤트 {len(rho_all)}개: 중앙 {np.median(rho_all):.3f} "
    f"({min(rho_all):.3f}~{max(rho_all):.3f})")

# ── 4b) 이식성 문장 — 여러 군집의 대비 상위에 동시 등장하는 문장 ──
# ⚠️ 자격 군집이 fire 3 / smoke 4 개뿐이라 "≥3 군집" 은 거의 전원일치 요구다.
#    폭(top-50 / top-200) × 임계(2 / 3) 를 전부 내고 해석은 보고서에서 한다.
portable, portable200, port_stats = {}, {}, {}
for e in EVENTS:
    cnt50 = np.zeros(NS, np.int32)
    cnt200 = np.zeros(NS, np.int32)
    for j in range(nq[e]):
        o = np.argsort(-CON[e][:, j])
        cnt50[o[:TOP50]] += 1
        cnt200[o[:200]] += 1
    portable[e], portable200[e] = cnt50, cnt200
    port_stats[e] = {}
    for wname, cnt in (("top50", cnt50), ("top200", cnt200)):
        for thr_ in (2, 3):
            m = cnt >= thr_
            port_stats[e][f"{wname}_ge{thr_}"] = dict(
                n=int(m.sum()),
                by_class={k: int(v) for k, v in collections.Counter(SCLS[m].tolist()).items()})
            log(f"{e}: {wname} 대비에 ≥{thr_} 군집 등장 문장 {int(m.sum()):,} "
                f"(자격군집 {nq[e]}개, 클래스 {dict(collections.Counter(SCLS[m].tolist()))})")

# ── 4c) 문장 클래스별 대비 분포 ──
con_dist = {}
for e in EVENTS:
    for c in CLASSES:
        m = SCLS == c
        v = mean_con[e][m]
        v = v[np.isfinite(v)]
        con_dist[f"{e}|{c}"] = dict(n=int(len(v)), mean=float(v.mean()),
                                    p90=float(np.percentile(v, 90)),
                                    p10=float(np.percentile(v, 10)),
                                    median=float(np.median(v)))
        log(f"대비[{e}] 문장클래스 {c:<9} n={len(v):>7,} mean={v.mean():+.5f} "
            f"p90={np.percentile(v, 90):+.5f}")

# ═════════════════════════════ 5) 원장 CSV ═════════════════════════════
wcsv(f"{CSVD}/30_contrast_ledger.csv",
     ["content_hash(문장해시)", "class(다수결클래스)", "class_agreement(클래스합의도)",
      "m_s(전역배경평균코사인)", "mean_contrast_fire(fire대비평균)",
      "mean_contrast_smoke(smoke대비평균)", "n_clusters_fire(fire자격군집수)",
      "n_clusters_smoke(smoke자격군집수)", "max_contrast_fire(fire대비최대)",
      "max_contrast_smoke(smoke대비최대)", "mean_specificity(특이도z평균)",
      "n_top50_fire(fire상위50등장군집수)", "n_top50_smoke(smoke상위50등장군집수)",
      "n_top200_fire(fire상위200등장군집수)", "n_top200_smoke(smoke상위200등장군집수)",
      "text(문장)"],
     [(hashes[i], SCLS[i], round(float(SAGR[i]), 3), round(float(m_s[i]), 5),
       round(float(mean_con["fire"][i]), 5), round(float(mean_con["smoke"][i]), 5),
       nq["fire"], nq["smoke"], round(float(max_con["fire"][i]), 5),
       round(float(max_con["smoke"][i]), 5), round(float(SPEC_Z[i].mean()), 4),
       int(portable["fire"][i]), int(portable["smoke"][i]),
       int(portable200["fire"][i]), int(portable200["smoke"][i]), text[hashes[i]])
      for i in range(NS)])
wcsv(f"{CSVD}/30b_cluster_top_contrast.csv",
     ["cluster(군집)", "event(이벤트)", "criterion(기준)", "rank(순위)", "contrast(대비)",
      "specificity_z(특이도z)", "class(문장클래스)", "class_agreement(합의도)",
      "m_s(전역배경)", "text(문장)"], rows_top)
wcsv(f"{CSVD}/30c_contrast_vs_specificity.csv",
     ["cluster(군집)", "event(이벤트)", "n_event_frames(이벤트프레임)",
      "n_normal_frames(정상측프레임)", "spearman_contrast_vs_specificity(순위상관)",
      "share_event_class_top50_contrast(대비상위50이벤트비중)",
      "share_event_class_top50_specificity(특이도상위50이벤트비중)",
      "c_top10_event(대비상위10중이벤트)", "c_top10_normal(대비상위10중normal)",
      "c_top10_other_event(대비상위10중타이벤트)", "c_top10_falldown(대비상위10중falldown)",
      "s_top10_event(특이도상위10중이벤트)", "s_top10_normal(특이도상위10중normal)",
      "s_top10_other_event(특이도상위10중타이벤트)",
      "s_top10_falldown(특이도상위10중falldown)"], rows_cmp)
wcsv(f"{CSVD}/30f_cell_support.csv",
     ["cluster(군집)", "ref_class(참조클래스)", "n_frames(프레임수)"], cell_rows)

# ═════════════════════════════ 6) 대비 선별 미니뱅크 → sourcei GT ═════════════════════════════
def pick(mask, score, n):
    idx = np.where(mask & np.isfinite(score))[0]
    return idx[np.argsort(-score[idx])[:n]].tolist()


base_ok = (SAGR >= AGREE_MIN)
sel, sel_note = {}, {}
for e in EVENTS:
    sel[e] = pick(base_ok & (SCLS == e), mean_con[e], BANK_PER_CLASS)
    sel_note[e] = f"군집 {nq[e]}개 평균 대비 상위 (다수결 {e}, 합의도 ≥{AGREE_MIN})"
sel["falldown"] = pick(base_ok & (SCLS == "falldown"), SPEC_RAW.max(axis=1), BANK_PER_CLASS)
sel_note["falldown"] = ("SAM3 참조 없음 → **대체 기준**: max_k(A_k[s,k] − m_s[s]) 특이도 상위 "
                        f"(다수결 falldown, 합의도 ≥{AGREE_MIN})")
# normal: 대비가 0 에 가장 가깝고 m_s 가 높은 '일반 정상 앵커'
abs_con = np.nanmean(np.abs(np.stack([mean_con[e] for e in EVENTS], 1)), 1)
nmask = base_ok & (SCLS == "normal") & np.isfinite(abs_con)
thr = float(np.percentile(abs_con[nmask], 25))
sel["normal"] = pick(nmask & (abs_con <= thr), m_s.astype(np.float32), BANK_PER_CLASS)
sel_note["normal"] = (f"|평균대비| 하위 25%(≤{thr:.5f}) 안에서 m_s 상위 "
                      f"(다수결 normal, 합의도 ≥{AGREE_MIN})")
for c in CLASSES:
    assert len(sel[c]) == BANK_PER_CLASS, (c, len(sel[c]))
    log(f"미니뱅크 {c}: {len(sel[c])}문장 — {sel_note[c]}")
    for i in sel[c][:3]:
        log(f"    con_f={mean_con['fire'][i]:+.4f} con_s={mean_con['smoke'][i]:+.4f} "
            f"m_s={m_s[i]:.4f} | {text[hashes[i]][:80]}")
bank_cols = np.asarray([i for c in CLASSES for i in sel[c]])
bank_lab = np.asarray([CLASSES.index(c) for c in CLASSES for _ in sel[c]], np.int32)
assert len(bank_cols) == 160 and len(set(bank_cols.tolist())) == 160, "미니뱅크 중복/개수 오류"

log("sourcei 적재…")
hds = fo.load_dataset("sourcei")
hids, hemb, hgt, hcam = hds.values(["id", "embedding", "ground_truth.label", "camera"])
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
assert list(hids) == list(d["ids"]), "sourcei id 순서가 preds.npz 와 다르다"
log("[SELF-CHECK] ✅ sourcei id 순서 == preds.npz")
Fh = np.asarray(hemb, np.float32)
Fh /= np.linalg.norm(Fh, axis=1, keepdims=True)
gt = np.asarray([CLASSES.index(x) for x in hgt], np.int8)
assert (gt == d["gt"]).all()
cam = np.asarray(hcam)
cams = np.unique(cam)
cidx = np.searchsorted(cams, cam)
base_pred = d[f"topk__{BASE}"]
base_acc = float((base_pred == gt).mean())
log(f"[SELF-CHECK] {BASE} top-K 정확도 {base_acc:.4f} (기대 0.706±0.001), "
    f"카메라 {len(cams)}, GT {dict(zip(CLASSES, np.bincount(gt, minlength=4).tolist()))}")
assert abs(base_acc - 0.706) <= 0.001, base_acc
log("[SELF-CHECK] ✅ v1.0.8.0 기준선 0.706 재현")


def macro_f1(g, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((p == c) & (g == c)).sum(); fp = ((p == c) & (g != c)).sum()
        fn = ((p != c) & (g == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


def tabulate(p):
    T = np.zeros((len(cams), 3, 3), np.float64)
    for kk, c in enumerate((1, 2, 3)):
        pc, gc = (p == c), (gt == c)
        np.add.at(T[:, kk, 0], cidx[pc & gc], 1)
        np.add.at(T[:, kk, 1], cidx[pc & ~gc], 1)
        np.add.at(T[:, kk, 2], cidx[~pc & gc], 1)
    return T


def mf1_tab(T):
    tp, fp, fn = T[..., 0], T[..., 1], T[..., 2]
    pr = tp / np.maximum(tp + fp, 1); rc = tp / np.maximum(tp + fn, 1)
    return (2 * pr * rc / np.maximum(pr + rc, 1e-12)).mean(-1)


BR = np.random.default_rng(0)
pick_b = BR.integers(0, len(cams), size=(NBOOT, len(cams)))
Wm = (pick_b[:, :, None] == np.arange(len(cams))[None, None, :]).sum(1).astype(np.float64)
Tb = tabulate(base_pred)
assert abs(mf1_tab(Tb.sum(0)) - macro_f1(gt, base_pred)) < 1e-9, "표 기반 macro-F1 ≠ 직접"
idx_by_cam = [np.where(cidx == i)[0] for i in range(len(cams))]
_bt = np.einsum("bc,cij->bij", Wm[:20], Tb)
_direct = np.array([macro_f1(gt[i], base_pred[i]) for i in
                    [np.concatenate([idx_by_cam[j] for j in pick_b[b]]) for b in range(20)]])
assert np.allclose(mf1_tab(_bt), _direct, atol=1e-9), "부트스트랩 표 근사 ≠ 직접 재표집"
log("[SELF-CHECK] ✅ 카메라 표 기반 부트스트랩 == 인덱스 재표집(20표본)")

Sc = SENT[bank_cols]
con_pred = np.empty(len(Fh), np.int8)
for s in range(0, len(Fh), 1000):
    con_pred[s:s + 1000] = topk_vote(Fh[s:s + 1000] @ Sc.T, bank_lab, 4, k=RULE_K)
boot = {"contrast160": mf1_tab(np.einsum("bc,cij->bij", Wm, tabulate(con_pred))),
        BASE: mf1_tab(np.einsum("bc,cij->bij", Wm, Tb))}
dd = boot["contrast160"] - boot[BASE]
eval_rows, point = [], {}
for name, p in (("contrast160", con_pred), (BASE, base_pred)):
    point[name] = dict(acc=float((p == gt).mean()), mf1=macro_f1(gt, p),
                       rec={c: float((p[gt == CLASSES.index(c)] == CLASSES.index(c)).mean())
                            for c in CLASSES},
                       prec={c: float((gt[p == CLASSES.index(c)] == CLASSES.index(c)).mean())
                             if (p == CLASSES.index(c)).any() else 0.0 for c in CLASSES})
    log(f"{name:<12} acc {point[name]['acc']:.4f} eventMacroF1 {point[name]['mf1']:.4f} "
        + " ".join(f"{c[:4]} R{point[name]['rec'][c]:.3f}" for c in CLASSES))
CI = dict(d=point["contrast160"]["mf1"] - point[BASE]["mf1"], mean=float(dd.mean()),
          lo=float(np.percentile(dd, 2.5)), hi=float(np.percentile(dd, 97.5)),
          p_gt0=float((dd > 0).mean()))
log(f"Δ eventMacroF1(대비뱅크 − {BASE}) = {CI['d']:+.4f} "
    f"[부트 95% {CI['lo']:+.4f}, {CI['hi']:+.4f}], P(Δ>0)={CI['p_gt0']:.3f}")
log(f"A2 시설입지 초안(참조 숫자, 재실행 안 함): acc {A2_ACC:.3f} / macroF1 {A2_MF1:.3f} "
    f"→ 대비뱅크 acc {point['contrast160']['acc']:.3f} / macroF1 {point['contrast160']['mf1']:.3f}")
for name in ("contrast160", BASE):
    eval_rows.append((name, 160 if name == "contrast160" else "",
                      round(point[name]["acc"], 4), round(point[name]["mf1"], 4),
                      *[round(point[name]["rec"][c], 4) for c in CLASSES],
                      *[round(point[name]["prec"][c], 4) for c in CLASSES],
                      round(CI["d"], 4) if name == "contrast160" else "",
                      round(CI["lo"], 4) if name == "contrast160" else "",
                      round(CI["hi"], 4) if name == "contrast160" else "",
                      round(CI["p_gt0"], 4) if name == "contrast160" else ""))
eval_rows.append(("A2_facility_location(참조)", 80, A2_ACC, A2_MF1, "", "", "", "",
                  "", "", "", "", "", "", "", ""))
wcsv(f"{CSVD}/30d_contrast_bank_eval.csv",
     ["bank(뱅크)", "n_sentences(문장수)", "acc(정확도)", "event_macro_f1(이벤트macroF1)",
      *[f"recall_{c}(재현율)" for c in CLASSES], *[f"precision_{c}(정밀도)" for c in CLASSES],
      "d_mf1_vs_v1080(ΔmacroF1)", "ci_lo(2.5%)", "ci_hi(97.5%)", "p_gt0(PΔ>0)"], eval_rows)

# ═════════════════════════════ 7) C2 — 군집설명분 vs 프로젝트 잔차 ═════════════════════════════
W = np.zeros((NP, NK), np.float64)
for (p, k), i in pkidx.items():
    W[pidx[p], k] = PKN[i]
W /= W.sum(axis=1, keepdims=True)
assert np.allclose(W.sum(axis=1), 1.0), "Σ_k w[p,k] ≠ 1"
log(f"[SELF-CHECK] ✅ Σ_k w[p,k] == 1 (전 {NP} 프로젝트, 최대오차 "
    f"{abs(W.sum(1) - 1).max():.2e})")
A_hat = (A_k @ W.T).astype(np.float32)                    # (NS, NP)
RES = A_p - A_hat
res_sd = RES.std(axis=1, ddof=0)
res_max_i = np.argmax(np.abs(RES), axis=1)
res_max = np.abs(RES[np.arange(NS), res_max_i])
log(f"잔차 RES {RES.shape}: sd 중앙 {np.median(res_sd):.5f}, "
    f"|R|max 중앙 {np.median(res_max):.5f}")
res_by_cls = {}
for c in CLASSES:
    m = SCLS == c
    res_by_cls[c] = dict(n=int(m.sum()), mean_sd=float(res_sd[m].mean()),
                         median_sd=float(np.median(res_sd[m])),
                         p90_sd=float(np.percentile(res_sd[m], 90)))
    log(f"잔차sd 클래스 {c:<9} n={int(m.sum()):>7,} mean={res_sd[m].mean():.5f} "
        f"median={np.median(res_sd[m]):.5f} p90={np.percentile(res_sd[m], 90):.5f}")
con_all = np.nanmean(np.stack([mean_con[e] for e in EVENTS], 1), 1)
rho_ms = float(spearmanr(res_sd, m_s).statistic)
rho_con = float(spearmanr(res_sd[np.isfinite(con_all)], con_all[np.isfinite(con_all)]).statistic)
log(f"Spearman(잔차sd, m_s) = {rho_ms:+.4f}; Spearman(잔차sd, 평균대비) = {rho_con:+.4f}")

# LOPO — |R| 상위 200 문장
top200 = np.argsort(-res_max)[:200]
lopo_naive, lopo_corr = [], []
sub = top200
PKS_sub = PKS[:, sub]                                     # (NPK, 200)
CKS_sub = np.zeros((NK, len(sub)), np.float64)
for (p, k), i in pkidx.items():
    CKS_sub[k] += PKS_sub[i]
for p in projects:
    pi = pidx[p]
    num = CKS_sub.copy()
    den = CKN.astype(np.float64).copy()
    for k in range(NK):
        i = pkidx.get((p, k))
        if i is not None:
            num[k] -= PKS_sub[i]
            den[k] -= PKN[i]
    good = den > 0
    Akm = np.full((NK, len(sub)), np.nan)
    Akm[good] = num[good] / den[good, None]
    w = W[pi]
    wm = w * good
    tot = wm.sum()
    if tot <= 0:
        continue
    hat = (wm[None, :] @ np.nan_to_num(Akm))[0] / tot
    lopo_naive.append(A_p[sub, pi] - A_hat[sub, pi])
    lopo_corr.append(A_p[sub, pi] - hat)
LN = np.abs(np.stack(lopo_naive, 1))
LC = np.abs(np.stack(lopo_corr, 1))
lopo = dict(n_sentences=len(sub), mean_abs_naive=float(LN.mean()),
            mean_abs_lopo=float(LC.mean()),
            sd_naive=float(np.stack(lopo_naive, 1).std(axis=1, ddof=0).mean()),
            sd_lopo=float(np.stack(lopo_corr, 1).std(axis=1, ddof=0).mean()))
lopo["ratio_lopo_over_naive"] = lopo["mean_abs_lopo"] / max(lopo["mean_abs_naive"], 1e-12)
log(f"LOPO(|R| 상위 200 문장 × {LN.shape[1]} 프로젝트): 평균|R| naive {lopo['mean_abs_naive']:.5f} "
    f"→ LOPO {lopo['mean_abs_lopo']:.5f} (배율 {lopo['ratio_lopo_over_naive']:.3f}); "
    f"잔차sd {lopo['sd_naive']:.5f} → {lopo['sd_lopo']:.5f}")

proj_specific = np.argsort(-res_sd)[:20]
# 이벤트 문장의 "자기 클래스 대비" (fire 문장은 fire 대비, smoke 문장은 smoke 대비)
own_con = np.full(NS, np.nan, np.float32)
for e in EVENTS:
    own_con[SCLS == e] = mean_con[e][SCLS == e]
# 이식성 = 자기클래스 대비 높음 + 프로젝트 잔차 작음. portable 카운트에 의존시키지 않는다
# (자격 군집이 3~4개뿐이라 카운트 임계는 거의 항상 0건이 된다 — 위 FIX 1 참조).
ev_mask = np.isin(SCLS, EVENTS) & (SAGR >= AGREE_MIN) & np.isfinite(own_con) & (own_con > 0)
score_port = (rank_pct(np.where(ev_mask, own_con, np.nan))
              - rank_pct(np.where(ev_mask, res_sd, np.nan)))
score_port[~ev_mask] = -np.inf
portable_top = np.argsort(-score_port)[:20]
port_by_event = {}
for e in EVENTS:
    m = ev_mask & (SCLS == e)
    sc = np.where(m, score_port, -np.inf)
    port_by_event[e] = np.argsort(-sc)[:20].tolist()
log(f"이식성 후보 이벤트 문장 {int(ev_mask.sum()):,} (자기클래스 대비 > 0, 합의도 ≥{AGREE_MIN})")
log("가장 프로젝트 특이적인 문장 5:")
for i in proj_specific[:5]:
    log(f"  sd={res_sd[i]:.5f} maxproj={projects[res_max_i[i]]} cls={SCLS[i]} | "
        f"{text[hashes[i]][:78]}")
for e in EVENTS:
    log(f"가장 이식성 높은 {e} 문장 5:")
    for i in port_by_event[e][:5]:
        log(f"  sd={res_sd[i]:.5f} con_{e}={mean_con[e][i]:+.4f} n_top50={int(portable[e][i])}/"
            f"{nq[e]} n_top200={int(portable200[e][i])}/{nq[e]} | {text[hashes[i]][:74]}")
log("가장 이식성 높은 이벤트 문장 5 (fire·smoke 통합):")
for i in portable_top[:5]:
    log(f"  sd={res_sd[i]:.5f} con={own_con[i]:+.4f} cls={SCLS[i]} | {text[hashes[i]][:78]}")

wcsv(f"{CSVD}/30e_project_residual.csv",
     ["content_hash(문장해시)", "class(다수결클래스)", "class_agreement(합의도)",
      "m_s(전역배경평균코사인)", "residual_sd(잔차표준편차)", "max_abs_residual(최대절대잔차)",
      "project_of_max(최대잔차프로젝트)", "mean_contrast_fire(fire대비평균)",
      "mean_contrast_smoke(smoke대비평균)", "portability_score(이식성점수)", "text(문장)"],
     [(hashes[i], SCLS[i], round(float(SAGR[i]), 3), round(float(m_s[i]), 5),
       round(float(res_sd[i]), 6), round(float(res_max[i]), 6), projects[res_max_i[i]],
       round(float(mean_con["fire"][i]), 5), round(float(mean_con["smoke"][i]), 5),
       (round(float(score_port[i]), 4) if np.isfinite(score_port[i]) else ""),
       text[hashes[i]]) for i in range(NS)])

# ═════════════════════════════ 8) 그림 ═════════════════════════════
# f34 — 대비 vs 특이도
fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.4))
ax = axes[0]
xs = []
for e in EVENTS:
    for j, (k, _ne, _nn) in enumerate(qual[e]):
        xs.append((e, k, share_s[e][j], share_c[e][j]))
for (e, k, ss, sc) in xs:
    ax.plot([0, 1], [ss, sc], "-", color="#c3c2b7", lw=0.9, zorder=1)
    ax.plot([0], [ss], "o", color="#8a887f", ms=5, zorder=2)
    ax.plot([1], [sc], "o", color=CLS_COL[e], ms=5, zorder=3)
ax.set_xlim(-0.25, 1.25)
ax.set_xticks([0, 1])
ax.set_xticklabels(["특이도(기존)", "대비(신규)"])
ax.set_ylabel("상위 50 문장 중 해당 이벤트 클래스 비중")
mc = np.mean([x[3] for x in xs]); ms_ = np.mean([x[2] for x in xs])
ax.set_title(f"군집×이벤트 {len(xs)}쌍: 이벤트 문장 비중 {ms_:.2f} → {mc:.2f}\n"
             f"(fire={len(qual['fire'])}군집·빨강, smoke={len(qual['smoke'])}군집·남색)",
             loc="left", fontsize=11)
despine(ax)
ax = axes[1]
data, labs, cols = [], [], []
for c in CLASSES:
    v = con_all[(SCLS == c) & np.isfinite(con_all)]
    data.append(v); labs.append(f"{c}\n(n={len(v):,})"); cols.append(CLS_COL[c])
bp = ax.boxplot(data, showfliers=False, patch_artist=True, widths=0.55)
ax.set_xticks(np.arange(1, len(labs) + 1))
ax.set_xticklabels(labs)
for b, c in zip(bp["boxes"], cols):
    b.set_facecolor(c); b.set_alpha(0.75); b.set_edgecolor("#55534d")
for med in bp["medians"]:
    med.set_color("#2b2a26")
ax.axhline(0, color="#2b2a26", lw=0.9, ls="--")
nm = con_all[(SCLS == "normal") & np.isfinite(con_all)].mean()
ax.set_ylabel("평균 대비 (이벤트셀 − 정상셀 코사인)")
ax.set_title(f"문장 클래스별 대비 분포 — normal 평균 {nm:+.5f} (0 근처가 정상)",
             loc="left", fontsize=11)
despine(ax)
fig.tight_layout()
fig.savefig(f"{FIGD}/f34_contrast_vs_specificity.png", dpi=140, facecolor=BG)
plt.close(fig)
log(f"→ {FIGD}/f34_contrast_vs_specificity.png")

# f35 — sourcei 평가
fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.2))
ax = axes[0]
ax.hist(dd, bins=60, color="#4a3aa7", alpha=0.72)
ax.axvline(0, color="#2b2a26", lw=1.0, ls="--")
ax.axvline(CI["d"], color="#e34948", lw=1.6)
ax.axvspan(CI["lo"], CI["hi"], color="#eda100", alpha=0.20)
ax.set_xlabel(f"Δ 이벤트 macro-F1 (대비뱅크 − {BASE})")
ax.set_ylabel(f"부트스트랩 표본 수 (카메라 {len(cams)}개 재표집, {NBOOT:,}회)")
ax.set_title(f"Δ macro-F1 = {CI['d']:+.4f} [95% {CI['lo']:+.4f}, {CI['hi']:+.4f}], "
             f"P(Δ>0)={CI['p_gt0']:.3f}", loc="left", fontsize=11)
despine(ax)
ax = axes[1]
x = np.arange(len(CLASSES)); w = 0.28
ax.bar(x - w, [point["contrast160"]["rec"][c] for c in CLASSES], w,
       color="#4a3aa7", label=f"대비뱅크 160문장 (acc {point['contrast160']['acc']:.3f})")
ax.bar(x, [point[BASE]["rec"][c] for c in CLASSES], w,
       color="#8a887f", label=f"{BASE} (acc {point[BASE]['acc']:.3f})")
ax.axhline(A2_MF1, color="#e34948", lw=1.2, ls=":",
           label=f"A2 시설입지 초안 macroF1 {A2_MF1:.3f} / acc {A2_ACC:.3f}")
ax.set_xticks(x); ax.set_xticklabels(CLASSES)
ax.set_ylabel("클래스별 재현율 (sourcei GT 7,498프레임)")
ax.set_title(f"대비뱅크 macroF1 {point['contrast160']['mf1']:.3f} vs "
             f"{BASE} {point[BASE]['mf1']:.3f} vs A2 {A2_MF1:.3f}", loc="left", fontsize=11)
ax.legend(fontsize=8, frameon=False)
despine(ax)
fig.tight_layout()
fig.savefig(f"{FIGD}/f35_contrast_bank_eval.png", dpi=140, facecolor=BG)
plt.close(fig)
log(f"→ {FIGD}/f35_contrast_bank_eval.png")

# f35b — 잔차 sd vs 평균 대비
fig, ax = plt.subplots(figsize=(9.2, 6.0))
RG = np.random.default_rng(1)
for c in ["normal", "falldown", "smoke", "fire"]:
    m = (SCLS == c) & np.isfinite(con_all)
    idx = np.where(m)[0]
    if len(idx) > 12000:
        idx = RG.choice(idx, 12000, replace=False)
    ax.scatter(con_all[idx], res_sd[idx], s=3, alpha=0.28, color=CLS_COL[c],
               label=f"{c} (n={int(m.sum()):,})", linewidths=0)
ax.axvline(0, color="#2b2a26", lw=0.8, ls="--")
q_con = float(np.nanpercentile(con_all[np.isin(SCLS, EVENTS)], 90))
q_sd = float(np.percentile(res_sd, 25))
ax.axvline(q_con, color="#55534d", lw=0.8, ls=":")
ax.axhline(q_sd, color="#55534d", lw=0.8, ls=":")
ax.annotate(f"이식성 좋은 이벤트 문장 코너\n(대비 ≥ p90 {q_con:+.3f}, 잔차sd ≤ p25 {q_sd:.4f})",
            xy=(q_con, q_sd), xycoords="data", xytext=(0.02, 0.93),
            textcoords="axes fraction", fontsize=9, ha="left",
            arrowprops=dict(arrowstyle="->", color="#2b2a26", lw=0.9,
                            connectionstyle="arc3,rad=-0.2"))
ax.set_xlabel("평균 대비 (fire·smoke 자격군집 평균)")
ax.set_ylabel("프로젝트 잔차 표준편차 sd_p(R[s,p])")
ax.set_title(f"잔차 = 군집구성으로 설명 안 되는 프로젝트 고유분 — "
             f"ρ(잔차sd, 대비)={rho_con:+.3f}, ρ(잔차sd, m_s)={rho_ms:+.3f}",
             loc="left", fontsize=11)
ax.legend(fontsize=9, frameon=False, markerscale=4, loc="lower left")
despine(ax)
fig.tight_layout()
fig.savefig(f"{FIGD}/f35b_project_residual.png", dpi=140, facecolor=BG)
plt.close(fig)
log(f"→ {FIGD}/f35b_project_residual.png")

# ═════════════════════════════ 9) 요약 JSON ═════════════════════════════
summary = dict(
    generated_at=time.strftime("%Y-%m-%d %H:%M:%S"),
    funnel=dict(frames_clustered=N_FC, projects=NP, clusters=NK,
                sam3_class_share=round(sam3_share, 4),
                raw_normalized_class={k: int(v) for k, v in raw_dist.items()},
                frames_per_ref_class=ref_tot, min_cell=MIN_CELL,
                cells_ge_min=int(ok_cell.sum()), cells_nonempty=int((CRN > 0).sum()),
                qualifying_clusters={e: [dict(cluster=k, n_event=ne, n_normal_ref=nn,
                                              **clu_proj[k]) for k, ne, nn in qual[e]]
                                     for e in EVENTS},
                qualifying_source_projects=sorted(_qp),
                cluster_project_composition={str(k): clu_proj[k] for k in range(NK)},
                n_qualifying={e: nq[e] for e in EVENTS}),
    selfchecks=dict(m_s_max_diff_cellsum=d_cr, m_s_max_diff_pksum=d_pk,
                    base_acc=base_acc, sum_w_max_err=float(abs(W.sum(1) - 1).max())),
    c1=dict(share_event_top50_contrast={e: dict(mean=float(np.mean(share_c[e])),
                                                min=float(min(share_c[e])),
                                                max=float(max(share_c[e]))) for e in EVENTS},
            share_event_top50_specificity={e: dict(mean=float(np.mean(share_s[e])),
                                                   min=float(min(share_s[e])),
                                                   max=float(max(share_s[e]))) for e in EVENTS},
            spearman_contrast_vs_specificity=dict(median=float(np.median(rho_all)),
                                                  min=float(min(rho_all)),
                                                  max=float(max(rho_all)), n=len(rho_all)),
            contrast_by_sentence_class=con_dist,
            portable_counts=port_stats,
            portable_examples={e: [dict(text=text[hashes[i]], cls=SCLS[i],
                                        mean_contrast=float(mean_con[e][i]),
                                        n_clusters_top50=int(portable[e][i]),
                                        residual_sd=float(res_sd[i]))
                                   for i in np.argsort(-np.where(
                                       (SCLS == e) & (SAGR >= AGREE_MIN),
                                       mean_con[e], -np.inf))[:8]] for e in EVENTS}),
    bank=dict(n=160, per_class=BANK_PER_CLASS, notes=sel_note,
              sentences={c: [dict(hash=hashes[i], text=text[hashes[i]],
                                  mean_contrast_fire=float(mean_con["fire"][i]),
                                  mean_contrast_smoke=float(mean_con["smoke"][i]),
                                  m_s=float(m_s[i]),
                                  spec_raw_max=float(SPEC_RAW[i].max()))
                             for i in sel[c]] for c in CLASSES}),
    sourcei_eval=dict(n=int(len(gt)), cameras=int(len(cams)), nboot=NBOOT,
                      point={k: v for k, v in point.items()}, ci=CI,
                      a2_reference=dict(acc=A2_ACC, event_macro_f1=A2_MF1,
                                        note="A2 시설입지 초안 — 재실행하지 않고 숫자만 참조")),
    c2=dict(residual_sd_by_class=res_by_cls, spearman_res_sd_vs_m_s=rho_ms,
            spearman_res_sd_vs_contrast=rho_con, lopo=lopo,
            self_inclusion_note=("Â[s,p] 의 A_k 에 project p 의 프레임이 포함된다(자기포함). "
                                 "그래서 naive 잔차는 과소추정이고 LOPO 가 참값에 가깝다."),
            most_project_specific=[dict(text=text[hashes[i]], cls=SCLS[i],
                                        residual_sd=float(res_sd[i]),
                                        max_abs_residual=float(res_max[i]),
                                        project_of_max=projects[res_max_i[i]])
                                   for i in proj_specific],
            n_portable_candidates=int(ev_mask.sum()),
            most_portable_event=[dict(text=text[hashes[i]], cls=SCLS[i],
                                      residual_sd=float(res_sd[i]),
                                      mean_contrast_own=float(own_con[i]))
                                 for i in portable_top],
            most_portable_by_event={e: [dict(text=text[hashes[i]], cls=SCLS[i],
                                             residual_sd=float(res_sd[i]),
                                             mean_contrast=float(mean_con[e][i]),
                                             n_clusters_top50=int(portable[e][i]),
                                             n_clusters_top200=int(portable200[e][i]),
                                             n_qualifying=nq[e],
                                             class_agreement=float(SAGR[i]))
                                        for i in port_by_event[e]] for e in EVENTS}),
    project_weights={p: {str(k): round(float(W[pidx[p], k]), 5)
                         for k in np.where(W[pidx[p]] > 0)[0]} for p in projects},
)
with open(f"{OUT}/contrast_affinity_summary.json", "w") as f:
    json.dump(summary, f, ensure_ascii=False, indent=1)
log(f"→ {OUT}/contrast_affinity_summary.json")
log(f"총 {time.time() - T0:.0f}s")
print("DONE", flush=True)
