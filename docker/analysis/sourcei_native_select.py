#!/usr/bin/env python3
"""sourcei-네이티브 배경통계 + **클래스별 정렬 방향**으로 뱅크를 다시 뽑아 검정한다.

`sourcei_native_stats.py` 가 낸 실측(카메라 중심화 y 기준 spearman):

    클래스     sd_frames(현행)   sd_sourcei(연결 후)
    normal      +0.014            **-0.717**
    falldown    +0.052            +0.033
    fire        +0.456            +0.674
    smoke       +0.182            **+0.882**
    전체        -0.415            +0.337

두 가지가 드러난다.
  ① 현행 `sd_frames` 는 장소를 통제하면 판별력과 **음의 상관**이다(-0.415).
     그런데 선택은 `sd_frames` **상위**를 취한다 → 방향이 반대다.
  ② `sd_sourcei` 는 양의 상관이지만 **normal 만 부호가 반대**(-0.717)다.
     좋은 normal 문장은 오히려 비특이적이어야 한다는 뜻 — 전 클래스 공통 "상위 취함" 규칙이 틀렸다.

그래서 네 가지를 같은 판정 설정(§23 승리본)에서 비교한다. 필터 신호만 바꾼다.
  base       : sd_frames 상위            (현행)
  hy         : sd_sourcei 상위           (모수만 교체)
  hy_signed  : sd_sourcei, **클래스별 부호**(normal 은 하위)  ← 실측이 가리키는 규칙
  fr_signed  : sd_frames,  클래스별 부호  (모수는 그대로 두고 방향만)

⚠️ 이 스크립트는 **부호를 데이터에서 정하지 않는다.** 위 상관은 `sourcei_native_stats.py` 가
   이미 낸 것이고, 여기서는 그 부호를 **고정 상수로 받아** 쓴다. 같은 데이터에서 부호를 고르고
   같은 데이터에서 이득을 재면 선택편의가 들어간다 — 그건 별도 홀드아웃이 필요하다.
   따라서 여기 결과는 **가설 검정이 아니라 효과크기 추정**으로만 읽어야 한다(보고서에 명시).
⚠️ 추론은 §27 규약대로 카메라 수준 짝비교 + 와일드 부트스트랩.
"""
from __future__ import annotations
import os, sys, json, csv, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import average_precision_score
from scipy import stats as sps
from prompt_cos_db import load_sentence_vectors, topk_vote, wave_iou

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
AB = f"{OUT}/filter_ab"
CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]
POOL_PER_CLS = 3000
K = 64
CFG = json.load(open(f"{OUT}/optbank/optbank.json"))["cfg"]
NBOOT = 10000
# sourcei_native_stats.py 실측 부호 (카메라 중심화 y 기준). +1 = sd 상위가 좋다.
SIGN = {"normal": -1, "falldown": +1, "fire": +1, "smoke": +1}

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt = np.array(d["gt"]); cam = np.array([str(x) for x in d["camera"]]); ids = list(d["ids"])
hy = fo.load_dataset("sourcei"); hid, hemb = hy.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
CAMS = np.unique(cam); CIDX = {c: np.where(cam == c)[0] for c in CAMS}

conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes = collections.defaultdict(dict)
for h, c, n_ in cur: votes[h][c] = n_
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes.items()}
cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1")
stext = dict(cur.fetchall())
st = np.load(f"{OUT}/sent_stats_byhash.npz", allow_pickle=True)
pos = {h: i for i, h in enumerate(st["hashes"])}
rowi = np.array([pos.get(h, -1) for h in hashes], np.int64); okm = rowi >= 0
sd_fr = np.full(len(hashes), np.nan, np.float32); sd_fr[okm] = st["sd"][rowi[okm]]
ms_fr = np.full(len(hashes), np.nan, np.float32); ms_fr[okm] = st["m_s_mean"][rowi[okm]]

cls_idx = {c: np.array([j for j, h in enumerate(hashes) if maj.get(h) == c and okm[j]]) for c in CLASSES}
print("공급 후보 " + str({c: len(v) for c, v in cls_idx.items()}), flush=True)

# sourcei 자체 군집 → 네이티브 통계 (전 공급 문장에 대해 한 번만)
km = MiniBatchKMeans(n_clusters=K, random_state=0, n_init=10, batch_size=1024).fit(FH)
cl = km.labels_
ALLJ = np.concatenate([cls_idx[c] for c in CLASSES])
V = SENT[ALLJ].astype(np.float32); V /= np.linalg.norm(V, axis=1, keepdims=True)
ms_hy_a = np.empty(len(ALLJ), np.float32); sd_hy_a = np.empty(len(ALLJ), np.float32)
for s0 in range(0, len(ALLJ), 4000):
    S = FH @ V[s0:s0 + 4000].T
    ms_hy_a[s0:s0 + 4000] = S.mean(0)
    A = np.stack([S[cl == k].mean(0) for k in range(K) if (cl == k).any()], 1)
    sd_hy_a[s0:s0 + 4000] = (A - A.mean(1, keepdims=True)).std(1)
    del S, A
sd_hy = dict(zip(ALLJ.tolist(), sd_hy_a.tolist()))
ms_hy = dict(zip(ALLJ.tolist(), ms_hy_a.tolist()))
print(f"네이티브 통계 완료 · 문장 {len(ALLJ):,}", flush=True)

def build(stat, signed):
    """stat='fr'|'hy' · signed=True 면 클래스별 부호 적용. 반환 (열 인덱스, 라벨, 텍스트)."""
    rows = []
    for ci, c in enumerate(CLASSES):
        ii = cls_idx[c]
        if stat == "fr": key = sd_fr[ii].astype(np.float64); m = ms_fr[ii].astype(np.float64)
        else:
            key = np.array([sd_hy[int(j)] for j in ii]); m = np.array([ms_hy[int(j)] for j in ii])
        s = SIGN[c] if signed else +1
        # 품질점수 q = 부호 적용 특이도 × (조용할수록 가점) — §23 과 같은 형태, 부호만 다름
        q = s * key * (1 - (m - m.min()) / (np.ptp(m) + 1e-9) * .5) if s > 0 else \
            (-key) * (1 - (m - m.min()) / (np.ptp(m) + 1e-9) * .5)
        keep = ii[np.argsort(-q)[:POOL_PER_CLS]]
        rows.extend([(int(j), ci) for j in keep])
    return rows

def select_bank(rows):
    """§23 규약: 클래스별 k=500, 공급만(생성 문장은 이 실험에서 제외 — 신호 비교가 목적)."""
    out = []
    for ci in range(len(CLASSES)):
        jj = [j for j, c in rows if c == ci][:CFG["k"]]
        out.extend([(j, ci) for j in jj])
    return out

def score_rows(rows):
    js = np.array([j for j, _ in rows]); lab = np.array([c for _, c in rows])
    P = SENT[js].astype(np.float32); P /= np.linalg.norm(P, axis=1, keepdims=True)
    mu = P.mean(0); mu /= np.linalg.norm(mu)
    PC = P - (P @ mu)[:, None] * mu[None, :]
    PC /= np.maximum(np.linalg.norm(PC, axis=1, keepdims=True), 1e-8)
    S = FH @ (PC if CFG.get("centered") else P).T
    pred = topk_vote(S, lab, 4)
    mem = {c: np.where(lab == i)[0] for i, c in enumerate(CLASSES) if (lab == i).any()}
    w = wave_iou(S, mem) if ("normal" in mem and len(mem) > 1) else {}
    return pred, w, lab

def f1_present(t, p):
    ev = []
    for c in EVENTS:
        i = CLASSES.index(c)
        if (t == i).sum() == 0: continue
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum())
        fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        ev.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(ev)) if ev else np.nan

VAR = [("base", "fr", False), ("hy", "hy", False),
       ("hy_signed", "hy", True), ("fr_signed", "fr", True)]
PRED, POOLED = {}, {}
for name, stat, signed in VAR:
    rows = select_bank(build(stat, signed))
    pred, w, lab = score_rows(rows)
    PRED[name] = pred
    aps = []
    for c in EVENTS:
        i = CLASSES.index(c)
        if c in w and 0 < (gt == i).sum() < len(gt):
            aps.append(float(average_precision_score((gt == i).astype(int), -w[c])))
    fpn = float((pred[gt == 0] > 0).mean())
    POOLED[name] = dict(n=len(rows), mf1=round(f1_present(gt, pred), 4),
                        pr_auc=round(float(np.mean(aps)) if aps else 0., 4),
                        fp_normal=round(fpn, 4), fp_over=bool(fpn > .05))
    print(f"{name:11} 문장 {len(rows)} · mF1 {POOLED[name]['mf1']:.4f} · "
          f"PR {POOLED[name]['pr_auc']:.4f} · 오탐 {fpn:.4f}", flush=True)

usable = [c for c in CAMS if np.isfinite(f1_present(gt[CIDX[c]], PRED["base"][CIDX[c]]))]
def camvec(n): return np.array([f1_present(gt[CIDX[c]], PRED[n][CIDX[c]]) for c in usable])
rng = np.random.default_rng(0)
BOOT = rng.choice(len(usable), size=(NBOOT, len(usable)), replace=True)
RES = {}
for name, *_ in VAR:
    if name == "base": continue
    dv = camvec(name) - camvec("base"); n = len(dv)
    m, sd_ = dv.mean(), dv.std(ddof=1); se = sd_ / np.sqrt(n)
    r = dv - m; W = rng.choice([-1., 1.], size=(NBOOT, n))
    tb = (W * r).mean(1) / np.maximum((W * r).std(1, ddof=1) / np.sqrt(n), 1e-12)
    t0 = m / max(se, 1e-12)
    b = dv[BOOT].mean(1)
    RES[name] = dict(delta=round(float(m), 5), sd=round(float(sd_), 5),
                     boot_ci=[round(float(np.percentile(b, 2.5)), 5), round(float(np.percentile(b, 97.5)), 5)],
                     p_wild=round(float((np.abs(tb) >= abs(t0)).mean()), 4),
                     p_wilcoxon=round(float(sps.wilcoxon(dv[dv != 0]).pvalue) if (dv != 0).any() else 1.0, 4),
                     n_cam=n)
    print(f"  Δ {name:11} {m:+.4f} CI{RES[name]['boot_ci']} · 와일드 p {RES[name]['p_wild']:.3f}", flush=True)

json.dump(dict(cfg=CFG, sign=SIGN, k_clusters=K, n_cam_macro=len(usable),
               pooled=POOLED, paired=RES,
               caveat="부호는 같은 데이터의 상관에서 왔다 — 효과크기 추정이지 가설검정이 아니다"),
          open(f"{AB}/sourcei_native_select.json", "w"), ensure_ascii=False, indent=1)
print(f"\n→ {AB}/sourcei_native_select.json")
