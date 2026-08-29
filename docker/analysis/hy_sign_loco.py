#!/usr/bin/env python3
"""부호 선택의 **선택편의 제거** — 카메라 홀드아웃(LOCO).

`sourcei_stat_ab.py` 는 클래스별 부호를 상수로 받아 썼고, 그 부호는 **같은 sourcei 데이터의
상관**에서 나왔다. 그래서 거기 Δ(+0.0284)는 낙관적일 수 있다 — 부호를 고른 데이터에서
그 부호의 이득을 다시 잰 셈이다.

여기서는 카메라 하나를 빼고(hold-out) **나머지 9대에서만 부호를 정한 뒤** 뺀 카메라에서 채점한다.
10번 반복해 폴드 밖 Δ 를 모은다. 이게 편의 없는 추정이다.

부호 결정 규칙(폴드 안에서만 실행):
  클래스 c 의 부호 = sign( spearman( sd_sourcei, y_c ) ),  y_c = 카메라 중심화 판별력
  |상관| < 0.05 면 신호 없음으로 보고 +1 (현행 방향) 유지 — 잡음으로 부호를 뒤집지 않는다.
"""
from __future__ import annotations
import os, sys, json, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo
from scipy import stats as sps
from prompt_cos_db import load_sentence_vectors, topk_vote

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; AB = f"{OUT}/filter_ab"
CLASSES = ["normal", "falldown", "fire", "smoke"]; EVENTS = CLASSES[1:]
POOL_PER_CLS = 3000; MIN_R = 0.05
CFG = json.load(open(f"{OUT}/optbank/optbank.json"))["cfg"]

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt = np.array(d["gt"]); cam = np.array([str(x) for x in d["camera"]]); ids = list(d["ids"])
hy = fo.load_dataset("sourcei"); hid, hemb = hy.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb

cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
vt = collections.defaultdict(dict)
for h, c, n in cur: vt[h][c] = n
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in vt.items()}
cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1")
stext = dict(cur.fetchall())
z = np.load(f"{OUT}/gen_vectors.npz", allow_pickle=True)
GV = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
import csv as _csv
gs = list(_csv.DictReader(open(f"{OUT}/csv/40_generated_sentences.csv", encoding="utf-8-sig")))
gen = []
for x in gs:
    k = x["kind(출처)"]
    if k == "gen": gen.append((x["text(문장)"], x["class(클래스)"], "생성(CuPL)"))
    elif k == "pair_ev": gen.append((x["text(문장)"], x["class(클래스)"], "생성(대조쌍)"))
    elif k == "pair_no": gen.append((x["text(문장)"], "normal", "생성(대조쌍)"))
sn = set(); gen = [g for g in gen if not (g[0] in sn or sn.add(g[0]))]

def pick(np_, name):
    st = np.load(np_, allow_pickle=True); p = {h: i for i, h in enumerate(st["hashes"])}
    r = np.array([p.get(h, -1) for h in hashes], np.int64); o = r >= 0
    a = np.full(len(hashes), np.nan, np.float32); a[o] = st[name][r[o]]; return a, o
sd_hy, ok = pick(f"{OUT}/sent_stats_sourcei.npz", "sd")
ms_hy, _ = pick(f"{OUT}/sent_stats_sourcei.npz", "m_s_mean")
sup = {c: np.array([j for j, h in enumerate(hashes) if maj.get(h) == c and ok[j]]) for c in CLASSES}
print("공급 " + str({c: len(v) for c, v in sup.items()}), flush=True)

ALLJ = np.concatenate([sup[c] for c in CLASSES])
VA = SENT[ALLJ].astype(np.float32); VA /= np.linalg.norm(VA, axis=1, keepdims=True)
LABA = np.concatenate([[i] * len(sup[c]) for i, c in enumerate(CLASSES)])
SA = FH @ VA.T                                        # [7498, 123549]
print(f"코사인 {SA.shape}", flush=True)

def signs_from(rows_mask):
    """rows_mask 프레임만 써서 클래스별 부호를 정한다 (폴드 안)."""
    X = SA[rows_mask].copy(); g = gt[rows_mask]; cm = cam[rows_mask]
    for c in np.unique(cm):
        m = cm == c
        if m.sum() > 1: X[m] -= X[m].mean(0, keepdims=True)
    out = {}
    for ci, c in enumerate(CLASSES):
        cols = LABA == ci
        own = g == ci
        if own.sum() == 0 or (~own).sum() == 0: out[c] = +1; continue
        y = X[own][:, cols].mean(0) - X[~own][:, cols].mean(0)
        r = sps.spearmanr(sd_hy[ALLJ[cols]], y).statistic
        out[c] = +1 if (not np.isfinite(r) or abs(r) < MIN_R) else int(np.sign(r))
    return out

def build_and_score(sign, eval_rows):
    rows, sdk = [], []
    for ci, c in enumerate(CLASSES):
        ii = sup[c]; sg = sign[c]
        key = sg * sd_hy[ii]
        keep = ii[~(key <= np.percentile(key, 25))]
        q = (sg * sd_hy[keep]) * (1 - (ms_hy[keep] - ms_hy[keep].min()) / (np.ptp(ms_hy[keep]) + 1e-9) * .5)
        keep = keep[np.argsort(-q)[:POOL_PER_CLS]]
        rows += [("sup", int(j), stext.get(hashes[j], ""), c) for j in keep]
        sdk += (sg * sd_hy[keep]).tolist()
    ns = len(rows)
    rows += [("gen", t, t, c) for t, c, _ in gen]
    P = np.stack([SENT[k] if s == "sup" else GV[k] for s, k, *_ in rows]).astype(np.float32)
    P /= np.linalg.norm(P, axis=1, keepdims=True)
    lab = np.array([CLASSES.index(r[3]) for r in rows]); src = np.array(["공급"] * ns + ["생성"] * (len(rows) - ns))
    sd_col = np.zeros(len(rows), np.float32); sd_col[:ns] = sdk; sd_col[ns:] = np.median(sd_col[:ns])
    ms_col = np.zeros(len(rows), np.float32); ms_col[:ns] = [ms_hy[k] for s, k, *_ in rows[:ns]]
    mu = P.mean(0); mu /= np.linalg.norm(mu)
    PC = P - (P @ mu)[:, None] * mu[None, :]; PC /= np.maximum(np.linalg.norm(PC, axis=1, keepdims=True), 1e-8)
    cols = []
    for ci in range(4):
        base = lab == ci; ng = int(round(CFG["k"] * .25))
        for msk, want in ((base & (src != "공급"), ng), (base & (src == "공급"), CFG["k"] - ng)):
            ii = np.where(msk)[0]
            if not len(ii) or not want: continue
            o = ii[np.argsort(ms_col[ii])]; V = P[o]; kp, kt = [], []
            for j in range(len(o)):
                if kt and float(np.max(V[j] @ V[kt].T)) > CFG["dedup"]: continue
                kt.append(j); kp.append(o[j])
            ii = np.array(kp, np.int64)
            cols += ii[np.argsort(-sd_col[ii])[:want]].tolist()
    cols = np.array(sorted(cols)); l = lab[cols]
    S = FH[eval_rows] @ (PC if CFG.get("centered") else P)[cols].T
    return topk_vote(S, l, 4), gt[eval_rows]

def mf1(t, p):
    ev = []
    for c in EVENTS:
        i = CLASSES.index(c)
        if (t == i).sum() == 0: continue
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum()); fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        ev.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(ev)) if ev else np.nan

BASE = json.load(open(f"{AB}/hy_stat_ab.json"))["camera_table"]["base"]
basecam = {r[0]: r[2] for r in BASE}
CAMS = [c for c in np.unique(cam) if basecam.get(c) is not None]
print(f"LOCO 폴드 {len(CAMS)}", flush=True)
res = []
for c in CAMS:
    tr = cam != c; te = np.where(cam == c)[0]
    sg = signs_from(tr)
    pred, t = build_and_score(sg, te)
    m = mf1(t, pred); b = basecam[c]
    res.append(dict(camera=c, n=int(len(te)), sign=sg, mf1_signed=round(m, 4),
                    mf1_base=round(b, 4), delta=round(m - b, 4)))
    print(f"  {c[:34]:36} Δ {m-b:+.4f}  부호 {sg}", flush=True)

dv = np.array([r["delta"] for r in res])
n = len(dv); mm = dv.mean(); sd_ = dv.std(ddof=1); se = sd_ / np.sqrt(n)
t = mm / max(se, 1e-12); p_t = float(2 * sps.t.sf(abs(t), n - 1))
w = sps.wilcoxon(dv[dv != 0]).pvalue if (dv != 0).any() else 1.0
rng = np.random.default_rng(0); r0 = dv - mm; W = rng.choice([-1., 1.], size=(10000, n))
tb = (W * r0).mean(1) / np.maximum((W * r0).std(1, ddof=1) / np.sqrt(n), 1e-12)
p_wild = float((np.abs(tb) >= abs(t)).mean())
ci = (mm - sps.t.ppf(.975, n - 1) * se, mm + sps.t.ppf(.975, n - 1) * se)
print(f"\n폴드 밖 Δ = {mm:+.4f}  95% CI [{ci[0]:+.4f}, {ci[1]:+.4f}]")
print(f"  짝 t p {p_t:.4f} · Wilcoxon p {float(w):.4f} · 와일드 p {p_wild:.4f} · 양수 폴드 {int((dv>0).sum())}/{n}")
sc = collections.Counter(tuple(sorted(r['sign'].items())) for r in res)
print("  폴드별 부호 안정성:", dict(collections.Counter(str(dict(k)) for k in sc.elements())))
json.dump(dict(folds=res, delta=round(float(mm), 5), ci=[round(float(ci[0]), 5), round(float(ci[1]), 5)],
               p_t=round(p_t, 4), p_wilcoxon=round(float(w), 4), p_wild=round(p_wild, 4),
               n_pos=int((dv > 0).sum()), n_folds=n, min_r=MIN_R),
          open(f"{AB}/hy_sign_loco.json", "w"), ensure_ascii=False, indent=1)
print(f"→ {AB}/hy_sign_loco.json")
