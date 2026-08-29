#!/usr/bin/env python3
"""필터 A/B **추론 재설계** — 1차 실행의 통계 설계가 틀렸다.

1차(`filter_ab.py`)는 변형마다 **독립** 카메라 부트스트랩 CI 를 냈다. 그건 이 비교에 쓸 수
없다. 모든 변형이 **같은 7,498 프레임·같은 15 카메라**에서 채점되므로 카메라 수준 변동이
두 추정치에 공통으로 들어간다. 차이를 직접 부트스트랩하면 그 공통분이 상쇄된다 —
"주변 CI 가 겹친다"는 사실만으로 유의하지 않다고 말할 수 없다.

문헌 근거로 세 가지를 바꾼다:
  ① **군집 수준 짝비교를 1차 분석으로.** 군집이 15개면(< 15~20) 개체 수준 회귀는
     견고하지 않다는 것이 클러스터 무작위 시험 문헌의 권고다. 카메라마다 요약값 하나를
     만들고 15쌍에 짝검정을 한다. 정보를 덜 쓰지만 가정 위반에 강하다.
  ② **와일드 클러스터 부트스트랩(Rademacher ±1).** G < 약 40 이면 군집-로버스트
     t 의 분산이 하향 편의되어 t 가 과대해지고 위양성이 폭증한다(CGM 2008).
     G=15 면 2^15=32,768 부호조합이라 Rademacher 로 충분하다.
  ③ **다중비교 보정.** 기준선 대비 5개 비교다. Holm 은 임의 종속에서 타당하고,
     같은 부트스트랩 재표본으로 **max-|t| 영분포**(Westfall–Young 단계강하)를 만들면
     상관구조를 직접 반영한다. 둘 다 낸다.

⚠️ 이벤트 클래스가 없는 카메라는 macro 가 정의되지 않는다(G2·G5) → 짝비교에서 제외하고
   기여 카메라 수를 반드시 함께 보고한다. 보조지표로 균형정확도(전 카메라 정의)도 낸다.
⚠️ 선택은 결정론이라 재실행이 안전하다. 부트스트랩을 캐시된 예측 위에서 돌리므로
   1차의 495초/변형 → 수초가 된다.
"""
from __future__ import annotations
import os, sys, json, csv, time, collections, re, itertools
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo
from sklearn.metrics import average_precision_score
from scipy import stats as sps
from prompt_cos_db import load_sentence_vectors, topk_vote, wave_iou

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
BANKDIR = f"{OUT}/optbank"
ABDIR = f"{OUT}/filter_ab"
os.makedirs(ABDIR, exist_ok=True)
CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]
POOL_PER_CLS = 3000
CFG = json.load(open(f"{BANKDIR}/optbank.json"))["cfg"]
NBOOT = int(os.environ.get("AB_NBOOT", "10000"))
FP_BUDGET = 0.05
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

def mem_gb():
    for l in open("/proc/meminfo"):
        if l.startswith("MemAvailable:"): return int(l.split()[1]) / 1048576
    return 0.0
if mem_gb() < 6: raise SystemExit(f"RAM {mem_gb():.1f}G < 6G — 중단")
log(f"설정 {CFG} · 부트 {NBOOT:,} · RAM {mem_gb():.1f}G")

# ══ 문장 + 해시 기준 통계 ═══════════════════════════════════════════
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

_st = np.load(f"{OUT}/sent_stats_byhash.npz", allow_pickle=True)
_pos = {h: i for i, h in enumerate(_st["hashes"])}
_row = np.array([_pos.get(h, -1) for h in hashes], np.int64)
OKS = _row >= 0
def _pick(n):
    a = np.full(len(hashes), np.nan, np.float32); a[OKS] = _st[n][_row[OKS]]; return a
m_s_mean, m_s_max, sd_sup = _pick("m_s_mean"), _pick("m_s_max"), _pick("sd")
log(f"문장 {SENT.shape} · 통계 보유 {int(OKS.sum()):,} · 미보유 {int((~OKS).sum()):,}")

z = np.load(f"{OUT}/gen_vectors.npz", allow_pickle=True)
GV = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
gs = list(csv.DictReader(open(f"{OUT}/csv/40_generated_sentences.csv", encoding="utf-8-sig")))
gen_rows = []
for x in gs:
    k = x["kind(출처)"]
    if k == "gen": gen_rows.append((x["text(문장)"], x["class(클래스)"], "생성(CuPL)"))
    elif k == "pair_ev": gen_rows.append((x["text(문장)"], x["class(클래스)"], "생성(대조쌍)"))
    elif k == "pair_no": gen_rows.append((x["text(문장)"], "normal", "생성(대조쌍)"))
sn = set(); gen_rows = [r for r in gen_rows if not (r[0] in sn or sn.add(r[0]))]
sup_idx = {c: [] for c in CLASSES}
for j, h in enumerate(hashes):
    c = maj.get(h)
    if c in CLASSES and OKS[j]: sup_idx[c].append(j)
log("공급 " + str({c: len(v) for c, v in sup_idx.items()}))

# ══ GT ════════════════════════════════════════════════════════════
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam, ids = d["gt"], d["camera"], list(d["ids"])
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
CAMS = np.unique(cam)
CIDX = {c: np.where(cam == c)[0] for c in CAMS}
log(f"프레임 {len(gt):,} · 카메라 {len(CAMS)}")

def build_pool(agg, polarity):
    ms = m_s_max if agg == "max" else m_s_mean
    rows, sdk, drops = [], [], {}
    for c in CLASSES:
        ii = np.array(sup_idx[c])
        unspec = sd_sup[ii] <= np.percentile(sd_sup[ii], 25)
        noisy = ms[ii] >= np.percentile(ms[ii], 75)
        rm = (unspec & noisy) if polarity == "and" else unspec
        keep = ii[~rm]; drops[c] = int(rm.sum())
        q = sd_sup[keep] * (1 - (ms[keep] - ms[keep].min()) / (np.ptp(ms[keep]) + 1e-9) * .5)
        keep = keep[np.argsort(-q)[:POOL_PER_CLS]]
        for j in keep: rows.append(("sup", int(j), stext.get(hashes[j], ""), c, "공급"))
        sdk.extend(sd_sup[keep].tolist())
    nsup = len(rows)
    for t, c, s in gen_rows: rows.append(("gen", t, t, c, s))
    P = np.stack([SENT[k] if s == "sup" else GV[k] for s, k, *_ in rows]).astype(np.float32)
    P /= np.linalg.norm(P, axis=1, keepdims=True)
    sd_col = np.zeros(len(rows), np.float32); sd_col[:nsup] = sdk
    sd_col[nsup:] = float(np.median(sd_col[:nsup]))
    ms_col = np.zeros(len(rows), np.float32)
    ms_col[:nsup] = [ms[k] for s, k, *_ in rows[:nsup]]
    key = [hashes[k] if s == "sup" else "gen:" + str(k) for s, k, *_ in rows]
    return P, np.array([CLASSES.index(r[3]) for r in rows], np.int32), \
        np.array([r[4] for r in rows]), [r[2] for r in rows], ms_col, sd_col, key, drops

def ngr(s, n=4):
    w = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower()).split()
    n = 3 if len(w) < 10 else n
    return {tuple(w[i:i+n]) for i in range(max(len(w)-n+1, 0))} or {tuple(w)}

def dd_cos(idx, thr, P, ms):
    o = idx[np.argsort(ms[idx])]; V = P[o]; kp, kt = [], []
    for j in range(len(o)):
        if kt and float(np.max(V[j] @ V[kt].T)) > thr: continue
        kt.append(j); kp.append(o[j])
    return np.array(kp, np.int64)

def dd_contain(idx, thr, texts, sd):
    o = idx[np.argsort(-sd[idx])]; G = [ngr(texts[j]) for j in o]; kp, kt = [], []
    for j in range(len(o)):
        g = G[j]
        if kt and any(len(g & G[k]) / max(len(g), 1) >= thr for k in kt): continue
        kt.append(j); kp.append(o[j])
    return np.array(kp, np.int64)

def select(P, lab, src, texts, ms, sd, mode, thr):
    k = CFG["k"]; frac = .25 if "25" in str(CFG["mix"]) else 0.
    cols = []
    for i, c in enumerate(CLASSES):
        base = lab == i; ng = int(round(k * frac))
        for msk, want in ((base & (src != "공급"), ng), (base & (src == "공급"), k - ng)):
            ii = np.where(msk)[0]
            if not len(ii) or not want: continue
            if mode == "contain": ii = dd_contain(ii, thr, texts, sd)
            elif mode == "cos": ii = dd_cos(ii, thr, P, ms)
            cols.extend(ii[np.argsort(-sd[ii])[:want]].tolist())
    return np.array(sorted(cols), np.int64)

VARIANTS = [
    ("base",          "mean", "or",  "cos",     CFG["dedup"], "현행 §23 승리본 (기준선)"),
    ("msmax",         "max",  "or",  "cos",     CFG["dedup"], "① 집계 MEAN→MAX (부록 A)"),
    ("contain0.8",    "mean", "or",  "contain", 0.8,          "② 방향성 containment τ=0.8 (Eq A.2)"),
    ("contain0.6",    "mean", "or",  "contain", 0.6,          "② τ 스윕 0.6"),
    ("and_polar",     "mean", "and", "cos",     CFG["dedup"], "③ 제거 OR→AND (Table 1)"),
    ("msmax+contain", "max",  "or",  "contain", 0.8,          "①+② 동시"),
]

# ══ 1) 변형별 예측 캐시 (결정론) ════════════════════════════════════
cache_pool, PRED, WIOU, SEL, DROPS, NSENT = {}, {}, {}, {}, {}, {}
for name, agg, pol, mode, thr, why in VARIANTS:
    kk = (agg, pol)
    if kk not in cache_pool: cache_pool[kk] = build_pool(*kk)
    P, lab, src, texts, ms, sd, key, drops = cache_pool[kk]
    mu = P.mean(0); mu /= np.linalg.norm(mu)
    PC = P - (P @ mu)[:, None] * mu[None, :]
    PC /= np.maximum(np.linalg.norm(PC, axis=1, keepdims=True), 1e-8)
    cols = select(P, lab, src, texts, ms, sd, mode, thr)
    S = FH @ (PC if CFG.get("centered") else P)[cols].T
    l = lab[cols]
    PRED[name] = topk_vote(S, l, 4)
    mem = {c: np.where(l == i)[0] for i, c in enumerate(CLASSES) if (l == i).any()}
    WIOU[name] = wave_iou(S, mem) if ("normal" in mem and len(mem) > 1) else {}
    SEL[name] = set(key[j] for j in cols); DROPS[name] = drops; NSENT[name] = len(cols)
    log(f"{name:15} 문장 {len(cols)} · 프리필터 제거 {drops}")
    del S

# 프레임별 예측을 영속화한다 — 없으면 GT 수정 시나리오마다 전체 재실행이 필요하다
# (2026-08-29 오탐 감사에서 실제로 그 비용을 치렀다).
np.savez_compressed(f"{ABDIR}/preds_by_variant.npz",
                    ids=np.array(ids), gt=gt, camera=cam,
                    **{f"pred__{k}": v for k, v in PRED.items()})
log(f"프레임별 예측 저장 → {ABDIR}/preds_by_variant.npz")

# 선택 집합 겹침 — 왜 어떤 변형이 동일한지 설명한다
JAC = {}
for a, b in itertools.combinations([v[0] for v in VARIANTS], 2):
    A, B = SEL[a], SEL[b]
    JAC[f"{a}|{b}"] = round(len(A & B) / max(len(A | B), 1), 4)
log("선택 자카드(기준선 대비) " + str({k.split('|')[1]: v for k, v in JAC.items() if k.startswith("base|")}))

# ══ 2) 지표 (프레임 집합 → 스칼라) ══════════════════════════════════
def f1_present(t, p):
    """존재 이벤트 클래스만 macro (G5). 반환 (mf1, n_present)."""
    ev = []
    for c in EVENTS:
        i = CLASSES.index(c)
        if (t == i).sum() == 0: continue
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum())
        fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        ev.append(2 * pr * rc / max(pr + rc, 1e-12))
    return (float(np.mean(ev)) if ev else np.nan), len(ev)

def bal_acc(t, p):
    """균형정확도 — 단일클래스 카메라에서도 정의된다(보조지표)."""
    rs = []
    for i in range(len(CLASSES)):
        m = t == i
        if m.sum(): rs.append(float((p[m] == i).mean()))
    return float(np.mean(rs)) if rs else np.nan

def fp_norm(t, p):
    m = t == 0
    return float((p[m] > 0).mean()) if m.sum() else np.nan

# 카메라 수준 요약 — 1차 분석 단위
CAMTAB = {}
for name in PRED:
    rows = []
    for c in CAMS:
        ii = CIDX[c]; t, p = gt[ii], PRED[name][ii]
        mf1, npz_ = f1_present(t, p)
        rows.append((c, len(ii), mf1, npz_, bal_acc(t, p), fp_norm(t, p)))
    CAMTAB[name] = rows
usable = [k for k, (c, n, mf1, npr, ba, fp) in enumerate(CAMTAB["base"]) if np.isfinite(mf1)]
log(f"macro 정의 카메라 {len(usable)}/{len(CAMS)} — 나머지는 이벤트 클래스 부재(G2)")

def camvec(name, col=2):
    return np.array([CAMTAB[name][k][col] for k in usable], float)

# ══ 3) 짝비교 — 군집 수준 1차 분석 ══════════════════════════════════
BASE = "base"
def paired_tests(dv):
    """dv = 카메라별 Δ (variant − base). n=15 이하라 정확검정을 함께 낸다."""
    n = len(dv); m = float(np.mean(dv)); sd = float(np.std(dv, ddof=1)) if n > 1 else 0.0
    se = sd / np.sqrt(n) if n > 1 else np.inf
    t = m / se if se > 0 else 0.0
    p_t = float(2 * sps.t.sf(abs(t), df=n - 1)) if n > 1 else 1.0
    ci = (m - sps.t.ppf(.975, n - 1) * se, m + sps.t.ppf(.975, n - 1) * se) if n > 1 else (np.nan, np.nan)
    nz = dv[dv != 0]
    p_w = float(sps.wilcoxon(nz, zero_method="wilcox", alternative="two-sided",
                             method="exact" if len(nz) <= 25 else "auto").pvalue) if len(nz) else 1.0
    k = int((nz > 0).sum())
    p_s = float(sps.binomtest(k, len(nz), .5).pvalue) if len(nz) else 1.0
    return dict(mean=m, ci=[ci[0], ci[1]], t=t, p_t=p_t, p_wilcoxon=p_w, p_sign=p_s,
                n_cam=n, n_nonzero=int(len(nz)), n_pos=k)

# 와일드 클러스터 부트스트랩 (Rademacher) — 카메라 Δ 의 평균=0 귀무
rng = np.random.default_rng(0)
def wild_p(dv, nb=NBOOT):
    n = len(dv); m = float(np.mean(dv))
    r = dv - m                                    # 귀무 하 잔차
    W = rng.choice([-1.0, 1.0], size=(nb, n))
    tb = (W * r).mean(1) / np.maximum((W * r).std(1, ddof=1) / np.sqrt(n), 1e-12)
    t0 = m / max(np.std(dv, ddof=1) / np.sqrt(n), 1e-12)
    return float((np.abs(tb) >= abs(t0)).mean()), tb, t0

# 짝 부트스트랩(카메라 복원추출, **모든 변형에 같은 재표본**)
BOOT = rng.choice(len(usable), size=(NBOOT, len(usable)), replace=True)
def paired_boot_ci(dv):
    b = dv[BOOT].mean(1)
    return [float(np.percentile(b, 2.5)), float(np.percentile(b, 97.5))]

RES, TB_ALL, T0_ALL = {}, {}, {}
for name, *_ , why in VARIANTS:
    if name == BASE: continue
    dv = camvec(name) - camvec(BASE)
    r = paired_tests(dv)
    r["boot_ci"] = paired_boot_ci(dv)
    r["p_wild"], tb, t0 = wild_p(dv)
    r["why"] = why
    r["delta_bal"] = float(np.mean(camvec(name, 4) - camvec(BASE, 4)))
    r["jaccard_vs_base"] = JAC.get(f"base|{name}", JAC.get(f"{name}|base"))
    RES[name] = r; TB_ALL[name] = tb; T0_ALL[name] = t0

# ══ 4) 다중비교 — Holm + Westfall–Young 단계강하 max-|t| ═════════════
names = list(RES)
raw = np.array([RES[n]["p_wild"] for n in names])
order = np.argsort(raw)
holm = np.empty_like(raw)
run = 0.0
for rank, i in enumerate(order):
    v = raw[i] * (len(raw) - rank)
    run = max(run, min(v, 1.0)); holm[i] = run
TBM = np.stack([np.abs(TB_ALL[n]) for n in names])          # [K, nboot]
maxt = TBM.max(0)
wy = np.array([float((maxt >= abs(T0_ALL[n])).mean()) for n in names])
for i, n in enumerate(names):
    RES[n]["p_holm"] = float(holm[i]); RES[n]["p_westfall_young"] = float(wy[i])
    RES[n]["significant_005"] = bool(holm[i] < .05 and wy[i] < .05)

# ══ 5) 전체(pooled) 지표 + 오탐 예산 ════════════════════════════════
POOLED = {}
for name in PRED:
    mf1, _ = f1_present(gt, PRED[name])
    w = WIOU[name]; aps = []
    for c in EVENTS:
        i = CLASSES.index(c)
        if c not in w or not (0 < (gt == i).sum() < len(gt)): continue
        aps.append(float(average_precision_score((gt == i).astype(int), -w[c])))
    POOLED[name] = dict(n_sentences=NSENT[name], mf1=round(mf1, 4),
                        pr_auc=round(float(np.mean(aps)) if aps else 0., 4),
                        bal_acc=round(bal_acc(gt, PRED[name]), 4),
                        fp_normal=round(fp_norm(gt, PRED[name]), 4),
                        fp_over_budget=bool(fp_norm(gt, PRED[name]) > FP_BUDGET),
                        prefilter_drops=DROPS[name])

json.dump(dict(cfg=CFG, n_boot=NBOOT, n_cameras=len(CAMS), n_cameras_macro=len(usable),
               fp_budget=FP_BUDGET, pooled=POOLED, paired=RES, jaccard=JAC,
               camera_table={k: [[str(r[0]), int(r[1])] + [None if not np.isfinite(x) else round(float(x), 4)
                                                           for x in r[2:]] for r in v]
                             for k, v in CAMTAB.items()}),
          open(f"{ABDIR}/inference.json", "w"), ensure_ascii=False, indent=1)
log(f"→ {ABDIR}/inference.json")
for n in names:
    r = RES[n]
    log(f"{n:15} Δ {r['mean']:+.4f} [{r['boot_ci'][0]:+.4f},{r['boot_ci'][1]:+.4f}] "
        f"· p_wild {r['p_wild']:.3f} → Holm {r['p_holm']:.3f} / WY {r['p_westfall_young']:.3f} "
        f"· 자카드 {r['jaccard_vs_base']:.3f} · {'유의' if r['significant_005'] else '유의하지 않음'}")
print("DONE")
