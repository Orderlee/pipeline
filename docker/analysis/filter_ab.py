#!/usr/bin/env python3
"""라벨-free 필터의 **설계 선택 A/B** — DatologyAI 20/20 VLM 논문(2605.11405v2) 부록 A 근거.

논문은 자기 품질필터의 임계값·ablation 을 공개하지 않는다. 하지만 부록 A 가 image+text
공동 기준을 **완전히 명세**하고, 그 설계 선택이 우리 필터와 다르다. 그 차이만 검정한다:

  ① 집계   부록 A: 문서는 **가장 닮은** 구성 이미지의 유사도를 물려받는다(MAX).
           우리 `m_s`: 프레임 90,084장 **평균**(MEAN).
           → 한 카메라에서만 터지는 문장은 MEAN 에선 통과하는데 그게 오탐 자석이다.
           `Ak_kmeans64.npy`(문장×64군집 평균)가 이미 있어 MAX 는 재계산 없이 나온다.
  ② 중복   부록 A Eq A.2 는 **방향성** containment 다("짧은 텍스트가 긴 문맥에 박히면
           역방향은 padding 에 희석된다"). 우리 dedup 은 대칭 코사인 0.97 하나다.
           → 방향성이면 "일반 부모를 버리고 구체 자식을 남긴다"를 규칙으로 쓸 수 있다.
           우리 실측에서 중복컷이 손해 65%·falldown 유지율 17% 였던 게 무방향성 탓일 수 있다.
  ③ 방향   부록 A Table 1 은 신호가 엇갈리면 **남긴다**(둘 다 걸릴 때만 제거).
           우리 `curate()` 는 유지조건 AND = 제거조건 OR 이라 반대다.

⚠️ 판정 설정은 §23 승리본으로 **고정**한다(k=500·혼합25%·dedup 0.97·key=sd·centered).
   필터만 바꿔야 A/B 다 — 설정까지 재탐색하면 무엇이 이겼는지 알 수 없다.
⚠️ 카메라 군집 부트스트랩만 쓴다(가드레일 G1, 실측 deff 232). 프레임 단위 CI 금지.
⚠️ 변형마다 체크포인트를 남긴다(G8) — 죽어도 앞 변형 결과는 보존된다.
"""
from __future__ import annotations
import os, sys, json, csv, time, collections, re
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "3")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, psycopg2, fiftyone as fo
from sklearn.metrics import average_precision_score
from prompt_cos_db import load_sentence_vectors, topk_vote, wave_iou

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
BANKDIR = f"{OUT}/optbank"
ABDIR = f"{OUT}/filter_ab"
os.makedirs(ABDIR, exist_ok=True)
CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]
POOL_PER_CLS = 3000
CFG = json.load(open(f"{BANKDIR}/optbank.json"))["cfg"]     # §23 승리 설정 고정
NBOOT = int(os.environ.get("AB_NBOOT", "2000"))
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

def mem_avail_gb():
    for line in open("/proc/meminfo"):
        if line.startswith("MemAvailable:"): return int(line.split()[1]) / 1024 / 1024
    return 0.0
if mem_avail_gb() < 6:
    raise SystemExit(f"메모리 부족 {mem_avail_gb():.1f}G < 6G — 공유 호스트 보호(오늘 OOM 2회)")
log(f"설정 고정 {CFG} · 가용 RAM {mem_avail_gb():.1f}G")

# ── 풀 구성 (sourcei_optbank 과 동일 규약) ─────────────────────────
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

# ⚠️ 통계는 **해시로** 조회한다. 행 인덱스로 저장된 옛 배열(`m_s_bg90k.npy` 등)은
#    `load_sentence_vectors` 에 ORDER BY 가 없어 DB 쓰기 한 번에 정렬이 깨진다
#    (2026-08-28 실측 피어슨 0.33). `rebuild_sent_stats.py` 가 해시 정렬본을 만든다.
_st = np.load(f"{OUT}/sent_stats_byhash.npz", allow_pickle=True)
_pos = {h: i for i, h in enumerate(_st["hashes"])}
_row = np.array([_pos.get(h, -1) for h in hashes], np.int64)
_have = _row >= 0
if not _have.all():
    log(f"⚠️ 통계 미보유 문장 {int((~_have).sum()):,} — 풀에서 제외한다")
def _pick(name):
    a = np.full(len(hashes), np.nan, np.float32)
    a[_have] = _st[name][_row[_have]]
    return a
m_s_mean, m_s_max, sd_sup = _pick("m_s_mean"), _pick("m_s_max"), _pick("sd")
STAT_OK = _have
log(f"공급 {SENT.shape} · m_s MEAN 평균 {m_s_mean.mean():.4f} / MAX 평균 {m_s_max.mean():.4f} "
    f"· 두 집계 상관 {np.corrcoef(m_s_mean, m_s_max)[0,1]:+.3f}")

z = np.load(f"{OUT}/gen_vectors.npz", allow_pickle=True)
GV_ALL = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
gsent = list(csv.DictReader(open(f"{OUT}/csv/40_generated_sentences.csv", encoding="utf-8-sig")))
gen_rows = []
for x in gsent:
    k = x["kind(출처)"]
    if k == "gen": gen_rows.append((x["text(문장)"], x["class(클래스)"], "생성(CuPL)"))
    elif k == "pair_ev": gen_rows.append((x["text(문장)"], x["class(클래스)"], "생성(대조쌍)"))
    elif k == "pair_no": gen_rows.append((x["text(문장)"], "normal", "생성(대조쌍)"))
seen = set(); gen_rows = [r for r in gen_rows if not (r[0] in seen or seen.add(r[0]))]

sup_idx = {c: [] for c in CLASSES}
for j, h in enumerate(hashes):
    c = maj.get(h)
    if c in CLASSES and STAT_OK[j]: sup_idx[c].append(j)
log("공급 클래스별 " + str({c: len(v) for c, v in sup_idx.items()}))

def build_pool(agg: str, polarity: str):
    """사전 프리필터 + 풀 구성. agg='mean'|'max' · polarity='or'|'and'.

    현행(base) = 특이도 하위 25% 컷(제거조건 OR 한 쪽만) → 품질점수 q 상위 3,000.
    polarity='and' = **시끄럽고 동시에 못 가르는** 문장만 제거(부록 A Table 1 방향).
    """
    ms = m_s_max if agg == "max" else m_s_mean
    rows, drops, sd_keep = [], {}, []
    for c in CLASSES:
        ii = np.array(sup_idx[c])
        sd_lo = np.percentile(sd_sup[ii], 25)
        ms_hi = np.percentile(ms[ii], 75)
        unspec, noisy = sd_sup[ii] <= sd_lo, ms[ii] >= ms_hi
        rm = (unspec & noisy) if polarity == "and" else unspec
        keep = ii[~rm]
        drops[c] = int(rm.sum())
        q = sd_sup[keep] * (1.0 - (ms[keep] - ms[keep].min()) / (np.ptp(ms[keep]) + 1e-9) * 0.5)
        keep = keep[np.argsort(-q)[:POOL_PER_CLS]]
        for j in keep: rows.append(("sup", int(j), stext.get(hashes[j], ""), c, "공급"))
        sd_keep.extend(sd_sup[keep].tolist())
    n_sup = len(rows)
    for t, c, s in gen_rows: rows.append(("gen", t, t, c, s))
    P = np.stack([SENT[k] if s == "sup" else GV_ALL[k] for s, k, _t, _c, _s in rows]).astype(np.float32)
    P /= np.linalg.norm(P, axis=1, keepdims=True)
    # 생성 문장의 특이도는 공급 분포의 중앙값으로 대체한다 (§23 과 동일 취급 — 생성문에는
    # 90k 배경 통계가 없다). 여기서 한 번만 정하고 아래는 이 배열만 쓴다.
    sd_col = np.zeros(len(rows), np.float32)
    sd_col[:n_sup] = np.array(sd_keep, np.float32)
    sd_col[n_sup:] = float(np.median(sd_col[:n_sup]))
    ms_col = np.zeros(len(rows), np.float32)
    ms_col[:n_sup] = np.array([ms[k] for s, k, *_ in rows[:n_sup]], np.float32)
    return P, np.array([CLASSES.index(r[3]) for r in rows], np.int32), \
        np.array([r[4] for r in rows]), [r[2] for r in rows], ms_col, sd_col, drops

# ── sourcei GT ────────────────────────────────────────────────────
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam, ids = d["gt"], d["camera"], list(d["ids"])
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids, "프레임 순서 불일치 — preds.npz 와 데이터셋이 어긋났다"
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
log(f"GT 프레임 {len(gt):,} · 카메라 {len(set(cam))}")

def f1_per_class(t, p):
    out = {}
    for i, c in enumerate(CLASSES):
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum())
        fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        out[c] = (2 * pr * rc / max(pr + rc, 1e-12), pr, rc, tp + fn)
    return out

def score(S, lab, rows):
    """topk mF1 · PR-AUC(분포 IoU) · 균형 · normal 오탐. 존재 클래스로만 macro (G5)."""
    pred = topk_vote(S[rows], lab, 4)
    t = gt[rows]
    present = [c for c in EVENTS if (t == CLASSES.index(c)).sum() > 0]
    if not present: return None
    per = f1_per_class(t, pred)
    ev = [per[c][0] for c in present]
    mem = {c: np.where(lab == i)[0] for i, c in enumerate(CLASSES) if (lab == i).any()}
    aps = []
    if "normal" in mem and len(mem) > 1:
        w = wave_iou(S[rows], mem)
        for c in present:
            if c not in w: continue
            y = (t == CLASSES.index(c)).astype(int)
            if 0 < y.sum() < len(y): aps.append(float(average_precision_score(y, -w[c])))
    return dict(mf1=float(np.mean(ev)), pr=float(np.mean(aps)) if aps else 0.0,
                bal=float(min(ev) / max(np.mean(ev), 1e-9)),
                fp_normal=round(float((pred[t == 0] > 0).mean()), 4) if (t == 0).any() else 0.0,
                per_class={c: round(per[c][0], 4) for c in CLASSES}, present=present)

# ── 중복 규칙 2종 ──────────────────────────────────────────────────
def ngrams(s, n_default=4):
    w = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower()).split()
    n = 3 if len(w) < 10 else n_default
    return {tuple(w[i:i + n]) for i in range(max(len(w) - n + 1, 0))} or {tuple(w)}

def dedupe_cos(idx, thr, P, ms):
    """현행 — 대칭 코사인. 조용한 문장 우선 보존."""
    order = idx[np.argsort(ms[idx])]
    V = P[order]; keep, kept = [], []
    for j in range(len(order)):
        if kept and float(np.max(V[j] @ V[kept].T)) > thr: continue
        kept.append(j); keep.append(order[j])
    return np.array(keep, dtype=np.int64)

def dedupe_contain(idx, thr, texts, sd_col):
    """부록 A Eq A.2 — **방향성** containment. 구체(특이도 높은) 문장을 먼저 확정하고,
    그 안에 내용이 담기는(C(cand→kept) ≥ thr) 일반 문장을 버린다."""
    order = idx[np.argsort(-sd_col[idx])]
    G = [ngrams(texts[j]) for j in order]
    keep, kept = [], []
    for j in range(len(order)):
        g = G[j]
        if kept and any(len(g & G[k]) / max(len(g), 1) >= thr for k in kept): continue
        kept.append(j); keep.append(order[j])
    return np.array(keep, dtype=np.int64)

# ── 선택 (§23 규약: 쿼터 혼합) ─────────────────────────────────────
def select(P, lab, src, texts, ms, sd_col, dedup_mode, dedup_thr):
    k, mix = CFG["k"], CFG["mix"]
    frac = 0.25 if "25" in str(mix) else 0.0
    cols = []
    for i, c in enumerate(CLASSES):
        base = lab == i
        n_gen = int(round(k * frac)); n_sup = k - n_gen
        for pool_mask, want in ((base & (src != "공급"), n_gen), (base & (src == "공급"), n_sup)):
            ii = np.where(pool_mask)[0]
            if len(ii) == 0 or want == 0: continue
            if dedup_mode == "contain": ii = dedupe_contain(ii, dedup_thr, texts, sd_col)
            elif dedup_mode == "cos": ii = dedupe_cos(ii, dedup_thr, P, ms)
            ii = ii[np.argsort(-sd_col[ii])[:want]]
            cols.extend(ii.tolist())
    return np.array(sorted(cols), dtype=np.int64)

def boot_ci(S, lab, n_boot=NBOOT, seed=0):
    """카메라 군집 부트스트랩 (G1). 프레임 단위 금지."""
    rng = np.random.default_rng(seed)
    lev = np.unique(cam); idx_by = {l: np.where(cam == l)[0] for l in lev}
    vals = []
    for _ in range(n_boot):
        pick = rng.choice(lev, len(lev), replace=True)
        rows = np.concatenate([idx_by[l] for l in pick])
        r = score(S, lab, rows)
        if r: vals.append(r["mf1"])
    v = np.array(vals)
    return float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))

VARIANTS = [
    dict(name="base",       agg="mean", polarity="or",  dedup="cos",     thr=CFG["dedup"],
         why="현행 §23 승리본 (재현 기준선)"),
    dict(name="msmax",      agg="max",  polarity="or",  dedup="cos",     thr=CFG["dedup"],
         why="① 집계 MEAN→MAX (부록 A: 최대 유사도 물려받기)"),
    dict(name="contain0.8", agg="mean", polarity="or",  dedup="contain", thr=0.8,
         why="② 중복 대칭cos→방향성 containment τ=0.8 (Eq A.2 기본값)"),
    dict(name="contain0.6", agg="mean", polarity="or",  dedup="contain", thr=0.6,
         why="② τ 스윕 — 더 공격적"),
    dict(name="and_polar",  agg="mean", polarity="and", dedup="cos",     thr=CFG["dedup"],
         why="③ 제거 OR→AND (Table 1: 신호 엇갈리면 남긴다)"),
    dict(name="msmax+contain", agg="max", polarity="or", dedup="contain", thr=0.8,
         why="①+② 동시 — 상호작용 확인"),
]

ck = f"{ABDIR}/checkpoint.jsonl"
done = set()
if os.path.exists(ck):
    for line in open(ck): done.add(json.loads(line)["name"])
    log(f"체크포인트 {len(done)}개 변형 완료 — skip")

ALL = np.arange(len(gt))
cache = {}
for v in VARIANTS:
    if v["name"] in done: continue
    t0 = time.time()
    key = (v["agg"], v["polarity"])
    if key not in cache: cache[key] = build_pool(*key)
    P, lab, src, texts, ms, sd_col, drops = cache[key]
    mu = P.mean(0); mu /= np.linalg.norm(mu)
    PC = P - (P @ mu)[:, None] * mu[None, :]
    PC /= np.maximum(np.linalg.norm(PC, axis=1, keepdims=True), 1e-8)
    cols = select(P, lab, src, texts, ms, sd_col, v["dedup"], v["thr"])
    S = (PC if CFG.get("centered") else P)
    Sm = FH @ S[cols].T
    r = score(Sm, lab[cols], ALL)
    lo, hi = boot_ci(Sm, lab[cols])
    comp = collections.Counter(f"{CLASSES[l]}/{'생성' if s != '공급' else '공급'}"
                               for l, s in zip(lab[cols], src[cols]))
    rec = dict(name=v["name"], why=v["why"], n_sentences=int(len(cols)),
               prefilter_drops=drops, composition=dict(comp),
               mf1=round(r["mf1"], 4), mf1_ci=[round(lo, 4), round(hi, 4)],
               pr_auc=round(r["pr"], 4), balance=round(r["bal"], 4),
               fp_normal=r["fp_normal"], per_class=r["per_class"],
               secs=round(time.time() - t0, 1))
    with open(ck, "a") as f: f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    log(f"{v['name']:16} 문장 {len(cols):4} · mF1 {r['mf1']:.4f} [{lo:.4f},{hi:.4f}] · "
        f"PR {r['pr']:.4f} · 균형 {r['bal']:.4f} · 오탐 {r['fp_normal']:.4f} ({rec['secs']:.0f}s)")

# ── 요약 카드 ──────────────────────────────────────────────────────
recs = [json.loads(l) for l in open(ck)]
by = {r["name"]: r for r in recs}
b = by.get("base")
with open(f"{ABDIR}/card.md", "w") as f:
    f.write("# 라벨-free 필터 설계 A/B — DatologyAI 20/20 부록 A 근거\n\n")
    f.write(f"판정 설정 고정: `{CFG}` · 카메라 군집 부트스트랩 {NBOOT}회 (G1)\n\n")
    f.write("| 변형 | 근거 | 문장 | topk mF1 [95% CI] | Δ vs base | 분포 PR-AUC | 균형 | normal 오탐 |\n")
    f.write("|---|---|---|---|---|---|---|---|\n")
    for r in recs:
        d = f"{r['mf1'] - b['mf1']:+.4f}" if b and r["name"] != "base" else "—"
        f.write(f"| `{r['name']}` | {r['why']} | {r['n_sentences']} | "
                f"{r['mf1']:.4f} [{r['mf1_ci'][0]:.4f}, {r['mf1_ci'][1]:.4f}] | {d} | "
                f"{r['pr_auc']:.4f} | {r['balance']:.4f} | {r['fp_normal']:.4f} |\n")
    f.write("\n## 읽는 법\n\n")
    f.write("- **CI 가 base 의 점추정을 포함하면 그 변형은 유의하지 않다.** 카메라 15대·유효표본 "
            "32 이므로(deff 232) 작은 Δ 는 해석 금지 — 가드레일 G1·G3.\n")
    f.write("- `normal 오탐`이 예산 5% 를 넘으면 mF1 이 올라도 **배치 불가**다 (G4).\n")
    f.write("- 문장 수가 변형마다 다르면 top-K 는 **문장 수를 세는 성질**이 있어 그 자체로 점수가 "
            "움직인다(중복컷 손해 65% 의 원인). 문장 수 열을 반드시 같이 볼 것.\n")
json.dump(dict(cfg=CFG, n_boot=NBOOT, variants=recs),
          open(f"{ABDIR}/filter_ab.json", "w"), ensure_ascii=False, indent=1)
with open(f"{ABDIR}/filter_ab.csv", "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.writer(fh)
    w.writerow(["variant", "why", "n_sentences", "mf1", "ci_lo", "ci_hi", "pr_auc",
                "balance", "fp_normal"] + [f"f1_{c}" for c in CLASSES])
    for r in recs:
        w.writerow([r["name"], r["why"], r["n_sentences"], r["mf1"], r["mf1_ci"][0], r["mf1_ci"][1],
                    r["pr_auc"], r["balance"], r["fp_normal"]] + [r["per_class"][c] for c in CLASSES])
log(f"→ {ABDIR}/card.md · filter_ab.json · filter_ab.csv")
print("DONE")
