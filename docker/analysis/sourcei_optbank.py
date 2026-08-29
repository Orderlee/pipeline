#!/usr/bin/env python3
"""sourcei 최적 프롬프트 뱅크 빌더 — top-K 와 분포-IoU 를 **동시에**, 클래스 균형까지.

보고서(§10·§13·§15·§16·§17·§19·§20)가 확정한 사실만으로 설계한다:

  · §15  특이도 하위 25% 컷은 31뱅크 중 0개가 손해 → **무위험 사전 컷**으로 채택
  · §15  중복 제거는 top-K 의 **표 가중치**를 깬다(손해의 65%가 규칙 탓) → 중복컷은 **탐색 노브**
  · §16  전역 문장평균 제거 후 재정규화가 30/31 뱅크 개선(부호검정 p=3e-8) → **탐색 노브**
  · §17  생성 문장은 조용하고(m_s 0.113 vs 0.122) 날카롭지만(특이도 0.0313 vs 0.0304)
         **균질해서 분포-IoU 가 무너진다**. 공급 문장은 그 반대 → **섞으면 서로를 메운다**(핵심 가설)
  · §19  경성 판정은 top-K ≳ argmax > IoU, 연속 점수는 IoU 가 최고 → **둘을 같이 목적함수에**
  · §20  예산 축소가 선택법보다 크게 먹힌다. facility-location 은 top-K 와 상극 → 품질 정렬 사용

목적함수 J = 0.40·top-K macro-F1 + 0.40·평균 PR-AUC(분포-IoU 연속값) + 0.20·클래스 균형
  균형 = min(클래스 F1) / mean(클래스 F1)  — "각 카테고리별로 잘 반응하고 밸런스가 맞는" 요구.
  세 성분을 원값 그대로 보고하므로 가중치를 바꿔 다시 고를 수 있다.

⚠️ **설정 선택도 과적합이다.** 216개 설정을 GT 로 고르면 유효표본 32(§19)에서 반드시 과적합한다.
   그래서 **중첩 교차검증**을 쓴다: 바깥 5폴드(카메라 그룹) — 안쪽에서 설정을 고르고 바깥 폴드로만
   채점. 그 평균이 **정직한 추정치**이고, 전체 데이터로 고른 설정은 "출하본"으로 따로 표기한다.
"""
import os, sys, json, csv, glob, collections, itertools, time
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote, wave_iou
import numpy as np, psycopg2, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.model_selection import GroupKFold
from sklearn.metrics import average_precision_score
import fiftyone as fo

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
BANKDIR = f"{OUT}/optbank"
os.makedirs(BANKDIR, exist_ok=True)
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

# ══════════════════════════════════════════════════════════════════
# 1) 후보 풀 — 공급 121,614 + 생성 340 + 대조쌍 이벤트문
# ══════════════════════════════════════════════════════════════════
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
# 자기뱅크 배제(OPTBANK_SUPPLY_ONLY, 기본 켜짐) — 이유:
#   bank_sentences 원장에는 우리가 만든 뱅크(vOPT.2026.08.28=hybrid, vGEN.2026.08.28=internal)도
#   등록돼 있다. 필터 없이 다수결을 내면 이 문장들이 "공급 후보 풀"로 되돌아와 다음 큐레이션이
#   자기 생성물을 다시 채택하는 구조가 된다 (학습 루프는 아니지만 큐레이션 경로로 자기증폭 —
#   CLAUDE.md "자기학습 금지" 취지 위반). 실측 오염도(2026-08-29, docker/analysis/verify_supply_only.py):
#     normal 1,000/71,109(1.41%) · falldown 1,000/28,490(3.51%)
#     fire   1,000/6,101 (16.39%, 공급이 얇아 가장 심함) · smoke 1,000/17,849(5.60%)
#   ⚠️ 단순 "우리 뱅크 해시는 전부 제외" 는 과도하다 — vOPT 는 공급 75% 혼합이라 같은 문장이
#   피아왓치 뱅크에도 존재할 수 있고(실측: vOPT 2,000문장 중 1,500개가 피아왓치와 중복), 그 문장은
#   공급 근거가 있으므로 후보 자격을 유지해야 한다. 그래서 "행을 지우는" 게 아니라 "다수결 집계를
#   prompt_banks.source='userwatch' 인 뱅크의 행만으로 낸다" — 공급에 전혀 없는(self-only) 해시만
#   빠지고(클래스당 625개, 총 2,500개), 공급과 겹치는 문장은 그대로 후보로 남는다(실측 1,500/1,500 생존).
#   기본을 켜둔 이유: 끄면 위 오염이 그대로 재현되고, 켜도 공급 겹침 문장은 안 잃는다 — 손해가 없다.
SUPPLY_ONLY = os.environ.get("OPTBANK_SUPPLY_ONLY", "1") not in ("0", "false", "False")
if SUPPLY_ONLY:
    cur.execute("""
        SELECT bs.content_hash, bs.class_label, count(*)
        FROM bank_sentences bs JOIN prompt_banks pb ON pb.bank_id = bs.bank_id
        WHERE pb.source = 'userwatch'
        GROUP BY 1, 2
    """)
else:
    cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes = collections.defaultdict(dict)
for h, c, n_ in cur: votes[h][c] = n_
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes.items()}
cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1")
stext = dict(cur.fetchall())
m_s_sup = np.load(f"{OUT}/m_s_bg90k.npy")
Ak = np.load(f"{OUT}/Ak_kmeans64.npy")
sd_sup = (Ak - Ak.mean(1, keepdims=True)).std(1)
del Ak
log(f"공급 문장 {SENT.shape}")

z = np.load(f"{OUT}/gen_vectors.npz", allow_pickle=True)
GV_ALL = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
gen = json.load(open("/workspace/gen_cupl.json"))
gsent = list(csv.DictReader(open(f"{OUT}/csv/40_generated_sentences.csv", encoding="utf-8-sig")))
gen_rows = []                                   # (text, cls, source)
for x in gsent:
    if x["kind(출처)"] == "gen": gen_rows.append((x["text(문장)"], x["class(클래스)"], "생성(CuPL)"))
    elif x["kind(출처)"] == "pair_ev": gen_rows.append((x["text(문장)"], x["class(클래스)"], "생성(대조쌍)"))
    elif x["kind(출처)"] == "pair_no": gen_rows.append((x["text(문장)"], "normal", "생성(대조쌍)"))
seen_t = set(); gen_rows = [r for r in gen_rows if not (r[0] in seen_t or seen_t.add(r[0]))]
log(f"생성 문장 {len(gen_rows)} " + str(collections.Counter(r[1] for r in gen_rows)))

# 공급 문장 사전 프리필터: 클래스 내 특이도 하위 25% 컷(§15 무위험) 후 클래스당 상위 3,000
POOL_PER_CLS = 3000
sup_idx = {c: [] for c in CLASSES}
for j, h in enumerate(hashes):
    c = maj.get(h)
    if c in CLASSES: sup_idx[c].append(j)
pool_rows = []                                  # (vec_source, key, text, cls, source)
for c in CLASSES:
    ii = np.array(sup_idx[c])
    keep = ii[sd_sup[ii] > np.percentile(sd_sup[ii], 25)]
    q = sd_sup[keep] * (1.0 - (m_s_sup[keep] - m_s_sup[keep].min()) / (np.ptp(m_s_sup[keep]) + 1e-9) * 0.5)
    keep = keep[np.argsort(-q)[:POOL_PER_CLS]]
    for j in keep: pool_rows.append(("sup", int(j), stext.get(hashes[j], ""), c, "공급"))
    log(f"  공급 {c}: {len(ii):,} → 특이도컷 후 상위 {len(keep):,}")
for t, c, src in gen_rows: pool_rows.append(("gen", t, t, c, src))

POOL = np.stack([SENT[k] if s == "sup" else GV_ALL[k] for s, k, _t, _c, _sr in pool_rows]).astype(np.float32)
POOL /= np.linalg.norm(POOL, axis=1, keepdims=True)
plab = np.array([CLASSES.index(r[3]) for r in pool_rows], np.int32)
psrc = np.array([r[4] for r in pool_rows])
ptext = [r[2] for r in pool_rows]
log(f"후보 풀 {POOL.shape} — " + str(collections.Counter(f"{CLASSES[l]}/{s}" for l, s in zip(plab, psrc))))

# ══════════════════════════════════════════════════════════════════
# 2) 풀 전체의 라벨-free 지표 (배경 m_s · 군집 특이도 · frames 대조)
# ══════════════════════════════════════════════════════════════════
cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
e2k = dict(cur.fetchall()); NK = 64
NP = POOL.shape[0]
Akp = np.zeros((NP, NK), np.float64); cnt = np.zeros(NK, np.int64); msum = np.zeros(NP, np.float64); ntot = 0
buf_v, buf_k = [], []
def flush():
    global ntot
    if not buf_v: return
    X = np.vstack(buf_v); X /= np.linalg.norm(X, axis=1, keepdims=True)
    S = X @ POOL.T
    msum[:] += S.sum(0); ntot += len(buf_k); kk = np.asarray(buf_k)
    for k0 in np.unique(kk):
        mm = kk == k0; Akp[:, k0] += S[mm].sum(0); cnt[k0] += int(mm.sum())
    buf_v.clear(); buf_k.clear()
with conn.cursor(name="fr3") as c2:
    c2.itersize = 4000
    c2.execute("SELECT entity_id, embedding::text FROM image_embeddings WHERE entity_type='frame'")
    for eid, vt in c2:
        k = e2k.get(eid)
        if k is None: continue
        buf_v.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32)); buf_k.append(k)
        if len(buf_v) >= 4000: flush()
flush()
assert ntot == 90084, ntot
p_ms = (msum / ntot).astype(np.float32)
Akp = (Akp / np.maximum(cnt, 1)).astype(np.float32)
p_sd = (Akp - Akp.mean(1, keepdims=True)).std(1)
del Akp
zz = np.load(f"{OUT}/frames_sub_24792.npz")
FF, ref = zz["FF"], zz["ref"]
p_con = np.zeros(NP)
n_ev, n_no = int((ref > 0).sum()), int((ref == 0).sum())
for s0 in range(0, len(FF), 2000):
    S = FF[s0:s0 + 2000] @ POOL.T; r = ref[s0:s0 + 2000]
    p_con += S[r > 0].sum(0) / n_ev - S[r == 0].sum(0) / n_no
log(f"라벨-free 지표 — m_s {p_ms.mean():.4f} · 특이도 {p_sd.mean():.5f} · 대조 {p_con.mean():+.4f}")
np.savez_compressed(f"{BANKDIR}/pool_stats.npz", ms=p_ms, sd=p_sd, con=p_con, lab=plab, src=psrc)

# ══════════════════════════════════════════════════════════════════
# 3) sourcei GT + 풀 코사인 1회 계산 (이후 모든 설정은 열 부분집합)
# ══════════════════════════════════════════════════════════════════
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; ids = list(d["ids"])
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
S_POOL = FH @ POOL.T                                   # [7498, NP]
mu_g = POOL.mean(0); mu_g /= np.linalg.norm(mu_g)
POOL_C = POOL - (POOL @ mu_g)[:, None] * mu_g[None, :]
POOL_C /= np.maximum(np.linalg.norm(POOL_C, axis=1, keepdims=True), 1e-8)
S_POOL_C = FH @ POOL_C.astype(np.float32).T            # §16 전역평균 제거판
log(f"코사인 행렬 {S_POOL.shape} (원본 + 전역평균제거판)")

def f1_per_class(t, p):
    out = {}
    for i, c in enumerate(CLASSES):
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum()); fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        out[c] = (2 * pr * rc / max(pr + rc, 1e-12), pr, rc)
    return out

def evaluate(cols, mask_rows, centered):
    """cols=선택 열, mask_rows=평가할 프레임. 반환 (topk_mF1, mean PR-AUC, balance, 상세)

    ⚠️ **그 폴드에 실제로 존재하는 이벤트 클래스로만 macro 를 낸다** (§5·§19 와 동일 규약).
       카메라 그룹 폴드에는 fire 가 아예 없는 폴드가 흔한데(fire 는 4대에만 존재), 없는 클래스의
       F1 을 0 으로 넣으면 구조적 0 이 목적함수를 지배한다 — 첫 실행에서 폴드1 이 mF1 0.014·균형
       0.000 으로 나온 원인이었다. 균형도 존재 클래스에 대해서만 정의한다."""
    S = (S_POOL_C if centered else S_POOL)[np.ix_(mask_rows, cols)]
    lab = plab[cols]
    pred = topk_vote(S, lab, 4)
    t = gt[mask_rows]
    present = [c for c in EVENTS if (t == CLASSES.index(c)).sum() > 0]
    if not present: return 0.0, 0.0, 0.0, dict(per_class={}, fp_normal=0.0, n=len(cols), present=[])
    per = f1_per_class(t, pred)
    ev = [per[c][0] for c in present]
    mf1 = float(np.mean(ev))
    bal = float(min(ev) / max(np.mean(ev), 1e-9))
    mem = {c: np.where(lab == i)[0] for i, c in enumerate(CLASSES) if (lab == i).any()}
    aps = []
    if "normal" in mem and len(mem) > 1:
        w = wave_iou(S, mem)
        for c in present:
            if c not in w: continue
            y = (t == CLASSES.index(c)).astype(int)
            if y.sum() == 0 or y.sum() == len(y): continue
            aps.append(float(average_precision_score(y, -w[c])))
    pr = float(np.mean(aps)) if aps else 0.0
    return mf1, pr, bal, dict(per_class={c: [round(v, 4) for v in per[c]] for c in CLASSES},
                              fp_normal=round(float((pred[t == 0] > 0).mean()), 4) if (t == 0).any() else 0.0,
                              n=len(cols), present=present)

# ══════════════════════════════════════════════════════════════════
# 4) 설정 격자 + 탐색
# ══════════════════════════════════════════════════════════════════
# v1 의 세 가지 결함을 고쳤다 (2026-08-28 실측):
#  (a) **오탐이 목적함수에 없었다** → 정상 프레임의 27%가 오경보인 뱅크가 1위로 뽑혔다
#      (공급 뱅크는 0.76%). 운영 불가다 → **오탐 예산을 하드 제약**으로 건다.
#  (b) 이벤트 3클래스만 macro 로 썼다 → normal 도 카테고리다. **4클래스 macro/균형**으로 바꾼다.
#  (c) GroupKFold 가 표본 수로만 나눠 **폴드에 카메라 1대**만 든 경우가 생겼고, 그 폴드에 없는
#      클래스 때문에 지표가 붕괴했다 → 카메라를 이벤트 다양성 기준으로 **직접 배분**하고,
#      폴드별 지표 대신 **폴드 밖 예측을 모아(pooled) 한 번에** 채점한다.
FP_BUDGET = float(os.environ.get("OPTBANK_FP_BUDGET", "0.05"))
W = dict(mf1=0.35, pr=0.35, bal=0.30)
def J(mf1, pr, bal): return W["mf1"] * mf1 + W["pr"] * pr + W["bal"] * bal

def rank_key(kind, idx):
    if kind == "sd": return p_sd[idx]
    if kind == "con": return p_con[idx]
    return p_sd[idx] * (1.0 - (p_ms[idx] - p_ms.min()) / (np.ptp(p_ms) + 1e-9) * 0.5)

def dedupe(idx, thr):
    if thr is None: return idx
    order = idx[np.argsort(p_ms[idx])]
    V = POOL[order]; keep, kept = [], []
    for j_ in range(len(order)):
        if kept and float(np.max(V[j_] @ V[kept].T)) > thr: continue
        kept.append(j_); keep.append(order[j_])
    return np.array(keep)

def select(cfg):
    """혼합은 **쿼터**로 뽑는다 — v1 은 두 출처를 같은 점수로 정렬해 생성 문장이 2,000 중 16개만
    들어갔다(가설을 사실상 검정하지 못했다)."""
    k, mix, thr, key = cfg["k"], cfg["mix"], cfg["dedup"], cfg["key"]
    cols = []
    for i_, c in enumerate(CLASSES):
        base = plab == i_
        if mix == "공급만": pools = [(np.where(base & (psrc == "공급"))[0], k)]
        elif mix == "생성만": pools = [(np.where(base & (psrc != "공급"))[0], k)]
        else:
            r = float(mix.rstrip("%").split("혼합")[1]) / 100.0
            ng = int(round(k * r))
            pools = [(np.where(base & (psrc != "공급"))[0], ng),
                     (np.where(base & (psrc == "공급"))[0], k - ng)]
        got = []
        for idx, want in pools:
            if len(idx) == 0 or want <= 0: continue
            idx = dedupe(idx, thr)
            q = rank_key(key, idx)
            got.append(idx[np.argsort(-q)[:min(want, len(idx))]])
        if not got: return None
        cols.append(np.concatenate(got))
    return np.concatenate(cols)

GRID = [dict(k=k, mix=mix, dedup=thr, key=key, centered=cen)
        for k in (60, 120, 250, 500)
        for mix in ("공급만", "생성만", "혼합10%", "혼합25%", "혼합50%")
        for thr in (None, 0.97, 0.95)
        for key in ("sd", "sdms", "con")
        for cen in (False, True)]
log(f"설정 격자 {len(GRID)}개 · 오탐 예산 ≤ {FP_BUDGET:.0%}")

sel_cache = {}
def cols_of(cfg):
    key = (cfg["k"], cfg["mix"], cfg["dedup"], cfg["key"])
    if key not in sel_cache: sel_cache[key] = select(cfg)
    return sel_cache[key]

def predict(cols, rows, centered):
    S = (S_POOL_C if centered else S_POOL)[np.ix_(rows, cols)]
    return topk_vote(S, plab[cols], 4), S

def metrics(pred, S, cols, rows):
    """4클래스 macro-F1 · 평균 PR-AUC(분포-IoU) · 균형 · 정상 오탐."""
    t = gt[rows]
    per = f1_per_class(t, pred)
    present = [c for c in CLASSES if (t == CLASSES.index(c)).sum() > 0]
    f1s = [per[c][0] for c in present]
    mf1 = float(np.mean(f1s)); bal = float(min(f1s) / max(np.mean(f1s), 1e-9))
    lab = plab[cols]; mem = {c: np.where(lab == i_)[0] for i_, c in enumerate(CLASSES) if (lab == i_).any()}
    aps = []
    if "normal" in mem and len(mem) > 1:
        w = wave_iou(S, mem)
        for c in EVENTS:
            if c not in w or (t == CLASSES.index(c)).sum() == 0: continue
            aps.append(float(average_precision_score((t == CLASSES.index(c)).astype(int), -w[c])))
    fp = float((pred[t == 0] > 0).mean()) if (t == 0).any() else 0.0
    return dict(mf1=mf1, prauc=float(np.mean(aps)) if aps else 0.0, balance=bal, fp=fp,
                per_class={c: [round(v, 4) for v in per[c]] for c in CLASSES}, present=present)

# ── 카메라를 이벤트 다양성 기준으로 직접 배분 (GroupKFold 대체) ──────
cam_stat = []
for c in np.unique(cam):
    m = cam == c
    cam_stat.append((c, int(m.sum()), len({int(x) for x in gt[m] if x > 0})))
cam_stat.sort(key=lambda t_: (-t_[2], -t_[1]))          # 이벤트 다양성 많은 순 → 라운드로빈
NF = 5
fold_of = {}
for i_, (c, _n, _d) in enumerate(cam_stat): fold_of[c] = i_ % NF
folds = []
for f_ in range(NF):
    te = np.where(np.array([fold_of[c] for c in cam]) == f_)[0]
    tr = np.where(np.array([fold_of[c] for c in cam]) != f_)[0]
    folds.append((tr, te))
    log(f"  폴드 {f_}: 카메라 {len({c for c in cam[te]})}대 · 프레임 {len(te):,} · "
        f"이벤트클래스 {sorted({CLASSES[x] for x in gt[te] if x > 0})}")

# ── 폴드 밖 예측 풀링: 각 프레임은 자기 카메라를 못 본 설정으로 예측된다 ──
oof = np.full(len(gt), -1, np.int8); oof_cfg = []
for f_, (tr, te) in enumerate(folds):
    best, bestJ = None, -1e9
    for cfg in GRID:
        cols = cols_of(cfg)
        if cols is None or len(cols) < 20: continue
        pr_, S_ = predict(cols, tr, cfg["centered"])
        m_ = metrics(pr_, S_, cols, tr)
        if m_["fp"] > FP_BUDGET: continue                 # 하드 제약
        v = J(m_["mf1"], m_["prauc"], m_["balance"])
        if v > bestJ: bestJ, best = v, cfg
    if best is None:
        log(f"  ⚠️ 폴드 {f_}: 오탐 예산을 만족하는 설정이 없다 — 제약을 풀어 최선을 고른다")
        for cfg in GRID:
            cols = cols_of(cfg)
            if cols is None: continue
            pr_, S_ = predict(cols, tr, cfg["centered"])
            m_ = metrics(pr_, S_, cols, tr)
            v = J(m_["mf1"], m_["prauc"], m_["balance"]) - 2.0 * max(0.0, m_["fp"] - FP_BUDGET)
            if v > bestJ: bestJ, best = v, cfg
    pr_te, _ = predict(cols_of(best), te, best["centered"])
    oof[te] = pr_te
    oof_cfg.append(dict(fold=f_, cfg=dict(best), n_te=int(len(te))))
    log(f"  폴드 {f_} 선택: k={best['k']} {best['mix']} dedup={best['dedup']} key={best['key']} cen={best['centered']}")
assert (oof >= 0).all()
per_oof = f1_per_class(gt, oof)
oof_f1 = [per_oof[c][0] for c in CLASSES]
HONEST = dict(macro_f1_4=round(float(np.mean(oof_f1)), 4),
              macro_f1_event=round(float(np.mean([per_oof[c][0] for c in EVENTS])), 4),
              balance=round(float(min(oof_f1) / np.mean(oof_f1)), 4),
              fp_normal=round(float((oof[gt == 0] > 0).mean()), 4),
              acc=round(float((oof == gt).mean()), 4),
              per_class={c: [round(v, 4) for v in per_oof[c]] for c in CLASSES})
log(f"폴드 밖 풀링(정직 추정) — 4클래스 mF1 {HONEST['macro_f1_4']} · 이벤트 mF1 {HONEST['macro_f1_event']} "
    f"· 균형 {HONEST['balance']} · 오탐 {HONEST['fp_normal']} · 정확도 {HONEST['acc']}")

# ── 출하본: 전체 데이터로 제약 하 최적 (과적합 포함, 표기함) ─────────
allrows = []
for cfg in GRID:
    cols = cols_of(cfg)
    if cols is None or len(cols) < 20: continue
    pr_, S_ = predict(cols, np.arange(len(gt)), cfg["centered"])
    m_ = metrics(pr_, S_, cols, np.arange(len(gt)))
    allrows.append(dict(cfg=cfg, n=len(cols), **{k_: v for k_, v in m_.items() if k_ != "per_class"},
                        J=J(m_["mf1"], m_["prauc"], m_["balance"]), feasible=m_["fp"] <= FP_BUDGET))
feas = [r for r in allrows if r["feasible"]]
log(f"오탐 예산 통과 설정 {len(feas)}/{len(allrows)}")
SHIP = max(feas or allrows, key=lambda r: r["J"])
log(f"출하본 {SHIP['cfg']} — n={SHIP['n']} 4클래스 mF1 {SHIP['mf1']:.4f} PR-AUC {SHIP['prauc']:.4f} "
    f"균형 {SHIP['balance']:.3f} 오탐 {SHIP['fp']:.4f}")
ship_cols = cols_of(SHIP["cfg"])
allrows.sort(key=lambda r: (-r["feasible"], -r["J"]))
json.dump(dict(honest_oof=HONEST, fold_cfg=oof_cfg, fp_budget=FP_BUDGET, weights=W,
               ship=dict(cfg=SHIP["cfg"], n=SHIP["n"], **{k_: round(v, 4) for k_, v in SHIP.items()
                                                          if k_ in ("mf1", "prauc", "balance", "fp", "J")}),
               grid_top20=[dict(cfg=r["cfg"], n=r["n"], feasible=r["feasible"],
                                **{k_: round(r[k_], 4) for k_ in ("mf1", "prauc", "balance", "fp", "J")})
                           for r in allrows[:20]], grid_size=len(GRID)),
          open(f"{BANKDIR}/search.json", "w"), ensure_ascii=False, indent=1)
with open(f"{OUT}/csv/50_optbank_search.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["rank", "feasible(오탐예산통과)", "k(클래스당)", "mix(출처)", "dedup(중복임계)", "key(품질키)",
                "centered(전역평균제거)", "n(문장수)", "macro_f1_4cls", "prauc_iou", "balance", "fp_normal", "J"])
    for i_, r in enumerate(allrows[:80], 1):
        c = r["cfg"]
        w.writerow([i_, "Y" if r["feasible"] else "N", c["k"], c["mix"], c["dedup"] or "없음", c["key"],
                    "Y" if c["centered"] else "N", r["n"], round(r["mf1"], 4), round(r["prauc"], 4),
                    round(r["balance"], 4), round(r["fp"], 4), round(r["J"], 4)])
np.savez_compressed(f"{BANKDIR}/oof_pred.npz", oof=oof, gt=gt, cam=cam)
log("→ csv/50_optbank_search.csv")

# ══════════════════════════════════════════════════════════════════
# 5) 최종 뱅크 확정 + 기준선 비교
# ══════════════════════════════════════════════════════════════════
CEN = SHIP["cfg"]["centered"]
Suse = S_POOL_C if CEN else S_POOL
lab_s = plab[ship_cols]
pred_full = topk_vote(Suse[:, ship_cols], lab_s, 4)
mem_s = {c: np.where(lab_s == i)[0] for i, c in enumerate(CLASSES)}
iou_full = wave_iou(Suse[:, ship_cols], mem_s)
per = f1_per_class(gt, pred_full)
log("출하본 전체 성능: " + " ".join(f"{c} F1 {per[c][0]:.3f}" for c in CLASSES))

def bank_baseline(bank):
    bd = load_banks(cur, [bank])[0]
    cols, names, seen = [], [], set()
    for h, c, _g in bd["rows"]:
        if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); names.append(c)
    cols = np.asarray(cols); cs = sorted(set(names))
    lb = np.array([cs.index(c) for c in names], np.int32)
    tg = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], np.int8)
    V = SENT[cols]
    pr_ = np.empty(len(FH), np.int8); io = {c: np.empty(len(FH), np.float32) for c in cs if c != "normal"}
    for s0 in range(0, len(FH), 1500):
        S = FH[s0:s0 + 1500] @ V.T
        pr_[s0:s0 + 1500] = tg[topk_vote(S, lb, len(cs))]
        w_ = wave_iou(S, {c: np.where(lb == i)[0] for i, c in enumerate(cs)})
        for c in io: io[c][s0:s0 + 1500] = w_[c]
    return pr_, io, len(cols)

rows_cmp = []
def add_cmp(name, pred, iou, n):
    p = f1_per_class(gt, pred)
    aps = []
    for c in EVENTS:
        if c in iou:
            y = (gt == CLASSES.index(c)).astype(int)
            aps.append(float(average_precision_score(y, -iou[c])))
    ev = [p[c][0] for c in EVENTS]
    all4 = [p[c][0] for c in CLASSES]
    rows_cmp.append(dict(bank=name, n=n, acc=round(float((pred == gt).mean()), 4),
                         macro_f1_4cls=round(float(np.mean(all4)), 4),
                         macro_f1=round(float(np.mean(ev)), 4), prauc=round(float(np.mean(aps)) if aps else 0.0, 4),
                         balance=round(float(min(all4) / max(np.mean(all4), 1e-9)), 4),
                         **{f"f1_{c}": round(p[c][0], 4) for c in CLASSES},
                         **{f"rec_{c}": round(p[c][2], 4) for c in CLASSES},
                         fp_normal=round(float((pred[gt == 0] > 0).mean()), 4)))
    log(f"  {name:<26} n={n:>6,} 4클래스 mF1 {rows_cmp[-1]['macro_f1_4cls']:.4f} 이벤트 mF1 {rows_cmp[-1]['macro_f1']:.4f} "
        f"PR-AUC {rows_cmp[-1]['prauc']:.4f} 균형 {rows_cmp[-1]['balance']:.3f} 오탐 {rows_cmp[-1]['fp_normal']:.4f}")

add_cmp("sourcei-OPT (본 보고서)", pred_full, iou_full, len(ship_cols))
for b in ("v1.0.8.1", "v1.0.8.0", "v1.0.12.0"):
    p_, i_, n_ = bank_baseline(b); add_cmp(f"{b} (공급)", p_, i_, n_)
with open(f"{OUT}/csv/51_optbank_compare.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(rows_cmp[0].keys()))
    w.writeheader()
    for r in rows_cmp: w.writerow(r)

# 최종 뱅크 문장 원장
with open(f"{OUT}/csv/52_optbank_sentences.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["class(클래스)", "source(출처)", "text(문장)", "m_s(배경)", "spec_sd(군집특이도)", "contrast(frames대조)"])
    for j in ship_cols:
        w.writerow([CLASSES[plab[j]], psrc[j], ptext[j], round(float(p_ms[j]), 5),
                    round(float(p_sd[j]), 5), round(float(p_con[j]), 5)])
comp = collections.Counter(f"{CLASSES[plab[j]]}/{psrc[j]}" for j in ship_cols)
log(f"최종 뱅크 {len(ship_cols)}문장 구성: {dict(comp)}")
json.dump(dict(n=len(ship_cols), cfg=SHIP["cfg"], composition={k: int(v) for k, v in comp.items()},
               per_class={c: [round(v, 4) for v in per[c]] for c in CLASSES},
               honest_oof=HONEST, fp_budget=FP_BUDGET,
               compare=rows_cmp),
          open(f"{BANKDIR}/optbank.json", "w"), ensure_ascii=False, indent=1)
np.savez_compressed(f"{BANKDIR}/optbank_vectors.npz",
                    vecs=(POOL_C if CEN else POOL)[ship_cols].astype(np.float32),
                    text=np.array([ptext[j] for j in ship_cols]),
                    cls=np.array([CLASSES[plab[j]] for j in ship_cols]),
                    src=np.array([psrc[j] for j in ship_cols]))
np.savez_compressed(f"{BANKDIR}/optbank_sourcei_pred.npz", pred=pred_full,
                    iou=np.stack([iou_full[c] for c in EVENTS], 1), ids=np.array(ids),
                    percls=np.stack([Suse[:, ship_cols][:, lab_s == i].max(1) for i in range(4)], 1))
log("→ csv/51_optbank_compare.csv, csv/52_optbank_sentences.csv, optbank/*.npz")
print("DONE")
