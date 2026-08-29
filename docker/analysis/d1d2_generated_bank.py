#!/usr/bin/env python3
"""D1 대조쌍 + 차 벡터 · D2 CuPL 식 LLM 생성 + 필터 — 규칙만으로 뱅크를 만들어 31종과 겨룬다.

목적 ③("그런 prompt 를 생성하려면 어떤 방식이 필요한지")에 대한 **실측** 답. 앞선 절에서 얻은
설계 규칙(클래스별 승리 템플릿·장소어휘 억제·대조 우선·현장 오탐 목록)만 주고 Gemini 로 340문장을
생성한 뒤(D2), 라벨 없는 컷으로 걸러 sourcei GT 로 채점한다. D1 은 같은 장면 접두에 이벤트/정상
술어만 바꾼 **대조쌍**을 만들고, 쌍의 차 방향 normalize(s_ev − s_no) 를 클래스 벡터로 쓴다.

⚠️ 해석 한계를 먼저 박아둔다: 생성에 쓴 규칙은 **sourcei GT 로부터 측정된 것**이다. 따라서 이
실험은 "새 현장으로의 일반화"가 아니라 **"측정된 설계 규칙이 뱅크 부피를 대체할 수 있나"** 를
재는 것이다. 카메라 홀드아웃 수치도 규칙이 15대 전체에서 나왔으므로 완전 무오염이 아니다.

필터는 전부 라벨 불필요:
  · m_s 컷      — 90,084 프레임 배경 평균이 상위인 문장 제거 (어디서나 반응하는 문장)
  · 특이도 컷   — kmeans64 군집 간 표준편차가 하위인 문장 제거 (군집을 못 가르는 문장)
  · 중복 컷     — 클래스 내 코사인 > 0.95 근접중복 제거
컷 비율은 E1 결과가 나온 뒤 **사전에 고정**(25%)하고, 사후 스윕은 따로 표기한다.
"""
import os, sys, json, csv, glob, re, collections, urllib.parse, urllib.request
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "4")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote, wave_iou
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.model_selection import GroupKFold
from sklearn.metrics import average_precision_score, roc_auc_score
import fiftyone as fo
from fiftyone import ViewField as F

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
GEN = "/workspace/gen_cupl.json"
EMB = os.environ.get("EMBED_URL", "http://embedding-service:8003/embed_text")
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

# ══════════════════════════════════════════════════════════════════
# 1) D1 대조쌍 생성 — 같은 장면 접두, 술어만 이벤트/정상
# ══════════════════════════════════════════════════════════════════
SCENES = ["a department store sales floor", "a shop aisle between clothing racks", "an escalator landing",
          "a back corridor", "a concrete stairwell", "a back-of-house storage room", "an underground parking level",
          "a food court seating area", "a server room", "a loading dock", "a dim basement passage",
          "a bright atrium walkway", "a cosmetics counter area", "a checkout area", "a service elevator lobby"]
EV = {"falldown": ["a person lies motionless on the ground", "a body is sprawled flat on the floor",
                   "someone has collapsed and does not move", "a person lies face down and still",
                   "an unattended person lies on their back"],
      "fire": ["an open flame is burning", "flames are spreading across a surface",
               "a small flame flickers on the floor", "an object is on fire and glowing",
               "a fire burns brightly"],
      "smoke": ["thick smoke is spreading", "white smoke drifts across the space",
                "dense grey smoke obscures the view", "smoke pools along the ceiling",
                "a thin plume of smoke rises"]}
NO = ["people are walking normally", "the space is empty and quiet", "a worker crouches to pick something up",
      "a cleaner is mopping the floor", "shoppers are browsing the shelves"]
pairs = []                                             # (class, ev_text, no_text)
for cls, evs in EV.items():
    for sc in SCENES:
        for k, ev in enumerate(evs):
            no = NO[k % len(NO)]
            pairs.append((cls, f"In {sc}, {ev}.", f"In {sc}, {no}."))
log(f"D1 대조쌍 {len(pairs)}쌍 (장면 {len(SCENES)} × 이벤트술어 5 × 3클래스)")

gen = json.load(open(GEN))
log("D2 생성문장 " + " ".join(f"{k} {len(v)}" for k, v in gen.items()))

# ══════════════════════════════════════════════════════════════════
# 2) /embed_text 로 인코딩 — 저장된 뱅크 벡터와 동일 공간(cos=1.000000 실측)
# ══════════════════════════════════════════════════════════════════
CACHE = f"{OUT}/gen_vectors.npz"
texts = []
for cls in CLASSES: texts += [(cls, "gen", s) for s in gen.get(cls, [])]
for cls, ev, no in pairs: texts += [(cls, "pair_ev", ev), ("normal", "pair_no", no)]
uniq = list(dict.fromkeys(t for _c, _k, t in texts))
if os.path.exists(CACHE):
    z = np.load(CACHE, allow_pickle=True)
    cached = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
else: cached = {}
need = [t for t in uniq if t not in cached]
log(f"인코딩 대상 고유 {len(uniq)} · 캐시 {len(uniq)-len(need)} · 신규 {len(need)}")
for i, t in enumerate(need):
    body = urllib.parse.urlencode({"text": t}).encode()
    r = json.loads(urllib.request.urlopen(urllib.request.Request(EMB, data=body), timeout=300).read())
    v = np.asarray(r["vector"], dtype=np.float32); cached[t] = v / np.linalg.norm(v)
    if (i + 1) % 100 == 0: log(f"  {i+1}/{len(need)}")
np.savez_compressed(CACHE, texts=np.array(list(cached)), vecs=np.stack([cached[t] for t in cached]))
GV = np.stack([cached[t] for t in uniq]); t2i = {t: i for i, t in enumerate(uniq)}
assert np.allclose(np.linalg.norm(GV, axis=1), 1.0, atol=1e-4)
log(f"생성 문장 벡터 {GV.shape}")

# 계약 검증: 저장된 뱅크 문장 3개를 다시 인코딩해 cos==1 확인
conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()
cur.execute("""SELECT s.content_hash, MIN(s.text) FROM bank_sentences s
               JOIN image_embeddings e ON e.entity_type='prompt' AND e.entity_id=s.content_hash
               GROUP BY 1 ORDER BY 1 LIMIT 3""")
chk = cur.fetchall()
for h, t in chk:
    body = urllib.parse.urlencode({"text": t}).encode()
    v = np.asarray(json.loads(urllib.request.urlopen(urllib.request.Request(EMB, data=body), timeout=300).read())["vector"], np.float32)
    v /= np.linalg.norm(v)
    cur.execute("SELECT embedding::text FROM image_embeddings WHERE entity_type='prompt' AND entity_id=%s", (h,))
    w = np.fromstring(cur.fetchone()[0].strip("[]"), sep=",", dtype=np.float32); w /= np.linalg.norm(w)
    c = float(v @ w); assert c > 0.9999, (h, c)
log(f"인코더 동일성 계약 OK — 저장 벡터 대조 cos {c:.8f} (3/3)")

# ══════════════════════════════════════════════════════════════════
# 3) 라벨 없는 통계: m_s(배경) · 특이도 SD(군집 간)
# ══════════════════════════════════════════════════════════════════
cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
e2k = dict(cur.fetchall()); NK = 64
NG = GV.shape[0]
Ak = np.zeros((NG, NK), np.float64); cnt = np.zeros(NK, np.int64); msum = np.zeros(NG, np.float64); ntot = 0
buf_v, buf_k = [], []
def flush():
    global ntot, msum
    if not buf_v: return
    X = np.vstack(buf_v); X /= np.linalg.norm(X, axis=1, keepdims=True)
    S = (X @ GV.T)
    msum += S.sum(0); ntot += len(buf_k); kk = np.asarray(buf_k)
    for k0 in np.unique(kk):
        mm = kk == k0; Ak[:, k0] += S[mm].sum(0); cnt[k0] += int(mm.sum())
    buf_v.clear(); buf_k.clear()
with conn.cursor(name="fr2") as c2:
    c2.itersize = 4000
    c2.execute("SELECT entity_id, embedding::text FROM image_embeddings WHERE entity_type='frame'")
    for eid, vt in c2:
        k = e2k.get(eid)
        if k is None: continue
        buf_v.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32)); buf_k.append(k)
        if len(buf_v) >= 4000: flush()
flush()
assert ntot == 90084, ntot
g_ms = (msum / ntot).astype(np.float32)
Ak = (Ak / np.maximum(cnt, 1)).astype(np.float32)
g_sd = (Ak - Ak.mean(1, keepdims=True)).std(1)
log(f"라벨-free 통계 — m_s {g_ms.mean():.4f}±{g_ms.std():.4f} · 특이도SD {g_sd.mean():.5f}")

# ══════════════════════════════════════════════════════════════════
# 4) sourcei GT / frames 표본
# ══════════════════════════════════════════════════════════════════
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; ids = list(d["ids"]); cams = np.unique(cam)
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
ds = fo.load_dataset("frames"); ncls_raw, femb = ds.match(F("modality") == "frame").values(["normalized_class", "image_embedding"])
ncls = np.array([x or "none" for x in ncls_raw])
fi = np.where(ncls == "fire")[0]; si = np.where(ncls == "smoke")[0]; ni = np.where(np.isin(ncls, ["none", "person"]))[0]
sub = np.concatenate([fi, si, RNG.choice(ni, 20000, replace=False)])
FF = np.asarray([femb[i] for i in sub], dtype=np.float32); FF /= np.linalg.norm(FF, axis=1, keepdims=True)
ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], dtype=np.int8); del femb
log(f"sourcei {FH.shape} · frames 표본 {FF.shape}")

def macro_f1(t, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = int(((p == c) & (t == c)).sum()); fp = int(((p == c) & (t != c)).sum()); fn = int(((p != c) & (t == c)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))
idx_by_cam = {c: np.where(cam == c)[0] for c in cams}
BOOT = [np.concatenate([idx_by_cam[c] for c in RNG.choice(cams, size=len(cams), replace=True)]) for _ in range(2000)]
def paired_ci(p1, p0):
    a = np.array([macro_f1(gt[m], p1[m]) - macro_f1(gt[m], p0[m]) for m in BOOT])
    return float(a.mean()), float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5)), float((a > 0).mean())

# ══════════════════════════════════════════════════════════════════
# 5) 후보 뱅크 구성
# ══════════════════════════════════════════════════════════════════
gi = collections.defaultdict(list)                    # (kind, cls) → GV 행
for cls, kind, t in texts: gi[(kind, cls)].append(t2i[t])
gi = {k: np.array(sorted(set(v))) for k, v in gi.items()}

def dedupe(rows, thr=0.95):
    order = rows[np.argsort(g_ms[rows])]              # 조용한 문장 우선
    V = GV[order]; keep, kept = [], []
    for j in range(len(order)):
        if kept and float(np.max(V[j] @ V[kept].T)) > thr: continue
        kept.append(j); keep.append(order[j])
    return np.array(keep)

def cut(rows, q_ms=25, q_sd=25, dd=True):
    r = rows[(g_ms[rows] < np.percentile(g_ms[rows], 100 - q_ms)) & (g_sd[rows] > np.percentile(g_sd[rows], q_sd))]
    return dedupe(r) if dd and len(r) > 1 else r

BANKS_GEN = {}
BANKS_GEN["GEN-raw (생성 전량)"] = {c: gi[("gen", c)] for c in CLASSES}
BANKS_GEN["GEN-filtered (m_s25+특이도25+중복)"] = {c: cut(gi[("gen", c)]) for c in CLASSES}
BANKS_GEN["GEN+pairs (대조쌍 이벤트문 추가)"] = {c: np.concatenate([cut(gi[("gen", c)]), gi.get(("pair_ev", c), np.array([], int))]).astype(int)
                                            if c != "normal" else np.concatenate([cut(gi[("gen", c)]), gi[("pair_no", "normal")]]).astype(int)
                                            for c in CLASSES}
# GEN-small: 클래스별 라벨-free 대조 점수 상위 30 (frames 약참조 사용 — fire/smoke 만 가능,
# falldown/normal 은 특이도 SD 로 대체. 대리지표가 클래스마다 다른 점을 표에 명시한다)
gcon = np.zeros(NG)
for s0 in range(0, len(FF), 2000):
    S = FF[s0:s0 + 2000] @ GV.T; r = ref[s0:s0 + 2000]
    gcon += S[r > 0].sum(0) / max((ref > 0).sum(), 1) - S[r == 0].sum(0) / max((ref == 0).sum(), 1)
small = {}
for c in CLASSES:
    rows = gi[("gen", c)]
    key = gcon if c in ("fire", "smoke") else g_sd
    small[c] = rows[np.argsort(-key[rows])[:30]]
BANKS_GEN["GEN-small30 (클래스당 30문장)"] = small

def score_gen(mem, FR):
    cs = [c for c in CLASSES if len(mem.get(c, [])) > 0]
    cols = np.concatenate([mem[c] for c in cs])
    lab = np.concatenate([np.full(len(mem[c]), i, np.int32) for i, c in enumerate(cs)])
    to_gt = np.array([CLASSES.index(c) for c in cs], np.int8)
    V = GV[cols]
    pred = np.empty(len(FR), np.int8); iou = {c: np.empty(len(FR), np.float32) for c in cs if c != "normal"}
    for s0 in range(0, len(FR), 1500):
        S = FR[s0:s0 + 1500] @ V.T
        pred[s0:s0 + 1500] = to_gt[topk_vote(S, lab, len(cs))]
        w = wave_iou(S, {c: np.where(lab == i)[0] for i, c in enumerate(cs)})
        for c in iou: iou[c][s0:s0 + 1500] = w[c]
    return pred, iou, cols, lab, cs, to_gt

def score_db(bank, FR):
    """저장된 뱅크 — 비교 기준선."""
    bd = load_banks(cur, [bank])[0]
    cols, names, seen = [], [], set()
    for h, c, _g in bd["rows"]:
        if h in H2C and h not in seen: seen.add(h); cols.append(H2C[h]); names.append(c)
    cols = np.asarray(cols); cs = sorted(set(names))
    lab = np.array([cs.index(c) for c in names], np.int32)
    to_gt = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], np.int8)
    V = SENT[cols]
    pred = np.empty(len(FR), np.int8); iou = {c: np.empty(len(FR), np.float32) for c in cs if c != "normal"}
    for s0 in range(0, len(FR), 1500):
        S = FR[s0:s0 + 1500] @ V.T
        pred[s0:s0 + 1500] = to_gt[topk_vote(S, lab, len(cs))]
        w = wave_iou(S, {c: np.where(lab == i)[0] for i, c in enumerate(cs)})
        for c in iou: iou[c][s0:s0 + 1500] = w[c]
    return pred, iou, len(cols)

H2C, SENT = load_sentence_vectors(cur)
log(f"저장 문장 {SENT.shape} (기준선 채점용)")

rows_out, prauc_rows = [], []
REFB = "v1.0.8.1"
base_pred, base_iou, base_n = score_db(REFB, FH)
base_mf1 = macro_f1(gt, base_pred)
log(f"기준선 {REFB}: 문장 {base_n:,} acc {(base_pred==gt).mean():.4f} mF1 {base_mf1:.4f}")
def prauc(iou, cls):
    y = (gt == CLASSES.index(cls)).astype(int)
    s = -iou[cls]                                     # IoU 작을수록 이벤트
    return float(average_precision_score(y, s)), float(roc_auc_score(y, s))

for cls in ("falldown", "fire", "smoke"):
    ap, au = prauc(base_iou, cls)
    prauc_rows.append(dict(bank=f"{REFB} (저장 뱅크 최고)", n=base_n, cls=cls, pr_auc=round(ap, 4), roc_auc=round(au, 4)))
rows_out.append(dict(bank=f"{REFB} (저장 뱅크 최고)", n_sent=base_n, acc=round(float((base_pred == gt).mean()), 4),
                     macro_f1=round(base_mf1, 4), d_mf1=0.0, ci_lo=0.0, ci_hi=0.0, p_gt0=0.5,
                     rec_fall=round(float((base_pred[gt == 1] == 1).mean()), 4), rec_fire=round(float((base_pred[gt == 2] == 2).mean()), 4),
                     rec_smoke=round(float((base_pred[gt == 3] == 3).mean()), 4),
                     fp_normal=round(float((base_pred[gt == 0] > 0).mean()), 4)))
for b2 in ("v1.0.8.0", "v1.0.12.0"):
    p2, i2, n2 = score_db(b2, FH); m2 = macro_f1(gt, p2); _mu, lo, hi, pg = paired_ci(p2, base_pred)
    rows_out.append(dict(bank=f"{b2} (저장 뱅크)", n_sent=n2, acc=round(float((p2 == gt).mean()), 4), macro_f1=round(m2, 4),
                         d_mf1=round(m2 - base_mf1, 4), ci_lo=round(lo, 4), ci_hi=round(hi, 4), p_gt0=round(pg, 3),
                         rec_fall=round(float((p2[gt == 1] == 1).mean()), 4), rec_fire=round(float((p2[gt == 2] == 2).mean()), 4),
                         rec_smoke=round(float((p2[gt == 3] == 3).mean()), 4), fp_normal=round(float((p2[gt == 0] > 0).mean()), 4)))
    for cls in ("falldown", "fire", "smoke"):
        ap, au = prauc(i2, cls); prauc_rows.append(dict(bank=f"{b2} (저장 뱅크)", n=n2, cls=cls, pr_auc=round(ap, 4), roc_auc=round(au, 4)))
    log(f"  {b2}: 문장 {n2:,} mF1 {m2:.4f} Δ{m2-base_mf1:+.4f}")

gen_preds = {}
for name, mem in BANKS_GEN.items():
    pred, iou, cols, lab, cs, to_gt = score_gen(mem, FH)
    gen_preds[name] = (pred, mem)
    mf1 = macro_f1(gt, pred); _mu, lo, hi, pg = paired_ci(pred, base_pred)
    n = int(sum(len(v) for v in mem.values()))
    rows_out.append(dict(bank=name, n_sent=n, acc=round(float((pred == gt).mean()), 4), macro_f1=round(mf1, 4),
                         d_mf1=round(mf1 - base_mf1, 4), ci_lo=round(lo, 4), ci_hi=round(hi, 4), p_gt0=round(pg, 3),
                         rec_fall=round(float((pred[gt == 1] == 1).mean()), 4), rec_fire=round(float((pred[gt == 2] == 2).mean()), 4),
                         rec_smoke=round(float((pred[gt == 3] == 3).mean()), 4), fp_normal=round(float((pred[gt == 0] > 0).mean()), 4)))
    for cls in ("falldown", "fire", "smoke"):
        if cls in iou:
            ap, au = prauc(iou, cls); prauc_rows.append(dict(bank=name, n=n, cls=cls, pr_auc=round(ap, 4), roc_auc=round(au, 4)))
    log(f"  {name}: 문장 {n:,} acc {(pred==gt).mean():.4f} mF1 {mf1:.4f} Δ{mf1-base_mf1:+.4f} CI[{lo:+.4f},{hi:+.4f}] "
        f"재현 fall {(pred[gt==1]==1).mean():.3f} fire {(pred[gt==2]==2).mean():.3f} smoke {(pred[gt==3]==3).mean():.3f} 오탐 {(pred[gt==0]>0).mean():.4f}")

# ── D1 차 벡터 뱅크: normalize(s_ev − s_no) 를 클래스 벡터로 ──────────
DV, dlab = [], []
cs_d = ["normal", "falldown", "fire", "smoke"]
for cls, ev, no in pairs:
    v = GV[t2i[ev]] - GV[t2i[no]]
    n_ = np.linalg.norm(v)
    if n_ < 1e-6: continue
    DV.append(v / n_); dlab.append(cs_d.index(cls))
for j in gi[("gen", "normal")]: DV.append(GV[j]); dlab.append(0)      # normal 은 원본 문장 유지
DV = np.stack(DV).astype(np.float32); dlab = np.array(dlab, np.int32)
to_gt_d = np.array([CLASSES.index(c) for c in cs_d], np.int8)
pred = np.empty(len(FH), np.int8); iou_d = {c: np.empty(len(FH), np.float32) for c in cs_d if c != "normal"}
for s0 in range(0, len(FH), 1500):
    S = FH[s0:s0 + 1500] @ DV.T
    pred[s0:s0 + 1500] = to_gt_d[topk_vote(S, dlab, len(cs_d))]
    w = wave_iou(S, {c: np.where(dlab == i)[0] for i, c in enumerate(cs_d)})
    for c in iou_d: iou_d[c][s0:s0 + 1500] = w[c]
mf1 = macro_f1(gt, pred); _mu, lo, hi, pg = paired_ci(pred, base_pred)
gen_preds["GEN-diff (대조쌍 차 벡터)"] = (pred, None)
rows_out.append(dict(bank="GEN-diff (대조쌍 차 벡터)", n_sent=int(len(DV)), acc=round(float((pred == gt).mean()), 4),
                     macro_f1=round(mf1, 4), d_mf1=round(mf1 - base_mf1, 4), ci_lo=round(lo, 4), ci_hi=round(hi, 4), p_gt0=round(pg, 3),
                     rec_fall=round(float((pred[gt == 1] == 1).mean()), 4), rec_fire=round(float((pred[gt == 2] == 2).mean()), 4),
                     rec_smoke=round(float((pred[gt == 3] == 3).mean()), 4), fp_normal=round(float((pred[gt == 0] > 0).mean()), 4)))
for cls in ("falldown", "fire", "smoke"):
    ap, au = prauc(iou_d, cls); prauc_rows.append(dict(bank="GEN-diff (대조쌍 차 벡터)", n=int(len(DV)), cls=cls, pr_auc=round(ap, 4), roc_auc=round(au, 4)))
log(f"  GEN-diff: 벡터 {len(DV):,} acc {(pred==gt).mean():.4f} mF1 {mf1:.4f} Δ{mf1-base_mf1:+.4f} CI[{lo:+.4f},{hi:+.4f}]")

# ── 카메라 홀드아웃 (규칙이 15대 전체에서 나왔으므로 부분 오염 — 표에 명시) ──
hold = []
gkf = GroupKFold(n_splits=5)
for name, (pred, _mem) in list(gen_preds.items()) + [(f"{REFB} (저장 뱅크 최고)", (base_pred, None))]:
    mf = []
    for _tr, te in gkf.split(np.arange(len(gt)), gt, groups=cam):
        cl = tuple(int(x) for x in np.unique(gt[te]) if x > 0)
        if cl: mf.append(macro_f1(gt[te], pred[te], classes=cl))
    hold.append(dict(bank=name, folds=len(mf), holdout_mf1=round(float(np.mean(mf)), 4), sd=round(float(np.std(mf)), 4)))
    log(f"  홀드아웃 {name}: {np.mean(mf):.4f} ± {np.std(mf):.4f}")

# ══════════════════════════════════════════════════════════════════
# 6) CSV
# ══════════════════════════════════════════════════════════════════
with open(f"{OUT}/csv/38_generated_bank.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "n_sent(문장수)", "acc(정확도)", "macro_f1", "d_mf1(v1081대비Δ)", "ci_lo", "ci_hi", "p_gt0",
                                      "rec_fall", "rec_fire", "rec_smoke", "fp_normal(정상오탐)"])
    w.writeheader()
    for r in rows_out: w.writerow(dict(zip(w.fieldnames, r.values())))
with open(f"{OUT}/csv/39_generated_prauc.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "n_sent(문장수)", "class(클래스)", "pr_auc", "roc_auc"])
    w.writeheader()
    for r in prauc_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
with open(f"{OUT}/csv/40_generated_sentences.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["class(클래스)", "kind(출처)", "text(문장)", "m_s(배경평균)", "spec_sd(군집특이도)", "contrast(frames대조)",
                "in_filtered(필터통과)", "in_small30(상위30)"])
    filt = BANKS_GEN["GEN-filtered (m_s25+특이도25+중복)"]
    for cls, kind, t in texts:
        j = t2i[t]
        w.writerow([cls, kind, t, round(float(g_ms[j]), 5), round(float(g_sd[j]), 5), round(float(gcon[j]), 5),
                    "Y" if (kind == "gen" and j in set(filt.get(cls, []))) else "",
                    "Y" if (kind == "gen" and j in set(small.get(cls, []))) else ""])
with open(f"{OUT}/csv/41_generated_holdout.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "folds(폴드수)", "holdout_mf1(카메라홀드아웃)", "sd(폴드간표준편차)"])
    w.writeheader()
    for r in hold: w.writerow(dict(zip(w.fieldnames, r.values())))
log("→ csv/38_generated_bank.csv, 39_generated_prauc.csv, 40_generated_sentences.csv, 41_generated_holdout.csv")

# ══════════════════════════════════════════════════════════════════
# 7) 그림
# ══════════════════════════════════════════════════════════════════
NOTE = ("생성 규칙은 sourcei GT 로부터 측정된 것 → 이 실험은 '새 현장 일반화'가 아니라 "
        "'측정된 설계 규칙이 뱅크 부피를 대체하나' 를 잰다 · 카메라 군집 부트스트랩 2,000회")
order = sorted(rows_out, key=lambda r: -r["macro_f1"])
fig, axes = plt.subplots(1, 3, figsize=(23, 7.4), gridspec_kw={"width_ratios": [1.25, 1.0, 0.9]})
ax = axes[0]
y = np.arange(len(order))
cols_ = ["#1baf7a" if r["bank"].startswith("GEN") else "#8a887f" for r in order]
b_ = ax.barh(y, [r["macro_f1"] for r in order], color=cols_, alpha=.9)
for i, r in enumerate(order):
    ax.text(r["macro_f1"] + .004, i, f"{r['macro_f1']:.3f}  ({r['n_sent']:,}문장)", va="center", fontsize=8.6)
ax.set_yticks(y); ax.set_yticklabels([r["bank"][:38] for r in order], fontsize=8.6); ax.invert_yaxis()
ax.set_xlim(0, max(r["macro_f1"] for r in order) * 1.28)
ax.set_xlabel("sourcei GT 이벤트 macro-F1 (top-K 투표)")
ax.set_title("① 규칙으로 만든 뱅크(초록) vs 공급 뱅크(회색)", loc="left", fontsize=11)
ax = axes[1]
x = np.arange(len(order)); w2 = 0.2
for k, (key, lab_, col) in enumerate([("rec_fall", "falldown 재현", CC["falldown"]), ("rec_fire", "fire 재현", CC["fire"]),
                                      ("rec_smoke", "smoke 재현", CC["smoke"]), ("fp_normal", "정상 오탐", "#52514e")]):
    ax.bar(x + (k - 1.5) * w2, [r[key] for r in order], w2 * 0.92, color=col, label=lab_)
ax.set_xticks(x); ax.set_xticklabels([r["bank"][:16] for r in order], rotation=32, ha="right", fontsize=7.6)
ax.legend(frameon=False, fontsize=9, ncol=2); ax.set_ylabel("비율")
ax.set_title("② 클래스별 재현율과 정상 오탐 — 부피가 아니라 무엇을 놓치나", loc="left", fontsize=11)
ax = axes[2]
hb = sorted(hold, key=lambda r: -r["holdout_mf1"])
yh = np.arange(len(hb))
ax.barh(yh, [r["holdout_mf1"] for r in hb], xerr=[r["sd"] for r in hb], color=["#1baf7a" if r["bank"].startswith("GEN") else "#8a887f" for r in hb],
        alpha=.9, error_kw=dict(ecolor="#52514e", lw=1))
for i, r in enumerate(hb): ax.text(r["holdout_mf1"] + r["sd"] + .006, i, f"{r['holdout_mf1']:.3f}", va="center", fontsize=8.6)
ax.set_yticks(yh); ax.set_yticklabels([r["bank"][:24] for r in hb], fontsize=8); ax.invert_yaxis()
ax.set_xlabel("카메라 홀드아웃 macro-F1 (GroupKFold 5, ±폴드 SD)")
ax.set_title("③ 카메라를 갈라도 유지되나", loc="left", fontsize=11)
fig.suptitle("D2 CuPL 식 생성 + 라벨-free 필터 · D1 대조쌍/차 벡터 — 340문장으로 12,511문장 공급 뱅크와 겨룬다\n" + NOTE,
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f42_generated_bank.png", dpi=160); plt.close(fig)
log("saved f42")

fig, axes = plt.subplots(1, 2, figsize=(16.5, 6.6))
ax = axes[0]
bl = sorted({r["bank"] for r in prauc_rows}, key=lambda b: -np.mean([r["pr_auc"] for r in prauc_rows if r["bank"] == b]))
x = np.arange(len(bl)); w2 = 0.26
for k, cls in enumerate(("falldown", "fire", "smoke")):
    v = [next((r["pr_auc"] for r in prauc_rows if r["bank"] == b and r["cls"] == cls), np.nan) for b in bl]
    ax.bar(x + (k - 1) * w2, v, w2 * 0.92, color=CC[cls], label=cls)
ax.set_xticks(x); ax.set_xticklabels([b[:18] for b in bl], rotation=32, ha="right", fontsize=7.8)
ax.set_ylabel("PR-AUC (분포-IoU 연속값, 임계 무관)"); ax.legend(frameon=False, fontsize=9)
ax.set_title("④ 임계 무관 랭킹 품질 — §13 이 고른 최적 점수 함수로 재평가", loc="left", fontsize=11)
ax = axes[1]
for cls in CLASSES:
    rows = gi[("gen", cls)]
    ax.scatter(g_ms[rows], g_sd[rows], s=26, color=CC[cls], alpha=.75, edgecolor="white", lw=.5, label=f"{cls} ({len(rows)})")
allg = np.concatenate([gi[("gen", c)] for c in CLASSES])
ax.axvline(np.percentile(g_ms[allg], 75), color="#e34948", ls="--", lw=1.2)
ax.axhline(np.percentile(g_sd[allg], 25), color="#2a78d6", ls="--", lw=1.2)
ax.text(np.percentile(g_ms[allg], 75), ax.get_ylim()[1], " m_s 상위 25% 컷 →", color="#e34948", fontsize=8.5, va="top")
ax.text(ax.get_xlim()[0], np.percentile(g_sd[allg], 25), "↓ 특이도 하위 25% 컷", color="#2a78d6", fontsize=8.5, va="top")
ax.set_xlabel("m_s — 90,084 프레임 배경 평균 코사인 (낮을수록 조용한 문장)")
ax.set_ylabel("특이도 SD — kmeans64 군집 간 표준편차 (높을수록 군집을 가른다)")
ax.legend(frameon=False, fontsize=9)
ax.set_title("⑤ 생성 문장 340개가 라벨-free 컷 평면에서 어디 놓이나", loc="left", fontsize=11)
fig.suptitle("생성 뱅크의 랭킹 품질과, 라벨 없이 문장을 거르는 두 축 — 컷 비율은 사전 고정(25%)\n" + NOTE, x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f43_generated_filter.png", dpi=160); plt.close(fig)
log("saved f43")
json.dump(dict(banks=rows_out, prauc=prauc_rows, holdout=hold, n_pairs=len(pairs),
               n_gen={k: len(v) for k, v in gen.items()},
               ms_stats=dict(mean=float(g_ms.mean()), sd=float(g_ms.std())),
               sd_stats=dict(mean=float(g_sd.mean()), sd=float(g_sd.std()))),
          open(f"{OUT}/generated_bank_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
