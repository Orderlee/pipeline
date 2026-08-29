#!/usr/bin/env python3
"""E1 프루닝 3컷 · C3 Biclustering · D3 문장 방향 산술 — 메모리 안전 + 체크포인트 판.

v1 은 뱅크 v1.0.12.0(49,140문장)에서 `7498×49140` 행렬(1.5GB)을 한 번에 잡아 OOM 으로 죽었다.
공유 호스트(62GB, 2인 사용)에서는 뱅크 크기가 4배 차이 나므로 **프레임을 행 청크로 흘린다**.
그리고 뱅크 하나가 끝날 때마다 CSV 에 append 해서, 죽어도 그 앞까지는 남는다(재시작 시 skip).

스테이지 순서를 값싼 것부터로 바꿨다: C3 → D3 → E1. v1 은 제일 비싼 E1 을 먼저 돌려
2,000초를 쓰고도 아무 산출물을 남기지 못했다.

공통 1패스(m_s, A_k)는 `Ak_kmeans64.npy` / `m_s_bg90k.npy` 캐시를 재사용한다.
중복컷 마스크는 **뱅크당 한 번만** 계산한다 — 중복은 컷의 성질이 아니라 뱅크의 성질이고,
컷마다 다시 계산하면 O(n²d) 를 3배 낸다.
"""
import os, sys, json, csv, glob, collections, gc
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "4")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.cluster import SpectralCoclustering
import fiftyone as fo
from fiftyone import ViewField as F

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
CKPT = f"{OUT}/e1_ckpt.jsonl"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)
def rss():
    return int(open("/proc/self/statm").read().split()[1]) * 4096 / 2**30

conn = psycopg2.connect(DSN); cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
NS = SENT.shape[0]
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes = collections.defaultdict(dict)
for h, c, n in cur: votes[h][c] = n
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes.items()}
cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1")
text = dict(cur.fetchall())
scls = np.array([CLASSES.index(maj[h]) if h in maj and maj[h] in CLASSES else -1 for h in hashes])
log(f"문장 {SENT.shape} · RSS {rss():.2f}GB")

# ── 공통 1패스 (캐시 필수 — v1 이 이미 만들어 뒀다) ────────────────────
cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
e2k = dict(cur.fetchall()); NK = 64
assert len(e2k) == 90084, len(e2k)
cnt = np.bincount(np.fromiter(e2k.values(), np.int64), minlength=NK)
Ak = np.load(f"{OUT}/Ak_kmeans64.npy"); m_s = np.load(f"{OUT}/m_s_bg90k.npy")
assert Ak.shape == (NS, NK), Ak.shape
recon = (Ak * (cnt / 90084)).sum(1)
assert np.abs(recon - m_s).max() < 1e-4, np.abs(recon - m_s).max()
R = Ak - Ak.mean(1, keepdims=True)
spec_sd = R.std(1)
log(f"1패스 캐시 — m_s {m_s.mean():.4f}±{m_s.std():.4f} · 특이도SD {spec_sd.mean():.5f} (재구성오차 {np.abs(recon-m_s).max():.2e})")

# ── sourcei GT / frames 표본 ─────────────────────────────────────────
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; ids = list(d["ids"]); cams = np.unique(cam)
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True)
del hemb
ds = fo.load_dataset("frames"); ncls_raw, femb = ds.match(F("modality") == "frame").values(["normalized_class", "image_embedding"])
ncls = np.array([x or "none" for x in ncls_raw])
fi = np.where(ncls == "fire")[0]; si = np.where(ncls == "smoke")[0]; ni = np.where(np.isin(ncls, ["none", "person"]))[0]
sub = np.concatenate([fi, si, RNG.choice(ni, 20000, replace=False)])
FF = np.asarray([femb[i] for i in sub], dtype=np.float32); FF /= np.linalg.norm(FF, axis=1, keepdims=True)
ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], dtype=np.int8)
del femb; gc.collect()
assert len(sub) == 24792 and len(fi) == 1578
log(f"sourcei {FH.shape} · frames 표본 {FF.shape} · RSS {rss():.2f}GB")

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

_m = json.load(open(f"{OUT}/metrics.json"))
BANKS = [str(b) for b in d["banks"] if set(_m["banks"][str(b)]["classes"]) & {"falldown", "fire", "smoke"}]
assert len(BANKS) == 31, len(BANKS)
FOCUS = ["v1.0.8.0", "v1.0.8.1"]
bank_defs = {b["version"]: b for b in load_banks(cur, BANKS)}
BANKS = [b for b in BANKS if b in bank_defs]
BANKS.sort(key=lambda b: len({h for h, _c, _g in bank_defs[b]["rows"] if h in h2c}))   # 작은 뱅크 먼저
log(f"대상 뱅크 {len(BANKS)}종 (작은 것부터)")

def bank_cols(bank):
    cols, names, seen = [], [], set()
    for h, c, _g in bank_defs[bank]["rows"]:
        if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); names.append(c)
    cols = np.asarray(cols); cs = sorted(set(names))
    lab = np.array([cs.index(c) for c in names], dtype=np.int32)
    to_gt = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], dtype=np.int8)
    return cols, lab, cs, to_gt

def score_chunked(FR, cols, lab, n_cls, to_gt, masks, chunk=1500):
    """프레임을 행 청크로 흘리며 여러 컷(masks)의 예측을 한 번의 행렬곱으로 모두 낸다.
    v1 의 OOM 지점 — 뱅크 전체 열을 프레임 전량과 곱해 1.5GB 를 잡았다."""
    V = SENT[cols]
    preds = {k: np.empty(len(FR), np.int8) for k in masks}
    for s0 in range(0, len(FR), chunk):
        S = FR[s0:s0 + chunk] @ V.T
        for k, mk in masks.items():
            preds[k][s0:s0 + chunk] = to_gt[topk_vote(S[:, mk], lab[mk], n_cls)]
        del S
    del V
    return preds

# ══════════════════════════════════════════════════════════════════
# C3 — Biclustering (문장군 × 프레임군집군)
# ══════════════════════════════════════════════════════════════════
Z = (R - R.mean(0)) / (R.std(0) + 1e-9)
W = np.clip(Z, 0, None)
act = np.where((W.sum(1) > 0) & (scls >= 0))[0]
log(f"C3 입력 {len(act):,} 문장 × {NK} 군집")
NB = 6
co = SpectralCoclustering(n_clusters=NB, random_state=0, svd_method="arpack").fit(W[act])
srow, scol = co.row_labels_, co.column_labels_
cur.execute("SELECT cluster_id, mode() WITHIN GROUP (ORDER BY project) FROM analysis.frame_cluster WHERE method='kmeans64' GROUP BY 1")
k2p = dict(cur.fetchall())
STOP = set("a an the of in on at to with and or is are by for from into near under over as its their this that there it".split())
c3_rows = []
for b in range(NB):
    sm = srow == b; km = np.where(scol == b)[0]
    if sm.sum() == 0: continue
    sidx = act[sm]
    cls_cnt = collections.Counter(CLASSES[c] for c in scls[sidx])
    wc = collections.Counter()
    for j in sidx[:4000]:
        for wd in str(text.get(hashes[j], "")).lower().split():
            wd = "".join(ch for ch in wd if ch.isalpha())
            if wd and wd not in STOP and len(wd) > 2: wc[wd] += 1
    projs = collections.Counter(k2p.get(int(k), "?") for k in km)
    c3_rows.append(dict(bicluster=b, n_sentences=int(sm.sum()), n_clusters=len(km),
                        clusters=" ".join(str(int(k)) for k in km),
                        projects=" ".join(f"{p}×{n}" for p, n in projs.most_common(4)),
                        cls_normal=cls_cnt.get("normal", 0), cls_falldown=cls_cnt.get("falldown", 0),
                        cls_fire=cls_cnt.get("fire", 0), cls_smoke=cls_cnt.get("smoke", 0),
                        top_words=" ".join(w0 for w0, _ in wc.most_common(10)),
                        mean_spec_sd=round(float(spec_sd[sidx].mean()), 5), mean_ms=round(float(m_s[sidx].mean()), 4)))
    log(f"  B{b}: 문장 {sm.sum():,} 군집 {len(km)} {list(projs.most_common(2))} {dict(cls_cnt)} | {c3_rows[-1]['top_words'][:64]}")
with open(f"{OUT}/csv/35_biclusters.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bicluster(동시군집)", "n_sentences(문장수)", "n_clusters(프레임군집수)", "clusters(군집id)",
                                      "projects(대표프로젝트)", "cls_normal", "cls_falldown", "cls_fire", "cls_smoke",
                                      "top_words(상위단어)", "mean_spec_sd(평균특이도SD)", "mean_ms(평균배경)"])
    w.writeheader()
    for r in c3_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
del Z, W, co; gc.collect()
log(f"→ csv/35_biclusters.csv ({len(c3_rows)}행) · RSS {rss():.2f}GB")

# ══════════════════════════════════════════════════════════════════
# D3 — 문장 방향 산술
# ══════════════════════════════════════════════════════════════════
mu_g = SENT.mean(0); mu_g /= np.linalg.norm(mu_g)
d3_rows = []
for bank in BANKS:
    cols, lab, cs, to_gt = bank_cols(bank)
    if "normal" not in cs: continue
    mu_n = SENT[cols[lab == cs.index("normal")]].mean(0); mu_n /= np.linalg.norm(mu_n)
    base = None
    for nm, mu in (("원본", None), ("normal 중심 제거후 정규화", mu_n), ("전역 문장평균 제거후 정규화", mu_g)):
        if mu is None: V = SENT[cols]
        else:
            V = SENT[cols] - (SENT[cols] @ mu)[:, None] * mu[None, :]
            V = (V / np.maximum(np.linalg.norm(V, axis=1, keepdims=True), 1e-8)).astype(np.float32)
        pred = np.empty(len(FH), np.int8)
        for s0 in range(0, len(FH), 1500):
            pred[s0:s0 + 1500] = to_gt[topk_vote(FH[s0:s0 + 1500] @ V.T, lab, len(cs))]
        mf1, acc = macro_f1(gt, pred), float((pred == gt).mean())
        if nm == "원본": base = pred; lo2 = hi2 = 0.0; pg2 = 0.5; base_mf1 = mf1
        else: _mu, lo2, hi2, pg2 = paired_ci(pred, base)
        if bank in FOCUS:
            prf = np.empty(len(FF), np.int8)
            for s0 in range(0, len(FF), 1500):
                prf[s0:s0 + 1500] = to_gt[topk_vote(FF[s0:s0 + 1500] @ V.T, lab, len(cs))]
            fr_fire, fr_fp = float((prf[ref == 2] == 2).mean()), float((prf[ref == 0] > 0).mean())
        else: fr_fire = fr_fp = float("nan")
        d3_rows.append(dict(bank=bank, variant=nm, acc=round(acc, 4), macro_f1=round(mf1, 4), d_mf1=round(mf1 - base_mf1, 4),
                            ci_lo=round(lo2, 4), ci_hi=round(hi2, 4), p_gt0=round(pg2, 3),
                            rec_fall=round(float((pred[gt == 1] == 1).mean()), 4), rec_fire=round(float((pred[gt == 2] == 2).mean()), 4),
                            rec_smoke=round(float((pred[gt == 3] == 3).mean()), 4),
                            fr_fire=round(fr_fire, 4), fr_fp=round(fr_fp, 4)))
        del V
    log(f"  D3 {bank:<11} " + " | ".join(f"{r['variant'][:12]} {r['macro_f1']:.3f}({r['d_mf1']:+.3f})" for r in d3_rows[-3:]))
with open(f"{OUT}/csv/34_direction_arithmetic.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "variant(문장변환)", "acc(정확도)", "macro_f1", "d_mf1(원본대비Δ)", "ci_lo", "ci_hi", "p_gt0",
                                      "rec_fall", "rec_fire", "rec_smoke", "fr_fire(frames)", "fr_fp(frames오탐)"])
    w.writeheader()
    for r in d3_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log(f"→ csv/34_direction_arithmetic.csv ({len(d3_rows)}행) · RSS {rss():.2f}GB")

# ══════════════════════════════════════════════════════════════════
# E1 — 프루닝 3컷 (뱅크당 체크포인트)
# ══════════════════════════════════════════════════════════════════
done = set()
if os.path.exists(CKPT):
    for ln in open(CKPT):
        try: done.add(json.loads(ln)["bank"])
        except Exception: pass
    log(f"E1 체크포인트 재개 — 완료 {len(done)} 뱅크")
ck = open(CKPT, "a")
e1_frames = []
for bank in BANKS:
    if bank in done: continue
    cols, lab, cs, to_gt = bank_cols(bank)
    n = len(cols); ms_b, sd_b = m_s[cols], spec_sd[cols]
    log(f"{bank}: 문장 {n:,} 클래스 {cs} · RSS {rss():.2f}GB")
    # 중복 마스크는 뱅크당 1회 (중복은 컷의 성질이 아니라 뱅크의 성질)
    dup_keep = np.ones(n, bool)
    for li in range(len(cs)):
        ii = np.where(lab == li)[0]
        if len(ii) < 2: continue
        order = ii[np.argsort(ms_b[ii])]                # 조용한(배경 낮은) 문장 우선 보존
        V = SENT[cols[order]]; kept = []
        for j in range(len(order)):
            if kept and float(np.max(V[j] @ V[kept].T)) > 0.95: dup_keep[order[j]] = False
            else: kept.append(j)
        del V
    log(f"  중복컷 마스크: {int(dup_keep.sum()):,}/{n:,} 유지 ({dup_keep.mean():.0%})")

    masks = {"기준선(전량)": np.ones(n, bool), "중복컷": dup_keep}
    for q in (10, 25, 50, 75):
        masks[f"주효과 상위 {q}% 컷"] = ms_b < np.percentile(ms_b, 100 - q)
        masks[f"특이도 하위 {q}% 컷"] = sd_b > np.percentile(sd_b, q)
    for q in (25, 50):
        masks[f"3컷 동시 (주효과{q}+특이도{q}+중복)"] = (ms_b < np.percentile(ms_b, 100 - q)) & (sd_b > np.percentile(sd_b, q)) & dup_keep
    masks = {k: v for k, v in masks.items() if v.sum() >= 40}
    preds = score_chunked(FH, cols, lab, len(cs), to_gt, masks)
    base_pred = preds["기준선(전량)"]
    assert abs(float((base_pred == gt).mean()) - float((d[f"topk__{bank}"] == gt).mean())) < 1e-6, bank
    base_mf1 = macro_f1(gt, base_pred)
    rows_out = []
    for name, mk in masks.items():
        p = preds[name]; mf1, acc = macro_f1(gt, p), float((p == gt).mean())
        mu, lo, hi, pg = (0.0, 0.0, 0.0, 0.5) if name == "기준선(전량)" else paired_ci(p, base_pred)
        rows_out.append(dict(bank=bank, variant=name, n_kept=int(mk.sum()), kept_share=round(float(mk.mean()), 4),
                             acc=round(acc, 4), macro_f1=round(mf1, 4), d_mf1=round(mf1 - base_mf1, 4),
                             ci_mean=round(mu, 4), ci_lo=round(lo, 4), ci_hi=round(hi, 4), p_gt0=round(pg, 3),
                             noninferior=("Y" if lo > -0.02 else "N")))
        log(f"  {name:<34} 유지 {mk.sum():>6,} ({mk.mean():.0%})  acc {acc:.3f} mF1 {mf1:.3f} Δ{mf1-base_mf1:+.3f} CI[{lo:+.3f},{hi:+.3f}] 비열등 {'Y' if lo>-0.02 else 'N'}")
    if bank in FOCUS:
        for name in [k for k in masks if k.startswith("3컷") or k == "중복컷"]:
            mk = masks[name]
            pf = score_chunked(FF, cols[mk], lab[mk], len(cs), to_gt, {"x": np.ones(int(mk.sum()), bool)})["x"]
            e1_frames.append(dict(bank=bank, variant=name, n=int(mk.sum()),
                                  fire_recall=round(float((pf[ref == 2] == 2).mean()), 4),
                                  smoke_recall=round(float((pf[ref == 3] == 3).mean()), 4),
                                  fp=round(float((pf[ref == 0] > 0).mean()), 4),
                                  firing=round(float((pf > 0).mean()), 4)))
            log(f"    frames {name}: fire {e1_frames[-1]['fire_recall']:.3f} smoke {e1_frames[-1]['smoke_recall']:.3f} 오탐 {e1_frames[-1]['fp']:.3f}")
    ck.write(json.dumps(dict(bank=bank, rows=rows_out, frames=[r for r in e1_frames if r["bank"] == bank]), ensure_ascii=False) + "\n"); ck.flush()
    del preds, masks, dup_keep; gc.collect()
ck.close()

e1_rows, e1_frames = [], []
for ln in open(CKPT):
    o = json.loads(ln); e1_rows += o["rows"]; e1_frames += o.get("frames", [])
seen = set(); e1_frames = [r for r in e1_frames if not (k := (r["bank"], r["variant"])) in seen and not seen.add(k)]
with open(f"{OUT}/csv/33_pruning.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "variant(프루닝안)", "n_kept(유지문장)", "kept_share(유지비율)", "acc(정확도)",
                                      "macro_f1", "d_mf1(기준선대비Δ)", "ci_mean", "ci_lo(2.5%)", "ci_hi(97.5%)", "p_gt0", "noninferior(CI하한>-0.02)"])
    w.writeheader()
    for r in e1_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log(f"→ csv/33_pruning.csv ({len(e1_rows)}행, 뱅크 {len({r['bank'] for r in e1_rows})}종)")

# ══════════════════════════════════════════════════════════════════
# 그림
# ══════════════════════════════════════════════════════════════════
NOTE = "카메라 군집 부트스트랩 2,000회 · sourcei GT 7,498/15카메라 · frames 표본 24,792 (SAM3 약참조) · 전체 31 뱅크"
CUTS = ["중복컷", "3컷 동시 (주효과25+특이도25+중복)", "3컷 동시 (주효과50+특이도50+중복)",
        "특이도 하위 25% 컷", "특이도 하위 50% 컷", "주효과 상위 25% 컷"]
fig, axes = plt.subplots(1, 3, figsize=(23, 7.6), gridspec_kw={"width_ratios": [1.45, 1.05, 0.85]})
ax = axes[0]
r0 = [r for r in e1_rows if r["bank"] == FOCUS[0]]
y = np.arange(len(r0))
ax.barh(y, [r["kept_share"] * 100 for r in r0], color="#c3c2b7")
ax2 = ax.twiny()
ax2.plot([r["d_mf1"] for r in r0], y, "o", ms=8, color="#eb6834")
for i, r in enumerate(r0): ax2.plot([r["ci_lo"], r["ci_hi"]], [i, i], color="#eb6834", lw=1.6, alpha=.6)
ax2.axvline(0, color="#52514e", lw=1); ax2.axvline(-0.02, color="#e34948", ls="--", lw=1)
ax2.text(-0.02, -0.85, "비열등 한계 −0.02", color="#e34948", fontsize=8.5, ha="center")
ax.set_yticks(y); ax.set_yticklabels([f"{r['variant']}  ({r['n_kept']:,}문장)" for r in r0], fontsize=9); ax.invert_yaxis()
ax.set_xlabel("유지 문장 비율 % (회색 막대)"); ax2.set_xlabel("Δ macro-F1 vs 전량 (주황 = 부트스트랩 평균, 선 = 95% CI)")
ax.set_title(f"① {FOCUS[0]} 프루닝 상세 — 라벨 없이 자른 비율 vs 성능 변화", loc="left", fontsize=11)

ax = axes[1]
MK = ["o", "s", "^", "D", "v", "P"]
for k, nm in enumerate(CUTS):
    rr = [r for r in e1_rows if r["variant"] == nm]
    if not rr: continue
    ni = sum(1 for r in rr if r["noninferior"] == "Y")
    ax.scatter([r["kept_share"] * 100 for r in rr], [r["d_mf1"] for r in rr], s=46, marker=MK[k],
               alpha=.8, edgecolor="white", lw=.6, label=f"{nm[:26]} — 비열등 {ni}/{len(rr)}")
ax.axhline(0, color="#52514e", lw=1); ax.axhline(-0.02, color="#e34948", ls="--", lw=1)
ax.set_xlabel("유지 문장 비율 %"); ax.set_ylabel("Δ macro-F1 vs 전량")
ax.legend(frameon=False, fontsize=8, loc="lower left")
ax.set_title("② 전체 31 뱅크 — 컷별 비열등(CI 하한 > −0.02) 뱅크 수", loc="left", fontsize=10.5)

ax = axes[2]
labs = [f"{r['bank']}\n{r['variant'][:14]}" for r in e1_frames]
x = np.arange(len(e1_frames)); w2 = 0.27
for k, (key, lab_, col) in enumerate([("fire_recall", "fire 재현율", "#e34948"), ("smoke_recall", "smoke 재현율", "#4a3aa7"), ("fp", "비화재 오탐", "#8a887f")]):
    v = [r[key] for r in e1_frames]; b_ = ax.bar(x + (k - 1) * w2, v, w2 * 0.92, color=col, label=lab_)
    for bx, vv in zip(b_, v): ax.text(bx.get_x() + bx.get_width() / 2, vv + 0.008, f"{vv:.2f}", ha="center", fontsize=7.5)
ax.set_xticks(x); ax.set_xticklabels(labs, fontsize=7.5); ax.legend(frameon=False, fontsize=8.5)
ax.set_title("③ 프루닝 뱅크의 frames 반응 (오탐이 늘지 않나)", loc="left", fontsize=11)
fig.suptitle("E1 프루닝 3컷 — 목표는 성능 향상이 아니라 **유지비 절감**: 라벨 없이 몇 %를 지워도 성능이 유지되나. §3 의 '유효 문장 4%' 와 대조해 읽는다\n" + NOTE,
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f38_pruning.png", dpi=160); plt.close(fig)
log("saved f38")

fig, axes = plt.subplots(1, 2, figsize=(16.5, 6.6))
ax = axes[0]
vs = ["normal 중심 제거후 정규화", "전역 문장평균 제거후 정규화"]
data = [[r["d_mf1"] for r in d3_rows if r["variant"] == v] for v in vs]
bp = ax.boxplot(data, widths=.5, patch_artist=True, medianprops=dict(color="#0b0b0b", lw=1.6))
for pc, c in zip(bp["boxes"], ["#2a78d6", "#1baf7a"]): pc.set_facecolor(c); pc.set_alpha(.55)
for k, yy in enumerate(data):
    ax.scatter(np.full(len(yy), k + 1) + RNG.normal(0, .045, len(yy)), yy, s=22, color="#52514e", alpha=.6, zorder=3)
    ax.text(k + 1, max(yy) + .012, f"개선 {sum(1 for q in yy if q > 0)}/{len(yy)} 뱅크\n중앙값 {np.median(yy):+.3f}", ha="center", fontsize=9)
ax.axhline(0, color="#e34948", lw=1.2, ls="--")
ax.set_xticks([1, 2]); ax.set_xticklabels(vs, fontsize=9); ax.set_ylabel("Δ macro-F1 vs 원본 문장 (전체 31 뱅크)")
ax.set_title("D3 문장 방향 산술 — 방향을 빼고 **다시 정규화**하면 판정이 바뀐다", loc="left", fontsize=11)
ax = axes[1]
yb = np.arange(len(c3_rows)); left = np.zeros(len(c3_rows))
for c in CLASSES:
    v = np.array([r[f"cls_{c}"] for r in c3_rows], dtype=float); tot = np.array([r["n_sentences"] for r in c3_rows], dtype=float)
    ax.barh(yb, v / np.maximum(tot, 1), left=left, color=CC[c], label=c); left += v / np.maximum(tot, 1)
ax.set_yticks(yb); ax.set_yticklabels([f"B{r['bicluster']} 문장{r['n_sentences']:,}·군집{r['n_clusters']}\n{r['top_words'][:38]}" for r in c3_rows], fontsize=7.5)
ax.invert_yaxis(); ax.set_xlabel("문장군의 클래스 구성"); ax.legend(frameon=False, ncol=4, fontsize=8.5, loc="lower right")
ax.set_title("C3 Biclustering — 문장군 × 프레임군집군 동시 분할", loc="left", fontsize=11)
fig.suptitle("D3 방향 산술 · C3 동시군집 — 정규화가 비선형이라 방향 제거가 실제로 판정을 바꾼다. 동시군집은 '자연 발생 현장군 팩'이 있는지 본다\n" + NOTE,
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f39_direction_bicluster.png", dpi=160); plt.close(fig)
log("saved f39")
json.dump(dict(e1=e1_rows, e1_frames=e1_frames, d3=d3_rows, c3=c3_rows,
               ms_stats=dict(mean=float(m_s.mean()), sd=float(m_s.std())),
               spec_sd_stats=dict(mean=float(spec_sd.mean()), sd=float(spec_sd.std()))),
          open(f"{OUT}/prune_bicluster_direction_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
