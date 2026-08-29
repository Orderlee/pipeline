#!/usr/bin/env python3
"""남은 분석 ② DPP(determinantal point process) 기반 문장 선택 vs facility-location.

§2(A2)는 facility-location 으로 "군집을 다 덮는" 부집합을 골랐다. DPP 는 목적이 다르다 —
**품질 × 다양성**을 행렬식으로 결합해, 서로 비슷한 문장이 같이 뽑히는 것을 확률적으로 억제한다.
L = diag(q) · S · diag(q) (q=품질, S=문장 간 코사인 유사도)의 MAP 부집합을 greedy 로 찾는다.

⚠️ 사전 예측: §15 에서 **근접중복 제거가 top-K 다수결을 망친다**(Δ −0.091, 손해의 65%가 규칙 탓)는
   것이 밝혀졌다. DPP 는 중복을 더 강하게 억제하므로 top-K 에서는 불리할 것으로 예상된다.
   그래서 **argmax·차 점수로도 같이 채점**한다 — 규칙에 따라 결론이 뒤집히는지가 이 분석의 요점이다.
   dppy 는 미설치라 greedy MAP(Chen et al. 2018 의 Cholesky 증분)을 직접 구현한다.

비교 대상(같은 예산 k): DPP / facility-location / 품질 상위 k / 무작위 k(5회) / 전량
"""
import os, sys, json, csv, glob, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
import fiftyone as fo

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
m_s = np.load(f"{OUT}/m_s_bg90k.npy")
Ak = np.load(f"{OUT}/Ak_kmeans64.npy")                      # 문장 × kmeans64 군집 평균 코사인
R = Ak - Ak.mean(1, keepdims=True); spec_sd = R.std(1)
Z = (R - R.mean(0)) / (R.std(0) + 1e-9)                     # 군집별 표준화 = 특이도 z
log(f"문장 {SENT.shape} · A_k {Ak.shape}")

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; cams = np.unique(cam)
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == list(d["ids"])
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
log(f"sourcei {FH.shape}")

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
    return float(a.mean()), float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))

# ══════════════════════════════════════════════════════════════════
# 선택 알고리즘
# ══════════════════════════════════════════════════════════════════
def dpp_greedy_map(V, q, k, eps=1e-10):
    """L = diag(q) S diag(q), S = V Vᵀ (V 는 L2정규화). Chen et al. 2018 증분 Cholesky greedy.
    O(k² N) 로 k 개를 고른다 — N=풀 크기. 반환: 선택 인덱스."""
    N = V.shape[0]
    di2 = q ** 2                                            # L 대각 = q² (S 대각 = 1)
    ci = np.zeros((k, N), np.float64)
    sel, mask = [], np.ones(N, bool)
    for it in range(k):
        cand = np.where(mask & (di2 > eps))[0]
        if len(cand) == 0: break
        j = cand[np.argmax(di2[cand])]
        sel.append(int(j)); mask[j] = False
        if it == k - 1: break
        # L[:, j] = q * q_j * (V @ V_j)
        Lj = q * q[j] * (V @ V[j])
        ei = (Lj - ci[:it, :].T @ ci[:it, j]) / np.sqrt(max(di2[j], eps))
        ci[it, :] = ei
        di2 = di2 - ei ** 2
        di2[j] = -np.inf
    return np.array(sel)

def facility_location_greedy(Acov, k):
    """max_S Σ_clusters max_{s∈S} Acov[s, cluster] — §2(A2)와 같은 목적."""
    N, C = Acov.shape
    cur = np.full(C, -np.inf); sel = []
    for _ in range(k):
        gain = np.maximum(Acov, cur[None, :]).sum(1) - cur.sum()
        gain[sel] = -np.inf
        j = int(np.argmax(gain)); sel.append(j)
        cur = np.maximum(cur, Acov[j])
    return np.array(sel)

# ══════════════════════════════════════════════════════════════════
# 뱅크별 실험
# ══════════════════════════════════════════════════════════════════
BANKS = ["v1.0.8.0", "v1.0.8.1", "v1.0.12.0"]               # 기준 2종 + 최대 뱅크
POOL_CAP = 4000                                             # 클래스당 후보 상한(품질 상위) — CPU 절약
K_PER_CLASS = 120
rows, sel_dump = [], []
bank_defs = {b["version"]: b for b in load_banks(cur, BANKS)}

def score(cols_sel, lab_sel, cs, to_gt, rule):
    V = SENT[cols_sel]
    pred = np.empty(len(FH), np.int8)
    for s0 in range(0, len(FH), 1500):
        S = FH[s0:s0 + 1500] @ V.T
        if rule == "topk":
            pred[s0:s0 + 1500] = to_gt[topk_vote(S, lab_sel, len(cs))]
        else:
            per = np.stack([np.where(lab_sel == i, S, -2.0).max(1) for i in range(len(cs))], 1)
            if rule == "diff" and "normal" in cs:
                ni = cs.index("normal"); base = per[:, ni].copy()
                for i in range(len(cs)):
                    if i != ni: per[:, i] -= base
                per[:, ni] = 0.0
            pred[s0:s0 + 1500] = to_gt[per.argmax(1)]
    del V
    return pred

for bank in BANKS:
    rows_ = bank_defs[bank]["rows"]
    cols, names, seen = [], [], set()
    for h, c, _g in rows_:
        if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); names.append(c)
    cols = np.asarray(cols); cs = sorted(set(names))
    lab = np.array([cs.index(c) for c in names], np.int32)
    to_gt = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], np.int8)
    log(f"{bank}: 고유문장 {len(cols):,} 클래스 {cs}")

    # 품질 q = 특이도 SD 를 [0,1] 로 (배경이 센 문장은 감점)
    q_all = spec_sd[cols] / (spec_sd[cols].max() + 1e-9)
    q_all = q_all * (1.0 - (m_s[cols] - m_s[cols].min()) / (m_s[cols].max() - m_s[cols].min() + 1e-9) * 0.5)

    picks = {"DPP (품질×다양성)": [], "facility-location (커버리지)": [], "품질 상위 k": [], "무작위 k": []}
    for li, c in enumerate(cs):
        ii = np.where(lab == li)[0]
        k = min(K_PER_CLASS, len(ii))
        pool = ii[np.argsort(-q_all[ii])[:POOL_CAP]] if len(ii) > POOL_CAP else ii
        Vp = SENT[cols[pool]]; qp = q_all[pool]
        Ap = np.clip(Z[cols[pool]], 0, None)                # 커버리지 행렬(양의 특이도)
        picks["DPP (품질×다양성)"].append(pool[dpp_greedy_map(Vp, qp, k)])
        picks["facility-location (커버리지)"].append(pool[facility_location_greedy(Ap, k)])
        picks["품질 상위 k"].append(ii[np.argsort(-q_all[ii])[:k]])
        picks["무작위 k"].append(RNG.choice(ii, k, replace=False))
        log(f"  {c}: 후보 {len(ii):,} → 풀 {len(pool):,} → k={k}")

    variants = {"기준선(전량)": np.arange(len(cols))}
    for nm, lst in picks.items(): variants[nm] = np.concatenate(lst)
    base = {}
    for rule in ("topk", "argmax", "diff"):
        pb = score(cols[variants["기준선(전량)"]], lab, cs, to_gt, rule)
        base[rule] = pb
    for nm, sel in variants.items():
        for rule in ("topk", "argmax", "diff"):
            p = base[rule] if nm == "기준선(전량)" else score(cols[sel], lab[sel], cs, to_gt, rule)
            mf1 = macro_f1(gt, p); acc = float((p == gt).mean())
            if nm == "기준선(전량)": mu = lo = hi = 0.0
            else: mu, lo, hi = paired_ci(p, base[rule])
            # 선택 다양성 진단: 뽑힌 문장 간 평균 코사인 (낮을수록 다양)
            if nm != "기준선(전량)" and len(sel) <= 600:
                Vs = SENT[cols[sel]]; G = Vs @ Vs.T
                np.fill_diagonal(G, np.nan); div = float(np.nanmean(G))
                cov = float((np.clip(Z[cols[sel]], 0, None).max(0) > 0).mean())     # 덮은 군집 비율
            else: div, cov = float("nan"), float("nan")
            rows.append(dict(bank=bank, variant=nm, rule=rule, n=int(len(sel)),
                             acc=round(acc, 4), macro_f1=round(mf1, 4), d_mf1=round(mu, 4),
                             ci_lo=round(lo, 4), ci_hi=round(hi, 4),
                             mean_cos=round(div, 4) if div == div else "",
                             cluster_cover=round(cov, 4) if cov == cov else ""))
            log(f"    {nm:<28} {rule:<7} n={len(sel):>6,} mF1 {mf1:.4f} Δ{mu:+.4f} CI[{lo:+.4f},{hi:+.4f}] "
                f"평균코사인 {div:.3f} 군집커버 {cov:.2f}" if div == div else
                f"    {nm:<28} {rule:<7} n={len(sel):>6,} mF1 {mf1:.4f}")
    for nm, sel in variants.items():
        if nm == "기준선(전량)": continue
        for j in sel[:0]: pass
    sel_dump.append(dict(bank=bank, picks={nm: [int(x) for x in np.concatenate(lst)] for nm, lst in picks.items()}))

with open(f"{OUT}/csv/47_dpp_selection.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "variant(선택법)", "rule(판정규칙)", "n(문장수)", "acc(정확도)",
                                      "macro_f1", "d_mf1(전량대비Δ)", "ci_lo", "ci_hi",
                                      "mean_cos(선택문장간 평균코사인)", "cluster_cover(덮은 군집비율)"])
    w.writeheader()
    for r in rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log(f"→ csv/47_dpp_selection.csv ({len(rows)}행)")

# ── 그림 ────────────────────────────────────────────────────────────
VAR = ["DPP (품질×다양성)", "facility-location (커버리지)", "품질 상위 k", "무작위 k"]
RUL = [("topk", "top-K 다수결"), ("argmax", "argmax"), ("diff", "차 점수")]
fig, axes = plt.subplots(1, 3, figsize=(21, 6.8), gridspec_kw={"width_ratios": [1.25, 1, 0.85]})
ax = axes[0]
x = np.arange(len(RUL)); w2 = 0.2
for k, nm in enumerate(VAR):
    v = [np.mean([r["d_mf1"] for r in rows if r["variant"] == nm and r["rule"] == ru]) for ru, _ in RUL]
    b_ = ax.bar(x + (k - 1.5) * w2, v, w2 * 0.9, color=["#2a78d6", "#1baf7a", "#eda100", "#8a887f"][k], label=nm)
    for bx, vv in zip(b_, v):
        ax.text(bx.get_x() + bx.get_width() / 2, vv + (0.004 if vv >= 0 else -0.012), f"{vv:+.3f}", ha="center", fontsize=8)
ax.axhline(0, color="#0b0b0b", lw=1)
ax.set_xticks(x); ax.set_xticklabels([n for _r, n in RUL])
ax.set_ylabel("Δ macro-F1 vs 전량 (뱅크 3종 평균)")
ax.legend(frameon=False, fontsize=8.5, ncol=2)
ax.set_title("① 같은 예산에서 선택법 × 판정규칙 — 규칙에 따라 순위가 바뀌나", loc="left", fontsize=11)
ax = axes[1]
for k, nm in enumerate(VAR):
    rr = [r for r in rows if r["variant"] == nm and r["rule"] == "topk" and r["mean_cos"] != ""]
    if not rr: continue
    ax.scatter([r["mean_cos"] for r in rr], [r["d_mf1"] for r in rr], s=90, marker=["o", "s", "^", "v"][k],
               color=["#2a78d6", "#1baf7a", "#eda100", "#8a887f"][k], edgecolor="white", lw=.8, label=nm)
ax.axhline(0, color="#0b0b0b", lw=1)
ax.set_xlabel("선택 문장 간 평균 코사인 (낮을수록 다양)"); ax.set_ylabel("Δ macro-F1 (top-K)")
ax.legend(frameon=False, fontsize=8.5)
ax.set_title("② 다양성을 강제하면 top-K 에서 무슨 일이 나나", loc="left", fontsize=11)
ax = axes[2]
for k, nm in enumerate(VAR):
    rr = [r for r in rows if r["variant"] == nm and r["rule"] == "topk" and r["cluster_cover"] != ""]
    if not rr: continue
    ax.bar(k, np.mean([r["cluster_cover"] for r in rr]), 0.6,
           color=["#2a78d6", "#1baf7a", "#eda100", "#8a887f"][k])
    ax.text(k, np.mean([r["cluster_cover"] for r in rr]) + .01, f"{np.mean([r['cluster_cover'] for r in rr]):.2f}", ha="center", fontsize=9.5)
ax.set_xticks(range(len(VAR))); ax.set_xticklabels([v.split(" (")[0] for v in VAR], fontsize=8.5, rotation=20, ha="right")
ax.set_ylabel("덮은 kmeans64 군집 비율")
ax.set_title("③ 커버리지는 누가 이기나", loc="left", fontsize=11)
fig.suptitle(f"R2 DPP(품질×다양성) vs facility-location(커버리지) — 클래스당 {K_PER_CLASS}문장 예산, 뱅크 3종\n"
             "greedy MAP 직접 구현(dppy 미설치) · sourcei GT 7,498/15카메라 · 카메라 군집 부트스트랩 2,000회",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f47_dpp_selection.png", dpi=150); plt.close(fig)
log("saved f47")
json.dump(dict(rows=rows, k_per_class=K_PER_CLASS, pool_cap=POOL_CAP),
          open(f"{OUT}/dpp_selection_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
