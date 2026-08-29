#!/usr/bin/env python3
"""E1 보조 — 중복컷이 왜 해로운가를 규칙 교차로 증명한다.

E1 본 실험: 근접중복(코사인>0.95) 제거는 31뱅크 중 4종만 개선, Δ 중앙값 −0.086.
그런데 손해가 **얼마나 지웠는지와 무관**했다(ρ=−0.028, p=0.88) → 부피 효과가 아니라 구성 효과다.

가설: top-K 다수결은 **문장 개수를 센다**. 같은 뜻 문장이 여러 벌 있으면 그 클래스의 표가
그만큼 늘어난다. 즉 근접중복은 잡음이 아니라 **표 가중치**다.
검정: argmax 는 클래스 내 최댓값만 보므로 문장을 복제해도 판정이 **불변**이다.
  → 중복컷이 top-K 에서만 성능을 떨어뜨리고 argmax 에서는 떨어뜨리지 않으면 가설이 맞다.
  → argmax 에서도 같이 떨어지면 "중복이 실제 정보였다"는 다른 뜻이다.
"""
import os, sys, json, csv, glob
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "4")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote
import numpy as np, psycopg2, matplotlib, time, collections
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()
H2C, SENT = load_sentence_vectors(cur)
m_s = np.load(f"{OUT}/m_s_bg90k.npy")
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; cams = np.unique(cam)
import fiftyone as fo
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == list(d["ids"])
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
log(f"문장 {SENT.shape} · sourcei {FH.shape}")

def macro_f1(t, p, cl=(1, 2, 3)):
    f = []
    for c in cl:
        tp = int(((p == c) & (t == c)).sum()); fp = int(((p == c) & (t != c)).sum()); fn = int(((p != c) & (t == c)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))
idx_by_cam = {c: np.where(cam == c)[0] for c in cams}
BOOT = [np.concatenate([idx_by_cam[c] for c in RNG.choice(cams, size=len(cams), replace=True)]) for _ in range(2000)]
def paired_ci(p1, p0):
    a = np.array([macro_f1(gt[m], p1[m]) - macro_f1(gt[m], p0[m]) for m in BOOT])
    return float(a.mean()), float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))

# 중복컷이 가장 해로웠던 뱅크 + FOCUS
worst = sorted([(x["bank(뱅크)"], float(x["d_mf1(기준선대비Δ)"]), float(x["kept_share(유지비율)"]))
                for x in csv.DictReader(open(f"{OUT}/csv/33_pruning.csv", encoding="utf-8-sig"))
                if x["variant(프루닝안)"] == "중복컷"], key=lambda t: t[1])
TARGETS = [b for b, _dd, _k in worst[:6]] + ["v1.0.8.0", "v1.0.8.1"]
TARGETS = list(dict.fromkeys(TARGETS))
log(f"대상 {len(TARGETS)}종: " + " ".join(f"{b}(Δ{dd:+.3f})" for b, dd, _k in worst[:6]))

rows = []
for bank in TARGETS:
    bd = load_banks(cur, [bank])[0]
    cols, names, seen = [], [], set()
    for h, c, _g in bd["rows"]:
        if h in H2C and h not in seen: seen.add(h); cols.append(H2C[h]); names.append(c)
    cols = np.asarray(cols); cs = sorted(set(names))
    lab = np.array([cs.index(c) for c in names], np.int32)
    to_gt = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], np.int8)
    ms_b = m_s[cols]
    keep = np.ones(len(cols), bool)
    for li in range(len(cs)):
        ii = np.where(lab == li)[0]
        if len(ii) < 2: continue
        order = ii[np.argsort(ms_b[ii])]
        V = SENT[cols[order]]; kept = []
        for j in range(len(order)):
            if kept and float(np.max(V[j] @ V[kept].T)) > 0.95: keep[order[j]] = False
            else: kept.append(j)
        del V
    # 클래스별 중복 제거 비율 — 구성 붕괴 정도
    comp = {}
    for li, c in enumerate(cs):
        n0 = int((lab == li).sum()); n1 = int((keep & (lab == li)).sum())
        comp[c] = (n0, n1, n1 / max(n0, 1))
    V = SENT[cols]
    P = {}
    for rule in ("topk", "argmax"):
        for tag, mk in (("전량", np.ones(len(cols), bool)), ("중복컷", keep)):
            pred = np.empty(len(FH), np.int8)
            for s0 in range(0, len(FH), 1500):
                S = V[mk].T
                Sb = FH[s0:s0 + 1500] @ S
                if rule == "topk": pred[s0:s0 + 1500] = to_gt[topk_vote(Sb, lab[mk], len(cs))]
                else:
                    per = np.stack([np.where(lab[mk] == i, Sb, -2.0).max(1) for i in range(len(cs))], 1)
                    pred[s0:s0 + 1500] = to_gt[per.argmax(1)]
            P[(rule, tag)] = pred
    del V
    out = dict(bank=bank, n_all=int(len(cols)), n_dedup=int(keep.sum()), kept_share=round(float(keep.mean()), 4))
    for rule in ("topk", "argmax"):
        b0, b1 = P[(rule, "전량")], P[(rule, "중복컷")]
        m0, m1 = macro_f1(gt, b0), macro_f1(gt, b1)
        mu, lo, hi = paired_ci(b1, b0)
        out[f"{rule}_all"] = round(m0, 4); out[f"{rule}_dedup"] = round(m1, 4)
        out[f"{rule}_delta"] = round(m1 - m0, 4); out[f"{rule}_ci_lo"] = round(lo, 4); out[f"{rule}_ci_hi"] = round(hi, 4)
    out["comp"] = {c: round(v[2], 3) for c, v in comp.items()}
    out["comp_spread"] = round(float(max(v[2] for v in comp.values()) - min(v[2] for v in comp.values())), 3)
    rows.append(out)
    log(f"  {bank:<11} 유지 {keep.mean():.0%}  top-K Δ{out['topk_delta']:+.4f} CI[{out['topk_ci_lo']:+.3f},{out['topk_ci_hi']:+.3f}]  "
        f"argmax Δ{out['argmax_delta']:+.4f} CI[{out['argmax_ci_lo']:+.3f},{out['argmax_ci_hi']:+.3f}]  클래스별 유지 {out['comp']}")

with open(f"{OUT}/csv/42_dup_mechanism.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["bank(뱅크)", "n_all(전량)", "n_dedup(중복컷후)", "kept_share(유지비율)",
                "topk_all", "topk_dedup", "topk_delta", "topk_ci_lo", "topk_ci_hi",
                "argmax_all", "argmax_dedup", "argmax_delta", "argmax_ci_lo", "argmax_ci_hi",
                "class_keep(클래스별 유지비율)", "comp_spread(클래스간 격차)"])
    for r in rows:
        w.writerow([r["bank"], r["n_all"], r["n_dedup"], r["kept_share"],
                    r["topk_all"], r["topk_dedup"], r["topk_delta"], r["topk_ci_lo"], r["topk_ci_hi"],
                    r["argmax_all"], r["argmax_dedup"], r["argmax_delta"], r["argmax_ci_lo"], r["argmax_ci_hi"],
                    json.dumps(r["comp"], ensure_ascii=False), r["comp_spread"]])
log(f"→ csv/42_dup_mechanism.csv ({len(rows)}행)")

td = [r["topk_delta"] for r in rows]; ad = [r["argmax_delta"] for r in rows]
log(f"요약 — top-K Δ 중앙값 {np.median(td):+.4f} (음수 {sum(1 for q in td if q<0)}/{len(td)}) · "
    f"argmax Δ 중앙값 {np.median(ad):+.4f} (음수 {sum(1 for q in ad if q<0)}/{len(ad)})")

fig, axes = plt.subplots(1, 2, figsize=(15.5, 6.4))
ax = axes[0]
y = np.arange(len(rows)); w2 = 0.36
b1 = ax.barh(y - w2 / 2, td, w2, color="#e34948", label="top-K 다수결 (문장 개수를 센다)")
b2 = ax.barh(y + w2 / 2, ad, w2, color="#2a78d6", label="argmax (복제에 불변)")
for i, r in enumerate(rows):
    ax.plot([r["topk_ci_lo"], r["topk_ci_hi"]], [i - w2 / 2] * 2, color="#0b0b0b", lw=1.1, alpha=.6)
    ax.plot([r["argmax_ci_lo"], r["argmax_ci_hi"]], [i + w2 / 2] * 2, color="#0b0b0b", lw=1.1, alpha=.6)
ax.axvline(0, color="#0b0b0b", lw=1)
ax.set_yticks(y); ax.set_yticklabels([f"{r['bank']}  유지 {r['kept_share']:.0%}" for r in rows], fontsize=9); ax.invert_yaxis()
ax.set_xlabel("중복컷 후 Δ macro-F1 (선 = 카메라 부트스트랩 95% CI)")
ax.legend(frameon=False, fontsize=9, loc="lower left")
ax.set_title(f"① 같은 중복컷을 두 규칙으로 채점\ntop-K Δ 중앙값 {np.median(td):+.3f} vs argmax {np.median(ad):+.3f}", loc="left", fontsize=11)
ax = axes[1]
ax.scatter([r["comp_spread"] for r in rows], td, s=70, color="#e34948", edgecolor="white", lw=.8, label="top-K")
ax.scatter([r["comp_spread"] for r in rows], ad, s=70, color="#2a78d6", marker="s", edgecolor="white", lw=.8, label="argmax")
for r in rows: ax.annotate(r["bank"], (r["comp_spread"], r["topk_delta"]), fontsize=7.5, xytext=(4, 3), textcoords="offset points")
ax.axhline(0, color="#0b0b0b", lw=1)
ax.set_xlabel("클래스 간 중복 제거율 격차 (최대 − 최소)")
ax.set_ylabel("Δ macro-F1")
ax.legend(frameon=False, fontsize=9)
ax.set_title("② 손해는 '얼마나 지웠나'가 아니라 '클래스마다 다르게 지웠나'에 붙는다", loc="left", fontsize=11)
fig.suptitle("E1 보조 — 근접중복은 잡음이 아니라 **표 가중치**다. top-K 는 문장 개수를 세므로 중복 제거가 클래스 표를 바꾼다.\n"
             "argmax 는 클래스 내 최댓값만 보므로 복제에 불변 → 두 규칙의 차이가 기제를 가른다 · sourcei GT 7,498/15카메라",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f44_dup_mechanism.png", dpi=160); plt.close(fig)
log("saved f44")
json.dump(rows, open(f"{OUT}/dup_mechanism_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
