#!/usr/bin/env python3
"""D1D2 보조 — 생성 뱅크의 PR-AUC 열세가 실제 품질인가, 추정기 인공물인가.

관측: GEN+pairs(499문장)는 top-K macro-F1 에서 v1.0.8.1(12,511문장)을 유의하게 이겼는데
      (0.5622 vs 0.5286, CI[+0.007,+0.060]) 분포-IoU PR-AUC 는 크게 졌다 (0.382 vs 0.628).

의심: 분포-IoU 는 **클래스당 문장 코사인으로 80-bin 히스토그램을 만든다.** fire 문장이 46개면
      bin 당 0.6개다 — 히스토그램이 성립하지 않는다. 즉 PR-AUC 열세가 뱅크 품질이 아니라
      **추정기의 표본 부족**일 수 있다.

검정 두 가지:
 (1) 히스토그램이 필요 없는 점수로 다시 랭킹한다 — 차 점수 (클래스 max cos − normal max cos).
     여기서도 GEN 이 지면 실제 품질 열세, 비슷해지면 추정기 인공물.
 (2) 공급 뱅크를 **생성 뱅크와 같은 크기로 균일 하향표집**해 분포-IoU PR-AUC 를 다시 낸다.
     공급 뱅크 점수가 GEN 수준으로 떨어지면 크기 의존이 확정된다 (20회 반복 평균).
"""
import os, sys, json, csv, glob, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "4")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, wave_iou
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.metrics import average_precision_score, roc_auc_score
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
H2C, SENT = load_sentence_vectors(cur)
d = np.load(f"{OUT}/preds.npz", allow_pickle=True); gt = d["gt"]
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == list(d["ids"])
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
z = np.load(f"{OUT}/gen_vectors.npz", allow_pickle=True)
GVEC = {str(t): v for t, v in zip(z["texts"], z["vecs"])}
log(f"저장 문장 {SENT.shape} · 생성 벡터 {len(GVEC)} · sourcei {FH.shape}")

# ── GEN+pairs 뱅크 재구성 (d1d2 와 동일 규칙) ─────────────────────────
gen = json.load(open("/workspace/gen_cupl.json"))
m_s_all = np.load(f"{OUT}/m_s_bg90k.npy")
SCENES = ["a department store sales floor", "a shop aisle between clothing racks", "an escalator landing",
          "a back corridor", "a concrete stairwell", "a back-of-house storage room", "an underground parking level",
          "a food court seating area", "a server room", "a loading dock", "a dim basement passage",
          "a bright atrium walkway", "a cosmetics counter area", "a checkout area", "a service elevator lobby"]
EV = {"falldown": ["a person lies motionless on the ground", "a body is sprawled flat on the floor",
                   "someone has collapsed and does not move", "a person lies face down and still",
                   "an unattended person lies on their back"],
      "fire": ["an open flame is burning", "flames are spreading across a surface",
               "a small flame flickers on the floor", "an object is on fire and glowing", "a fire burns brightly"],
      "smoke": ["thick smoke is spreading", "white smoke drifts across the space",
                "dense grey smoke obscures the view", "smoke pools along the ceiling", "a thin plume of smoke rises"]}
NO = ["people are walking normally", "the space is empty and quiet", "a worker crouches to pick something up",
      "a cleaner is mopping the floor", "shoppers are browsing the shelves"]
pair_ev = collections.defaultdict(list); pair_no = []
for cls, evs in EV.items():
    for sc in SCENES:
        for k, ev in enumerate(evs):
            pair_ev[cls].append(f"In {sc}, {ev}.")
            pair_no.append(f"In {sc}, {NO[k % len(NO)]}.")
gsub = list(csv.DictReader(open(f"{OUT}/csv/40_generated_sentences.csv", encoding="utf-8-sig")))
filt = collections.defaultdict(list)
for x in gsub:
    if x["kind(출처)"] == "gen" and x["in_filtered(필터통과)"] == "Y": filt[x["class(클래스)"]].append(x["text(문장)"])
GENB = {c: list(dict.fromkeys(filt[c] + (pair_ev[c] if c != "normal" else pair_no))) for c in CLASSES}
log("GEN+pairs 재구성 " + " ".join(f"{c} {len(v)}" for c, v in GENB.items()))

def mat_gen(mem):
    cs = [c for c in CLASSES if mem.get(c)]
    V = np.stack([GVEC[t] for c in cs for t in mem[c]]).astype(np.float32)
    lab = np.concatenate([np.full(len(mem[c]), i, np.int32) for i, c in enumerate(cs)])
    return V, lab, cs

def mat_db(bank):
    bd = load_banks(cur, [bank])[0]
    cols, names, seen = [], [], set()
    for h, c, _g in bd["rows"]:
        if h in H2C and h not in seen: seen.add(h); cols.append(H2C[h]); names.append(c)
    cols = np.asarray(cols); cs = sorted(set(names))
    lab = np.array([cs.index(c) for c in names], np.int32)
    return SENT[cols], lab, cs, cols

def scores(V, lab, cs, chunk=1500):
    """두 점수를 함께 낸다: 차 점수(히스토그램 불필요) + 분포-IoU(히스토그램 필요)."""
    ncl = len(cs); ni = cs.index("normal") if "normal" in cs else None
    per = np.empty((len(FH), ncl), np.float32)
    iou = {c: np.empty(len(FH), np.float32) for c in cs if c != "normal"}
    mem = {c: np.where(lab == i)[0] for i, c in enumerate(cs)}
    for s0 in range(0, len(FH), chunk):
        S = FH[s0:s0 + chunk] @ V.T
        for i in range(ncl): per[s0:s0 + chunk, i] = S[:, mem[cs[i]]].max(1)
        if ni is not None:
            w = wave_iou(S, mem)
            for c in iou: iou[c][s0:s0 + chunk] = w[c]
    diff = per.copy()
    if ni is not None:
        for i in range(ncl):
            if i != ni: diff[:, i] = per[:, i] - per[:, ni]
    return per, diff, iou, cs

def prauc(y, s): return float(average_precision_score(y, s)), float(roc_auc_score(y, s))

rows = []
TARGET = ["GEN+pairs (499문장)", "v1.0.8.1 (12,511문장)", "v1.0.8.0 (12,480문장)"]
data = {}
V, lab, cs = mat_gen(GENB); data[TARGET[0]] = scores(V, lab, cs)
for nm, b in ((TARGET[1], "v1.0.8.1"), (TARGET[2], "v1.0.8.0")):
    V, lab, cs, _cols = mat_db(b); data[nm] = scores(V, lab, cs)
for nm, (per, diff, iou, cs) in data.items():
    for cls in ("falldown", "fire", "smoke"):
        if cls not in cs: continue
        y = (gt == CLASSES.index(cls)).astype(int); i = cs.index(cls)
        ap_d, au_d = prauc(y, diff[:, i])
        ap_m, au_m = prauc(y, per[:, i])
        ap_i, au_i = prauc(y, -iou[cls])
        rows.append(dict(bank=nm, cls=cls, n_cls=int((lab == i).sum()) if nm == TARGET[0] else None,
                         pr_diff=round(ap_d, 4), pr_maxcos=round(ap_m, 4), pr_iou=round(ap_i, 4),
                         roc_diff=round(au_d, 4), roc_iou=round(au_i, 4)))
        log(f"  {nm:<24} {cls:<9} PR-AUC  차점수 {ap_d:.4f} | max코사인 {ap_m:.4f} | 분포-IoU {ap_i:.4f}")

# ── (2) 공급 뱅크를 GEN 크기로 하향표집 → 분포-IoU PR-AUC 가 크기에 의존하나 ──
V8, lab8, cs8, _c8 = mat_db("v1.0.8.1")
gsz = {c: len(GENB[c]) for c in CLASSES}
log(f"하향표집 목표 크기 {gsz}")
down = collections.defaultdict(list)
for rep in range(20):
    sel = []
    for i, c in enumerate(cs8):
        ii = np.where(lab8 == i)[0]
        k = min(gsz.get(c, len(ii)), len(ii))
        sel.append(RNG.choice(ii, k, replace=False))
    sel = np.concatenate(sel)
    lab_s = lab8[sel]
    per_s, diff_s, iou_s, _ = scores(V8[sel], np.array([cs8.index(cs8[i]) for i in lab_s], np.int32), cs8)
    for cls in ("falldown", "fire", "smoke"):
        y = (gt == CLASSES.index(cls)).astype(int); i = cs8.index(cls)
        down[(cls, "iou")].append(prauc(y, -iou_s[cls])[0])
        down[(cls, "diff")].append(prauc(y, diff_s[:, i])[0])
    if rep == 0: log(f"  하향표집 rep0 완료 (총 {len(sel)}문장)")
dn = []
for cls in ("falldown", "fire", "smoke"):
    full_i = next(r["pr_iou"] for r in rows if r["bank"] == TARGET[1] and r["cls"] == cls)
    full_d = next(r["pr_diff"] for r in rows if r["bank"] == TARGET[1] and r["cls"] == cls)
    gi_ = next(r["pr_iou"] for r in rows if r["bank"] == TARGET[0] and r["cls"] == cls)
    gd_ = next(r["pr_diff"] for r in rows if r["bank"] == TARGET[0] and r["cls"] == cls)
    a = np.array(down[(cls, "iou")]); b = np.array(down[(cls, "diff")])
    dn.append(dict(cls=cls, full_iou=full_i, down_iou=round(float(a.mean()), 4), down_iou_sd=round(float(a.std()), 4), gen_iou=gi_,
                   full_diff=full_d, down_diff=round(float(b.mean()), 4), down_diff_sd=round(float(b.std()), 4), gen_diff=gd_))
    log(f"  {cls:<9} 분포-IoU: 전량 {full_i:.4f} → 하향표집 {a.mean():.4f}±{a.std():.4f} (GEN {gi_:.4f})   "
        f"차점수: 전량 {full_d:.4f} → 하향표집 {b.mean():.4f}±{b.std():.4f} (GEN {gd_:.4f})")

with open(f"{OUT}/csv/43_ranking_fair.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "class(클래스)", "n_cls(클래스문장수)", "pr_diff(차점수)", "pr_maxcos(max코사인)",
                                      "pr_iou(분포IoU)", "roc_diff", "roc_iou"])
    w.writeheader()
    for r in rows: w.writerow(dict(zip(w.fieldnames, r.values())))
with open(f"{OUT}/csv/44_downsample_control.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["class(클래스)", "full_iou(v1081 전량)", "down_iou(GEN크기로 하향표집)", "down_iou_sd",
                                      "gen_iou(GEN+pairs)", "full_diff", "down_diff", "down_diff_sd", "gen_diff"])
    w.writeheader()
    for r in dn: w.writerow(dict(zip(w.fieldnames, r.values())))
log("→ csv/43_ranking_fair.csv, csv/44_downsample_control.csv")

fig, axes = plt.subplots(1, 2, figsize=(16.5, 6.6))
ax = axes[0]
x = np.arange(3); w2 = 0.26
for k, nm in enumerate(TARGET):
    for j, (key, hatch) in enumerate((("pr_diff", ""), ("pr_iou", "///"))):
        v = [next((r[key] for r in rows if r["bank"] == nm and r["cls"] == c), np.nan) for c in ("falldown", "fire", "smoke")]
        ax.bar(x + (k - 1) * w2 + (j - .5) * w2 / 2.2, v, w2 / 2.3, hatch=hatch,
               color=["#1baf7a", "#8a887f", "#c3c2b7"][k], edgecolor="white", lw=.5,
               label=f"{nm[:20]} {'차 점수' if j==0 else '분포-IoU'}")
ax.set_xticks(x); ax.set_xticklabels(["falldown", "fire", "smoke"])
ax.set_ylabel("PR-AUC"); ax.legend(frameon=False, fontsize=7.8, ncol=2)
ax.set_title("① 점수 함수를 바꾸면 순위가 뒤집히나 — 빈칸=차 점수, 사선=분포-IoU", loc="left", fontsize=11)
ax = axes[1]
x = np.arange(3); w2 = 0.24
for k, (key, lab_, col) in enumerate((("full_iou", "v1.0.8.1 전량 12,511", "#8a887f"),
                                     ("down_iou", "v1.0.8.1 → GEN 크기로 하향표집", "#2a78d6"),
                                     ("gen_iou", "GEN+pairs 499", "#1baf7a"))):
    v = [r[key] for r in dn]
    err = [r["down_iou_sd"] for r in dn] if key == "down_iou" else None
    b_ = ax.bar(x + (k - 1) * w2, v, w2 * 0.9, color=col, yerr=err, error_kw=dict(ecolor="#52514e", lw=1), label=lab_)
    for bx, vv in zip(b_, v): ax.text(bx.get_x() + bx.get_width() / 2, vv + .012, f"{vv:.3f}", ha="center", fontsize=8)
ax.set_xticks(x); ax.set_xticklabels([r["cls"] for r in dn]); ax.set_ylabel("분포-IoU PR-AUC")
ax.legend(frameon=False, fontsize=8.5)
ax.set_title("② 같은 뱅크를 GEN 크기로 줄이면 분포-IoU 가 어디까지 내려가나 (20회 평균)", loc="left", fontsize=11)
fig.suptitle("생성 뱅크의 PR-AUC 열세는 품질인가 추정기인가 — 분포-IoU 는 클래스당 문장으로 80-bin 히스토그램을 만든다\n"
             "sourcei GT 7,498 · GEN+pairs 클래스당 문장 " + " ".join(f"{c} {len(GENB[c])}" for c in CLASSES),
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f45_ranking_fair.png", dpi=160); plt.close(fig)
log("saved f45")
json.dump(dict(rows=rows, downsample=dn, gen_sizes={c: len(v) for c, v in GENB.items()}),
          open(f"{OUT}/ranking_fair_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
