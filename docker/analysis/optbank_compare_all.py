#!/usr/bin/env python3
"""sourcei-OPT vs **공급 뱅크 31종 전부** — 같은 지표로 한 표에 세운다.

재채점하지 않는다: `preds.npz` 에 전 뱅크의 `topk__*`(판정)와 `iou__*`(분포-IoU 연속값)가
이미 들어 있다. OPT 는 `optbank/optbank_sourcei_pred.npz` 에서 읽는다.
지표 정의는 뱅크 빌더와 동일 — 4클래스 macro-F1 · 이벤트 macro-F1 · 평균 PR-AUC · 균형 · 정상 오탐.
"""
import os, sys, json, csv, glob, collections
sys.path.insert(0, "/workspace")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = "2"
import numpy as np, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.metrics import average_precision_score

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; BANKDIR = f"{OUT}/optbank"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]; EVENTS = CLASSES[1:]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}

d = np.load(f"{OUT}/preds.npz", allow_pickle=True); gt, cam = d["gt"], d["camera"]
m = json.load(open(f"{OUT}/metrics.json"))
BANKS = [str(b) for b in d["banks"] if set(m["banks"][str(b)]["classes"]) & set(EVENTS)]
assert len(BANKS) == 31
cnt = {b: int(m["banks"][b]["n_sent"]) for b in BANKS}   # metrics.json 의 키는 n_sent

def f1s(t, p):
    o = {}
    for i, c in enumerate(CLASSES):
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum()); fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); o[c] = 2 * pr * rc / max(pr + rc, 1e-12)
    return o

def row(name, pred, iou3, n):
    f = f1s(gt, pred); all4 = [f[c] for c in CLASSES]; ev = [f[c] for c in EVENTS]
    aps = []
    for k, c in enumerate(EVENTS):
        y = (gt == CLASSES.index(c)).astype(int)
        if y.sum(): aps.append(float(average_precision_score(y, -iou3[:, k])))
    return dict(bank=name, n=n, acc=round(float((pred == gt).mean()), 4),
                macro_f1_4cls=round(float(np.mean(all4)), 4), macro_f1_event=round(float(np.mean(ev)), 4),
                prauc=round(float(np.mean(aps)), 4), balance=round(float(min(all4) / np.mean(all4)), 4),
                fp_normal=round(float((pred[gt == 0] > 0).mean()), 4),
                **{f"f1_{c}": round(f[c], 4) for c in CLASSES})

rows = []
op = np.load(f"{BANKDIR}/optbank_sourcei_pred.npz", allow_pickle=True)
rows.append(row("sourcei-OPT", op["pred"], op["iou"], 2000))
for b in BANKS:
    rows.append(row(b, d[f"topk__{b}"], d[f"iou__{b}"].astype(np.float32), cnt.get(b, 0)))
rows.sort(key=lambda r: -r["macro_f1_4cls"])
for i, r in enumerate(rows, 1): r["rank"] = i
with open(f"{OUT}/csv/54_optbank_vs_all.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["rank", "bank", "n", "acc", "macro_f1_4cls", "macro_f1_event",
                                      "prauc", "balance", "fp_normal"] + [f"f1_{c}" for c in CLASSES])
    w.writeheader()
    for r in rows: w.writerow({k: r[k] for k in w.fieldnames})
opt_i = next(i for i, r in enumerate(rows) if r["bank"] == "sourcei-OPT")
print(f"OPT 순위: 4클래스 mF1 {opt_i+1}/{len(rows)} · "
      f"PR-AUC {sorted(rows,key=lambda r:-r['prauc']).index(rows[opt_i])+1}/{len(rows)} · "
      f"균형 {sorted(rows,key=lambda r:-r['balance']).index(rows[opt_i])+1}/{len(rows)} · "
      f"오탐 {sorted(rows,key=lambda r:r['fp_normal']).index(rows[opt_i])+1}/{len(rows)}")
for r in rows[:6]: print(f"  {r['rank']:>2} {r['bank']:<14} n={r['n']:>6,} 4cls {r['macro_f1_4cls']:.4f} "
                         f"PR-AUC {r['prauc']:.4f} 균형 {r['balance']:.3f} 오탐 {r['fp_normal']:.4f}")

fig, axes = plt.subplots(1, 3, figsize=(23, 8), gridspec_kw={"width_ratios": [1.35, 1, 1]})
ax = axes[0]; y = np.arange(len(rows))
col = ["#1baf7a" if r["bank"] == "sourcei-OPT" else "#c3c2b7" for r in rows]
ax.barh(y, [r["macro_f1_4cls"] for r in rows], color=col)
for i, r in enumerate(rows):
    ax.text(r["macro_f1_4cls"] + .004, i, f"{r['macro_f1_4cls']:.3f}", va="center", fontsize=7.5,
            )
ax.set_yticks(y); ax.set_yticklabels([f"{r['bank']}" for r in rows], fontsize=7.2); ax.invert_yaxis()
ax.set_xlabel("4클래스 macro-F1"); ax.set_xlim(0, max(r["macro_f1_4cls"] for r in rows) * 1.12)
ax.set_title(f"① 전 버전 순위 — sourcei-OPT 는 {opt_i+1}위 / {len(rows)}", loc="left", fontsize=11)
ax = axes[1]
sup = [r for r in rows if r["bank"] != "sourcei-OPT"]; o = rows[opt_i]
ax.scatter([r["prauc"] for r in sup], [r["macro_f1_4cls"] for r in sup], s=52, color="#8a887f",
           alpha=.75, edgecolor="white", lw=.6, label="공급 뱅크 31종")
ax.scatter([o["prauc"]], [o["macro_f1_4cls"]], s=210, marker="*", color="#1baf7a",
           edgecolor="#0b0b0b", lw=.8, label="sourcei-OPT", zorder=5)
for r in sorted(sup, key=lambda r: -r["macro_f1_4cls"])[:4]:
    ax.annotate(r["bank"], (r["prauc"], r["macro_f1_4cls"]), fontsize=7.5, xytext=(4, 3), textcoords="offset points")
ax.set_xlabel("PR-AUC (분포-IoU, 랭킹)"); ax.set_ylabel("4클래스 macro-F1 (판정)")
ax.legend(frameon=False, fontsize=9)
ax.set_title("② 판정 × 랭킹 평면 — 우상단이 좋다", loc="left", fontsize=11)
ax = axes[2]
ax.scatter([r["fp_normal"] for r in sup], [r["macro_f1_4cls"] for r in sup], s=52, color="#8a887f",
           alpha=.75, edgecolor="white", lw=.6, label="공급 뱅크 31종")
ax.scatter([o["fp_normal"]], [o["macro_f1_4cls"]], s=210, marker="*", color="#1baf7a",
           edgecolor="#0b0b0b", lw=.8, label="sourcei-OPT", zorder=5)
ax.axvline(0.05, color="#e34948", ls="--", lw=1.2)
ax.text(0.052, min(r["macro_f1_4cls"] for r in rows), "오탐 예산 5%", color="#e34948", fontsize=9)
ax.set_xscale("symlog", linthresh=0.01)
ax.set_xlabel("정상 프레임 오탐률 (symlog)"); ax.set_ylabel("4클래스 macro-F1")
ax.legend(frameon=False, fontsize=9)
ax.set_title("③ 성능 vs 오경보 — 예산 안에서 가장 높은 점이 목표", loc="left", fontsize=11)
fig.suptitle("sourcei-OPT vs 공급 뱅크 31종 전부 — 같은 GT·같은 지표 정의 (sourcei 7,498프레임/15카메라)\n"
             "⚠️ 뱅크 간 차이는 §5·§19 기준으로 **통계적으로 분해되지 않는다**(deff 232·ICC 0.83) — 순위는 참고값",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.91]); fig.savefig(f"{OUT}/fig/f52_optbank_vs_all.png", dpi=150); plt.close(fig)
print("saved f52 → csv/54_optbank_vs_all.csv")
