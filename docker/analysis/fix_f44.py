#!/usr/bin/env python3
"""f44 ② 교체 — 원래 패널은 x 범위가 0.473~0.501(변동 없음)인데 '격차가 손해를 만든다'는
관계를 암시했다. 7뱅크가 거의 같은 격차를 가지므로 그 주장을 이 데이터로는 못 한다.
근거가 되는 사실은 **클래스별 제거율 비대칭 자체**(falldown 17% vs normal 65%)다 → 그것을 그린다."""
import csv, json, glob
import numpy as np, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
rows = list(csv.DictReader(open(f"{OUT}/csv/42_dup_mechanism.csv", encoding="utf-8-sig")))
td = [float(x["topk_delta"]) for x in rows]; ad = [float(x["argmax_delta"]) for x in rows]
fig, axes = plt.subplots(1, 2, figsize=(16, 6.6), gridspec_kw={"width_ratios": [1.15, 0.9]})
ax = axes[0]; y = np.arange(len(rows)); w2 = 0.36
ax.barh(y - w2 / 2, td, w2, color="#e34948", label="top-K 다수결 (문장 개수를 센다)")
ax.barh(y + w2 / 2, ad, w2, color="#2a78d6", label="argmax (복제에 불변)")
for i, r in enumerate(rows):
    ax.plot([float(r["topk_ci_lo"]), float(r["topk_ci_hi"])], [i - w2 / 2] * 2, color="#0b0b0b", lw=1.1, alpha=.65)
    ax.plot([float(r["argmax_ci_lo"]), float(r["argmax_ci_hi"])], [i + w2 / 2] * 2, color="#0b0b0b", lw=1.1, alpha=.65)
ax.axvline(0, color="#0b0b0b", lw=1)
ax.set_yticks(y); ax.set_yticklabels([f"{r['bank(뱅크)']}  유지 {float(r['kept_share(유지비율)']):.0%}" for r in rows], fontsize=9); ax.invert_yaxis()
ax.set_xlabel("중복컷 후 Δ macro-F1 (선 = 카메라 부트스트랩 95% CI)")
ax.legend(frameon=False, fontsize=9, loc="lower left")
ax.set_title(f"① 같은 중복컷을 두 규칙으로 채점\ntop-K Δ 중앙값 {np.median(td):+.3f} vs argmax {np.median(ad):+.3f} — 손해의 {1-np.median(ad)/np.median(td):.0%}는 규칙 탓", loc="left", fontsize=11)
ax = axes[1]
CLS = ["falldown", "smoke", "fire", "normal"]
keeps = {c: [json.loads(r["class_keep(클래스별 유지비율)"]).get(c, np.nan) for r in rows] for c in CLS}
x = np.arange(len(CLS))
for i, c in enumerate(CLS):
    v = [q for q in keeps[c] if q == q]
    ax.bar(i, np.mean(v), 0.62, color=CC[c], alpha=.9)
    ax.scatter(np.full(len(v), i), v, s=26, color="#0b0b0b", alpha=.55, zorder=3)
    ax.text(i, np.mean(v) + .022, f"{np.mean(v):.1%}", ha="center", fontsize=10)
ax.set_xticks(x); ax.set_xticklabels(CLS); ax.set_ylim(0, 0.82)
ax.set_ylabel("중복컷 후 남는 문장 비율 (뱅크 7종, 점 = 개별 뱅크)")
ax.set_title("② 중복은 클래스마다 전혀 다르게 쌓여 있다\nfalldown 은 문장 6개 중 5개가 근접중복 — 컷이 이 클래스의 표만 없앤다", loc="left", fontsize=11)
fig.suptitle("E1 보조 — 근접중복은 잡음이 아니라 **표 가중치**다. top-K 는 문장 개수를 세므로 중복 제거가 클래스 표를 바꾼다.\n"
             "argmax 는 클래스 내 최댓값만 보므로 복제에 불변 → 두 규칙의 차이가 기제를 가른다 · sourcei GT 7,498/15카메라 · 코사인 > 0.95 를 중복으로 봄",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f44_dup_mechanism.png", dpi=150); plt.close(fig)
print("f44 재작성")
