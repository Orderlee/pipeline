#!/usr/bin/env python3
"""frames_fire_banks.py 산출물 → 전 뱅크 화재 반응 차트 3장 (§11 확장)."""
import csv, glob
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.colors import LinearSegmentedColormap

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; FIG = f"{OUT}/fig"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7",
                     "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b",
                     "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
RC = {"argmax": "#2a78d6", "topk": "#eb6834", "wave": "#1baf7a"}
RN = {"argmax": "argmax (top-1)", "topk": "top-K 투표 (K=10)", "wave": "분포-IoU (제품 규칙, thr 0.15)"}
SEQ = LinearSegmentedColormap.from_list("seq", ["#fdecea", "#e34948", "#5c1210"])
NOTE = "참조 라벨 = SAM3 검출(auto_generated 의사라벨, GT 아님). 표본 24,792 = SAM3 fire 1,578 + smoke 3,214 + 비화재 20,000. 뱅크·조건 간 상대 비교로 읽을 것"
rows = list(csv.DictReader(open(f"{OUT}/csv/18_fire_by_bank.csv", encoding="utf-8-sig")))


def vkey(b): return tuple(int(x) for x in b.lstrip("vV").split("."))
banks = sorted({r["bank"] for r in rows}, key=vkey)
extra = {r["bank"] for r in rows if int(r["n_classes"]) != 4}
ylab = [b + (" †" if b in extra else "") for b in banks]
def g(b, r, k):
    for x in rows:
        if x["bank"] == b and x["rule"] == r:
            v = x[k]; return float(v) if v not in ("", "None") else np.nan
LBL = {"v1.0.8.0", "v1.0.8.1", "v1.0.8.4", "v1.0.4.2", "v1.0.12.0", "v1.0.2.1", "V1.0.10.3", "v1.0.3.0"}


def save(fig, name):
    fig.tight_layout(); fig.savefig(f"{FIG}/{name}.png", dpi=160); plt.close(fig); print("saved", name)


# ── F19 fire 재현율 vs 비화재 오탐율, 31뱅크 × 3규칙 ─────────────────────
fig, ax = plt.subplots(figsize=(12, 8))
for r in ["argmax", "topk", "wave"]:
    xs = [g(b, r, "fp_rate_nonfire") * 100 for b in banks]; ys = [g(b, r, "fire_recall") for b in banks]
    ax.scatter(xs, ys, s=55, color=RC[r], label=RN[r], edgecolor="#fcfcfb", lw=1, alpha=.9)
    for b, x, y in zip(banks, xs, ys):
        if b in LBL: ax.annotate(b, (x, y), textcoords="offset points", xytext=(5, 3), fontsize=7.5, color=RC[r])
ax.set_xlabel("비화재(SAM3 none/person) 프레임 중 fire 로 판정한 비율 % (오탐)"); ax.set_ylabel("SAM3 fire 프레임 중 fire 로 판정한 비율 (재현율)")
ax.legend(frameon=False, loc="lower right")
ax.set_title(f"전 뱅크 31종 × 3규칙 — fire 재현율 vs 오탐율\n재현율 1위 뱅크(v1.0.4.2·v1.0.8.4·v1.0.12.0)는 오탐도 1위 = sourcei GT 최하위였던 '많이 쏘는' 대용량 뱅크. 분포-IoU 는 오탐 절반, 재현율도 낮다\n{NOTE}", loc="left", fontsize=11)
save(fig, "f19_fire_recall_vs_fp_all_banks")

# ── F20 뱅크 × 조건 재현율 히트맵 (top-K) ────────────────────────────────
cols = [("rec_area_lt_0_001", "점 불꽃\n<0.1%"), ("rec_area_0_001_0_01", "0.1~1%"), ("rec_area_0_01_0_1", "1~10%"), ("rec_area_ge_0_1", ">10%"),
        ("rec_1box", "박스 1개"), ("rec_2plus_box", "박스 2개+"), ("rec_no_smoke", "연기 없음"), ("rec_with_smoke", "연기 동반"),
        ("rec_fire_smoke_proj", "fire_smoke\n현장"), ("rec_icce", "cohort-b"), ("rec_appdata", "appdata"),
        ("rec_margin_neg", "마진 음수"), ("rec_margin_pos", "마진 양수"), ("fire_recall", "전체")]
fig, axes = plt.subplots(1, 3, figsize=(21, 11))
for ax, r in zip(axes, ["argmax", "topk", "wave"]):
    M = np.array([[g(b, r, k) for k, _ in cols] for b in banks])
    im = ax.imshow(M, cmap=SEQ, vmin=0, vmax=1, aspect="auto")
    for i in range(M.shape[0]):
        for j in range(M.shape[1]):
            if not np.isnan(M[i, j]): ax.text(j, i, f"{M[i, j]:.2f}", ha="center", va="center", fontsize=7, color="white" if M[i, j] > .6 else "#0b0b0b")
    ax.set_xticks(range(len(cols))); ax.set_xticklabels([c[1] for c in cols], fontsize=8, rotation=45, ha="right")
    ax.set_yticks(range(len(banks))); ax.set_yticklabels(ylab if r == "argmax" else [], fontsize=8.5); ax.grid(False)
    ax.set_title(f"{RN[r]} — 조건별 fire 재현율", loc="left", fontsize=11)
fig.suptitle(f"전 뱅크 31종 × 조건 — 어느 뱅크·규칙이든 같은 모양: 점 불꽃·박스 1개·연기 없음에서 낮고, 마진 음수면 argmax 0 / top-K ≤0.2\n"
             f"† = 4클래스 밖 클래스 보유. {NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f20_fire_conditions_all_banks")

# ── F21 smoke→fire 혼동 × fire∪smoke 재현율 ─────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(15, 10))
Y = np.arange(len(banks))
ax = axes[0]
for r in ["argmax", "topk", "wave"]:
    ax.plot([g(b, r, "smoke_to_fire") for b in banks], Y, "o", ms=6, color=RC[r], label=RN[r], mec="#fcfcfb")
ax.set_yticks(Y); ax.set_yticklabels(ylab, fontsize=8.5); ax.invert_yaxis(); ax.set_xlim(0, .7)
ax.set_xlabel("SAM3 smoke 프레임 중 fire 로 판정한 비율 (smoke→fire 혼동)"); ax.set_title("smoke → fire 혼동 (낮을수록 좋음)", loc="left", fontsize=11); ax.legend(frameon=False, fontsize=8.5, loc="lower right")
ax = axes[1]
for r in ["argmax", "topk", "wave"]:
    ax.plot([g(b, r, "fire_or_smoke_recall") for b in banks], Y, "s", ms=6, color=RC[r], label=RN[r], mec="#fcfcfb")
    ax.plot([g(b, r, "fire_recall") for b in banks], Y, "o", ms=4, color=RC[r], mfc="none", mew=1.2)
ax.set_yticks(Y); ax.set_yticklabels([], fontsize=8.5); ax.invert_yaxis(); ax.set_xlim(0.5, 1)
ax.set_xlabel("SAM3 fire 프레임 중 fire∪smoke(■) / fire 만(○) 판정 비율"); ax.set_title("fire∪smoke 로 넓히면 재현율은 얼마나 회복되나", loc="left", fontsize=11)
fig.suptitle(f"전 뱅크 fire↔smoke 혼동 — max 계열은 뱅크 무관하게 smoke 의 30~45% 를 fire 라 하고, 분포-IoU 는 † 뱅크(v1.0.3.0·4.0)에서 55% 까지 튄다\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f21_fire_smoke_confusion_all_banks")

# 요약
for r in ["argmax", "topk", "wave"]:
    rec = np.array([g(b, r, "fire_recall") for b in banks]); fp = np.array([g(b, r, "fp_rate_nonfire") for b in banks])
    print(r, "recall↔fp Spearman:", round(float(np.corrcoef(np.argsort(np.argsort(rec)), np.argsort(np.argsort(fp)))[0, 1]), 2),
          " 점불꽃 평균", round(float(np.nanmean([g(b, r, "rec_area_lt_0_001") for b in banks])), 2),
          " 마진음수 평균", round(float(np.nanmean([g(b, r, "rec_margin_neg") for b in banks])), 2))
