#!/usr/bin/env python3
"""§28 도판 3종. 모든 수치는 검증된 산출물에서만 읽는다."""
import os, json, glob
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": .25, "figure.dpi": 150})
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; AB = f"{OUT}/filter_ab"
FIG = f"{AB}/fig"; os.makedirs(FIG, exist_ok=True)
BLU, ORG, GRY, GRN, RED = "#0072B2", "#D55E00", "#999999", "#009E73", "#CC3311"

# ── H1 연결 효과: 문장 신호는 개선, 뱅크 성능은 아님 ──────────────
fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [1, 1.15]})
cls = ["normal", "falldown", "fire", "smoke"]
fr = [0.014, 0.052, 0.456, 0.182]; hyv = [-0.717, 0.033, 0.674, 0.882]
x = np.arange(4); w = .36
a1.bar(x - w/2, fr, w, color=GRY, label="현행 sd_frames")
a1.bar(x + w/2, hyv, w, color=BLU, label="연결 후 sd_sourcei")
a1.axhline(0, color="k", lw=1)
for i, (u, v) in enumerate(zip(fr, hyv)):
    a1.text(i - w/2, u + (.03 if u >= 0 else -.06), f"{u:+.3f}", ha="center", fontsize=8.5)
    a1.text(i + w/2, v + (.03 if v >= 0 else -.06), f"{v:+.3f}", ha="center", fontsize=8.5,
            color=(RED if v < 0 else "k"), weight=("bold" if abs(v) > .6 else "normal"))
a1.set_xticks(x); a1.set_xticklabels(cls); a1.set_ylim(-.95, 1.35)
a1.set_ylabel("판별력과의 spearman (카메라 통제)")
a1.set_title("H1a · 문장 단위 신호는 크게 개선된다\nnormal 만 부호가 반대(−0.717)", fontsize=11.5)
a1.legend(frameon=False, fontsize=9, loc="upper left")

names = ["base\n(현행)", "hy\n(모수만)", "hy_signed\n(모수+부호)", "fr_signed\n(부호만)"]
mf1 = [.5482, .5076, .5181, .5072]; pr = [.6853, .2549, .3181, .2944]; fp = [.049, .0379, .2489, .2369]
xx = np.arange(4); w2 = .26
a2.bar(xx - w2, mf1, w2, color=BLU, label="topk macro-F1")
a2.bar(xx, pr, w2, color=GRN, label="분포 IoU PR-AUC")
a2.bar(xx + w2, fp, w2, color=ORG, label="normal 오탐")
a2.axhline(.05, color=RED, ls="--", lw=1.4)
a2.text(-0.45, .058, "오탐 예산 5%", color=RED, fontsize=8.5, ha="left")
for i in range(4):
    for off, v in ((-w2, mf1[i]), (0, pr[i]), (w2, fp[i])):
        a2.text(i + off, v + .015, f"{v:.3f}", ha="center", fontsize=7.8)
a2.set_xticks(xx); a2.set_xticklabels(names, fontsize=9); a2.set_ylim(0, .92)
a2.set_title("H1b · 그런데 뱅크 성능으로는 안 바뀐다\nmacro-F1 만 보면 속는다 — PR-AUC 반토막·오탐 5배",
             fontsize=11.5)
a2.legend(frameon=False, fontsize=8.5, ncol=3, loc="upper left")
fig.tight_layout(); fig.savefig(f"{FIG}/h1_connect_effect.png"); plt.close(fig)

# ── H2 GT 파서 버그와 판정 반전 ───────────────────────────────────
fig, (b1, b2) = plt.subplots(1, 2, figsize=(13, 4.4), gridspec_kw={"width_ratios": [1, 1]})
lab = ["파일명 로마자\n미인식", "캡션 활용형\n미인식", "캡션 부정문\n오인식"]
val = [152, 25, 94]
col = [ORG, ORG, RED]
b1.barh(lab, val, color=col, height=.55)
for i, v in enumerate(val): b1.text(v + 3, i, f"{v}장", va="center", fontsize=10)
b1.set_xlim(0, 185); b1.set_xlabel("영향 프레임 수")
b1.set_title("H2a · `kind_of()` 파서 결함 3종 (271장)\n"
             "한글 음절: `넘어지` 는 `넘어짐`·`넘어진` 과 매치 안 됨", fontsize=11.5)
b1.invert_yaxis()

sc = ["R0 원본", "R3 파서수정"]
bf = [.0490, .0192]; mf = [.0551, .0257]
xx = np.arange(2); w3 = .34
b2.bar(xx - w3/2, bf, w3, color=GRY, label="base")
b2.bar(xx + w3/2, mf, w3, color=BLU, label="msmax")
b2.axhline(.05, color=RED, ls="--", lw=1.6)
b2.text(1.42, .0525, "G4 예산 5%", color=RED, fontsize=9, ha="right")
for i in range(2):
    b2.text(i - w3/2, bf[i] + .0018, f"{bf[i]:.4f}", ha="center", fontsize=9)
    b2.text(i + w3/2, mf[i] + .0018, f"{mf[i]:.4f}", ha="center", fontsize=9,
            color=(RED if mf[i] > .05 else GRN), weight="bold")
b2.set_xticks(xx); b2.set_xticklabels([f"{s}\n분모 {n:,}" for s, n in zip(sc, (4323, 4164))])
b2.set_ylabel("normal 오탐"); b2.set_ylim(0, .068)
b2.set_title("H2b · msmax 의 G4 탈락이 뒤집힌다\n근거 없던 159장이 오탐의 55%(msmax)·62%(base)", fontsize=11.5)
b2.legend(frameon=False, fontsize=9.5)
fig.tight_layout(); fig.savefig(f"{FIG}/h2_gt_parser.png"); plt.close(fig)

# ── H3 군집 커버리지 ──────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 4.3))
proj = ["cohort-b", "appdata", "violence", "cohort-a", "loc-c 3종", "source-f", "fire_smoke", "나머지 15종"]
tot = [73390, 24572, 144, 33766, 23239, 6140, 3464, 23475]
cov = [0, 0, 0, 33766, 23239, 6140, 3464, 23475]
xx = np.arange(len(proj))
ax.bar(xx, tot, color=GRY, label="임베딩 보유", width=.62)
ax.bar(xx, cov, color=BLU, label="군집 포함", width=.62)
for i, (t, c) in enumerate(zip(tot, cov)):
    ax.text(i, t + 1800, f"{100*c/t:.0f}%", ha="center", fontsize=9,
            color=(RED if c == 0 else "#333"), weight=("bold" if c == 0 else "normal"))
ax.set_xticks(xx); ax.set_xticklabels(proj, rotation=22, ha="right", fontsize=9)
ax.set_ylabel("프레임 수"); ax.set_ylim(0, 84000)
ax.set_title("H3 · 커버리지 48% 는 무작위 누락이 아니다 — 3개 프로젝트가 통째로 0%\n"
             "kmeans64 는 cohort-b·appdata 편입 이전의 스냅샷", fontsize=11.5)
ax.legend(frameon=False, fontsize=9.5)
fig.tight_layout(); fig.savefig(f"{FIG}/h3_coverage.png"); plt.close(fig)
print("도판:", [f for f in sorted(os.listdir(FIG)) if f.startswith("h")])
