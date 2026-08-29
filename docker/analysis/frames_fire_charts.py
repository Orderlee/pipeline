#!/usr/bin/env python3
"""frames_fire_conditions.py 산출물 → 차트 3장 (fire 가 어떤 상황에서 잘 반응하나, SAM3 약참조)."""
import json, csv, glob, collections
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"; FIG = f"{OUT}/fig"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7",
                     "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b",
                     "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
FIRE = "#e34948"; GRAY = "#c3c2b7"; BLUE = "#2a78d6"; ORANGE = "#eb6834"
C = json.load(open(f"{OUT}/fire_conditions.json"))
rows = list(csv.DictReader(open(f"{OUT}/csv/16_fire_frames_sam3.csv", encoding="utf-8-sig")))
led = list(csv.DictReader(open(f"{OUT}/csv/17_fire_sentence_ledger_frames.csv", encoding="utf-8-sig")))
NOTE = "참조 라벨 = SAM3 검출(auto_generated 의사라벨, GT 아님). 절대값이 아니라 조건 간 상대 비교로 읽을 것"


def save(fig, name):
    fig.tight_layout(); fig.savefig(f"{FIG}/{name}.png", dpi=160); plt.close(fig); print("saved", name)


def bars(ax, d, title, xlabel="프롬프트(v1.0.8.0) 가 fire 로 판정한 비율", order=None, base=None):
    keys = order or list(d.keys()); keys = [k for k in keys if d[k][1] is not None]
    vals = [d[k][1] for k in keys]; ns = [d[k][0] for k in keys]
    y = np.arange(len(keys))
    ax.barh(y, vals, color=[FIRE if v >= (base or 0) else GRAY for v in vals], height=0.6)
    for i, (v, n) in enumerate(zip(vals, ns)):
        ax.text(v + .01, i, f"{v:.1%}  (n={n:,})", va="center", fontsize=9)
    if base is not None: ax.axvline(base, color="#52514e", ls=":", lw=1); ax.text(base, -0.7, f"전체 {base:.1%}", fontsize=8.5, ha="center", color="#52514e")
    ax.set_yticks(y); ax.set_yticklabels(keys); ax.invert_yaxis(); ax.set_xlim(0, 1.18); ax.set_xlabel(xlabel); ax.set_title(title, loc="left", fontsize=11)


base = np.mean([int(r["hit_fire"]) for r in rows])
# ── F16 조건별 재현율 ───────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(18, 9))
bars(axes[0, 0], C["by_max_area"], "① 불꽃 크기 (SAM3 fire 박스 최대 면적, 프레임 대비)", base=base,
     order=["0~0.001", "0.001~0.003", "0.003~0.01", "0.01~0.03", "0.03~0.1", "0.1~1.01"])
axes[0, 0].set_yticklabels(["<0.1% (점)", "0.1~0.3%", "0.3~1%", "1~3%", "3~10%", ">10% (화면 덮음)"])
bars(axes[0, 1], C["by_n_boxes"], "② 불꽃 박스 수", base=base, order=["1", "2~3", "4+"])
bars(axes[0, 2], C["by_with_smoke"], "③ 연기(SAM3 smoke) 동반 여부", base=base)
bars(axes[1, 0], C["by_fire_minus_normal"], "④ 코사인 마진 cos(fire) − cos(normal)", base=base,
     order=["-1~-0.02", "-0.02~0", "0~0.02", "0.02~0.05", "0.05~0.1", "0.1~1"])
axes[1, 0].set_yticklabels(["< −0.02", "−0.02~0", "0~0.02", "0.02~0.05", "0.05~0.1", "> 0.1"])
bars(axes[1, 1], C["by_conf"], "⑤ SAM3 신뢰도 (참조 라벨 자체의 확신)", base=base, order=["0.7~0.8", "0.8~0.9", "0.9~1.01"])
bars(axes[1, 2], {k: v for k, v in C["by_project"].items() if v[0] >= 15}, "⑥ 현장", base=base)
fig.suptitle(f"화재 프롬프트는 어떤 상황에서 반응하나 — frames 188k 중 SAM3 fire 검출 {len(rows):,} 프레임, 전체 재현율 {base:.1%}\n"
             f"큰 불꽃·여러 박스·연기 동반·마진 양수일 때 90%+, 화면의 0.1% 미만 점 불꽃은 44% · 마진 음수면 0%.  {NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f16_fire_conditions")

# ── F17 현장별 재현율 vs 오탐율 ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 6))
fp = C["fp_by_project"]; rc = C["by_project"]
for p, (nneg, nfp, r) in fp.items():
    rec = rc.get(p, (0, None))
    x = r; y = rec[1] if rec[1] is not None else -0.05
    s = 40 + 4 * np.sqrt(nneg)
    ax.scatter(x, y, s=s, color=FIRE if rec[0] >= 15 else GRAY, alpha=.8, edgecolor="#fcfcfb")
    if nneg >= 400 or rec[0] >= 15:
        ax.annotate(f"{p}\n(fire {rec[0]}, 비화재 {nneg:,})", (x, y), textcoords="offset points", xytext=(6, 4), fontsize=8, color="#52514e")
ax.set_xscale("symlog", linthresh=0.001); ax.set_xlabel("오탐율: SAM3 가 fire 를 못 본 프레임 중 프롬프트가 fire 라 한 비율 (symlog)")
ax.set_ylabel("재현율: SAM3 fire 프레임 중 프롬프트 fire 비율 (−0.05 = SAM3 fire 없음)")
ax.axhline(base, color="#52514e", ls=":", lw=1)
ax.set_title(f"현장별 재현율 vs 오탐율 — 오탐은 화재 데이터셋(fire_smoke 33%)에 몰려 있다 = SAM3 가 놓친 진짜 화재일 가능성. 다른 현장은 0~2%\n{NOTE}", loc="left", fontsize=11)
save(fig, "f17_fire_project_recall_fp")

# ── F18 fire 문장 원장 + 구문 ──────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(19, 9), gridspec_kw={"width_ratios": [1.5, 1]})
ax = axes[0]
good = [r for r in led if float(r["selectivity"]) >= 0.8 and int(r["hit"]) >= 40][:12]
bad = sorted([r for r in led if float(r["selectivity"]) < 0.5 and int(r["trap"]) >= 30], key=lambda r: -int(r["trap"]))[:8]
rs = good + bad; y = np.arange(len(rs))
ax.barh(y, [int(r["hit"]) for r in rs], color=FIRE, label="hit (SAM3 fire 프레임을 끌어당김)")
ax.barh(y, [-int(r["trap"]) for r in rs], color=GRAY, label="trap (비화재/연기 프레임을 가로챔)")
for i, r in enumerate(rs):
    t = r["text"]; t = t if len(t) <= 78 else t[:77] + "…"
    ax.text(0, i, f"  [{float(r['selectivity']):.2f}] {t}", va="center", fontsize=7.8, bbox=dict(facecolor="#fcfcfb", alpha=.75, edgecolor="none", pad=1))
ax.axhline(len(good) - .5, color=ORANGE, ls="--", lw=1)
ax.set_yticks([]); ax.invert_yaxis(); ax.set_xlabel("← trap   |   hit →   ([ ] 선택도)")
ax.set_title(f"fire 문장 원장 (frames 표본 {C['ledger_sample']['n_frames']:,}프레임 전역 top-10) — 위: 선택도≥0.8 & hit≥40 / 점선 아래: 오탈취 상위", loc="left", fontsize=10.5)
ax.legend(frameon=False, fontsize=8.5, loc="lower right")
ax = axes[1]
def dedup(rows_, n):
    out, seen = [], []
    for g, h, t, s in rows_:
        if any(g in o or o in g for o in seen): continue
        seen.append(g); out.append((g, h, t, s))
        if len(out) == n: break
    return out
wh = dedup(C["fire_phrase_white"], 10); bl = dedup(C["fire_phrase_black"], 10); b0 = C["fire_phrase_base"]
rows_ = wh + bl; y = np.arange(len(rows_))
ax.barh(y, [r[3] - b0 for r in rows_], color=[FIRE if r[3] >= b0 else GRAY for r in rows_])
for i, r in enumerate(rows_):
    ax.text(0.004 if r[3] >= b0 else -0.004, i, f"{r[0]}  ({r[1] + r[2]:,})", va="center", ha="left" if r[3] >= b0 else "right", fontsize=8.5)
ax.axvline(0, color="#52514e", lw=1); ax.set_yticks([]); ax.invert_yaxis(); ax.set_xlim(-b0 - .05, 1 - b0 + .05)
ax.set_xlabel(f"구문 선택도 − fire 기준선({b0:.2f})"); ax.set_title("fire 구문 — 위 10 넣을 구문 / 아래 10 피할 구문 (등장≥150)", loc="left", fontsize=10.5)
fig.suptitle(f"frames 전체에서 이기는 fire 문장 — '실내(office·warehouse)·밝은 환경·구석 위치·flames' 가 이기고, '불꽃(sparks)·공사장·도로·서사형(a fire has…)' 이 가로챈다\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f18_fire_sentences_frames")
print("fire hit top:", [(r["text"][:60], r["hit"]) for r in led[:5]])
