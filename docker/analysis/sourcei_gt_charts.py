#!/usr/bin/env python3
"""sourcei_gt_rules.py 산출물(preds.npz/metrics.json/percls_*.npy) → 검증 지표 + 차트 PNG.

GT 없이 낸 노션 보고서(frames 90,084)의 주장을 GT 가 있는 sourcei 7,498 프레임에서 대조한다.
**전 뱅크 원칙**: 뱅크별 차트는 이벤트 클래스가 있는 db_backed 뱅크 전부(31)를 다룬다 —
소수 버전만 뽑아 그리지 않는다(v2.0.5.x 4종은 normal+class_5 만 있어 이벤트 지표가 정의되지 않아 제외).
31개 계열을 색으로 구분하지 않는다 — 뱅크 축은 히트맵/도트플롯의 행으로 놓는다 (dataviz: 범주색 8개 상한).
"""
import json, os, glob
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.colors import LinearSegmentedColormap
from scipy.stats import spearmanr

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
FIG = f"{OUT}/fig"; os.makedirs(FIG, exist_ok=True)
for f in glob.glob("/workspace/.fonts/*.tt[fc]"):
    fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.color": "#e6e5e1",
                     "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
                     "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e",
                     "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
RC = {"argmax": "#2a78d6", "topk": "#eb6834", "wave": "#1baf7a"}
RN = {"argmax": "argmax (top-1)", "topk": "top-K 투표 (K=10)", "wave": "분포-IoU (제품 규칙)"}
CLASSES = ["normal", "falldown", "fire", "smoke"]
SEQ = LinearSegmentedColormap.from_list("seq", ["#eef3fb", "#2a78d6", "#0b2e5c"])   # 단일 색상 순차

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
m = json.load(open(f"{OUT}/metrics.json"))
meta = np.load(f"{OUT}/frame_meta.npz", allow_pickle=True)
assert (meta["ids"] == d["ids"]).all()
gt = d["gt"]; cam = d["camera"]; src = d["gt_source"]
RULES = ["argmax", "topk", "wave"]


def vkey(b):
    return tuple(int(x) for x in b.lstrip("vV").split("."))


banks = sorted([b for b in d["banks"] if not b.startswith("v2.")], key=vkey)   # 이벤트 클래스 보유 전 뱅크, 버전순
assert len(banks) == 31, len(banks)
# 추가 클래스(smoking/class_N) 보유 여부는 **classes 필드**로만 판정한다.
# 버전 접두사(v1.*)로 대신 판정했다가 v1.0.3.0·3.1·4.0(smoking 보유)을 놓쳤다 — 이름은 계약이 아니다.
extra = [b for b in banks if len(m["banks"][b]["classes"]) != 4]
core = [b for b in banks if b not in extra]
mf1 = {r: np.array([m["banks"][b]["rules"][r]["macro_f1_ev"] for b in banks]) for r in RULES}
acc = {r: np.array([m["banks"][b]["rules"][r]["acc"] for b in banks]) for r in RULES}
summary = {"n_frames": int(len(gt)), "n_banks": len(banks), "banks": banks, "n_core": len(core), "extra_banks": extra}
Y = np.arange(len(banks))
ylab = [b + (" †" if b in extra else "") for b in banks]
NOTE = f"† = 4클래스 밖 클래스(smoking/class_N) 보유 뱅크 {len(extra)}종 — 그 예측은 '기타'로 집계"


def macro_f1(pred, g, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((pred == c) & (g == c)).sum(); fp = ((pred == c) & (g != c)).sum(); fn = ((pred != c) & (g == c)).sum()
        p = tp / max(tp + fp, 1); r = tp / max(tp + fn, 1); f.append(2 * p * r / max(p + r, 1e-12))
    return float(np.mean(f))


def save(fig, name):
    fig.tight_layout(); fig.savefig(f"{FIG}/{name}.png", dpi=160); plt.close(fig); print("saved", name)


def heat(ax, M, xt, title, fmt="{:.2f}", vmin=None, vmax=None, cmap=SEQ, mark_max=False, ylabels=True, fs=7.5, yl=None):
    im = ax.imshow(M, cmap=cmap, aspect="auto", vmin=vmin, vmax=vmax)
    lo, hi = im.get_clim()
    for i in range(M.shape[0]):
        j_best = int(np.nanargmax(M[i])) if mark_max else -1
        for j in range(M.shape[1]):
            v = M[i, j]
            if np.isnan(v): continue
            t = (v - lo) / max(hi - lo, 1e-9)
            ax.text(j, i, fmt.format(v) + ("★" if j == j_best else ""), ha="center", va="center", fontsize=fs, color="white" if t > .6 else "#0b0b0b")
    ax.set_xticks(range(M.shape[1])); ax.set_xticklabels(xt, fontsize=8.5)
    ax.set_yticks(range(M.shape[0])); ax.set_yticklabels((yl if yl is not None else ylab) if ylabels else [], fontsize=8.5)
    ax.grid(False); ax.set_title(title, loc="left", fontsize=11)
    return im


# ── F1 규칙×뱅크 macro-F1 (31뱅크 도트플롯) ─────────────────────────────
fig, ax = plt.subplots(figsize=(9, 11))
for i in Y:
    ax.plot([mf1["wave"][i], max(mf1["topk"][i], mf1["argmax"][i])], [i, i], color="#c3c2b7", lw=1, zorder=0)
for r in RULES:
    ax.plot(mf1[r], Y, "o", ms=7, color=RC[r], label=RN[r], mec="#fcfcfb", mew=1)
    ax.axvline(mf1[r].mean(), color=RC[r], lw=1, ls=":", alpha=.8)
    ax.text(mf1[r].mean(), -1.2, f"평균 {mf1[r].mean():.3f}", color=RC[r], ha="center", fontsize=8.5)
ax.set_yticks(Y); ax.set_yticklabels(ylab, fontsize=9); ax.set_ylim(len(banks) - .5, -2)
ax.set_xlabel("이벤트 3클래스 macro-F1 (GT 7,498 프레임)"); ax.set_xlim(0.2, 0.6)
ax.set_title(f"판정 규칙 × 전 뱅크 {len(banks)}종 — GT 기준: top-K ≥ argmax > 분포-IoU@0.15 (예외 없음)\n{NOTE}", loc="left", fontsize=12)
ax.legend(frameon=False, loc="lower right")
save(fig, "f01_rule_bank_macrof1")
summary["rule_mean_mf1"] = {r: float(mf1[r].mean()) for r in RULES}
summary["rule_mean_acc"] = {r: float(acc[r].mean()) for r in RULES}
summary["wave_worst_all_banks"] = bool((mf1["wave"] < np.minimum(mf1["topk"], mf1["argmax"])).all())
summary["topk_ge_argmax_banks"] = int((mf1["topk"] >= mf1["argmax"]).sum())
summary["best_bank"] = {r: banks[int(mf1[r].argmax())] for r in RULES}

# ── F2 클래스별 recall / precision (전 뱅크 평균) ────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), sharey=True)
w = 0.26
for k, (metric, title) in enumerate([("r", "재현율 (recall)"), ("p", "정밀도 (precision)")]):
    ax = axes[k]
    for i, r in enumerate(RULES):
        vals = [np.mean([m["banks"][b]["rules"][r]["per_class"][c][metric] for b in banks]) for c in CLASSES]
        bars = ax.bar(np.arange(4) + (i - 1) * w, vals, w * 0.92, color=RC[r], label=RN[r])
        for bx, v in zip(bars, vals):
            ax.text(bx.get_x() + bx.get_width() / 2, v + .01, f"{v:.2f}", ha="center", fontsize=8, color="#52514e")
    ax.set_xticks(np.arange(4)); ax.set_xticklabels(CLASSES); ax.set_title(title, loc="left"); ax.set_ylim(0, 1.08)
axes[0].legend(frameon=False, fontsize=9)
fig.suptitle(f"클래스별 성능 ({len(banks)}뱅크 평균) — 정밀도는 세 규칙 동등, 차이는 전부 재현율. 분포-IoU 는 smoke 0.08 · fire 0.19", x=0.01, ha="left", fontsize=13)
save(fig, "f02_class_recall_precision")
summary["class_recall_mean"] = {r: {c: float(np.mean([m["banks"][b]["rules"][r]["per_class"][c]["r"] for b in banks])) for c in CLASSES} for r in RULES}

# ── F2b 클래스별 재현율 히트맵 — 전 뱅크 ────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(15, 11))
for k, r in enumerate(RULES):
    M = np.array([[m["banks"][b]["rules"][r]["per_class"][c]["r"] for c in CLASSES] for b in banks])
    heat(axes[k], M, CLASSES, f"{RN[r]} — 클래스별 재현율", vmin=0, vmax=1, ylabels=(k == 0))
fig.suptitle(f"전 뱅크 {len(banks)}종 × 클래스 재현율 — 분포-IoU 의 smoke 열은 뱅크 무관하게 비어 있다 (규칙 구조 문제, 뱅크 문제 아님)\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f02b_recall_heatmap")

# ── F3 GT-free 발화율(frames) vs GT macro-F1 ────────────────────────────
fe = {}
for l in open("/workspace/.cron_logs/frames_rule_env.tsv"):
    p = l.strip().split("|"); fe[p[0]] = dict(n=int(p[1]), topk=int(p[2]), wave=int(p[3]), argmax=int(p[4]))
fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.8))
summary["frames_eventrate_vs_gt_rho"] = {}
LBL = ("v1.0.8.0", "v1.0.8.4", "v1.0.8.1", "v1.0.12.0", "v1.0.4.2", "v1.0.3.2", "v5.0.5.0", "v1.0.2.1")
for ax, r in zip(axes, RULES):
    ev = np.array([fe[b][r] / fe[b]["n"] * 100 for b in banks])
    rho = spearmanr(ev, mf1[r]).correlation; summary["frames_eventrate_vs_gt_rho"][r] = float(rho)
    ax.scatter(ev, mf1[r], s=46, color=RC[r], edgecolor="#fcfcfb", lw=1)
    for b, xx, yy in zip(banks, ev, mf1[r]):
        if b in LBL: ax.annotate(b, (xx, yy), textcoords="offset points", xytext=(4, 3), fontsize=8, color="#52514e")
    ax.set_title(f"{RN[r]}   ρ = {rho:+.2f}", loc="left", fontsize=11)
    ax.set_xlabel("frames(21현장, GT 없음) 이벤트 발화율 %"); ax.set_ylabel("sourcei GT macro-F1" if r == "argmax" else "")
fig.suptitle(f"노션 §7-4 '발화율 표로 뱅크를 고르면 안 된다' 검증 ({len(banks)}뱅크) — max 계열은 발화율이 높을수록 GT 성능이 낮다", x=0.01, ha="left", fontsize=13)
save(fig, "f03_eventrate_vs_gt")

# ── F4 규칙 간 뱅크 순위 상관 ───────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
summary["gt_rank_rho"] = {}
for ax, (a, b) in zip(axes, [("topk", "argmax"), ("topk", "wave")]):
    rho = spearmanr(mf1[a], mf1[b]).correlation; summary["gt_rank_rho"][f"{a}_{b}"] = float(rho)
    ax.scatter(mf1[a], mf1[b], s=46, color="#2a78d6", edgecolor="#fcfcfb")
    for bk, xx, yy in zip(banks, mf1[a], mf1[b]):
        if bk in LBL: ax.annotate(bk, (xx, yy), textcoords="offset points", xytext=(4, 3), fontsize=8, color="#52514e")
    ax.set_xlabel(f"{RN[a]} macro-F1"); ax.set_ylabel(f"{RN[b]} macro-F1")
    ax.set_title(f"뱅크 순위 상관 Spearman ρ = {rho:+.2f}  (노션 GT-free: {'+0.87' if b == 'argmax' else '+0.03'})", loc="left", fontsize=11)
fig.suptitle(f"'규칙은 두 가족(max 계열 vs 분포 계열)' — GT 성능 순위에서도 재현 ({len(banks)}뱅크)", x=0.01, ha="left", fontsize=13)
save(fig, "f04_rule_rank_corr")

# ── F5 혼동 구조 — 전 뱅크 (누락·이벤트 간 혼동·normal 오탐 비율) ────────
fig, axes = plt.subplots(1, 3, figsize=(15, 11))
cols = ["falldown→normal", "fire→normal", "smoke→normal", "fire→smoke", "smoke→fire", "normal→이벤트"]
for k, r in enumerate(RULES):
    M = np.zeros((len(banks), len(cols)))
    for i, b in enumerate(banks):
        p = d[f"{r}__{b}"]
        rate = lambda gc, pc: (p[gt == gc] == pc).mean()
        M[i] = [rate(1, 0), rate(2, 0), rate(3, 0), rate(2, 3), rate(3, 2), (p[gt == 0] > 0).mean()]
    heat(axes[k], M, cols, f"{RN[r]} — 행 정규화 혼동 비율", vmin=0, vmax=1, ylabels=(k == 0), fs=7)
    axes[k].tick_params(axis="x", rotation=30)
fig.suptitle(f"전 뱅크 {len(banks)}종 혼동 구조 — 오류는 이벤트→normal 누락에 집중. 이벤트 간 혼동(fire↔smoke)·normal 오탐은 어느 뱅크·규칙에서도 소수\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f05_confusion_all_banks")

# ── F6 뱅크 × 클래스 F1 (top-K) + 규칙별 정확도 ─────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 11))
M = np.array([[m["banks"][b]["rules"]["topk"]["per_class"][c]["f1"] for c in CLASSES[1:]] for b in banks])
heat(axes[0], M, CLASSES[1:], "클래스별 F1 (top-K 규칙)", vmin=0, vmax=0.8)
A = np.array([[m["banks"][b]["rules"][r]["acc"] for r in RULES] for b in banks])
heat(axes[1], A, [RN[r] for r in RULES], "정확도 (규칙별, ★ 뱅크별 최고 규칙)", fmt="{:.3f}", vmin=0.5, vmax=0.72, ylabels=False, mark_max=True)
base = banks.index("v1.0.8.0")
for ax in axes:
    ax.axhline(base - .5, color="#eb6834", lw=1.2); ax.axhline(base + .5, color="#eb6834", lw=1.2)
fig.suptitle(f"전 뱅크 {len(banks)}종 — 기준선 v1.0.8.0(주황 테두리) 대비 전면 교체본 v1.0.8.4 는 fire F1 0.50→0.37 퇴행. v1.0.8.1 이 최고(fire 0.65, acc 0.710)\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f06_bank_class_f1_all")
summary["v1084_vs_v1080"] = {r: {"acc_delta": m["banks"]["v1.0.8.4"]["rules"][r]["acc"] - m["banks"]["v1.0.8.0"]["rules"][r]["acc"],
                                 "mf1_delta": m["banks"]["v1.0.8.4"]["rules"][r]["macro_f1_ev"] - m["banks"]["v1.0.8.0"]["rules"][r]["macro_f1_ev"]} for r in RULES}
groups = {}
for i, b in enumerate(banks):
    groups.setdefault(tuple(np.round(A[i], 6)), []).append(b)
summary["dup_groups"] = [v for v in groups.values() if len(v) > 1]   # 동일 점수 = 동일 문장집합 (노션 '실질 28' 대조)

# ── F7 argmax 마진 구간별 정확도 — 전 뱅크 히트맵 ──────────────────────
edges = [0, .005, .01, .02, .03, .05, .08, .2]
xt = [f"{lo:g}~{hi:g}" for lo, hi in zip(edges[:-1], edges[1:])]
M = np.full((len(banks), len(xt)), np.nan)
for i, b in enumerate(banks):
    mg = d[f"margin__{b}"].astype(float); ok = d[f"argmax__{b}"] == gt
    for j, (lo, hi) in enumerate(zip(edges[:-1], edges[1:])):
        sel = (mg >= lo) & (mg < hi)
        if sel.sum() >= 30: M[i, j] = ok[sel].mean()
fig, ax = plt.subplots(figsize=(11, 11))
heat(ax, M, xt, "argmax 결정 마진 구간별 정확도 (n<30 구간은 공란)", vmin=0.4, vmax=1.0)
ax.axvline(2.5, color="#eb6834", lw=1.5, ls="--"); ax.text(2.55, -0.9, "노션 채택 기준 0.02", color="#eb6834", fontsize=9)
ax.set_xlabel("argmax 마진 (1등−2등 클래스 max 코사인)")
n_worst = int((np.nanargmin(np.nan_to_num(M, nan=9), 1) == 3).sum()); summary["margin_worst_bin_is_002_003"] = n_worst
fig.suptitle(f"전 뱅크 {len(banks)}종: 마진은 정확도를 단조 예측하지 않는다 — 0.02~0.03 구간 정확도가 0.005~0.02 구간보다 낮음(31뱅크 공통)\n"
             f"노션 마진≥0.02 는 문장 채택 기준이며 프레임 판정 신뢰도 게이트로 이식되지 않음\n{NOTE}", x=0.01, ha="left", fontsize=11.5)
save(fig, "f07_margin_accuracy_all")

# ── F8 IoU 임계 스윕 — 전 뱅크 히트맵 + 요약 곡선 ───────────────────────
thrs = np.round(np.arange(0.05, 0.61, 0.025), 3)
S = np.zeros((len(banks), len(thrs))); SA = np.zeros_like(S)
for i, b in enumerate(banks):
    I = d[f"iou__{b}"].astype(np.float32)
    for j, t in enumerate(thrs):
        pred = np.where((I < t).any(1), I.argmin(1) + 1, 0)
        S[i, j] = macro_f1(pred, gt); SA[i, j] = (pred == gt).mean()
fig, axes = plt.subplots(1, 2, figsize=(17, 11), gridspec_kw={"width_ratios": [3.2, 1.6]})
heat(axes[0], S, [f"{t:g}" for t in thrs], "분포-IoU 임계별 macro-F1 (★ 뱅크별 최고)", vmin=0.1, vmax=0.62, mark_max=True, fs=6.5)
j015 = list(thrs).index(0.15)
axes[0].axvline(j015, color="#eb6834", lw=1.5, ls="--")
axes[0].set_xlabel("발화 임계 (IoU < thr → 이벤트)"); axes[0].tick_params(axis="x", rotation=90)
ax = axes[1]
q25, q50, q75 = np.percentile(S, [25, 50, 75], axis=0)
ax.fill_between(thrs, q25, q75, color=RC["wave"], alpha=.2, label="뱅크 사분위 범위")
ax.plot(thrs, q50, color=RC["wave"], lw=2.2, label=f"분포-IoU macro-F1 중앙값 ({len(banks)}뱅크)")
ax.plot(thrs, np.percentile(SA, 50, axis=0), color="#0e7a54", lw=1.6, ls="--", label="분포-IoU 정확도 중앙값")
ax.axhline(np.median(mf1["topk"]), color=RC["topk"], ls=":", lw=1.5, label=f"top-K macro-F1 중앙값 {np.median(mf1['topk']):.3f}")
ax.axhline(np.median(mf1["argmax"]), color=RC["argmax"], ls=":", lw=1.5, label=f"argmax 중앙값 {np.median(mf1['argmax']):.3f}")
ax.axvline(0.15, color="#eb6834", ls="--", lw=1.5); ax.text(0.16, 0.12, "제품 임계 0.15", color="#eb6834", fontsize=9)
ax.set_xlabel("발화 임계"); ax.set_ylabel("macro-F1 / 정확도"); ax.legend(frameon=False, fontsize=8.5, loc="lower center"); ax.set_title("임계 곡선 요약", loc="left", fontsize=11)
best_thr = thrs[S.argmax(1)]
summary["wave_thr_best_median"] = float(np.median(best_thr)); summary["wave_thr_best_range"] = [float(best_thr.min()), float(best_thr.max())]
summary["wave_beats_topk_at_best"] = int((S.max(1) > mf1["topk"]).sum())
fig.suptitle(f"전 뱅크 {len(banks)}종 임계 스윕 — 제품 임계 0.15 는 모든 뱅크에서 곡선 초입. 뱅크별 최적 임계 {best_thr.min():g}~{best_thr.max():g}(중앙값 {np.median(best_thr):.3f}), "
             f"최적 임계에서 분포-IoU 가 top-K 를 넘는 뱅크 {summary['wave_beats_topk_at_best']}/{len(banks)}\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f08_iou_thr_sweep_all")

# ── F8b 카메라 홀드아웃 — 전 뱅크 도트플롯 ──────────────────────────────
cams_all = np.unique(cam)
H = {k: np.zeros(len(banks)) for k in ("at015", "tuned", "topk", "argmax")}; THR = []
for i, b in enumerate(banks):
    I = d[f"iou__{b}"].astype(np.float32); acc_f = {k: [] for k in H}; tt = []
    for fold in (0, 1):
        tr = np.isin(cam, cams_all[fold::2]); te = ~tr
        f1_at = lambda t, s: macro_f1(np.where((I[s] < t).any(1), I[s].argmin(1) + 1, 0), gt[s])
        tbest = float(thrs[int(np.argmax([f1_at(t, tr) for t in thrs]))]); tt.append(tbest)
        acc_f["at015"].append(f1_at(0.15, te)); acc_f["tuned"].append(f1_at(tbest, te))
        acc_f["topk"].append(macro_f1(d[f"topk__{b}"][te], gt[te])); acc_f["argmax"].append(macro_f1(d[f"argmax__{b}"][te], gt[te]))
    for k in H: H[k][i] = np.mean(acc_f[k])
    THR.append(tt)
fig, ax = plt.subplots(figsize=(10, 11))
for i in Y:
    ax.plot([H["at015"][i], H["tuned"][i]], [i, i], color="#c3c2b7", lw=1, zorder=0)
ax.plot(H["at015"], Y, "o", ms=7, color=RC["wave"], mfc="none", mew=1.8, label="분포-IoU @0.15 (제품)")
ax.plot(H["tuned"], Y, "o", ms=7, color="#0e7a54", label="분포-IoU @학습카메라 최적 thr")
ax.plot(H["topk"], Y, "s", ms=6, color=RC["topk"], label=RN["topk"])
ax.plot(H["argmax"], Y, "^", ms=6, color=RC["argmax"], label=RN["argmax"])
ax.set_yticks(Y); ax.set_yticklabels([f"{b}{' †' if b in extra else ''}  (thr {THR[i][0]:g}/{THR[i][1]:g})" for i, b in enumerate(banks)], fontsize=8.5); ax.invert_yaxis()
ax.set_xlabel("테스트 카메라 macro-F1 (짝/홀 카메라 2-fold 평균)"); ax.legend(frameon=False, fontsize=9, loc="upper center", bbox_to_anchor=(0.5, -0.05), ncol=4)
wins = int((H["tuned"] > H["topk"]).sum()); summary["holdout_tuned_beats_topk"] = wins
summary["holdout_mean"] = {k: float(v.mean()) for k, v in H.items()}
ax.set_title(f"카메라 홀드아웃 — 임계를 다른 카메라에서 골라도 분포-IoU 가 top-K 를 앞서는 뱅크 {wins}/{len(banks)}\n"
             f"평균 macro-F1: IoU@0.15 {H['at015'].mean():.2f} → 튠 {H['tuned'].mean():.2f} vs top-K {H['topk'].mean():.2f}\n{NOTE}", loc="left", fontsize=11)
save(fig, "f08b_iou_thr_holdout_all")

# ── F9 클래스 오프셋 α 스윕 — 전 뱅크 히트맵 ────────────────────────────
alphas = np.round(np.linspace(0, 1, 11), 2)
pbanks = [b for b in banks if os.path.exists(f"{OUT}/percls_{b}.npy")]
G = np.zeros((len(pbanks), len(alphas))); GA = np.zeros_like(G); FR = np.zeros_like(G); NR = np.zeros_like(G)
for i, b in enumerate(pbanks):
    per = np.load(f"{OUT}/percls_{b}.npy")
    for j, a in enumerate(alphas):
        f1s, accs, fr, nr = [], [], [], []
        for fold in (0, 1):
            tr = np.isin(cam, cams_all[fold::2]); te = ~tr
            off = per[tr].mean(0) - per[tr].mean(0)[0]
            pred = (per[te] - a * off).argmax(1)
            f1s.append(macro_f1(pred, gt[te])); accs.append((pred == gt[te]).mean())
            fr.append((pred[gt[te] == 2] == 2).mean()); nr.append((pred[gt[te] == 0] == 0).mean())
        G[i, j], GA[i, j], FR[i, j], NR[i, j] = map(np.mean, (f1s, accs, fr, nr))
ylab_p = [b + (" †" if b in extra else "") for b in pbanks]
fig, axes = plt.subplots(1, 2, figsize=(15, 11))
heat(axes[0], G, [f"{a:g}" for a in alphas], "macro-F1 (★ 뱅크별 최고 α)", vmin=0.3, vmax=0.6, mark_max=True, yl=ylab_p)
heat(axes[1], GA, [f"{a:g}" for a in alphas], "정확도", vmin=0.3, vmax=0.75, ylabels=False)
for ax in axes: ax.set_xlabel("오프셋 강도 α (0=보정 없음, 1=클래스 평균차 전량 차감)")
best_a = alphas[G.argmax(1)]
summary["offset_best_alpha_median"] = float(np.median(best_a)); summary["offset_acc_drop_at1_mean"] = float((GA[:, 0] - GA[:, -1]).mean())
summary["offset_normal_recall_at1_mean"] = float(NR[:, -1].mean()); summary["offset_mf1_gain_at_best_mean"] = float(np.mean(G.max(1) - G[:, 0]))
fig.suptitle(f"클래스 오프셋(z-보정) α 스윕, 전 뱅크 {len(pbanks)}종 (카메라 홀드아웃) — 최적 α 중앙값 {np.median(best_a):.1f} (macro-F1 +{summary['offset_mf1_gain_at_best_mean']:.2f}); "
             f"α=1 이면 정확도 평균 −{summary['offset_acc_drop_at1_mean']:.2f}, normal 재현율 {NR[:, -1].mean():.2f}\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f09_offset_alpha_all")

# ── F10 GT 신뢰도 — 전 뱅크 ────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 11), gridspec_kw={"width_ratios": [1.2, 1.6]})
srcs = ["caption", "filename", "folder", "none"]; counts = {s: int((src == s).sum()) for s in srcs}
Msrc = np.array([[m["banks"][b]["rules"]["topk"]["per_gt_source"][s] for s in srcs] for b in banks])
heat(axes[0], Msrc, [f"{s}\n(n={counts[s]:,})" for s in srcs], "GT 출처별 정확도 (top-K)", vmin=0, vmax=1)
fie = meta["frame_in_event"].astype(int); sel = (gt == 1) & (fie > 0)
bins = [1, 2, 3, 5, 10, 20, 50, 100, 1000]
xt2 = [f"{lo}~{hi - 1}" if hi - 1 > lo else f"{lo}" for lo, hi in zip(bins[:-1], bins[1:])]
Mf = np.full((len(banks), len(xt2)), np.nan)
for i, b in enumerate(banks):
    p = d[f"topk__{b}"]
    for j, (lo, hi) in enumerate(zip(bins[:-1], bins[1:])):
        s = sel & (fie >= lo) & (fie < hi)
        if s.sum() >= 20: Mf[i, j] = (p[s] == 1).mean()
heat(axes[1], Mf, xt2, "falldown GT 재현율 × 이벤트 내 프레임 순번 (top-K, n<20 공란)", vmin=0, vmax=0.8, ylabels=False)
axes[1].set_xlabel("frame_in_event")
fig.suptitle(f"GT 신뢰도 ({len(banks)}뱅크) — caption 출처(≈normal) 0.92 vs folder 0.17; 낙상 GT 는 영상 윈도우 라벨이라 후반 프레임(50~99)은 전 뱅크 재현율 ≤ {np.nanmax(Mf[:, -2]):.2f}\n{NOTE}", x=0.01, ha="left", fontsize=12)
save(fig, "f10_gt_reliability_all")
summary["gt_source_acc_topk"] = {s: float(Msrc[:, j].mean()) for j, s in enumerate(srcs)}

# ── F11 카메라 간 뱅크 순위 전이 ────────────────────────────────────────
elig = [c for c in cams_all if ((cam == c).sum() >= 100) and len(np.unique(gt[cam == c])) >= 2]
R = np.zeros((len(elig), len(banks)))
for i, c in enumerate(elig):
    s = cam == c
    for j, b in enumerate(banks):
        R[i, j] = macro_f1(d[f"topk__{b}"][s], gt[s], classes=tuple(int(x) for x in np.unique(gt[s]) if x > 0))
C = np.array([[spearmanr(R[i], R[j]).correlation for j in range(len(elig))] for i in range(len(elig))])
valid = ~np.isnan(C); C = np.nan_to_num(C, nan=0.0)
fig, ax = plt.subplots(figsize=(8.5, 7))
im = ax.imshow(C, cmap="RdBu", vmin=-1, vmax=1)
for i in range(len(elig)):
    for j in range(len(elig)):
        ax.text(j, i, f"{C[i, j]:+.2f}" if valid[i, j] else "n/a", ha="center", va="center", fontsize=8, color="white" if abs(C[i, j]) > .6 else "#0b0b0b")
short = [c.replace("cheung", "층").replace("jiha", "지하")[:26] + f"\n(n={int((cam == c).sum()):,})" for c in elig]
ax.set_xticks(range(len(elig))); ax.set_yticks(range(len(elig))); ax.set_xticklabels(short, rotation=60, ha="right", fontsize=7.5); ax.set_yticklabels(short, fontsize=7.5); ax.grid(False)
fig.colorbar(im, ax=ax, shrink=.7, label="뱅크 순위 Spearman ρ")
off_diag = C[(~np.eye(len(elig), dtype=bool)) & valid]
ax.set_title(f"카메라 간 뱅크 순위 상관 (top-K macro-F1, {len(banks)}뱅크) — 중앙값 ρ={np.median(off_diag):+.2f}\n노션 '승자는 현장 특이(중앙값 0.369)' 와 부합: 카메라별 1위 뱅크가 서로 다르다", loc="left", fontsize=11)
save(fig, "f11_camera_rank_transfer")
summary["camera_rank_rho_median"] = float(np.median(off_diag)); summary["camera_elig"] = elig
summary["camera_best_bank"] = {c: banks[int(R[i].argmax())] for i, c in enumerate(elig)}

# ── F12 규칙 일치율 sourcei vs frames — 전 뱅크 산점 ────────────────────
fe2 = {}
for l in open("/workspace/.cron_logs/frames_rule_env.tsv"):
    p = l.strip().split("|"); fe2[p[0]] = (float(p[5]) / 100, float(p[6]) / 100, float(p[7]) / 100)
fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.6))
labels = ["top-K ↔ IoU", "top-K ↔ argmax", "IoU ↔ argmax"]; keys = ["tw", "ta", "wa"]
for k, ax in enumerate(axes):
    fx = np.array([fe2[b][k] for b in banks]); hy = np.array([m["banks"][b]["agree"][keys[k]] for b in banks])
    ax.plot([.7, 1], [.7, 1], color="#c3c2b7", lw=1); ax.scatter(fx, hy, s=40, color="#2a78d6", edgecolor="#fcfcfb")
    for b, xx, yy in zip(banks, fx, hy):
        if abs(xx - yy) > 0.05: ax.annotate(b, (xx, yy), textcoords="offset points", xytext=(4, 3), fontsize=8, color="#52514e")
    ax.set_xlabel("frames 21현장 일치율"); ax.set_ylabel("sourcei 일치율" if k == 0 else ""); ax.set_title(labels[k] + f"  ρ={spearmanr(fx, hy).correlation:+.2f}", loc="left", fontsize=11)
    ax.set_xlim(.7, 1); ax.set_ylim(.7, 1)
fig.suptitle(f"규칙 일치율은 두 데이터셋에서 대체로 같다 ({len(banks)}뱅크, 대각선=동일) — 예외는 대용량 뱅크(v1.0.12.0·v1.0.4.2·v1.0.3.2): sourcei 에서 규칙이 더 갈린다", x=0.01, ha="left", fontsize=12.5)
save(fig, "f12_agreement_transfer")
summary["agree_sourcei_mean"] = [float(np.mean([m["banks"][b]["agree"][k] for b in banks])) for k in keys]
summary["agree_frames_mean"] = [float(np.mean([fe2[b][k] for b in banks])) for k in range(3)]

json.dump(summary, open(f"{OUT}/summary.json", "w"), ensure_ascii=False, indent=1)
print(json.dumps({k: v for k, v in summary.items() if k != "banks"}, ensure_ascii=False, indent=1))
