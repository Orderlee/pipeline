#!/usr/bin/env python3
"""f38 / f42 / f43 판독성·정확성 수정 — 이미 만든 CSV 만 읽어 다시 그린다(재계산 없음).

f38 ①: twiny 로 막대(0~100%)와 Δ(−0.3~0.1)를 한 축에 겹쳐 0 선이 막대 위를 지났다 → 좌우 분리.
f38 ③: x 라벨이 겹쳤다 → 뱅크/컷 2줄 축약.
f42 ②: x 라벨이 16자에서 잘려 'GEN-filtered (m_' 로 보였다 → 짧은 별칭.
f43 ⑤: 참고선을 **전체** 분위로 그렸는데 실제 컷은 **클래스 내** 분위다 → 클래스별 선으로 교체.
"""
import csv, json, glob
import numpy as np, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
R = lambda p: list(csv.DictReader(open(f"{OUT}/csv/{p}", encoding="utf-8-sig")))

# ── f38 ─────────────────────────────────────────────────────────────
e1 = R("33_pruning.csv"); F = lambda x, k: float(x[k])
FOCUS = "v1.0.8.0"
CUTS = ["중복컷", "3컷 동시 (주효과25+특이도25+중복)", "3컷 동시 (주효과50+특이도50+중복)",
        "특이도 하위 10% 컷", "특이도 하위 25% 컷", "특이도 하위 50% 컷", "주효과 상위 25% 컷"]
fig = plt.figure(figsize=(24, 7.8))
gs = fig.add_gridspec(1, 4, width_ratios=[0.95, 0.85, 1.15, 0.85], wspace=0.28)
r0 = [x for x in e1 if x["bank(뱅크)"] == FOCUS]
y = np.arange(len(r0))
ax = fig.add_subplot(gs[0, 0])
ax.barh(y, [F(x, "kept_share(유지비율)") * 100 for x in r0], color="#c3c2b7")
for i, x in enumerate(r0): ax.text(F(x, "kept_share(유지비율)") * 100 + 1.5, i, f"{int(x['n_kept(유지문장)']):,}", va="center", fontsize=8)
ax.set_yticks(y); ax.set_yticklabels([x["variant(프루닝안)"] for x in r0], fontsize=9); ax.invert_yaxis()
ax.set_xlim(0, 118); ax.set_xlabel("유지 문장 비율 %")
ax.set_title(f"① {FOCUS} — 라벨 없이 얼마를 지우나", loc="left", fontsize=11)
ax2 = fig.add_subplot(gs[0, 1], sharey=ax)
for i, x in enumerate(r0):
    lo, hi = F(x, "ci_lo(2.5%)"), F(x, "ci_hi(97.5%)")
    c = "#1baf7a" if lo > -0.02 else "#e34948"
    ax2.plot([lo, hi], [i, i], color=c, lw=2, alpha=.75)
    ax2.plot([F(x, "d_mf1(기준선대비Δ)")], [i], "o", ms=7, color=c)
ax2.axvline(0, color="#0b0b0b", lw=1); ax2.axvline(-0.02, color="#e34948", ls="--", lw=1)
ax2.text(-0.02, len(r0) - 0.2, "비열등 한계", color="#e34948", fontsize=8, ha="center", va="top")
ax2.tick_params(labelleft=False); ax2.set_xlabel("Δ macro-F1 vs 전량 (점=평균, 선=95% CI)")
ax2.set_title("② 성능은 어떻게 되나 (초록=비열등)", loc="left", fontsize=11)
ax = fig.add_subplot(gs[0, 2])
MK = ["o", "s", "^", "D", "v", "P", "X"]
for k, nm in enumerate(CUTS):
    rr = [x for x in e1 if x["variant(프루닝안)"] == nm]
    if not rr: continue
    ni = sum(1 for x in rr if x["noninferior(CI하한>-0.02)"] == "Y")
    ax.scatter([F(x, "kept_share(유지비율)") * 100 for x in rr], [F(x, "d_mf1(기준선대비Δ)") for x in rr],
               s=44, marker=MK[k], alpha=.8, edgecolor="white", lw=.6, label=f"{nm} — 비열등 {ni}/{len(rr)}")
ax.axhline(0, color="#0b0b0b", lw=1); ax.axhline(-0.02, color="#e34948", ls="--", lw=1)
ax.set_xlabel("유지 문장 비율 %"); ax.set_ylabel("Δ macro-F1 vs 전량")
ax.legend(frameon=False, fontsize=8.2, loc="lower left")
ax.set_title("③ 전체 31 뱅크 — 컷별 비열등(CI 하한 > −0.02) 뱅크 수", loc="left", fontsize=11)
ax = fig.add_subplot(gs[0, 3])
fr = json.load(open(f"{OUT}/prune_bicluster_direction_summary.json"))["e1_frames"]
short = {"중복컷": "중복", "3컷 동시 (주효과25+특이도25+중복)": "3컷25", "3컷 동시 (주효과50+특이도50+중복)": "3컷50"}
x = np.arange(len(fr)); w2 = 0.27
for k, (key, lab_, col) in enumerate([("fire_recall", "fire 재현율", "#e34948"), ("smoke_recall", "smoke 재현율", "#4a3aa7"), ("fp", "비화재 오탐", "#8a887f")]):
    v = [r[key] for r in fr]; b_ = ax.bar(x + (k - 1) * w2, v, w2 * 0.9, color=col, label=lab_)
    for bx, vv in zip(b_, v): ax.text(bx.get_x() + bx.get_width() / 2, vv + 0.01, f"{vv:.2f}", ha="center", fontsize=7.5)
ax.set_ylim(0, 1.08)
ax.set_xticks(x); ax.set_xticklabels([f"{r['bank']}\n{short.get(r['variant'], r['variant'][:8])}\n{r['n']:,}문장" for r in fr], fontsize=7.8)
ax.legend(frameon=False, fontsize=8.5, loc="upper center", ncol=1)
ax.set_title("④ frames 반응 — 오탐이 늘지 않나", loc="left", fontsize=11)
fig.suptitle("E1 프루닝 3컷 — 목표는 성능 향상이 아니라 **유지비 절감**: 라벨 없이 몇 %를 지워도 성능이 유지되나\n"
             "카메라 군집 부트스트랩 2,000회 · sourcei GT 7,498/15카메라 · frames 표본 24,792 (SAM3 약참조) · 전체 31 뱅크",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f38_pruning.png", dpi=150); plt.close(fig)
print("f38 재작성")

# ── f42 ─────────────────────────────────────────────────────────────
g = R("38_generated_bank.csv"); hold = R("41_generated_holdout.csv")
ALIAS = {"GEN+pairs (대조쌍 이벤트문 추가)": "GEN+pairs\n499문장", "GEN-filtered (m_s25+특이도25+중복)": "GEN-filtered\n199문장",
         "GEN-raw (생성 전량)": "GEN-raw\n340문장", "GEN-small30 (클래스당 30문장)": "GEN-small30\n120문장",
         "GEN-diff (대조쌍 차 벡터)": "GEN-diff\n335벡터", "v1.0.8.1 (저장 뱅크 최고)": "v1.0.8.1\n12,511문장",
         "v1.0.8.0 (저장 뱅크)": "v1.0.8.0\n12,480문장", "v1.0.12.0 (저장 뱅크)": "v1.0.12.0\n49,140문장"}
order = sorted(g, key=lambda x: -float(x["macro_f1"]))
fig, axes = plt.subplots(1, 3, figsize=(23, 7.6), gridspec_kw={"width_ratios": [1.2, 1.05, 0.85]})
ax = axes[0]; y = np.arange(len(order))
ax.barh(y, [float(x["macro_f1"]) for x in order], color=["#1baf7a" if x["bank(뱅크)"].startswith("GEN") else "#8a887f" for x in order], alpha=.92)
for i, x in enumerate(order):
    ci = f"CI[{float(x['ci_lo']):+.3f},{float(x['ci_hi']):+.3f}]" if x["bank(뱅크)"] != "v1.0.8.1 (저장 뱅크 최고)" else "기준"
    ax.text(float(x["macro_f1"]) + .006, i, f"{float(x['macro_f1']):.3f}   {int(x['n_sent(문장수)']):,}문장   {ci}", va="center", fontsize=8.4)
ax.set_yticks(y); ax.set_yticklabels([x["bank(뱅크)"][:34] for x in order], fontsize=8.6); ax.invert_yaxis()
ax.set_xlim(0, max(float(x["macro_f1"]) for x in order) * 1.62)
ax.set_xlabel("sourcei GT 이벤트 macro-F1 (top-K 투표)")
ax.set_title("① 규칙으로 만든 뱅크(초록) vs 공급 뱅크(회색)", loc="left", fontsize=11)
ax = axes[1]; x = np.arange(len(order)); w2 = 0.2
for k, (key, lab_, col) in enumerate([("rec_fall", "falldown 재현", CC["falldown"]), ("rec_fire", "fire 재현", CC["fire"]),
                                      ("rec_smoke", "smoke 재현", CC["smoke"]), ("fp_normal(정상오탐)", "정상 오탐", "#0b0b0b")]):
    v = [float(z[key]) for z in order]
    ax.bar(x + (k - 1.5) * w2, v, w2 * 0.9, color=col, label=lab_)
ax.set_xticks(x); ax.set_xticklabels([ALIAS.get(z["bank(뱅크)"], z["bank(뱅크)"][:12]) for z in order], fontsize=7.4)
ax.legend(frameon=False, fontsize=9, ncol=2); ax.set_ylabel("비율")
ax.set_title("② 재현율과 정상 오탐 — 이긴 뱅크는 **더 잘 짖는다**(오탐 4.3배)", loc="left", fontsize=11)
ax = axes[2]
hb = sorted(hold, key=lambda x: -float(x["holdout_mf1(카메라홀드아웃)"])); yh = np.arange(len(hb))
ax.barh(yh, [float(x["holdout_mf1(카메라홀드아웃)"]) for x in hb], xerr=[float(x["sd(폴드간표준편차)"]) for x in hb],
        color=["#1baf7a" if x["bank(뱅크)"].startswith("GEN") else "#8a887f" for x in hb], alpha=.92,
        error_kw=dict(ecolor="#52514e", lw=1))
for i, x in enumerate(hb): ax.text(float(x["holdout_mf1(카메라홀드아웃)"]) + float(x["sd(폴드간표준편차)"]) + .008, i, f"{float(x['holdout_mf1(카메라홀드아웃)']):.3f}", va="center", fontsize=8.6)
ax.set_yticks(yh); ax.set_yticklabels([x["bank(뱅크)"][:26] for x in hb], fontsize=8); ax.invert_yaxis()
ax.set_xlabel("카메라 홀드아웃 macro-F1 (GroupKFold 5, ±폴드 SD)")
ax.set_title("③ 카메라를 갈라도 유지되나 — 폴드 SD 가 차이보다 크다", loc="left", fontsize=11)
fig.suptitle("D2 CuPL 식 생성 + 라벨-free 필터 · D1 대조쌍/차 벡터 — 499문장으로 12,511문장 공급 뱅크를 이긴다(단, 오탐은 4.3배)\n"
             "⚠️ 생성 규칙은 sourcei GT 로부터 측정된 것 → '새 현장 일반화'가 아니라 '측정된 설계 규칙이 뱅크 부피를 대체하나' 를 잰다",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f42_generated_bank.png", dpi=150); plt.close(fig)
print("f42 재작성")

# ── f43 ─────────────────────────────────────────────────────────────
s = [x for x in R("40_generated_sentences.csv") if x["kind(출처)"] == "gen"]
p = R("39_generated_prauc.csv")
fig, axes = plt.subplots(1, 2, figsize=(17, 6.8))
ax = axes[0]
bl = sorted({x["bank(뱅크)"] for x in p}, key=lambda b: -np.mean([float(x["pr_auc"]) for x in p if x["bank(뱅크)"] == b]))
x_ = np.arange(len(bl)); w2 = 0.26
for k, cls in enumerate(("falldown", "fire", "smoke")):
    v = [next((float(z["pr_auc"]) for z in p if z["bank(뱅크)"] == b and z["class(클래스)"] == cls), np.nan) for b in bl]
    ax.bar(x_ + (k - 1) * w2, v, w2 * 0.9, color=CC[cls], label=cls)
ax.set_xticks(x_); ax.set_xticklabels([ALIAS.get(b, b[:12]) for b in bl], fontsize=7.4)
ax.set_ylabel("PR-AUC (분포-IoU 연속값)"); ax.legend(frameon=False, fontsize=9)
ax.set_title("④ 임계 무관 랭킹은 **반대로** 공급 뱅크가 이긴다 — 추정기 문제인지 f45 에서 가른다", loc="left", fontsize=11)
ax = axes[1]
for cls in CLASSES:
    v = [x for x in s if x["class(클래스)"] == cls]
    ms = np.array([float(x["m_s(배경평균)"]) for x in v]); sd = np.array([float(x["spec_sd(군집특이도)"]) for x in v])
    ax.scatter(ms, sd, s=26, color=CC[cls], alpha=.75, edgecolor="white", lw=.5, label=f"{cls} ({len(v)})")
    ax.plot([np.percentile(ms, 75)] * 2, [sd.min(), sd.max()], color=CC[cls], ls="--", lw=1.1, alpha=.8)
    ax.plot([ms.min(), ms.max()], [np.percentile(sd, 25)] * 2, color=CC[cls], ls=":", lw=1.1, alpha=.8)
ax.set_xlabel("m_s — 90,084 프레임 배경 평균 코사인 (낮을수록 조용한 문장)")
ax.set_ylabel("특이도 SD — kmeans64 군집 간 표준편차 (높을수록 군집을 가른다)")
ax.legend(frameon=False, fontsize=9, loc="lower left")
ax.set_title("⑤ 생성 문장 340개와 컷 위치 — 컷은 **클래스 내** 분위다\n파선 = 그 클래스의 m_s 75분위, 점선 = 특이도 25분위", loc="left", fontsize=10.5)
fig.suptitle("생성 뱅크의 랭킹 품질과, 라벨 없이 문장을 거르는 두 축 — 컷 비율은 사전 고정(클래스 내 25%)\n"
             "⚠️ 생성 규칙은 sourcei GT 로부터 측정된 것 · 카메라 군집 부트스트랩 2,000회", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f43_generated_filter.png", dpi=150); plt.close(fig)
print("f43 재작성")
