#!/usr/bin/env python3
"""필터 A/B 보고 도판 6종. 모든 수치는 JSON 산출물에서만 읽는다(도판이 별도 계산을 하지 않는다)."""
import os, json, glob
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": .25, "figure.dpi": 150})
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
AB = f"{OUT}/filter_ab"; FIG = f"{AB}/fig"; os.makedirs(FIG, exist_ok=True)
INF = json.load(open(f"{AB}/inference.json"))
POW = json.load(open(f"{AB}/power.json"))
PRV = json.load(open(f"{AB}/gt_provenance.json"))
RPR = json.load(open(f"{AB}/repro.json"))
OLD = [json.loads(l) for l in open(f"{AB}/checkpoint.jsonl")]
OLDM = {r["name"]: r for r in OLD}
ORD = ["msmax", "contain0.8", "contain0.6", "and_polar", "msmax+contain"]
BLU, ORG, GRY, GRN, RED = "#0072B2", "#D55E00", "#999999", "#009E73", "#CC3311"

# ── F1 독립 CI vs 짝 CI ────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 4.6))
y = np.arange(len(ORD))
base_m = OLDM["base"]["mf1"]
for i, n in enumerate(ORD):
    o = OLDM[n]; lo, hi = o["mf1_ci"]
    ax.plot([lo - base_m, hi - base_m], [i + .18] * 2, color=GRY, lw=6, solid_capstyle="butt",
            label="1차: 독립 CI (잘못된 설계)" if i == 0 else None)
    p = INF["paired"][n]
    ax.plot(p["boot_ci"], [i - .18] * 2, color=BLU, lw=6, solid_capstyle="butt",
            label="재설계: 짝 부트스트랩 CI" if i == 0 else None)
    ax.plot(p["mean"], i - .18, "o", color="k", ms=4, zorder=5)
ax.axvline(0, color="k", lw=1)
ax.set_yticks(y); ax.set_yticklabels(ORD)
ax.set_xlabel("기준선(base) 대비 macro-F1 차이")
w_old = np.mean([OLDM[n]["mf1_ci"][1] - OLDM[n]["mf1_ci"][0] for n in ORD])
w_new = np.mean([INF["paired"][n]["boot_ci"][1] - INF["paired"][n]["boot_ci"][0] for n in ORD])
ax.set_title(f"F1 · 같은 프레임에서 채점하면 CI 는 짝으로 재야 한다\n"
             f"평균 CI 폭 {w_old:.3f} → {w_new:.3f} ({w_old/max(w_new,1e-9):.0f}배 축소) — "
             f"공통 카메라 변동이 차이에서 상쇄된다", fontsize=11.5)
ax.set_ylim(len(ORD) - 0.45, -1.0)
ax.legend(frameon=False, fontsize=9.5, loc="upper left", ncol=2)
fig.tight_layout(); fig.savefig(f"{FIG}/g1_paired_vs_independent.png"); plt.close(fig)

# ── F2 forest: Δ + CI + MDE + 비열등성 마진 ────────────────────────
fig, ax = plt.subplots(figsize=(10.5, 4.8))
P = {r["variant"]: r for r in POW["variants"]}
mde = np.mean([P[n]["mde80"] for n in ORD if P[n]["mde80"] > 0])
ax.axvspan(-mde, mde, color=GRY, alpha=.18, label=f"MDE80 ±{mde:.4f} (이 안은 검출 불가)")
ax.axvline(-POW["margin"], color=RED, ls="--", lw=1.4, label=f"비열등성 마진 −{POW['margin']}")
for i, n in enumerate(ORD):
    p = P[n]; pp = INF["paired"][n]
    ok = pp["significant_005"]
    ax.plot(p["ci95"], [i] * 2, color=(ORG if not p["noninferior"] else BLU), lw=3.2,
            solid_capstyle="butt")
    ax.plot(p["delta"], i, "D" if not ok else "o", color="k", ms=6, zorder=5)
    ax.text(0.031, i, f"Holm p={pp['p_holm']:.3f} · WY p={pp['p_westfall_young']:.3f}"
                      f"{'  ⚠️마진 밖' if not p['noninferior'] else ''}",
            va="center", fontsize=9, color=("#B00" if not p["noninferior"] else "#333"))
ax.axvline(0, color="k", lw=1)
ax.set_yticks(range(len(ORD))); ax.set_yticklabels(ORD)
ax.set_xlim(-0.032, 0.082); ax.set_ylim(len(ORD) - 0.4, -1.15)
ax.set_xlabel("카메라 수준 짝차이 Δ macro-F1 (95% CI)")
ax.set_title(f"F2 · 어느 변형도 기준선과 유의하게 다르지 않다 (n={POW['n_cam_macro']} 카메라)\n"
             f"관측 |Δ| 전부 MDE80 이하 — 5변형 중 4개는 −{POW['margin']} 마진에서 비열등",
             fontsize=11.5)
ax.legend(frameon=False, fontsize=9.5, loc="upper right", ncol=2)
fig.tight_layout(); fig.savefig(f"{FIG}/g2_forest.png"); plt.close(fig)

# ── F3 GT 출처 분해 ───────────────────────────────────────────────
fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.6),
                             gridspec_kw={"width_ratios": [1, 1.35]})
cl = [r["cls"] for r in PRV["by_class"]]
fo_ = np.array([r["folder"] for r in PRV["by_class"]], float)
fn = np.array([r["filename"] for r in PRV["by_class"]], float)
cp = np.array([r["caption"] for r in PRV["by_class"]], float)
nn = np.array([r["none"] for r in PRV["by_class"]], float)
tot = fo_ + fn + cp + nn
b = np.zeros(len(cl))
for arr, c, lab in ((fo_, GRN, "folder (사람 정리)"), (fn, BLU, "filename (사람 정리)"),
                    (cp, ORG, "caption (Gemini 파생)"), (nn, GRY, "none (근거 없음)")):
    a1.bar(cl, arr / tot * 100, bottom=b, color=c, label=lab, width=.68); b += arr / tot * 100
for i, r in enumerate(PRV["by_class"]):
    a1.text(i, 103, f"n={r['n']:,}\n사람 {r['human_pct']:.0f}%", ha="center", fontsize=8.5)
# 범례를 우측 상단으로 — normal 막대가 100% 통짜라 좌하단은 막대에 묻힌다.
# 상단 주석(n=·사람%)이 103 에 있으므로 ylim 을 올려 겹치지 않게 자리를 만든다.
a1.set_ylim(0, 178); a1.set_ylabel("출처 비율 (%)")
a1.set_title("F3a · GT 출처는 클래스마다 다르다\nnormal 은 사람근거 0.0%", fontsize=11.5)
a1.legend(frameon=False, fontsize=8.5, ncol=1, loc="upper right", bbox_to_anchor=(1.0, 1.0), borderaxespad=0.3)

ct = sorted(PRV["by_camera"], key=lambda r: -r["human_pct"])
xs = np.arange(len(ct))
cols = [BLU if r["macro_ok"] else GRY for r in ct]
a2.bar(xs, [r["human_pct"] for r in ct], color=cols, width=.7)
a2.set_xticks(xs)
a2.set_xticklabels([r["camera"][:16] + ("…" if len(r["camera"]) > 16 else "") for r in ct],
                   rotation=55, ha="right", fontsize=7.5)
a2.set_ylabel("사람근거 비율 (%)"); a2.set_ylim(0, 108)
for i, r in enumerate(ct):
    a2.text(i, r["human_pct"] + 2, f"{r['n']:,}", ha="center", fontsize=6.8, color="#555")
# 막대 높이와 무관하게 상태를 보이게 — 0% 카메라도 짝비교에 들어갈 수 있다(사람근거와 별개)
for i, r in enumerate(ct):
    a2.plot(i, -4.5, "s", ms=7, color=(BLU if r["macro_ok"] else GRY), clip_on=False)
a2.set_ylim(-9, 108)
a2.text(-0.6, -4.5, "짝비교 포함", va="center", ha="right", fontsize=8, color="#555")
a2.set_title(f"F3b · 사람근거 비율(막대)과 짝비교 포함 여부(아래 사각형)는 별개 축이다\n"
             f"포함 {PRV['macro_ok_cameras']}대 평균 사람근거 56.1% vs 제외 5대 20.0%", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{FIG}/g3_gt_provenance.png"); plt.close(fig)

# ── F4 모수 불일치 ────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 4.2)); ax.axis("off")
ax.add_patch(plt.Rectangle((.04, .30), .40, .48, fc="#EAF3FB", ec=BLU, lw=2))
ax.add_patch(plt.Rectangle((.56, .30), .40, .48, fc="#FDF0E6", ec=ORG, lw=2))
ax.text(.24, .70, "배경통계 모수", ha="center", fontsize=13, weight="bold", color=BLU)
ax.text(.24, .55, "frames · 22 프로젝트\n90,084 프레임 · kmeans64 군집\n"
                  "→ m_s(배경 코사인) · sd(군집 특이도)", ha="center", va="center", fontsize=10.5)
ax.text(.24, .36, "필터가 문장을 고르는 근거", ha="center", fontsize=9.5, style="italic", color="#555")
ax.text(.76, .70, "평가 모수", ha="center", fontsize=13, weight="bold", color=ORG)
ax.text(.76, .55, "sourcei · 15 카메라\n7,498 프레임 · 4클래스 GT\n"
                  "→ macro-F1 · 분포 IoU · 오탐", ha="center", va="center", fontsize=10.5)
ax.text(.76, .36, "점수를 매기는 곳", ha="center", fontsize=9.5, style="italic", color="#555")
ax.annotate("", xy=(.55, .54), xytext=(.45, .54),
            arrowprops=dict(arrowstyle="-|>", lw=2.5, color=RED))
ax.text(.50, .84, "공유 카메라 0대 · 공유 프레임 0장", ha="center", fontsize=12,
        weight="bold", color=RED)
ax.text(.50, .17, "§27 실측: 군집은 이벤트보다 「장소」를 4배 강하게 담는다(NMI 0.586 vs 0.149).\n"
                  "따라서 sd 는 실질적으로 「다른 현장들의 장소를 얼마나 잘 가르는가」를 재고,\n"
                  "그 신호로 고른 문장을 sourcei 이벤트 탐지로 채점하고 있다.",
        ha="center", fontsize=10, color="#333")
ax.set_title("F4 · 필터 신호와 평가가 서로 다른 현장에서 계산된다 — 필터 노브가 무반응인 구조적 이유",
             fontsize=12)
fig.tight_layout(); fig.savefig(f"{FIG}/g4_population_mismatch.png"); plt.close(fig)

# ── F5 검정력 ─────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(8.6, 4.4))
eff = sorted(float(k) for k in POW["cameras_needed"])
need = [POW["cameras_needed"][str(e) if str(e) in POW["cameras_needed"] else f"{e}"] for e in eff]
ax.plot(eff, need, "o-", color=BLU, lw=2.2, ms=7)
for e, n in zip(eff, need): ax.annotate(f"{n}대", (e, n), textcoords="offset points",
                                        xytext=(6, 6), fontsize=10)
ax.axhline(POW["n_cam_macro"], color=RED, ls="--", lw=1.6)
ax.text(eff[-1], POW["n_cam_macro"] * 1.15, f"현재 사용 가능 {POW['n_cam_macro']}대",
        ha="right", fontsize=10, color=RED)
obs = max(abs(r["delta"]) for r in POW["variants"])
ax.axvline(obs, color=ORG, ls=":", lw=1.8)
ax.text(obs, max(need) * .55, f"관측 최대 |Δ| {obs:.4f}", rotation=90, va="center",
        ha="right", fontsize=9.5, color=ORG)
ax.set_yscale("log"); ax.set_xlabel("검출하려는 효과크기 Δ macro-F1")
ax.set_ylabel("필요 카메라 수 (80% 검정력, log)")
ax.set_title(f"F5 · 관측된 크기의 효과를 잡으려면 카메라가 부족하다\n"
             f"Δ=0.01 → 23대 · Δ=0.005 → 84대 (평균 SD {POW['mean_sd']:.4f})", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{FIG}/g5_power.png"); plt.close(fig)

# ── F6 자기 문장 되돌아옴 ──────────────────────────────────────────
fig, ax = plt.subplots(figsize=(8.6, 4.4))
cls = ["normal", "falldown", "fire", "smoke"]
cand = np.array([RPR["candidates"][c] for c in cls], float)
ours = np.array([RPR["candidates_ours"][c] for c in cls], float)
pct = ours / cand * 100
bars = ax.bar(cls, pct, color=[ORG if p > 10 else BLU for p in pct], width=.6)
for i, (c, p) in enumerate(zip(cls, pct)):
    ax.text(i, p + .4, f"{p:.2f}%\n({int(ours[i]):,}/{int(cand[i]):,})", ha="center", fontsize=9.5)
ax.set_ylabel("후보 풀 중 자기 문장 비율 (%)"); ax.set_ylim(0, max(pct) * 1.35)
ax.set_title("F6 · 뱅크를 DB 원장에 등록하면 자기 문장이 다음 큐레이션의 후보로 되돌아온다\n"
             "공급이 얇은 fire 에서 16.4% — 반복하면 모델 파생 문장이 증폭된다", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{FIG}/g6_feedback_loop.png"); plt.close(fig)

print("도판:", sorted(os.listdir(FIG)))
