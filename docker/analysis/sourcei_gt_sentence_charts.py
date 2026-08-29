#!/usr/bin/env python3
"""sourcei_gt_sentences.py 산출물(sentences.json / sentence_ledger.npz) → 프롬프트 작성 가이드 차트."""
import json, re, glob, collections
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
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
CLASSES = ["normal", "falldown", "fire", "smoke"]
S = json.load(open(f"{OUT}/sentences.json")); L = np.load(f"{OUT}/sentence_ledger.npz", allow_pickle=True)
hit, trap, lab, text = L["hit"], L["trap"], L["lab"], L["text"]
summary = {}


def save(fig, name):
    fig.tight_layout(); fig.savefig(f"{FIG}/{name}.png", dpi=160); plt.close(fig); print("saved", name)


def short(t, n=88):
    return t if len(t) <= n else t[:n - 1] + "…"


# ── F13 클래스별 상위 문장: hit vs trap (선택도 상위 10 + 오탈취 상위 6) ────
fig, axes = plt.subplots(2, 2, figsize=(18, 14.5))
for ax, c in zip(axes.ravel(), CLASSES):
    good = S["per_class"][c]["good"][:10]; bad = [b for b in S["per_class"][c]["bad"] if b["sel"] < 0.6][:6]
    rows = good + bad; y = np.arange(len(rows))
    ax.barh(y, [r["hit"] for r in rows], color=CC[c], label=f"hit (GT={c} 프레임을 끌어당김)")
    ax.barh(y, [-r["trap"] for r in rows], color="#c3c2b7", label="trap (다른 클래스 프레임을 가로챔)")
    for i, r in enumerate(rows):
        ax.text(0, i, f"  [{r['sel']:.2f}] " + short(r["text"], 80), va="center", ha="left", fontsize=7.6, color="#0b0b0b",
                bbox=dict(facecolor="#fcfcfb", alpha=.75, edgecolor="none", pad=1))
    ax.axhline(len(good) - .5, color="#e34948", lw=1, ls="--")
    ax.set_yticks([]); ax.invert_yaxis(); ax.set_xlabel("← trap 수   |   hit 수 →  ([ ] = 선택도 hit/(hit+trap))")
    ax.set_title(f"{c} — 위: 선택도×규모 상위 10 / 아래(점선 밑): 오탈취 상위 6   (GT {S['per_class'][c]['gt_frames']:,} 프레임, 활성 문장 {S['per_class'][c]['n_active']:,})", loc="left", fontsize=10.5)
    ax.legend(frameon=False, fontsize=8.5, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2)
fig.suptitle(f"sourcei 에서 이기는 문장 vs 가로채는 문장 — 합집합 뱅크 {S['n_sentences']:,}문장, 프레임별 전역 top-10 원장 (합집합 top-K 정확도 {S['union_topk_acc']:.3f})\n"
             "읽는 법: hit 만 있고 trap 이 없는 문장이 이 현장의 '쓸 문장'. normal 의 trap 문장 = 이벤트를 삼키는 문장", x=0.01, ha="left", fontsize=12.5)
save(fig, "f13_sentence_hit_trap")

# ── F14 구문 대조: 클래스별 white/black 구문 선택도 ─────────────────────
fig, axes = plt.subplots(1, 4, figsize=(20, 7.5))
for ax, c in zip(axes, CLASSES):
    P = S["phrases"][c]; base = P["base_sel"]
    def dedup(rows, n):
        out, seen = [], []
        for g, h, t, s in rows:
            if any(g in o or o in g for o in seen): continue
            seen.append(g); out.append((g, h, t, s))
            if len(out) == n: break
        return out
    wh = dedup(P["white"], 8); bl = dedup(P["black"], 8)
    rows = wh + bl; y = np.arange(len(rows))
    ax.barh(y, [r[3] - base for r in rows], color=[CC[c] if r[3] >= base else "#c3c2b7" for r in rows])
    for i, r in enumerate(rows):
        ax.text(0.003 if r[3] >= base else -0.003, i, f"{r[0]}  ({r[1] + r[2]:,})", va="center", ha="left" if r[3] >= base else "right", fontsize=8.5)
    ax.axvline(0, color="#52514e", lw=1); ax.set_yticks([]); ax.invert_yaxis()
    ax.set_xlabel(f"구문 선택도 − 클래스 기준선({base:.2f})"); ax.set_xlim(-max(base, 0.05) - .05, 1 - base + .08)
    ax.set_title(f"{c}  — 위 8: 넣을 구문 / 아래 8: 피할 구문", loc="left", fontsize=10.5)
fig.suptitle("구문(1~3-gram) 대조 — 문장의 hit/trap 을 구문에 귀속(등장 ≥200), 괄호=등장 프레임수. 자세 어휘(lying·slumped·crouching)가 normal 문장에 들어가면 이벤트를 삼키고,\n"
             "falldown 문장은 장면 선행 템플릿('It is a staircase. …')이면 정상 계단 프레임에 오탈취된다. fire/smoke 는 '작은 밝은 점·바닥·흰 연기 확산' 이 이긴다", x=0.01, ha="left", fontsize=12)
save(fig, "f14_phrase_contrast")

# ── F15 누락 프레임을 가로챈 normal 문장 (블랙리스트) + 템플릿 유형별 선택도 ──
fig, axes = plt.subplots(1, 2, figsize=(18, 8), gridspec_kw={"width_ratios": [1.5, 1]})
ax = axes[0]
bt = [b for b in S["beaters"] if b["class"] == "normal"][:16]; y = np.arange(len(bt))
ax.barh(y, [b["n_missed_frames_won"] for b in bt], color="#8a887f")
for i, b in enumerate(bt):
    ax.text(2, i, short(b["text"], 95), va="center", fontsize=8.3, color="#0b0b0b", bbox=dict(facecolor="#fcfcfb", alpha=.75, edgecolor="none", pad=1))
ax.set_yticks([]); ax.invert_yaxis(); ax.set_xlabel("이벤트 GT 프레임이 normal 로 누락될 때 1등이었던 횟수")
ax.set_title("누락을 만든 normal 문장 상위 16 — 에스컬레이터·청소·굽힘/쪼그림 자세 문장 (sourcei 블랙리스트 후보)", loc="left", fontsize=10.5)
# 템플릿 유형: 장면 선행("It is a X.") / 자세 선행(lying|fallen|collapsed 로 시작하는 주어부) / 카메라 서술("cctv|camera|feed") / 기타
def kind(t):
    """문장 템플릿 유형. 순서가 중요 — 앞 규칙이 먼저 잡는다.
    '현상 선행' 은 fire/smoke 문장의 표준형("A tiny fire appears…", "Smoke is rising…")인데,
    이 버킷이 없던 첫 판에서는 fire 문장 27개가 전부 '기타' 로 떨어져 표가 비어 보였다."""
    tl = t.lower()
    if re.match(r"^it is an? ", tl): return "장면 선행 (It is a …)"
    if re.search(r"cctv|camera|feed|footage|surveillance", tl): return "카메라 서술 (cctv feed …)"
    if re.search(r"^(a|an|one|the)?\s*(single |lone |far |distant )?(person|man|woman|worker|figure|individual|someone|adult|people)", tl): return "인물 선행 (A person …)"
    if re.search(r"^(a |an |the |there is |visible |noticeable |white |thick |dense |drifting )*(tiny |small |very small |minor |large |big |bright )*(fire|flame|flames|smoke|fumes|haze|blaze|spark)", tl): return "현상 선행 (A fire/Smoke …)"
    return "기타"
ax = axes[1]
kinds = ["장면 선행 (It is a …)", "인물 선행 (A person …)", "현상 선행 (A fire/Smoke …)", "카메라 서술 (cctv feed …)", "기타"]
M = np.full((4, len(kinds)), np.nan); N = np.zeros((4, len(kinds)), int)
for ci in range(4):
    for kj, k in enumerate(kinds):
        idx = [j for j in np.where((lab == ci) & (hit + trap >= 10))[0] if kind(str(text[j])) == k]
        if idx: M[ci, kj] = hit[idx].sum() / (hit[idx].sum() + trap[idx].sum()); N[ci, kj] = len(idx)
im = ax.imshow(M, cmap=matplotlib.colors.LinearSegmentedColormap.from_list("s", ["#eef3fb", "#2a78d6", "#0b2e5c"]), vmin=0.4, vmax=1, aspect="auto")
for i in range(4):
    for j in range(len(kinds)):
        if not np.isnan(M[i, j]): ax.text(j, i, f"{M[i, j]:.2f}\n(n={N[i, j]})", ha="center", va="center", fontsize=8.5, color="white" if M[i, j] > .76 else "#0b0b0b")
        else: ax.text(j, i, "해당 문장 없음", ha="center", va="center", fontsize=7.5, color="#8a887f")
ax.set_xticks(range(len(kinds))); ax.set_xticklabels(kinds, fontsize=8.5, rotation=15); ax.set_yticks(range(4)); ax.set_yticklabels(CLASSES); ax.grid(False)
ax.set_title("문장 템플릿 유형별 선택도 (활성 문장 ≥10회 등장)", loc="left", fontsize=10.5)
summary["template_sel"] = {CLASSES[i]: {kinds[j]: (None if np.isnan(M[i, j]) else float(M[i, j])) for j in range(len(kinds))} for i in range(4)}
summary["template_n"] = {CLASSES[i]: {kinds[j]: int(N[i, j]) for j in range(len(kinds))} for i in range(4)}
fig.suptitle("sourcei 형 환경(실내 리테일: 에스컬레이터·하역장·쓰레기처리장)의 프롬프트 작성 규칙 근거", x=0.01, ha="left", fontsize=12.5)
save(fig, "f15_blacklist_templates")

# 요약 숫자
summary["normal_trap_top"] = [(b["text"], b["n_missed_frames_won"]) for b in bt[:8]]
summary["class_sel_base"] = {c: S["phrases"][c]["base_sel"] for c in CLASSES}
conc = {}
for ci, c in enumerate(CLASSES):   # 상위 문장 몇 개가 hit 의 절반을 내는가 (유효 문장 수)
    h = np.sort(hit[lab == ci])[::-1]; cs = np.cumsum(h) / max(h.sum(), 1)
    conc[c] = {"n_active": int((h > 0).sum()), "n_for_50pct_hits": int((cs < 0.5).sum() + 1), "n_for_90pct_hits": int((cs < 0.9).sum() + 1)}
summary["hit_concentration"] = conc
json.dump(summary, open(f"{OUT}/sentence_summary.json", "w"), ensure_ascii=False, indent=1)
print(json.dumps(summary, ensure_ascii=False, indent=1))
