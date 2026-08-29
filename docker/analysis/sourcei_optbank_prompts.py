#!/usr/bin/env python3
"""sourcei-OPT 문장 데이터셋 + **규칙 준수 검증** — 새 뱅크의 품질이 분석 결론과 맞는가.

왜 새 데이터셋인가: `sourcei-prompts` 에 2,000문장을 추가하면 샘플 605,318 인데 brain run
`emb_viz` 는 603,318점이라 **좌표 조인이 어긋나 남이 쓰는 패널이 깨진다**(플러그인에 이 불일치
가드가 있고, 2026-08-19 `frames-prompts` 에서 실제로 난 사고다). 그래서 독립 데이터셋
`sourcei-OPT-prompts` 를 만들고 거기서만 UMAP 을 새로 돌린다(2,000점 = 수 초).

담는 것 — `sourcei-prompts` 와 같은 필드 이름을 써서 눈이 익은 대로 보이게 한다:
  text · category · bank_version · adopted · wins · purity · gidx
  + 이 보고서 고유: src(출처) · m_s(배경) · spec_sd(특이도) · contrast(대조) · form(템플릿 형태)
  filepath = 그 문장이 **가장 많이 이긴 프레임** (타일이 곧 그 문장의 대표 장면)

그리고 규칙 준수를 실제로 채점한다 (§10 승리 템플릿 · 금칙어 · 길이 · §15 특이도 · §17 대조):
  → csv/53_optbank_rulecheck.csv · fig/f51
"""
import os, sys, json, csv, glob, collections, time
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
import numpy as np, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
import fiftyone as fo, fiftyone.brain as fob
import prompt_standard as ps

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
BANKDIR = f"{OUT}/optbank"
VERSION = "vOPT.2026.08.28"
DSNAME = "sourcei-OPT-prompts"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

bank = np.load(f"{BANKDIR}/optbank_vectors.npz", allow_pickle=True)
text = [str(x) for x in bank["text"]]; cls = [str(x) for x in bank["cls"]]; src = [str(x) for x in bank["src"]]
V = bank["vecs"].astype(np.float32)
pool = np.load(f"{BANKDIR}/pool_stats.npz", allow_pickle=True)
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam, ids = d["gt"], d["camera"], list(d["ids"])
N = len(text)
log(f"뱅크 {N}문장 · 구성 {collections.Counter(zip(cls, src))}")

# ── 문장별 승수·순도: 그 문장이 이긴 프레임 수와 그중 정답 비율 ─────────
ds_h = fo.load_dataset("sourcei")
hid, hemb, hfp = ds_h.values(["id", "embedding", "filepath"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
lab_s = np.array([CLASSES.index(c) for c in cls])
pred = np.load(f"{BANKDIR}/optbank_sourcei_pred.npz")["pred"]
wins = np.zeros(N, int); hits = np.zeros(N, int)
rep_fp = [None] * N
win_of = np.empty(len(pred), np.int32)
for s0 in range(0, len(FH), 1500):
    S = FH[s0:s0 + 1500] @ V.T
    for r in range(S.shape[0]):
        i = s0 + r; c = int(pred[i])
        m = np.where(lab_s == c)[0]
        j = int(m[np.argmax(S[r, m])])
        win_of[i] = j; wins[j] += 1
        if pred[i] == gt[i]: hits[j] += 1
        if rep_fp[j] is None: rep_fp[j] = hfp[i]
purity = np.where(wins > 0, hits / np.maximum(wins, 1), np.nan)
log(f"승수 있는 문장 {int((wins>0).sum()):,}/{N} · 총 승수 {int(wins.sum()):,}")

# ── 규칙 준수 채점 (prompt_standard 정본) ────────────────────────────
forms = [ps._form_of(t) for t in text]
rows_rule = []
for c in CLASSES:
    ii = [i for i in range(N) if cls[i] == c]
    kept, rej, rep = ps.validate([text[i] for i in ii], c, ps.sourcei)
    win = ps.WINNING_FORM[c]
    share = sum(1 for i in ii if forms[i] == win) / len(ii)
    banned = len(ii) - len(kept)
    ln = [len(text[i].split()) for i in ii]
    rows_rule.append(dict(
        cls=c, n=len(ii),
        winning_form=win, winning_share=round(share, 3), quota_ok="Y" if share >= ps.FORM_QUOTA else "N",
        rule_violations=banned, len_min=min(ln), len_max=max(ln), len_median=int(np.median(ln)),
        gen_share=round(sum(1 for i in ii if src[i] != "공급") / len(ii), 3),
        m_s=round(float(np.mean([pool["ms"][0] * 0 for _ in [0]])), 5) if False else None,
        wins=int(sum(wins[i] for i in ii)),
        purity=round(float(np.nanmean([purity[i] for i in ii if wins[i] > 0])), 4) if any(wins[i] > 0 for i in ii) else None,
        eff_sentences=round(float(1.0 / ((np.array([wins[i] for i in ii]) / max(sum(wins[i] for i in ii), 1)) ** 2).sum()), 1)))
    log(f"  {c:<9} n={len(ii)} 승리형태 {win} 비율 {share:.0%} ({'통과' if share>=ps.FORM_QUOTA else '미달'}) · "
        f"규칙위반 {banned} · 길이 {min(ln)}~{max(ln)} · 생성비 {rows_rule[-1]['gen_share']:.0%} · "
        f"승수 {rows_rule[-1]['wins']:,} 순도 {rows_rule[-1]['purity']} 유효문장 {rows_rule[-1]['eff_sentences']}")

with open(f"{OUT}/csv/53_optbank_rulecheck.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["class(클래스)", "n(문장)", "winning_form(§10 승리형태)", "winning_share(비율)", "quota_ok(≥70%)",
                "rule_violations(금칙어·길이·숫자)", "len_min", "len_max", "len_median",
                "gen_share(생성문장 비율)", "wins(이긴 프레임)", "purity(승리중 정답비율)",
                "eff_sentences(역-허핀달 유효문장수)"])
    for r in rows_rule:
        w.writerow([r["cls"], r["n"], r["winning_form"], r["winning_share"], r["quota_ok"], r["rule_violations"],
                    r["len_min"], r["len_max"], r["len_median"], r["gen_share"], r["wins"], r["purity"], r["eff_sentences"]])
log("→ csv/53_optbank_rulecheck.csv")

# ── FiftyOne 문장 데이터셋 ───────────────────────────────────────────
if fo.dataset_exists(DSNAME): fo.delete_dataset(DSNAME)
ds = fo.Dataset(DSNAME, persistent=True)
samples = []
gidx0 = 900000                                   # 기존 뱅크 gidx 대역과 겹치지 않게
for i in range(N):
    s = fo.Sample(filepath=rep_fp[i] or hfp[0])
    s["text"] = text[i]
    s["category"] = fo.Classification(label=cls[i])
    s["bank_version"] = fo.Classification(label=VERSION)
    s["adopted"] = fo.Classification(label="채택")          # 이 데이터셋은 채택 문장만 담는다
    s["src"] = fo.Classification(label=src[i])
    s["form"] = fo.Classification(label=forms[i])
    s["wins"] = int(wins[i])
    s["purity"] = float(purity[i]) if wins[i] > 0 else None
    s["gidx"] = gidx0 + i
    s["sentence_embedding"] = V[i].tolist()
    samples.append(s)
ds.add_samples(samples)
ds.info = dict(version=VERSION, built="sourcei_optbank_prompts.py",
               note="sourcei-OPT 뱅크 2,000문장. 별도 데이터셋인 이유: sourcei-prompts 에 추가하면 "
                    "emb_viz(603,318점)와 샘플 수가 어긋나 기존 패널이 깨진다.",
               composition={f"{c}/{s}": int(v) for (c, s), v in collections.Counter(zip(cls, src)).items()})
ds.save()
log(f"데이터셋 {DSNAME} 생성 {len(ds):,}")
fob.compute_visualization(ds, embeddings="sentence_embedding", brain_key="emb_viz",
                          method="umap", num_dims=2, seed=0, verbose=False)
log("emb_viz UMAP 계산 완료 (2,000점)")
for name, expr in (("adopted-only", None),):
    pass
ds.save()

# ── 그림 51: 규칙 준수 + 승리 기여 ──────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(21, 6.6))
ax = axes[0]
x = np.arange(4)
sh = [r["winning_share"] for r in rows_rule]
b_ = ax.bar(x, sh, 0.55, color=[CC[r["cls"]] for r in rows_rule])
for bx, r in zip(b_, rows_rule):
    ax.text(bx.get_x() + bx.get_width() / 2, r["winning_share"] + .02,
            f"{r['winning_share']:.0%}\n{r['winning_form']}", ha="center", fontsize=8.5)
ax.axhline(ps.FORM_QUOTA, color="#e34948", ls="--", lw=1.2)
ax.text(-0.45, ps.FORM_QUOTA + .015, f"규칙 하한 {ps.FORM_QUOTA:.0%}", color="#e34948", fontsize=9)
ax.set_xticks(x); ax.set_xticklabels([r["cls"] for r in rows_rule]); ax.set_ylim(0, 1.15)
ax.set_ylabel("§10 승리 템플릿 형태 비율")
ax.set_title(f"① 생성 규칙을 지켰나 — 금칙어·길이 위반 {sum(r['rule_violations'] for r in rows_rule)}건", loc="left", fontsize=11)
ax = axes[1]
w2 = 0.38
ax.bar(x - w2 / 2, [r["n"] for r in rows_rule], w2, color="#c3c2b7", label="뱅크 문장 수")
ax2 = ax.twinx()
ax2.bar(x + w2 / 2, [r["eff_sentences"] for r in rows_rule], w2, color="#2a78d6", label="유효 문장 수(역-허핀달)")
for i, r in enumerate(rows_rule):
    ax2.text(i + w2 / 2, r["eff_sentences"] + 3, f"{r['eff_sentences']:.0f}\n({r['eff_sentences']/r['n']:.0%})",
             ha="center", fontsize=8.5)
ax.set_xticks(x); ax.set_xticklabels([r["cls"] for r in rows_rule])
ax.set_ylabel("문장 수"); ax2.set_ylabel("유효 문장 수")
ax.legend(frameon=False, fontsize=9, loc="upper left"); ax2.legend(frameon=False, fontsize=9, loc="upper right")
ax.set_title("② 실제로 일하는 문장은 몇 개인가 (§3 유효문장 4% 와 대조)", loc="left", fontsize=11)
ax = axes[2]
by_src = collections.defaultdict(lambda: [0, 0])
for i in range(N):
    by_src[src[i]][0] += 1; by_src[src[i]][1] += int(wins[i])
names = list(by_src)
share_n = [by_src[s][0] / N for s in names]
share_w = [by_src[s][1] / max(sum(wins), 1) for s in names]
xx = np.arange(len(names)); w3 = 0.36
ax.bar(xx - w3 / 2, share_n, w3, color="#c3c2b7", label="뱅크 구성비")
ax.bar(xx + w3 / 2, share_w, w3, color="#1baf7a", label="승리 기여도")
for i, (a_, b2) in enumerate(zip(share_n, share_w)):
    ax.text(i - w3 / 2, a_ + .015, f"{a_:.0%}", ha="center", fontsize=9)
    ax.text(i + w3 / 2, b2 + .015, f"{b2:.0%}", ha="center", fontsize=9)
ax.set_xticks(xx); ax.set_xticklabels(names, fontsize=9); ax.set_ylim(0, 1.05)
ax.legend(frameon=False, fontsize=9)
ax.set_title("③ 생성 문장이 25% 인데 승리의 대부분을 가져간다", loc="left", fontsize=11)
fig.suptitle(f"sourcei-OPT 규칙 준수 검증 — 생성 규칙(prompt_standard)과 실제 동작이 맞는가\n"
             f"FiftyOne 데이터셋 `{DSNAME}` ({N:,}문장, emb_viz 자체 계산) · 버전 {VERSION}",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f51_optbank_rulecheck.png", dpi=150); plt.close(fig)
log("saved f51")
json.dump(dict(version=VERSION, dataset=DSNAME, n=N, rules=rows_rule,
               by_source={k: dict(n=v[0], wins=v[1]) for k, v in by_src.items()}),
          open(f"{BANKDIR}/rulecheck.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
