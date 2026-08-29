#!/usr/bin/env python3
"""sourcei-OPT 뱅크를 FiftyOne `sourcei` 데이터셋에 적용 + 보고서 그림.

FiftyOne 에 무엇을 띄우는가 — 문장 데이터셋을 새로 만들지 않고 **프레임에 필드를 쓴다**.
이유: 눈으로 확인할 값어치가 있는 것은 "어떤 프레임에서 새 뱅크가 공급 뱅크와 다르게 판단하나"
이고, 그건 프레임 위에서만 보인다. 문장 목록은 CSV(`csv/52_optbank_sentences.csv`)로 충분하다.

쓰는 필드 (전부 `optbank_` 접두 — 기존 필드와 충돌 없음)
  optbank_pred        Classification  새 뱅크 판정
  optbank_correct     Classification  correct / wrong
  optbank_vs_supply   Classification  OPT만 맞음 / 공급만 맞음 / 둘 다 맞음 / 둘 다 틀림  ← 핵심 뷰
  optbank_margin      Float           top1 − top2 클래스 최대 코사인
  optbank_iou_min     Float           이벤트 3클래스 분포-IoU 최솟값 (작을수록 이벤트)
  optbank_top_prompt  String          그 프레임을 이긴 문장
  optbank_src         Classification  이긴 문장의 출처(공급/생성(CuPL)/생성(대조쌍))
"""
import os, sys, json, csv, glob, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
import numpy as np, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
import fiftyone as fo

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
BANKDIR = f"{OUT}/optbank"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
EVENTS = ["falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
import time
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

meta = json.load(open(f"{BANKDIR}/optbank.json"))
srch = json.load(open(f"{BANKDIR}/search.json"))
bank = np.load(f"{BANKDIR}/optbank_vectors.npz", allow_pickle=True)
pred_npz = np.load(f"{BANKDIR}/optbank_sourcei_pred.npz", allow_pickle=True)
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam, ids = d["gt"], d["camera"], list(d["ids"])
pred = pred_npz["pred"]; percls = pred_npz["percls"]; iou = pred_npz["iou"]
btext = [str(x) for x in bank["text"]]; bcls = [str(x) for x in bank["cls"]]; bsrc = [str(x) for x in bank["src"]]
log(f"뱅크 {len(btext)}문장 · 예측 {len(pred)} · 구성 {meta['composition']}")

# 이긴 문장: 예측 클래스 안에서 최대 코사인 문장
V = bank["vecs"].astype(np.float32)
ds = fo.load_dataset("sourcei")
hid, hemb = ds.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
lab_s = np.array([CLASSES.index(c) for c in bcls])
win_idx = np.empty(len(pred), np.int32)
for s0 in range(0, len(FH), 1500):
    S = FH[s0:s0 + 1500] @ V.T
    for r in range(S.shape[0]):
        c = int(pred[s0 + r])
        m = np.where(lab_s == c)[0]
        win_idx[s0 + r] = m[int(np.argmax(S[r, m]))]
srt = np.sort(percls, 1); margin = (srt[:, -1] - srt[:, -2]).astype(float)
iou_min = iou.min(1).astype(float)
sup_pred = d["topk__v1.0.8.1"]
log("이긴 문장 산출 완료")

# ── FiftyOne 필드 쓰기 ───────────────────────────────────────────────
def cls_field(vals): return [fo.Classification(label=str(v)) for v in vals]
ok = (pred == gt); ok_sup = (sup_pred == gt)
vs = np.where(ok & ~ok_sup, "OPT만 맞음",
     np.where(~ok & ok_sup, "공급만 맞음",
     np.where(ok & ok_sup, "둘 다 맞음", "둘 다 틀림")))
ds.set_values("optbank_pred", cls_field([CLASSES[p] if p >= 0 else "?" for p in pred]))
ds.set_values("optbank_correct", cls_field(["correct" if o else "wrong" for o in ok]))
ds.set_values("optbank_vs_supply", cls_field(vs.tolist()))
ds.set_values("optbank_margin", margin.tolist())
ds.set_values("optbank_iou_min", iou_min.tolist())
ds.set_values("optbank_top_prompt", [btext[i] for i in win_idx])
ds.set_values("optbank_src", cls_field([bsrc[i] for i in win_idx]))
ds.info = dict(ds.info or {}, optbank=dict(
    n_sentences=len(btext), config=meta["cfg"], composition=meta["composition"],
    honest_oof=srch["honest_oof"], fp_budget=srch["fp_budget"],
    built="sourcei_optbank.py", note="공급 75% + 생성 25% 혼합 · 전역 문장평균 제거 · dedup 0.97"))
ds.save()
log("FiftyOne 필드 기록 완료 — " + str(collections.Counter(vs.tolist())))
log(f"  이긴 문장 출처 분포: {dict(collections.Counter(bsrc[i] for i in win_idx))}")

# 사이드바에서 바로 보이도록 기본 필드 노출
try:
    ds.app_config.sidebar_groups = None
    ds.save()
except Exception as e:
    log(f"  (app_config 조정 생략: {e})")

# ── 그림 49: 성능 비교 ──────────────────────────────────────────────
cmp_rows = list(csv.DictReader(open(f"{OUT}/csv/51_optbank_compare.csv", encoding="utf-8-sig")))
order = sorted(cmp_rows, key=lambda r: -float(r["macro_f1_4cls"]))
fig, axes = plt.subplots(1, 3, figsize=(22, 6.8), gridspec_kw={"width_ratios": [1.15, 1.05, 0.95]})
ax = axes[0]
x = np.arange(len(order)); w = 0.26
for k, (key, lab_, col) in enumerate([("macro_f1_4cls", "4클래스 macro-F1", "#1baf7a"),
                                      ("prauc", "PR-AUC (분포-IoU)", "#2a78d6"),
                                      ("balance", "균형 (최소F1/평균F1)", "#eda100")]):
    v = [float(r[key]) for r in order]
    b_ = ax.bar(x + (k - 1) * w, v, w * 0.9, color=col, label=lab_)
    for bx, vv in zip(b_, v): ax.text(bx.get_x() + bx.get_width() / 2, vv + .008, f"{vv:.3f}", ha="center", fontsize=8)
ax.set_xticks(x); ax.set_xticklabels([f"{r['bank'].split(' (')[0]}\n{int(r['n']):,}문장" for r in order], fontsize=8.5)
ax.legend(frameon=False, fontsize=9); ax.set_ylim(0, 0.85)
best_bal = max(order, key=lambda r: float(r["balance"]))["bank"].split(" (")[0]
ax.set_title("① 판정(macro-F1)과 랭킹(PR-AUC)은 sourcei-OPT 가 1위\n"
             f"균형은 {best_bal} 0.756 vs OPT 0.748 — **균형은 1위가 아니다**", loc="left", fontsize=10.5)
ax = axes[1]
for k, (key, lab_, col) in enumerate([("f1_normal", "normal", CC["normal"]), ("f1_falldown", "falldown", CC["falldown"]),
                                      ("f1_fire", "fire", CC["fire"]), ("f1_smoke", "smoke", CC["smoke"])]):
    v = [float(r[key]) for r in order]
    ax.bar(x + (k - 1.5) * 0.2, v, 0.19, color=col, label=lab_)
ax.set_xticks(x); ax.set_xticklabels([r["bank"].split(" (")[0] for r in order], fontsize=8.5, rotation=12)
ax.legend(frameon=False, fontsize=9, ncol=2); ax.set_ylabel("클래스별 F1")
ax.set_title("② 카테고리 균형 — 한 클래스도 버리지 않았나", loc="left", fontsize=11)
ax = axes[2]
fp = [float(r["fp_normal"]) for r in order]
b_ = ax.bar(x, fp, 0.55, color=["#e34948" if f > 0.05 else "#1baf7a" for f in fp])
for bx, vv in zip(b_, fp): ax.text(bx.get_x() + bx.get_width() / 2, vv + .006, f"{vv:.3f}", ha="center", fontsize=9)
ax.axhline(0.05, color="#e34948", ls="--", lw=1.2)
ax.text(-0.4, 0.056, "오탐 예산 5%", color="#e34948", fontsize=9, ha="left")
ax.set_xticks(x); ax.set_xticklabels([r["bank"].split(" (")[0] for r in order], fontsize=8.5, rotation=12)
ax.set_ylabel("정상 프레임 오탐률")
ax.set_title("③ 오탐 예산 — 초록=예산 내", loc="left", fontsize=11)
h = srch["honest_oof"]
fig.suptitle("sourcei-OPT 프롬프트 뱅크 — top-K(판정)와 분포-IoU(랭킹)를 동시에 최적화, 오탐 5% 하드 제약\n"
             f"정직한 추정(폴드 밖 예측 풀링, 카메라를 못 본 설정으로만 예측): 4클래스 mF1 {h['macro_f1_4']} · "
             f"균형 {h['balance']} · 오탐 {h['fp_normal']} · 정확도 {h['acc']}",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f49_optbank.png", dpi=150); plt.close(fig)
log("saved f49")

# ── 그림 50: 구성 · 탐색 · 이긴 문장 출처 ──────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(21, 6.6))
ax = axes[0]
comp = meta["composition"]
src_names = ["공급", "생성(CuPL)", "생성(대조쌍)"]
bottom = np.zeros(4)
for k, sn in enumerate(src_names):
    v = np.array([comp.get(f"{c}/{sn}", 0) for c in CLASSES], float)
    ax.bar(np.arange(4), v, 0.6, bottom=bottom, color=["#8a887f", "#1baf7a", "#2a78d6"][k], label=sn)
    for i, (vv, bb) in enumerate(zip(v, bottom)):
        if vv > 20: ax.text(i, bb + vv / 2, f"{int(vv)}", ha="center", va="center", fontsize=9, color="white")
    bottom += v
ax.set_xticks(range(4)); ax.set_xticklabels(CLASSES); ax.legend(frameon=False, fontsize=9)
ax.set_ylabel("문장 수"); ax.set_title("① 최종 뱅크 구성 — 클래스당 500, 공급 75% + 생성 25%", loc="left", fontsize=11)
ax = axes[1]
top = srch["grid_top20"][:12]
y = np.arange(len(top))
ax.barh(y, [t["J"] for t in top], color=["#1baf7a" if t["feasible"] else "#c3c2b7" for t in top])
for i, t in enumerate(top):
    ax.text(t["J"] + .004, i, f"mF1 {t['mf1']:.3f} · IoU {t['prauc']:.3f} · 오탐 {t['fp']:.3f}", va="center", fontsize=7.8)
ax.set_yticks(y); ax.set_yticklabels([f"k={t['cfg']['k']} {t['cfg']['mix']} dd={t['cfg']['dedup'] or '없음'}" for t in top], fontsize=7.8)
ax.invert_yaxis(); ax.set_xlim(0, max(t["J"] for t in top) * 1.55)
ax.set_xlabel("목적함수 J = 0.35·mF1 + 0.35·PR-AUC + 0.30·균형")
ax.set_title(f"② 탐색 상위 12 — 오탐 예산 통과 {sum(1 for r in srch['grid_top20'] if r['feasible'])}/20 표시", loc="left", fontsize=11)
ax = axes[2]
wsrc = collections.Counter(bsrc[i] for i in win_idx)
tot = sum(wsrc.values())
lbl = [f"{k}\n{v:,}프레임 ({v/tot:.0%})" for k, v in wsrc.most_common()]
ax.pie([v for _k, v in wsrc.most_common()], labels=lbl, colors=["#8a887f", "#1baf7a", "#2a78d6"][:len(wsrc)],
       autopct=None, startangle=90, wedgeprops=dict(width=0.45, edgecolor="white"))
inbank = collections.Counter(bsrc)
ax.set_title("③ 실제로 프레임을 이긴 문장의 출처\n"
             f"뱅크 구성비 공급 {inbank['공급']/len(bsrc):.0%} → 승리 기여 {wsrc.get('공급',0)/tot:.0%}", loc="left", fontsize=11)
fig.suptitle("sourcei-OPT 구성과 선택 근거 — 혼합이 모든 폴드에서 선택됐다(§17 가설 확인)\n"
             f"설정: {meta['cfg']} · 후보 풀 12,640 (공급 12,000 + 생성 640)", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f50_optbank_composition.png", dpi=150); plt.close(fig)
log("saved f50")
json.dump(dict(vs_supply={k: int(v) for k, v in collections.Counter(vs.tolist()).items()},
               winner_src={k: int(v) for k, v in wsrc.items()},
               bank_src={k: int(v) for k, v in inbank.items()}),
          open(f"{BANKDIR}/apply_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
