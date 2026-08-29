#!/usr/bin/env python3
"""E2 임계-무관 랭킹 지표(PR-AUC) + E3 이벤트 단위 인과 집계 — 둘 다 저장된 산출물만 쓴다(재채점 없음).

E2: 규칙 비교는 항상 '임계'를 고르는 일과 뒤섞인다(§3 의 IoU 0.15 사례). 클래스 점수를 연속값으로
    두고 PR-AUC 를 재면 임계 선택을 배제한 채 **뱅크의 랭킹 품질**만 비교할 수 있다.
    입력 = percls_<bank>.npy (뱅크별 [7498,4] 클래스별 max 코사인), iou__<bank>(분포-IoU 연속값),
    margin__<bank>. 카메라 군집 부트스트랩으로 CI.
E3: sourcei GT 는 (src_video, event_index) 단위 윈도우 라벨이다(789 이벤트, 프레임/이벤트 중앙값 3).
    프레임 단위 지표는 같은 라벨을 평균 9.5회 센다. 여기서는 **인과적(과거만 보는) trailing window**
    집계 규칙을 소수만 사전 지정해 이벤트 단위 macro-F1 을 재고, 카메라 홀드아웃으로 창 길이를 고른다.
"""
import os, sys, json, csv, glob, collections
os.environ.setdefault("COS_THREADS", "4")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "4")
import numpy as np, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.metrics import average_precision_score, roc_auc_score
import fiftyone as fo

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RC = {"argmax": "#2a78d6", "topk": "#eb6834", "wave": "#1baf7a", "contrast": "#4a3aa7"}
RNG = np.random.default_rng(0)

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; ids = list(d["ids"])
cams = np.unique(cam)
def vkey(b): return tuple(int(x) for x in b.lstrip("vV").split("."))
banks = sorted([b for b in d["banks"] if not b.startswith("v2.")], key=vkey)
assert len(banks) == 31, len(banks)
print(f"프레임 {len(gt):,} · 카메라 {len(cams)} · 뱅크 {len(banks)}")

# ══════════════════════════════════════════════════════════════════
# E2 — 임계 무관 랭킹 지표
# ══════════════════════════════════════════════════════════════════
def boot_cam(fn, nboot=2000):
    """카메라 군집 부트스트랩: fn(mask) -> 스칼라. 반환 (평균, lo, hi)."""
    idx_by_cam = {c: np.where(cam == c)[0] for c in cams}
    vals = []
    for _ in range(nboot):
        pick = RNG.choice(cams, size=len(cams), replace=True)
        m = np.concatenate([idx_by_cam[c] for c in pick])
        v = fn(m)
        if v is not None and not np.isnan(v): vals.append(v)
    a = np.array(vals); return float(a.mean()), float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))

rows, missing = [], []
scores = {}   # (bank, source) -> [n,4] 연속 점수 (클수록 그 클래스)
for b in banks:
    p = f"{OUT}/percls_{b}.npy"
    if not os.path.exists(p): missing.append(b); continue
    per = np.load(p).astype(np.float32)                       # [n,4] 클래스별 max 코사인
    scores[(b, "maxcos")] = per
    # 차 점수: 각 이벤트 클래스 점수 − normal 점수 (§7 의 결론을 점수로 옮긴 것)
    diff = per.copy()
    for c in (1, 2, 3): diff[:, c] = per[:, c] - per[:, 0]
    scores[(b, "diff")] = diff
    # 분포-IoU 연속값: IoU 가 작을수록 이벤트 → 부호 반전
    I = d[f"iou__{b}"].astype(np.float32)                     # [n,3] falldown/fire/smoke
    iou = np.zeros_like(per); iou[:, 0] = 0.0
    for j, c in enumerate((1, 2, 3)): iou[:, c] = -I[:, j]
    scores[(b, "iou")] = iou
print(f"percls 없는 뱅크: {missing}")

for (b, src), S in scores.items():
    for ci, cname in enumerate(CLASSES[1:], start=1):
        y = (gt == ci).astype(int); s = S[:, ci]
        pos, neg = int(y.sum()), int((gt == 0).sum())
        sel = (gt == ci) | (gt == 0)                          # 이벤트 vs normal (다른 이벤트 제외)
        ap = average_precision_score(y[sel], s[sel]); auc = roc_auc_score(y[sel], s[sel])
        base = y[sel].mean()
        rows.append(dict(bank=b, score=src, cls=cname, n_pos=pos, n_neg=neg,
                         pr_auc=round(float(ap), 4), chance=round(float(base), 4),
                         lift=round(float(ap / base), 3), roc_auc=round(float(auc), 4)))
with open(f"{OUT}/csv/31_ranking_prauc.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "score(점수)", "cls(클래스)", "n_pos", "n_neg",
                                      "pr_auc(PR-AUC)", "chance(무작위=양성비율)", "lift(무작위대비배수)", "roc_auc"])
    w.writeheader()
    for r in rows: w.writerow(dict(zip(w.fieldnames, r.values())))
print(f"→ csv/31_ranking_prauc.csv ({len(rows)}행)")

summ = {"e2": {}}
for src in ["maxcos", "diff", "iou"]:
    for cname in CLASSES[1:]:
        v = [r["pr_auc"] for r in rows if r["score"] == src and r["cls"] == cname]
        a = [r["roc_auc"] for r in rows if r["score"] == src and r["cls"] == cname]
        summ["e2"][f"{src}_{cname}"] = dict(pr_auc_mean=round(float(np.mean(v)), 4), pr_auc_min=round(float(np.min(v)), 4),
                                            pr_auc_max=round(float(np.max(v)), 4), roc_auc_mean=round(float(np.mean(a)), 4))
        print(f"  {src:<7} {cname:<9} PR-AUC 평균 {np.mean(v):.3f} [{np.min(v):.3f}~{np.max(v):.3f}]  ROC-AUC 평균 {np.mean(a):.3f}")

# 뱅크 순위: 랭킹 품질(PR-AUC 평균) vs 하드 판정(top-K macro-F1)
m = json.load(open(f"{OUT}/metrics.json"))
from scipy.stats import spearmanr
pr_by_bank = {b: float(np.mean([r["pr_auc"] for r in rows if r["bank"] == b and r["score"] == "diff"])) for b in sorted({r["bank"] for r in rows}, key=vkey)}
mf1_by_bank = {b: m["banks"][b]["rules"]["topk"]["macro_f1_ev"] for b in pr_by_bank}
bl = list(pr_by_bank)
rho = spearmanr([pr_by_bank[b] for b in bl], [mf1_by_bank[b] for b in bl]).correlation
summ["e2"]["spearman_prauc_vs_topk_mf1"] = round(float(rho), 3)
summ["e2"]["best_bank_by_prauc"] = max(pr_by_bank, key=pr_by_bank.get)
summ["e2"]["best_bank_by_topk_mf1"] = max(mf1_by_bank, key=mf1_by_bank.get)
print(f"  랭킹품질↔하드판정 Spearman ρ={rho:+.3f} | PR-AUC 1위 {summ['e2']['best_bank_by_prauc']} vs top-K 1위 {summ['e2']['best_bank_by_topk_mf1']}")

# 쌍대 CI: diff 점수가 maxcos 보다 나은가 (기준 뱅크 v1.0.8.0, 클래스별)
summ["e2"]["diff_vs_maxcos_ci"] = {}
for ci, cname in enumerate(CLASSES[1:], start=1):
    S1, S0 = scores[("v1.0.8.0", "diff")][:, ci], scores[("v1.0.8.0", "maxcos")][:, ci]
    def f(mask):
        y = (gt[mask] == ci).astype(int); sel = (gt[mask] == ci) | (gt[mask] == 0)
        if y[sel].sum() < 5 or (1 - y[sel]).sum() < 5: return None
        return average_precision_score(y[sel], S1[mask][sel]) - average_precision_score(y[sel], S0[mask][sel])
    mu, lo, hi = boot_cam(f)
    summ["e2"]["diff_vs_maxcos_ci"][cname] = dict(mean=round(mu, 4), lo=round(lo, 4), hi=round(hi, 4))
    print(f"  v1.0.8.0 {cname}: PR-AUC(차) − PR-AUC(max) = {mu:+.3f} [{lo:+.3f}, {hi:+.3f}]")

# ══════════════════════════════════════════════════════════════════
# E3 — 이벤트 단위 인과 집계
# ══════════════════════════════════════════════════════════════════
ds = fo.load_dataset("sourcei")
fids, sv, ei, fie, tsec = ds.values(["id", "src_video", "event_index", "frame_in_event", "t_sec"])
assert list(fids) == ids, "sourcei 순서 불일치"
ev_key = np.array([f"{a}#{b}" for a, b in zip(sv, ei)])
order_in_ev = np.array([x if x is not None else 0 for x in fie], dtype=int)
t = np.array([x if x is not None else 0.0 for x in tsec], dtype=float)
ev_ids = np.unique(ev_key)
ev_rows = {}
for e in ev_ids:
    idx = np.where(ev_key == e)[0]
    idx = idx[np.argsort(order_in_ev[idx])]
    g = gt[idx]
    assert len(set(g.tolist())) == 1, f"이벤트 {e} 에 클래스 혼합"
    ev_rows[e] = dict(idx=idx, cls=int(g[0]), cam=cam[idx[0]], n=len(idx))
print(f"\n이벤트 {len(ev_ids):,}개 · 클래스 분포 {collections.Counter(CLASSES[v['cls']] for v in ev_rows.values())}")
print(f"프레임/이벤트: 중앙값 {np.median([v['n'] for v in ev_rows.values()]):.0f} 평균 {np.mean([v['n'] for v in ev_rows.values()]):.1f} 최대 {max(v['n'] for v in ev_rows.values())}")

def macro_f1(true, pred, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = int(((pred == c) & (true == c)).sum()); fp = int(((pred == c) & (true != c)).sum()); fn = int(((pred != c) & (true == c)).sum())
        p = tp / max(tp + fp, 1); r = tp / max(tp + fn, 1); f.append(2 * p * r / max(p + r, 1e-12))
    return float(np.mean(f))

# 집계 규칙 (전부 인과적: 이벤트의 앞쪽 W 프레임만 본다 = 미래 미참조)
def aggregate(bank, W, rule, min_hits=1):
    """이벤트별 예측. rule: 'any'(창 안에 이벤트 예측 1회 이상) / 'persist'(연속 min_hits) / 'majority'."""
    P = d[f"topk__{bank}"]
    out_true, out_pred, out_cam = [], [], []
    for e, v in ev_rows.items():
        seq = P[v["idx"]][:W] if W > 0 else P[v["idx"]]
        seq = seq[seq >= 0]
        if len(seq) == 0: pred = 0
        elif rule == "any":
            ev_hits = seq[seq > 0]
            pred = int(collections.Counter(ev_hits.tolist()).most_common(1)[0][0]) if len(ev_hits) >= min_hits else 0
        elif rule == "persist":
            best, run, cur = 0, 0, -1
            for x in seq:
                if x > 0 and x == cur: run += 1
                elif x > 0: cur, run = int(x), 1
                else: cur, run = -1, 0
                if run >= min_hits: best = cur; break
            pred = best
        else:  # majority
            pred = int(collections.Counter(seq.tolist()).most_common(1)[0][0])
        out_true.append(v["cls"]); out_pred.append(pred); out_cam.append(v["cam"])
    return np.array(out_true), np.array(out_pred), np.array(out_cam)

e3_rows = []
for bank in ["v1.0.8.0", "v1.0.8.1", "v1.0.8.4"]:
    # 프레임 단위 기준선
    Pf = d[f"topk__{bank}"]
    e3_rows.append(dict(bank=bank, unit="frame", rule="-", W=0, min_hits=0, n=len(gt),
                        acc=round(float((Pf == gt).mean()), 4), macro_f1=round(macro_f1(gt, Pf), 4),
                        rec_fall=round(float((Pf[gt == 1] == 1).mean()), 4), rec_fire=round(float((Pf[gt == 2] == 2).mean()), 4),
                        rec_smoke=round(float((Pf[gt == 3] == 3).mean()), 4), fp_normal=round(float((Pf[gt == 0] > 0).mean()), 4)))
    for rule, mh in [("any", 1), ("any", 2), ("persist", 2), ("persist", 3), ("majority", 1)]:
        for W in [3, 5, 10, 0]:
            T, P, C = aggregate(bank, W, rule, mh)
            e3_rows.append(dict(bank=bank, unit="event", rule=rule, W=(W if W else 999), min_hits=mh, n=len(T),
                                acc=round(float((P == T).mean()), 4), macro_f1=round(macro_f1(T, P), 4),
                                rec_fall=round(float((P[T == 1] == 1).mean()), 4), rec_fire=round(float((P[T == 2] == 2).mean()), 4),
                                rec_smoke=round(float((P[T == 3] == 3).mean()), 4), fp_normal=round(float((P[T == 0] > 0).mean()), 4)))
with open(f"{OUT}/csv/32_event_aggregation.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "unit(단위)", "rule(집계규칙)", "W(창길이프레임)", "min_hits(최소발화)",
                                      "n(표본)", "acc(정확도)", "macro_f1(이벤트macroF1)", "rec_fall", "rec_fire", "rec_smoke", "fp_normal(정상오탐)"])
    w.writeheader()
    for r in e3_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
print(f"→ csv/32_event_aggregation.csv ({len(e3_rows)}행)")
for r in e3_rows:
    if r["bank"] == "v1.0.8.0": print(f"  {r['unit']:<6}{r['rule']:<9}W={r['W']:<4}mh={r['min_hits']}  acc {r['acc']:.3f} mF1 {r['macro_f1']:.3f}  fall/fire/smoke {r['rec_fall']:.2f}/{r['rec_fire']:.2f}/{r['rec_smoke']:.2f}  정상오탐 {r['fp_normal']:.3f}")

# 창 길이를 카메라 홀드아웃으로 고른다 (낙관 편향 제거)
ev_cam = np.array([v["cam"] for v in ev_rows.values()])
folds = [(np.isin(ev_cam, cams[i::2]), ~np.isin(ev_cam, cams[i::2])) for i in (0, 1)]
sel_rows = []
for bank in ["v1.0.8.0", "v1.0.8.1"]:
    for fi, (tr, te) in enumerate(folds):
        best, bestv = None, -1
        for rule, mh in [("any", 1), ("any", 2), ("persist", 2), ("persist", 3), ("majority", 1)]:
            for W in [3, 5, 10, 0]:
                T, P, C = aggregate(bank, W, rule, mh)
                v = macro_f1(T[tr], P[tr])
                if v > bestv: bestv, best = v, (rule, mh, W)
        rule, mh, W = best
        T, P, C = aggregate(bank, W, rule, mh)
        Tf, Pf = gt, d[f"topk__{bank}"]
        fmask = np.isin(cam, np.unique(ev_cam[te]))
        sel_rows.append(dict(bank=bank, fold=fi, chosen=f"{rule}/mh{mh}/W{W if W else 999}",
                             train_mf1=round(bestv, 4), test_event_mf1=round(macro_f1(T[te], P[te]), 4),
                             test_event_acc=round(float((P[te] == T[te]).mean()), 4),
                             test_frame_mf1=round(macro_f1(Tf[fmask], Pf[fmask]), 4),
                             test_frame_acc=round(float((Pf[fmask] == Tf[fmask]).mean()), 4), n_test_events=int(te.sum())))
        print(f"  홀드아웃 {bank} fold{fi}: 선택 {rule}/mh{mh}/W{W if W else 999} → 이벤트 mF1 {sel_rows[-1]['test_event_mf1']:.3f} (프레임 {sel_rows[-1]['test_frame_mf1']:.3f}), 이벤트 {int(te.sum())}개")
with open(f"{OUT}/csv/32b_event_holdout.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "fold(폴드)", "chosen(선택규칙)", "train_mf1(학습카메라)", "test_event_mf1", "test_event_acc", "test_frame_mf1", "test_frame_acc", "n_test_events"])
    w.writeheader()
    for r in sel_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
summ["e3"] = dict(n_events=len(ev_ids), by_class={CLASSES[k]: v for k, v in collections.Counter(v["cls"] for v in ev_rows.values()).items()},
                  frames_per_event_median=float(np.median([v["n"] for v in ev_rows.values()])), holdout=sel_rows,
                  best_event_rule_overall=max([r for r in e3_rows if r["unit"] == "event" and r["bank"] == "v1.0.8.0"], key=lambda r: r["macro_f1"]))

# ══════════════════════════════════════════════════════════════════
# 그림
# ══════════════════════════════════════════════════════════════════
NOTE = "카메라 군집 부트스트랩 2,000회 · sourcei GT 7,498프레임/789이벤트/15카메라 (윈도우 라벨이라 절대값보다 상대 비교)"
fig, axes = plt.subplots(1, 3, figsize=(18, 6.6))
for ax, cname in zip(axes, CLASSES[1:]):
    for src, lab in [("maxcos", "클래스 max 코사인"), ("diff", "차 점수 (이벤트−normal)"), ("iou", "분포-IoU 연속값")]:
        v = sorted([r["pr_auc"] for r in rows if r["score"] == src and r["cls"] == cname])
        ax.plot(np.linspace(0, 1, len(v)), v, "o-", ms=4, color=RC["argmax" if src == "maxcos" else ("topk" if src == "diff" else "wave")], label=f"{lab} (평균 {np.mean(v):.3f})")
    ch = [r["chance"] for r in rows if r["cls"] == cname][0]
    ax.axhline(ch, color="#c3c2b7", ls="--"); ax.text(0.02, ch + 0.01, f"무작위 {ch:.3f}", fontsize=8.5, color="#52514e")
    ax.set_xlabel("뱅크 31종 (PR-AUC 오름차순)"); ax.set_ylabel("PR-AUC" if cname == "falldown" else "")
    ax.set_title(f"{cname} — 임계 무관 랭킹 품질 (이벤트 vs normal)", loc="left", fontsize=11); ax.legend(frameon=False, fontsize=8.5, loc="upper left")
fig.suptitle("E2 임계 무관 랭킹 지표 — 하드 판정(macro-F1)이 아니라 연속 점수의 PR-AUC.\n"
             "**분포-IoU 연속값이 세 클래스 전부에서 최고** (0.655 / 0.757 / 0.802) > 차 점수(0.637 / 0.632 / 0.605) > 클래스 max 코사인(0.546 / 0.212 / 0.410)\n"
             f"즉 §3 에서 IoU@0.15 가 최하위였던 것은 순전히 **임계 문제**였고, 점수 함수로서는 IoU 가 가장 좋다 — D1 정본 규칙의 가장 강한 근거. 랭킹품질↔top-K macro-F1 ρ={rho:+.2f}\n{NOTE}", x=0.01, ha="left", fontsize=10.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f36_ranking_prauc.png", dpi=160); plt.close(fig)
print("saved f36")

fig, axes = plt.subplots(1, 2, figsize=(16, 7), gridspec_kw={"width_ratios": [1.35, 1]})
ax = axes[0]
base = [r for r in e3_rows if r["bank"] == "v1.0.8.0"]
labs = [f"{r['unit']}\n{r['rule']} mh{r['min_hits']} W{r['W']}" if r["unit"] == "event" else "frame\n(기준선)" for r in base]
x = np.arange(len(base)); mf = [r["macro_f1"] for r in base]; ac = [r["acc"] for r in base]
ax.bar(x - 0.2, mf, 0.38, color="#eb6834", label="이벤트 macro-F1")
ax.bar(x + 0.2, ac, 0.38, color="#8a887f", label="정확도")
for i, (a, b_) in enumerate(zip(mf, ac)):
    ax.text(i - 0.2, a + 0.008, f"{a:.2f}", ha="center", fontsize=7.5); ax.text(i + 0.2, b_ + 0.008, f"{b_:.2f}", ha="center", fontsize=7.5)
ax.set_xticks(x); ax.set_xticklabels(labs, fontsize=7, rotation=90); ax.legend(frameon=False, fontsize=9)
ax.set_title("v1.0.8.0 — 프레임 단위 vs 이벤트 단위 집계 규칙", loc="left", fontsize=11)
ax = axes[1]
w2 = 0.35
for k, (key, lab, col) in enumerate([("test_frame_mf1", "프레임 단위", "#8a887f"), ("test_event_mf1", "이벤트 단위(홀드아웃 선택)", "#eb6834")]):
    v = [r[key] for r in sel_rows]; b_ = ax.bar(np.arange(len(sel_rows)) + (k - 0.5) * w2, v, w2 * 0.92, color=col, label=lab)
    for bx, vv in zip(b_, v): ax.text(bx.get_x() + bx.get_width() / 2, vv + 0.005, f"{vv:.3f}", ha="center", fontsize=8.5)
ax.set_xticks(range(len(sel_rows))); ax.set_xticklabels([f"{r['bank']}\nfold{r['fold']} ({r['n_test_events']}이벤트)\n{r['chosen']}" for r in sel_rows], fontsize=8)
ax.set_ylabel("이벤트 3클래스 macro-F1"); ax.legend(frameon=False, fontsize=9)
ax.set_title("카메라 홀드아웃 — 창 길이·규칙을 학습 카메라에서 고르고 테스트 카메라에서 평가", loc="left", fontsize=11)
fig.suptitle(f"E3 이벤트 단위 인과 집계 — 이벤트 {len(ev_ids)}개(프레임/이벤트 중앙값 {np.median([v['n'] for v in ev_rows.values()]):.0f}). "
             "라벨 단위가 이벤트이므로 프레임 지표는 같은 라벨을 여러 번 센다\n집계는 전부 인과적(창 안의 앞쪽 프레임만 사용, 미래 미참조). "
             f"{NOTE}", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f37_event_aggregation.png", dpi=160); plt.close(fig)
print("saved f37")
json.dump(summ, open(f"{OUT}/ranking_event_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
