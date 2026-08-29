#!/usr/bin/env python3
"""남은 분석 ① 혼합효과 모형 — 카메라 이질성을 명시적으로 뺀 뱅크·규칙 효과.

§5 는 카메라 군집 부트스트랩(비모수)으로 "뱅크 차이는 유의하지 않고 규칙 차이는 유의하다"를 냈다.
혼합효과 모형은 같은 질문을 **모수적으로** 다시 묻는다: 카메라를 랜덤효과로 명시하고 남은
뱅크 고정효과가 유의한가. 두 방법이 일치하면 §5 의 결론이 방법에 의존하지 않는다는 뜻이다.

⚠️ statsmodels·pymer4 가 컨테이너에 없다(실측). 설치하지 않고 **직접 적합**한다 —
랜덤절편 2수준 모형은 프로파일 REML 을 1모수(분산비 θ = σ²_camera / σ²_resid)로 줄일 수 있어
scipy.optimize.minimize_scalar 하나로 끝난다. 데이터가 31뱅크 × 15카메라 = 465셀이라 가볍다.

모형:  y[b,c] = μ + bank_b(고정) + camera_c(랜덤, N(0,σ²_u)) + ε[b,c](N(0,σ²_e))
  · y = 그 (뱅크, 카메라) 셀의 이벤트 macro-F1 (그리고 정확도로 한 번 더)
  · 뱅크 효과 검정 = LRT (full vs 뱅크 없는 모형), 규칙 효과도 같은 방식
  · ICC = σ²_u / (σ²_u + σ²_e) → §5 의 ICC 0.51 과 대조
"""
import os, sys, json, csv, glob, itertools
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ[_v] = THR
import numpy as np, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from scipy.optimize import minimize_scalar
from scipy.stats import chi2
from scipy import linalg

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]
_m = json.load(open(f"{OUT}/metrics.json"))
BANKS = [str(b) for b in d["banks"] if set(_m["banks"][str(b)]["classes"]) & {"falldown", "fire", "smoke"}]
assert len(BANKS) == 31, len(BANKS)
RULES = ["argmax", "topk", "wave"]
cams = np.unique(cam)
log(f"뱅크 {len(BANKS)} · 규칙 {len(RULES)} · 카메라 {len(cams)}")

def macro_f1(t, p, classes):
    f = []
    for c in classes:
        tp = int(((p == c) & (t == c)).sum()); fp = int(((p == c) & (t != c)).sum()); fn = int(((p != c) & (t == c)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))

# ── (뱅크, 규칙, 카메라) 셀 만들기 ───────────────────────────────────
# ⚠️ 카메라 안에 이벤트 클래스가 없으면 macro-F1 이 정의되지 않는다 → 그 카메라에 실제 존재하는
#    클래스만으로 macro 를 낸다(§5 와 동일 규약). 클래스 구성이 카메라마다 달라 셀 간 비교는
#    "카메라 안에서의 상대 비교"로만 유효하고, 그래서 카메라 랜덤절편이 필요하다.
rows = []
cam_classes = {}
for c in cams:
    m = cam == c
    cl = tuple(int(x) for x in np.unique(gt[m]) if x > 0)
    cam_classes[c] = cl
usable = [c for c in cams if cam_classes[c] and (cam == c).sum() >= 50]
log(f"사용 가능 카메라 {len(usable)}/{len(cams)} (이벤트 클래스 존재 + n≥50)")
for b in BANKS:
    for r in RULES:
        p = d[f"{r}__{b}"]
        for c in usable:
            m = cam == c
            rows.append(dict(bank=b, rule=r, camera=str(c), n=int(m.sum()),
                             mf1=macro_f1(gt[m], p[m], cam_classes[c]),
                             acc=float((p[m] == gt[m]).mean())))
log(f"셀 {len(rows)} = {len(BANKS)}×{len(RULES)}×{len(usable)}")

# ── 랜덤절편 LMM (프로파일 REML, 1모수) ──────────────────────────────
def fit_lmm(y, X, grp):
    """y = Xβ + Zu + ε, u ~ N(0, σ²_u I), ε ~ N(0, σ²_e I).
    θ = σ²_u/σ²_e 로 프로파일하면 V = σ²_e (I + θ Z Zᵀ) 이고 σ²_e 는 해석적으로 빠진다.
    반환: dict(loglik(REML), sigma2_u, sigma2_e, icc, beta, se, n, p)"""
    n, p = X.shape
    lev = sorted(set(grp)); Z = np.zeros((n, len(lev)))
    for i, g in enumerate(grp): Z[i, lev.index(g)] = 1.0

    def neg_reml(logth):
        th = np.exp(logth)
        V = np.eye(n) + th * (Z @ Z.T)
        L = linalg.cholesky(V, lower=True)
        Vi_X = linalg.cho_solve((L, True), X)
        Vi_y = linalg.cho_solve((L, True), y)
        XtViX = X.T @ Vi_X
        beta = linalg.solve(XtViX, X.T @ Vi_y, assume_a="pos")
        res = y - X @ beta
        Vi_res = linalg.cho_solve((L, True), res)
        rss = float(res @ Vi_res)
        s2e = rss / (n - p)
        logdetV = 2.0 * np.sum(np.log(np.diag(L)))
        sign, logdetXtViX = np.linalg.slogdet(XtViX)
        # REML 로그가능도 (상수항 제외)
        ll = -0.5 * (logdetV + logdetXtViX + (n - p) * np.log(s2e) + (n - p))
        return -ll

    r = minimize_scalar(neg_reml, bounds=(-12.0, 12.0), method="bounded",
                        options=dict(xatol=1e-6))
    th = float(np.exp(r.x))
    V = np.eye(n) + th * (Z @ Z.T)
    L = linalg.cholesky(V, lower=True)
    Vi_X = linalg.cho_solve((L, True), X); Vi_y = linalg.cho_solve((L, True), y)
    XtViX = X.T @ Vi_X
    beta = linalg.solve(XtViX, X.T @ Vi_y, assume_a="pos")
    res = y - X @ beta
    s2e = float(res @ linalg.cho_solve((L, True), res)) / (n - p)
    cov = s2e * linalg.inv(XtViX)
    return dict(loglik=float(-r.fun), theta=th, sigma2_u=th * s2e, sigma2_e=s2e,
                icc=th / (1.0 + th), beta=beta, se=np.sqrt(np.diag(cov)), n=n, p=p)

def design(rows, key, target):
    y = np.array([r[target] for r in rows], float)
    grp = [r["camera"] for r in rows]
    if key is None:
        return y, np.ones((len(rows), 1)), grp, ["절편"]
    lev = sorted({r[key] for r in rows})
    X = np.zeros((len(rows), len(lev)))                     # 셀평균 모수화(절편 없음)
    for i, r in enumerate(rows): X[i, lev.index(r[key])] = 1.0
    return y, X, grp, lev

res = {}
for target in ("mf1", "acc"):
    # (A) 뱅크 효과 — 규칙을 top-K 로 고정해 교락 제거
    sub = [r for r in rows if r["rule"] == "topk"]
    y0, X0, g0, _ = design(sub, None, target)
    y1, X1, g1, lev1 = design(sub, "bank", target)
    f0, f1 = fit_lmm(y0, X0, g0), fit_lmm(y1, X1, g1)
    # ⚠️ REML 로그가능도는 고정효과 구조가 다르면 직접 비교할 수 없다 → ML LRT 대신
    #    Wald F 근사(뱅크 간 대비)를 쓴다. 자유도는 카메라 수 기반으로 보수적으로 잡는다.
    K = len(lev1)
    C = np.zeros((K - 1, K))                                # 뱅크 1 대비 나머지
    for i in range(K - 1): C[i, 0] = -1.0; C[i, i + 1] = 1.0
    n, p = X1.shape
    lev = sorted(set(g1)); Z = np.zeros((n, len(lev)))
    for i, gg in enumerate(g1): Z[i, lev.index(gg)] = 1.0
    V = f1["sigma2_e"] * (np.eye(n) + f1["theta"] * (Z @ Z.T))
    Vi = linalg.inv(V); XtViX = X1.T @ Vi @ X1
    covb = linalg.inv(XtViX)
    Cb = C @ f1["beta"]; M = C @ covb @ C.T
    Fstat = float(Cb @ linalg.solve(M, Cb) / (K - 1))
    df2 = max(len(lev) - 1, 1)                              # 보수적: 카메라 수 − 1
    from scipy.stats import f as fdist
    p_bank = float(fdist.sf(Fstat, K - 1, df2))
    res[f"bank_{target}"] = dict(k=K, F=round(Fstat, 4), df1=K - 1, df2=df2, p=p_bank,
                                 icc=round(f1["icc"], 4), sigma2_u=round(f1["sigma2_u"], 6),
                                 sigma2_e=round(f1["sigma2_e"], 6),
                                 icc_intercept_only=round(f0["icc"], 4),
                                 spread=round(float(f1["beta"].max() - f1["beta"].min()), 4),
                                 se_median=round(float(np.median(f1["se"])), 4))
    log(f"  [{target}] 뱅크 효과: F({K-1},{df2})={Fstat:.3f} p={p_bank:.4f} · ICC {f1['icc']:.3f} "
        f"(절편만 {f0['icc']:.3f}) · 뱅크 추정치 폭 {f1['beta'].max()-f1['beta'].min():+.4f} · 평균 SE {np.median(f1['se']):.4f}")

    # (B) 규칙 효과 — 뱅크를 v1.0.8.0/v1.0.8.1 로 고정
    for bank in ("v1.0.8.0", "v1.0.8.1"):
        sub = [r for r in rows if r["bank"] == bank]
        y1, X1, g1, lev1 = design(sub, "rule", target)
        f1 = fit_lmm(y1, X1, g1)
        K = len(lev1)
        C = np.zeros((K - 1, K))
        for i in range(K - 1): C[i, 0] = -1.0; C[i, i + 1] = 1.0
        n = len(y1); lev = sorted(set(g1)); Z = np.zeros((n, len(lev)))
        for i, gg in enumerate(g1): Z[i, lev.index(gg)] = 1.0
        V = f1["sigma2_e"] * (np.eye(n) + f1["theta"] * (Z @ Z.T))
        Vi = linalg.inv(V); covb = linalg.inv(X1.T @ Vi @ X1)
        Cb = C @ f1["beta"]; M = C @ covb @ C.T
        Fstat = float(Cb @ linalg.solve(M, Cb) / (K - 1))
        df2 = max(len(lev) - 1, 1)
        p_rule = float(fdist.sf(Fstat, K - 1, df2))
        res[f"rule_{target}_{bank}"] = dict(bank=bank, levels=lev1, k=K, F=round(Fstat, 4),
                                            df1=K - 1, df2=df2, p=p_rule, icc=round(f1["icc"], 4),
                                            beta={l: round(float(b), 4) for l, b in zip(lev1, f1["beta"])},
                                            se={l: round(float(s), 4) for l, s in zip(lev1, f1["se"])})
        log(f"  [{target}] {bank} 규칙 효과: F({K-1},{df2})={Fstat:.3f} p={p_rule:.4f} · "
            + " ".join(f"{l} {b:.3f}±{s:.3f}" for l, b, s in zip(lev1, f1["beta"], f1["se"])))

# ── 필요 카메라 수 역산 (혼합효과 분산 성분으로) ─────────────────────
bm = res["bank_mf1"]
s2u, s2e = bm["sigma2_u"], bm["sigma2_e"]
# 두 뱅크 쌍대 비교의 표준오차 ≈ sqrt(2 σ²_e / n_cam) (카메라 효과는 쌍대차에서 상쇄)
for delta in (0.02, 0.05, 0.10):
    n_need = int(np.ceil(2 * s2e * (2.8 / delta) ** 2))
    res.setdefault("power", {})[f"delta_{delta}"] = n_need
    log(f"  Δ={delta:.2f} 를 80% 검정력으로 잡으려면 카메라 {n_need}대 (σ²_e={s2e:.5f})")

with open(f"{OUT}/csv/45_mixed_effects.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["analysis(분석)", "target(지표)", "k(수준수)", "F", "df1", "df2", "p", "ICC(카메라)",
                "sigma2_u(카메라분산)", "sigma2_e(잔차분산)", "spread(추정치폭)", "se_median"])
    for tgt in ("mf1", "acc"):
        r = res[f"bank_{tgt}"]
        w.writerow(["뱅크 효과 (규칙=top-K 고정)", tgt, r["k"], r["F"], r["df1"], r["df2"], round(r["p"], 5),
                    r["icc"], r["sigma2_u"], r["sigma2_e"], r["spread"], r["se_median"]])
        for bank in ("v1.0.8.0", "v1.0.8.1"):
            r = res[f"rule_{tgt}_{bank}"]
            w.writerow([f"규칙 효과 ({bank} 고정)", tgt, r["k"], r["F"], r["df1"], r["df2"], round(r["p"], 5),
                        r["icc"], "", "", "", ""])
with open(f"{OUT}/csv/46_cells.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "rule(규칙)", "camera(카메라)", "n(프레임)", "macro_f1", "acc"])
    w.writeheader()
    for r in rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log("→ csv/45_mixed_effects.csv, csv/46_cells.csv")

# ── 그림 ────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(21, 6.6), gridspec_kw={"width_ratios": [1.15, 1, 0.9]})
ax = axes[0]
sub = [r for r in rows if r["rule"] == "topk"]
by_cam = {}
for r in sub: by_cam.setdefault(r["camera"], []).append(r["mf1"])
order = sorted(by_cam, key=lambda c: -np.median(by_cam[c]))
bp = ax.boxplot([by_cam[c] for c in order], widths=.6, patch_artist=True,
                medianprops=dict(color="#0b0b0b", lw=1.5))
for pc in bp["boxes"]: pc.set_facecolor("#8a887f"); pc.set_alpha(.5)
ax.set_xticks(range(1, len(order) + 1)); ax.set_xticklabels([c[:14] for c in order], rotation=40, ha="right", fontsize=7.5)
ax.set_ylabel("이벤트 macro-F1 (뱅크 31종 분포)")
ax.set_title(f"① 카메라가 뱅크보다 훨씬 크게 흔든다 — ICC {res['bank_mf1']['icc']:.2f}\n"
             f"카메라 간 편차가 뱅크 간 편차({res['bank_mf1']['spread']:+.3f})를 압도한다", loc="left", fontsize=10.5)
ax = axes[1]
labs, Fs, ps = [], [], []
for tgt, tname in (("mf1", "macro-F1"), ("acc", "정확도")):
    labs.append(f"뱅크 31종\n({tname})"); Fs.append(res[f"bank_{tgt}"]["F"]); ps.append(res[f"bank_{tgt}"]["p"])
    for bank in ("v1.0.8.0", "v1.0.8.1"):
        labs.append(f"규칙 3종\n{bank} ({tname})"); Fs.append(res[f"rule_{tgt}_{bank}"]["F"]); ps.append(res[f"rule_{tgt}_{bank}"]["p"])
x = np.arange(len(labs))
b_ = ax.bar(x, Fs, 0.62, color=["#e34948" if p >= .05 else "#1baf7a" for p in ps], alpha=.9)
for bx, F_, p_ in zip(b_, Fs, ps):
    ax.text(bx.get_x() + bx.get_width() / 2, F_ + max(Fs) * .02, f"F={F_:.2f}\np={p_:.3f}", ha="center", fontsize=8)
ax.set_xticks(x); ax.set_xticklabels(labs, fontsize=7.6)
ax.set_ylabel("Wald F (카메라 랜덤절편 통제 후)")
ax.set_title("② 카메라를 모형에서 뺀 뒤에도 — 초록=유의(p<0.05), 빨강=비유의", loc="left", fontsize=10.5)
ax = axes[2]
ks = list(res["power"].keys()); vs = [res["power"][k] for k in ks]
b_ = ax.bar(range(len(ks)), vs, 0.55, color="#2a78d6", alpha=.9)
for bx, v in zip(b_, vs): ax.text(bx.get_x() + bx.get_width() / 2, v * 1.02, f"{v}대", ha="center", fontsize=9.5)
ax.set_yscale("log"); ax.set_xticks(range(len(ks))); ax.set_xticklabels([k.replace("delta_", "Δ=") for k in ks])
ax.set_ylabel("필요 카메라 수 (검정력 80%, 로그축)")
ax.set_title("③ 잔차분산으로 역산한 필요 카메라 수", loc="left", fontsize=10.5)
fig.suptitle("R1 혼합효과 모형 — 카메라를 랜덤절편으로 명시하고 남은 뱅크·규칙 효과 (프로파일 REML 직접 적합, statsmodels 미사용)\n"
             f"sourcei GT 7,498 · 셀 {len(rows)} = 뱅크31 × 규칙3 × 카메라{len(usable)} · §5 의 카메라 군집 부트스트랩 결론과 대조",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f46_mixed_effects.png", dpi=150); plt.close(fig)
log("saved f46")
json.dump(res, open(f"{OUT}/mixed_effects_summary.json", "w"), ensure_ascii=False, indent=1,
          default=lambda o: o.tolist() if hasattr(o, "tolist") else str(o))
print("DONE")
