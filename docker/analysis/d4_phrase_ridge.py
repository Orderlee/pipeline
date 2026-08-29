#!/usr/bin/env python3
"""D4 — 구문 β 재추정 (Ridge). "어떤 문구를 쓰라는 건가" 를 **부분계수**로 답한다.

§10 의 구문 대조는 한계 빈도(marginal count)라 교락에 취약하다: "server room" 이 "white smoke"
와 늘 같이 나오면 둘 다 좋아 보인다. Ridge 는 구문을 **서로 경쟁시켜** 다른 구문을 통제한
순효과 β 를 준다.

두 목표를 각각 회귀하고 계수를 비교한다 — 이게 이 분석의 핵심 질문이다:
  y_gt   : **sourcei GT 대조** — 그 문장 클래스의 GT 이벤트 프레임 평균코사인 − GT normal 평균  ← 사람 라벨
  y_free : **frames 표본 대조** — fire·smoke 프레임 평균코사인 − 비이벤트 평균  ← 라벨 불필요
           ⚠️ SAM3 파생 약참조. GT 아님.
  (보조) y_sel : GT 선택도 hit/(hit+trap). **한 번이라도 이긴 문장 2,507개에만 정의**되므로
         event 클래스는 표본이 300 미만으로 떨어진다 → 주 목표로 쓸 수 없다. normal 만 참고로 낸다.
두 대조 목표는 정의가 같고 참조 프레임만 다르므로 계수 비교가 사과-사과다.
β_gt 와 β_free 가 일치하면 **사람 라벨 없이도 프롬프트를 큐레이션할 수 있다**. 어긋나면
라벨-free 대리지표는 문구 수준에서 못 쓴다. 어느 쪽이든 결론이다.

폴드 밖 R² 도 같이 낸다. R²≈0 이면 "문구 규칙" 자체가 허구라는 뜻이고, 그것도 보고한다.
"""
import os, sys, json, csv, glob, re, collections
sys.path.insert(0, "/workspace")
os.environ.setdefault("COS_THREADS", "5")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"): os.environ.setdefault(_v, "5")
from prompt_cos_db import load_sentence_vectors
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
import scipy.sparse as sp
from sklearn.linear_model import Ridge
from sklearn.model_selection import KFold
import fiftyone as fo
from fiftyone import ViewField as F

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

conn = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"); cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1")
text = dict(cur.fetchall())
led = np.load(f"{OUT}/sentence_ledger.npz", allow_pickle=True)
hit, trap, lab = led["hit"].astype(np.float64), led["trap"].astype(np.float64), led["lab"]
led_hashes = [str(x) for x in led["hashes"]] if "hashes" in led else None
log(f"문장 {SENT.shape} · 원장 {len(hit):,}행 (hashes {'있음' if led_hashes else '없음'})")
if led_hashes is None:
    # 원장은 합집합 뱅크 순서로 저장됐다 — 재구성한다 (sourcei_gt_sentences.py 와 동일 순서)
    cur.execute("""SELECT DISTINCT s.content_hash FROM bank_sentences s
                   JOIN image_embeddings e ON e.entity_type='prompt' AND e.entity_id=s.content_hash
                   ORDER BY 1""")
    led_hashes = [r[0] for r in cur.fetchall()]
assert len(led_hashes) == len(hit), (len(led_hashes), len(hit))
cols = np.array([h2c[h] for h in led_hashes])
NS = len(cols)

# ── y_sel (보조) : GT 선택도 — 이긴 문장에만 정의 ──────────────────────
appear = hit + trap
y_sel = np.where(appear > 0, hit / np.maximum(appear, 1), np.nan)
log(f"y_sel 유효 {int(np.isfinite(y_sel).sum()):,}/{len(y_sel):,} (한 번이라도 이긴 문장만)")

# ── y_gt : sourcei GT 대조 — 모든 문장에 정의된다 ─────────────────────
dd = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gtl, ids_h = dd["gt"], list(dd["ids"])
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids_h
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True); del hemb
cols_pre = np.array([h2c[h] for h in led_hashes])
Vpre = SENT[cols_pre]
gsum = np.zeros((4, len(cols_pre)), np.float64); gcnt = np.zeros(4, np.int64)
for s0 in range(0, len(FH), 1500):
    C = FH[s0:s0 + 1500] @ Vpre.T; gl = gtl[s0:s0 + 1500]
    for c in range(4):
        m = gl == c
        if m.any(): gsum[c] += C[m].sum(0); gcnt[c] += int(m.sum())
gmean = gsum / np.maximum(gcnt, 1)[:, None]
log(f"sourcei GT 프레임 분포 {dict(zip(CLASSES, gcnt.tolist()))}")
y_gt = np.full(len(cols_pre), np.nan)
for c in range(1, 4):
    m = lab == c
    y_gt[m] = gmean[c][m] - gmean[0][m]
m0 = lab == 0
y_gt[m0] = gmean[0][m0] - gmean[1:, m0].mean(0)      # normal 은 반대 방향 대조
del Vpre, FH
log(f"y_gt(GT 대조) 유효 {int(np.isfinite(y_gt).sum()):,} · 범위 [{np.nanmin(y_gt):+.4f},{np.nanmax(y_gt):+.4f}]")

# ── y_free : frames 표본 대조 (라벨 불필요, SAM3 약참조) ──────────────
# ⚠️ `ds.values("image_embedding")` 를 전량(199,972 × 1024) 호출하면 파이썬 리스트로 ~8GB 를
#    잡아 공유 호스트(가용 0.5GB 상황)에서 2.6시간 스왑 쓰래싱을 냈다. 라벨만 먼저 전량 받고
#    **필요한 24,792개만** select 해서 임베딩을 받는다 (2026-08-27 실측 사고).
FSUB = f"{OUT}/frames_sub_24792.npz"
if os.path.exists(FSUB):
    zz = np.load(FSUB)
    FF, ref = zz["FF"], zz["ref"]
    log(f"frames 표본 캐시 재사용 {FF.shape}")
else:
    ds = fo.load_dataset("frames")
    view = ds.match(F("modality") == "frame")
    sid, ncls_raw = view.values(["id", "normalized_class"])      # 문자열만 — 가볍다
    ncls = np.array([x or "none" for x in ncls_raw])
    fi = np.where(ncls == "fire")[0]; si = np.where(ncls == "smoke")[0]
    ni = np.where(np.isin(ncls, ["none", "person"]))[0]
    sub = np.concatenate([fi, si, RNG.choice(ni, 20000, replace=False)])
    ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], dtype=np.int8)
    ids_sub = [sid[i] for i in sub]
    emb = ds.select(ids_sub, ordered=True).values("image_embedding")
    FF = np.asarray(emb, dtype=np.float32); FF /= np.linalg.norm(FF, axis=1, keepdims=True)
    del emb
    assert len(FF) == 24792 and len(fi) == 1578, (len(FF), len(fi))
    np.savez_compressed(FSUB, FF=FF, ref=ref)
    log(f"frames 표본 새로 만들어 캐시 {FF.shape}")
Vs = SENT[cols]
mu_ev = np.zeros(NS); mu_no = np.zeros(NS)
n_ev = int((ref > 0).sum()); n_no = int((ref == 0).sum())
for s0 in range(0, len(FF), 2000):
    C = FF[s0:s0 + 2000] @ Vs.T; r = ref[s0:s0 + 2000]
    mu_ev += C[r > 0].sum(0); mu_no += C[r == 0].sum(0)
del Vs
y_free = mu_ev / n_ev - mu_no / n_no
y_free = np.where(lab == 0, -y_free, y_free)          # normal 은 y_gt 와 같은 방향 규약
log(f"y_free 범위 [{y_free.min():+.4f},{y_free.max():+.4f}] 평균 {y_free.mean():+.4f}")

# ── 설계행렬: 1~3-gram (문서빈도 ≥ 50) ────────────────────────────────
STOP = set("a an the of in on at to with and or is are by for from into onto near under over as its their his her this that there it".split())
def grams(t):
    w = re.findall(r"[a-z]+", str(t).lower()); g = set()
    for n in (1, 2, 3):
        for i in range(len(w) - n + 1):
            p = w[i:i + n]
            if n == 1 and p[0] in STOP: continue
            g.add(" ".join(p))
    return g
G = [grams(text.get(h, "")) for h in led_hashes]
df = collections.Counter()
for g in G: df.update(g)
vocab = sorted([g for g, n in df.items() if n >= 50])
v2i = {g: i for i, g in enumerate(vocab)}
rows_, cols_ = [], []
for i, g in enumerate(G):
    for t_ in g:
        j = v2i.get(t_)
        if j is not None: rows_.append(i); cols_.append(j)
X = sp.csr_matrix((np.ones(len(rows_), np.float32), (rows_, cols_)), shape=(NS, len(vocab)))
log(f"설계행렬 {X.shape} (nnz {X.nnz:,}, 밀도 {X.nnz/(X.shape[0]*X.shape[1]):.4f})")

def fit_class(ci, y, name):
    """클래스별 Ridge — 폴드 밖 R² + 5폴드 부호 일치도."""
    m = (lab == ci) & np.isfinite(y)
    if name == "y_sel": m &= appear >= 10
    idx = np.where(m)[0]
    if len(idx) < 300: return None
    Xi, yi = X[idx], y[idx]
    keep = np.asarray((Xi > 0).sum(0)).ravel() >= 20
    Xi = Xi[:, keep]; vi = [vocab[j] for j in np.where(keep)[0]]
    yi = (yi - yi.mean()) / (yi.std() + 1e-12)
    best = None
    for al in (1.0, 10.0, 100.0, 1000.0):
        oof = np.zeros(len(idx))
        for tr, te in KFold(5, shuffle=True, random_state=0).split(Xi):
            r = Ridge(alpha=al, solver="sparse_cg", max_iter=3000, tol=1e-5, fit_intercept=True).fit(Xi[tr], yi[tr])
            oof[te] = r.predict(Xi[te])
        r2 = 1 - ((yi - oof) ** 2).sum() / ((yi - yi.mean()) ** 2).sum()
        if best is None or r2 > best[1]: best = (al, r2, oof)
    al, r2, oof = best
    B = np.stack([Ridge(alpha=al, solver="sparse_cg", max_iter=3000, tol=1e-5).fit(Xi[tr], yi[tr]).coef_
                  for tr, _ in KFold(5, shuffle=True, random_state=1).split(Xi)])
    beta = B.mean(0); sign_ok = (np.sign(B) == np.sign(beta)).mean(0)
    dfv = np.asarray((Xi > 0).sum(0)).ravel()
    log(f"  {name} {CLASSES[ci]:<9} n={len(idx):>6,} 구문 {Xi.shape[1]:>5}  α={al:<6g} OOF R²={r2:+.4f}")
    return dict(cls=CLASSES[ci], target=name, n=int(len(idx)), n_phrase=int(Xi.shape[1]), alpha=al, oof_r2=float(r2),
                vocab=vi, beta=beta, sign=sign_ok, df=dfv)

log("Ridge 적합")
fits = {}
for name, y in (("y_gt", y_gt), ("y_free", y_free), ("y_sel", y_sel)):
    for ci in range(4):
        f_ = fit_class(ci, y, name)
        if f_: fits[(name, ci)] = f_

# ── β_gt vs β_free 일치도 ────────────────────────────────────────────
agree = []
for ci in range(4):
    a, b = fits.get(("y_gt", ci)), fits.get(("y_free", ci))
    if not a or not b: continue
    common = sorted(set(a["vocab"]) & set(b["vocab"]))
    ia = {g: i for i, g in enumerate(a["vocab"])}; ib = {g: i for i, g in enumerate(b["vocab"])}
    va = np.array([a["beta"][ia[g]] for g in common]); vb = np.array([b["beta"][ib[g]] for g in common])
    from scipy.stats import spearmanr, pearsonr
    rho = float(spearmanr(va, vb).statistic); pr = float(pearsonr(va, vb).statistic)
    # 상위 100 구문 겹침 (부호·순위 둘 다 본다)
    ta = set(np.array(common)[np.argsort(-va)[:100]]); tb = set(np.array(common)[np.argsort(-vb)[:100]])
    agree.append(dict(cls=CLASSES[ci], n_common=len(common), spearman=round(rho, 4), pearson=round(pr, 4),
                      top100_overlap=len(ta & tb), sign_agree=round(float((np.sign(va) == np.sign(vb)).mean()), 4),
                      r2_gt=round(a["oof_r2"], 4), r2_free=round(b["oof_r2"], 4)))
    log(f"  {CLASSES[ci]:<9} 공통구문 {len(common):>5}  ρ(β_gt,β_free)={rho:+.3f}  부호일치 {(np.sign(va)==np.sign(vb)).mean():.1%}  top100 겹침 {len(ta&tb)}")

# ── CSV ─────────────────────────────────────────────────────────────
with open(f"{OUT}/csv/36_phrase_beta.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["target(목표)", "class(클래스)", "rank(순위)", "phrase(구문)", "beta(부분계수)", "sign_stability(5폴드부호일치)",
                "df(문장수)", "oof_r2(폴드밖R²)", "alpha", "direction(방향)"])
    for (name, ci), f_ in sorted(fits.items()):
        o = np.argsort(-f_["beta"])
        for tag, sel in (("상위(+)", o[:40]), ("하위(−)", o[::-1][:40])):
            for rk, j in enumerate(sel, 1):
                w.writerow([name, f_["cls"], rk, f_["vocab"][j], round(float(f_["beta"][j]), 5),
                            round(float(f_["sign"][j]), 3), int(f_["df"][j]), round(f_["oof_r2"], 4), f_["alpha"], tag])
with open(f"{OUT}/csv/37_beta_agreement.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["class(클래스)", "n_common(공통구문)", "spearman(ρ)", "pearson(r)", "top100_overlap(상위겹침)",
                                      "sign_agree(부호일치율)", "r2_gt", "r2_free"])
    w.writeheader()
    for r in agree: w.writerow(dict(zip(w.fieldnames, r.values())))
log("→ csv/36_phrase_beta.csv, csv/37_beta_agreement.csv")

# ── 그림 ────────────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(21, 11.5), gridspec_kw={"width_ratios": [1, 1, 0.9]})
for col, ci in enumerate((1, 2, 3)):
    for row, name in enumerate(("y_gt", "y_free")):
        ax = axes[row][col]; f_ = fits.get((name, ci))
        if not f_: ax.axis("off"); continue
        o = np.argsort(-f_["beta"]); sel = np.concatenate([o[:10], o[::-1][:10][::-1]])
        vals = f_["beta"][sel]; y = np.arange(len(sel))
        colr = [CC[CLASSES[ci]] if v > 0 else "#8a887f" for v in vals]
        ax.barh(y, vals, color=colr, alpha=.9)
        for i, j in enumerate(sel):
            ax.text(0 if vals[i] < 0 else 0, i, f"  {f_['vocab'][j]}" if vals[i] > 0 else f"{f_['vocab'][j]}  ",
                    va="center", ha="left" if vals[i] > 0 else "right", fontsize=7.6)
        ax.set_yticks([]); ax.invert_yaxis(); ax.axvline(0, color="#0b0b0b", lw=1)
        ax.set_xlabel("Ridge 부분계수 β")
        tt = "GT 선택도" if name == "y_gt" else "frames 대조 (라벨 불필요)"
        ax.set_title(f"{CLASSES[ci]} · 목표={tt}\nOOF R²={f_['oof_r2']:+.3f} · 구문 {f_['n_phrase']:,} · 문장 {f_['n']:,}", loc="left", fontsize=10)
axr = axes[0][2] if False else None
# 우측 열은 이미 클래스에 쓰였으므로 일치도는 별도 축에 겹쳐 그린다
fig.tight_layout(rect=[0, 0, 1, 0.93])
fig.suptitle("D4 구문 β 재추정 (Ridge) — 구문을 서로 경쟁시킨 **순효과**. 위=사람 라벨 목표, 아래=라벨 없는 대조 목표.\n"
             "두 줄의 부호가 같으면 사람 라벨 없이 문구를 고를 수 있다 · " +
             " · ".join(f"{a['cls']} ρ={a['spearman']:+.2f}/부호 {a['sign_agree']:.0%}" for a in agree),
             x=0.01, ha="left", fontsize=11.5)
fig.savefig(f"{OUT}/fig/f40_phrase_beta.png", dpi=150); plt.close(fig)
log("saved f40")

fig, axes = plt.subplots(1, 2, figsize=(15.5, 6.4))
ax = axes[0]
x = np.arange(len(agree)); w2 = 0.34
b1 = ax.bar(x - w2 / 2, [a["r2_gt"] for a in agree], w2, color="#2a78d6", label="목표=GT 선택도")
b2 = ax.bar(x + w2 / 2, [a["r2_free"] for a in agree], w2, color="#1baf7a", label="목표=frames 대조(라벨 불필요)")
for bb in (b1, b2):
    for bx in bb: ax.text(bx.get_x() + bx.get_width() / 2, bx.get_height() + .004, f"{bx.get_height():.3f}", ha="center", fontsize=8.5)
ax.axhline(0, color="#0b0b0b", lw=1)
ax.set_xticks(x); ax.set_xticklabels([a["cls"] for a in agree]); ax.set_ylabel("폴드 밖 R² (구문이 설명하는 분산)")
ax.legend(frameon=False, fontsize=9); ax.set_title("① 문구가 품질을 얼마나 설명하나 — R²≈0 이면 '문구 규칙'은 허구", loc="left", fontsize=11)
ax = axes[1]
for i, a in enumerate(agree):
    ax.barh(i, a["spearman"], color=CC[a["cls"]], alpha=.9)
    ax.text(a["spearman"] + (.01 if a["spearman"] >= 0 else -.01), i,
            f"ρ={a['spearman']:+.3f} · 부호일치 {a['sign_agree']:.0%} · top100 겹침 {a['top100_overlap']}/100",
            va="center", ha="left" if a["spearman"] >= 0 else "right", fontsize=9)
ax.axvline(0, color="#0b0b0b", lw=1)
ax.set_yticks(range(len(agree))); ax.set_yticklabels([a["cls"] for a in agree]); ax.invert_yaxis()
ax.set_xlabel("ρ(β_GT , β_라벨없음)"); ax.set_xlim(-1.05, 1.35)
ax.set_title("② 라벨 없는 목표가 사람 라벨 목표를 대신할 수 있나", loc="left", fontsize=11)
fig.suptitle("D4 — 문구 규칙의 설명력과, 라벨 없이 문구를 고를 수 있는지\n"
             "frames 대조는 SAM3 파생 약참조 (GT 아님) · sourcei GT 7,498 프레임 · 구문 = 1~3-gram, 문서빈도 ≥ 50", x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f41_beta_agreement.png", dpi=160); plt.close(fig)
log("saved f41")
json.dump(dict(agreement=agree, fits={f"{k[0]}|{CLASSES[k[1]]}": dict(n=v["n"], n_phrase=v["n_phrase"], alpha=v["alpha"], oof_r2=v["oof_r2"],
                top_pos=[[v["vocab"][j], round(float(v["beta"][j]), 5)] for j in np.argsort(-v["beta"])[:25]],
                top_neg=[[v["vocab"][j], round(float(v["beta"][j]), 5)] for j in np.argsort(v["beta"])[:25]]) for k, v in fits.items()},
               n_vocab=len(vocab), n_sentences=NS),
          open(f"{OUT}/phrase_ridge_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
