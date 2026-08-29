#!/usr/bin/env python3
"""A2 — 라벨 없이(무감독) 프롬프트 뱅크를 고르는 실험.

문장 × 군집 친화도(analysis.sentence_affinity, group_kind='cluster')만으로 뱅크 초안을 만든다.
GT 라벨은 **선택 과정에 전혀 쓰지 않는다** — sourcei GT 는 사후 평가에만 쓴다.

핵심 아이디어(시설입지 / facility location):
  A[s,g] = 문장 s 가 군집 g 의 프레임들과 갖는 평균 코사인.
  · 주효과 제거: R = A − rowmean(A)  ("어디서나 큰" 문장의 크기 성분을 뺀다)
  · 군집 표준화: Z = (R − colmean)/colstd  (군집마다 코사인 스케일이 달라 비교 불가)
  · 커버리지 가중 W = clip(Z, 0, None)  — 양의 특이도만 "덮는다"고 본다.
  목적함수 F(S) = Σ_g max_{s∈S} W[s,g]  (단조·부분모듈) → 탐욕이 (1−1/e) 보장.
  즉 "서로 다른 군집을 골고루 덮는 문장 집합"을 고른다. 라벨이 필요 없다.

제약: (a) 이미 뽑힌 같은 클래스 문장과 코사인 > 0.95 인 근사중복은 건너뛴다,
      (b) 주효과 패널티 — 탐욕 기준 = 한계이득 − λ·zscore(m_s), λ=0.5 (m_s = rowmean(A)).
      λ=0 실행도 같이 내서 패널티 효과를 분리한다. z 는 **해당 클래스 후보군 안에서** 계산한다.

⚠️ 적재 범위 한계(결과 해석의 전제): 친화도는 9현장(cohort-a, source-m-cohort_2024/2025,
   sourcea, partner-d_poc, source-o, sourcej, source-g, sembcorp)의 55군집만 덮는다.
   fire 가 많은 현장(fire_smoke·cohort-b·appdata 등)이 **빠져 있다** — 즉 fire 클래스의 선택 근거가
   가장 약하다. 이 한계는 모든 출력물(csv/json/그림)에 명시한다.

출력: csv/22_submodular_bank_draft.csv, csv/22b_coverage_curves.csv, csv/22c_draft_eval.csv,
      submodular_summary.json, fig/f24_coverage_curves.png, fig/f25_draft_vs_baseline.png
"""
import os, sys, json, csv, time, heapq, glob, traceback

os.environ.setdefault("COS_THREADS", "4")
sys.path.insert(0, "/workspace")
from prompt_cos_db import load_banks, load_sentence_vectors, topk_vote, RULE_K  # noqa: E402  (BLAS 캡을 numpy 앞에서 건다)
import numpy as np        # noqa: E402
import psycopg2           # noqa: E402
import matplotlib         # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt       # noqa: E402
import matplotlib.font_manager as fm  # noqa: E402

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSVD, FIGD = f"{OUT}/csv", f"{OUT}/fig"
DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
CLASSES = ["normal", "falldown", "fire", "smoke"]
ORDER = ["falldown", "fire", "smoke", "normal"]
KS = [10, 20, 40, 80]
KMAX = 80
LAMBDAS = [0.5, 0.0]
DUP_COS = 0.95
AGREE_MIN = 0.7
NBOOT = 2000
BASE = "v1.0.8.0"
COL = {"base": "#8a887f", "draft": "#4a3aa7", "random": "#c3c2b7", "loud": "#eb6834"}
LIMIT_NOTE = ("친화도 적재는 9현장 55군집만 — fire 다수 현장(fire_smoke·cohort-b·appdata 등) 미포함. "
              "fire 클래스 선택 근거가 가장 약하다.")
T0 = time.time()


def log(m):
    print(f"[{time.strftime('%H:%M:%S')} +{time.time()-T0:6.0f}s] {m}", flush=True)


def wcsv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    log(f"→ {path} {len(rows)}행")


def vkey(b):
    return tuple(int(x) for x in b.lstrip("vV").split("."))


# ───────────────────────────── 1) 적재 ─────────────────────────────
log("문장 벡터 적재…")
conn = psycopg2.connect(DSN)
cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
log(f"문장 벡터 {SENT.shape}, L2 오차 max {abs(np.linalg.norm(SENT, axis=1) - 1).max():.2e}")

cur.execute("""
  WITH c AS (SELECT content_hash, class_label, count(*) n,
                    row_number() OVER (PARTITION BY content_hash ORDER BY count(*) DESC, class_label) rn,
                    sum(count(*)) OVER (PARTITION BY content_hash) tot
             FROM bank_sentences GROUP BY 1,2)
  SELECT c.content_hash, c.class_label, c.n::float8/c.tot, t.txt
  FROM c JOIN (SELECT content_hash, MIN(text) txt FROM bank_sentences GROUP BY 1) t USING(content_hash)
  WHERE c.rn = 1""")
maj_cls, agree, text = {}, {}, {}
for h, c, a, t in cur:
    maj_cls[h], agree[h], text[h] = c, float(a), t
log(f"문장 클래스 다수결 {len(maj_cls):,}")
assert set(maj_cls) == set(h2c), "bank_sentences 해시 집합 ≠ prompt 임베딩 해시 집합"

cur.execute("SELECT DISTINCT group_key FROM analysis.sentence_affinity WHERE group_kind='cluster' ORDER BY 1")
groups = [g for (g,) in cur]
gi = {g: k for k, g in enumerate(groups)}
projects = sorted({g.split("#")[0] for g in groups})
log(f"군집 {len(groups)} / 현장 {len(projects)}: {projects}")

A = np.full((SENT.shape[0], len(groups)), np.nan, np.float32)
NF = np.zeros(len(groups), np.int64)
n_rows = 0
with conn.cursor(name="aff") as c2:
    c2.itersize = 200000
    c2.execute("SELECT content_hash, group_key, n_frames, mean_cos FROM analysis.sentence_affinity "
               "WHERE group_kind='cluster'")
    for h, g, nf, mc in c2:
        A[h2c[h], gi[g]] = mc
        NF[gi[g]] = nf
        n_rows += 1
nan_share = float(np.isnan(A).mean())
log(f"[SELF-CHECK] 친화도 행렬 {A.shape}, DB 행 {n_rows:,}, 결손 {nan_share:.4%}, "
    f"군집 프레임수 {NF.min():,}~{NF.max():,}")
assert A.shape == (121614, 55), A.shape
assert nan_share < 0.01, nan_share
log("[SELF-CHECK] ✅ 친화도 121,614 × 55, 결손 < 1%")

# ───────────────────────────── 2) 특이도 / 커버리지 가중 ─────────────────────────────
m_s = np.nanmean(A, axis=1)                      # 문장 주효과 ("어디서나 큰가")
R = A - m_s[:, None]
colmean_R = np.nanmean(R, axis=0)
bad = np.isnan(R)
R[bad] = np.take(colmean_R, np.where(bad)[1])     # 결손 → 군집 평균 (→ z=0, 중립)
Z = (R - R.mean(0)) / (R.std(0) + 1e-9)
W = np.clip(Z, 0, None).astype(np.float32)
log(f"특이도 Z: mean {Z.mean():+.3f} sd {Z.std():.3f} / W>0 비율 {(W > 0).mean():.1%} / "
    f"m_s {m_s.min():.3f}~{m_s.max():.3f} (평균 {m_s.mean():.3f})")

hashes = [None] * SENT.shape[0]
for h, i in h2c.items():
    hashes[i] = h
mcls = np.array([maj_cls.get(h, "") for h in hashes])
magr = np.array([agree.get(h, 0.0) for h in hashes])
cands = {c: np.where((mcls == c) & (magr >= AGREE_MIN))[0] for c in ORDER}
for c in ORDER:
    log(f"후보 {c:<9} {len(cands[c]):>6,} (다수결 {int((mcls == c).sum()):,} 중 합의도 ≥ {AGREE_MIN})")


def cov(idx):
    idx = np.asarray(idx, dtype=np.int64)
    return float(W[idx].max(0).sum()) if len(idx) else 0.0


F_all = {c: cov(cands[c]) for c in ORDER}
log("F(전체 후보) = " + "  ".join(f"{c} {F_all[c]:.1f}" for c in ORDER))


# ───────────────────────────── 3) lazy greedy (CELF) ─────────────────────────────
def greedy(cand, lam, kmax=KMAX):
    """반환 dict(sel, gain_raw, gain_pen, F). 탐욕 기준 = 한계이득 − λ·z(m_s)."""
    Wc = W[cand]
    z = (m_s[cand] - m_s[cand].mean()) / (m_s[cand].std() + 1e-12)
    penal = (lam * z).astype(np.float32)
    curv = np.zeros(W.shape[1], np.float32)
    alive = np.ones(len(cand), bool)
    heap = [(-(float(Wc[i].sum()) - float(penal[i])), int(i), 0) for i in range(len(cand))]
    heapq.heapify(heap)
    sel, g_raw, g_pen, Fc, n_eval = [], [], [], [], 0
    for it in range(kmax):
        while True:
            neg, i, stamp = heapq.heappop(heap)
            if not alive[i]:
                continue
            if stamp == it:
                break
            n_eval += 1
            g = float(np.maximum(Wc[i] - curv, 0).sum()) - float(penal[i])
            heapq.heappush(heap, (-g, i, it))
        # 브루트포스 대조 — lazy 가 진짜 최댓값을 골랐는지 (동률 허용, 값으로 비교)
        gv = np.maximum(Wc - curv, 0).sum(1) - penal
        gv[~alive] = -np.inf
        assert abs(float(gv.max()) - (-neg)) < 1e-3 + 1e-5 * abs(float(gv.max())), \
            f"lazy≠brute at it={it}: {gv.max()} vs {-neg}"
        raw = float(np.maximum(Wc[i] - curv, 0).sum())
        curv = np.maximum(curv, Wc[i])
        sel.append(int(cand[i])); g_raw.append(raw); g_pen.append(float(-neg)); Fc.append(float(curv.sum()))
        alive[i] = False
        d = SENT @ SENT[cand[i]]                       # 근사중복 제거 (같은 클래스 후보에만 적용)
        alive &= (d[cand] <= DUP_COS)
        alive[i] = False
        if not alive.any():
            log(f"  ⚠️ 후보 소진 at k={it+1}")
            break
    return dict(sel=sel, gain_raw=g_raw, gain_pen=g_pen, F=Fc, n_eval=n_eval)


runs = {}
for c in ORDER:
    for lam in LAMBDAS:
        t = time.time()
        r = greedy(cands[c], lam)
        runs[(c, lam)] = r
        gp = np.array(r["gain_pen"])
        assert (np.diff(gp) <= 1e-5).all(), f"{c} λ={lam}: 탐욕 기준이 증가했다 → 부분모듈성 위반"
        raw_ok = bool((np.diff(np.array(r["gain_raw"])) <= 1e-5).all())
        log(f"[SELF-CHECK] greedy {c:<9} λ={lam} k={len(r['sel'])} "
            f"F={r['F'][-1]:.1f}/{F_all[c]:.1f}={r['F'][-1]/F_all[c]:.1%} "
            f"기준 비증가 ✅ (원한계이득도 비증가: {raw_ok}) 재평가 {r['n_eval']:,}회 {time.time()-t:.0f}s")
log("[SELF-CHECK] ✅ 모든 실행에서 탐욕 한계이득 비증가(부분모듈성)")

# ───────────────────────────── 4) 베이스라인 커버리지 ─────────────────────────────
banks_db = {b["version"]: b for b in load_banks(cur, [BASE])}
assert BASE in banks_db, f"{BASE} 뱅크 없음"
bank_cols_by_cls, bank_lab = {}, []
bcols = []
for chash, cls, _g in banks_db[BASE]["rows"]:
    assert cls in CLASSES, f"{BASE} 에 예상 밖 클래스 {cls}"
    bank_cols_by_cls.setdefault(cls, []).append(h2c[chash])
    bcols.append(h2c[chash]); bank_lab.append(CLASSES.index(cls))
bcols = np.asarray(bcols); bank_lab = np.asarray(bank_lab, np.int32)
log(f"{BASE} 문장 {len(bcols):,} = " + " ".join(f"{c} {len(v):,}" for c, v in sorted(bank_cols_by_cls.items())))

RNG = np.random.default_rng(0)
curves, cov_extra = [], {}
for c in ORDER:
    cd = cands[c]
    for lam in LAMBDAS:
        r = runs[(c, lam)]
        for k in KS:
            curves.append((c, f"submodular_l{lam:g}", k, r["F"][min(k, len(r["F"])) - 1] / F_all[c]))
    # (i) 무작위 20회 평균
    rnd = np.zeros(len(KS))
    for _ in range(20):
        pick = RNG.choice(cd, size=min(KMAX, len(cd)), replace=False)
        rnd += [cov(pick[:k]) / F_all[c] for k in KS]
    for k, v in zip(KS, rnd / 20):
        curves.append((c, "random", k, float(v)))
    # (ii) 주효과 상위 (시끄러운 문장)
    loud = cd[np.argsort(-m_s[cd])]
    for k in KS:
        curves.append((c, "top_by_mean", k, cov(loud[:k]) / F_all[c]))
    # (iii) v1.0.8.0 실제 뱅크 (해당 클래스) — 자기 크기 + W합 상위 k
    bc = np.asarray(bank_cols_by_cls.get(c, []), dtype=np.int64)
    bo = bc[np.argsort(-W[bc].sum(1))] if len(bc) else bc
    for k in KS:
        curves.append((c, "bank_v1080", k, cov(bo[:k]) / F_all[c]))
    curves.append((c, "bank_v1080", int(len(bc)), cov(bc) / F_all[c]))
    cov_extra[c] = dict(bank_own_size=int(len(bc)), bank_own_cov=cov(bc) / F_all[c],
                        bank_k40=cov(bo[:40]) / F_all[c], loud40=cov(loud[:40]) / F_all[c],
                        F_all=F_all[c], n_cand=int(len(cd)))
    log(f"커버리지@40 {c:<9} 서브모듈 {runs[(c, 0.5)]['F'][39]/F_all[c]:.1%} "
        f"(λ=0 {runs[(c, 0.0)]['F'][39]/F_all[c]:.1%})  무작위 {rnd[2]/20:.1%}  "
        f"시끄러운 {cov(loud[:40])/F_all[c]:.1%}  {BASE}@40 {cov(bo[:40])/F_all[c]:.1%}  "
        f"{BASE}@자기크기({len(bc):,}) {cov(bc)/F_all[c]:.1%}")

wcsv(f"{CSVD}/22b_coverage_curves.csv",
     ["class(클래스)", "method(선택방법)", "k(문장수)", "coverage_share(커버리지비율)"],
     [(c, m, k, round(v, 4)) for c, m, k, v in curves])

# ───────────────────────────── 5) 초안 CSV ─────────────────────────────
rows22 = []
for c in ORDER:
    for lam in LAMBDAS:
        r = runs[(c, lam)]
        sel = np.asarray(r["sel"])
        Wsel = W[sel]                          # [k, G] — 최종 집합(k=80) 기준 argmax 제공자
        best = Wsel.argmax(0)
        has = Wsel.max(0) > 0
        nbest = np.bincount(best[has], minlength=len(sel))
        for rk, s in enumerate(sel):
            h = hashes[s]
            rows22.append((c, rk + 1, h, text[h], round(r["gain_raw"][rk], 4),
                           round(r["F"][rk] / F_all[c], 4), round(float(m_s[s]), 4),
                           round(float(agree[h]), 3), int(nbest[rk]), lam))
wcsv(f"{CSVD}/22_submodular_bank_draft.csv",
     ["class(클래스)", "rank(순위)", "content_hash(문장해시)", "text(문장)", "marginal_gain(한계이득)",
      "cumulative_coverage_share(누적커버리지비율)", "main_effect_m(주효과평균코사인)",
      "class_agreement(클래스합의도)", "n_groups_covered_best(최선제공군집수·k=80집합기준)", "lambda(주효과패널티)"],
     rows22)

# ───────────────────────────── 6) 초안 뱅크 구성 ─────────────────────────────
def make_draft(k, lam=0.5):
    cols, lab = [], []
    for c in CLASSES:                                  # 열 순서는 CLASSES 순
        s = runs[(c, lam)]["sel"][:k]
        cols += s
        lab += [CLASSES.index(c)] * len(s)
    return np.asarray(cols), np.asarray(lab, np.int32)


draft40_cols, draft40_lab = make_draft(40)
draft80_cols, draft80_lab = make_draft(80)
assert len(draft40_cols) == 160 and len({hashes[i] for i in draft40_cols}) == 160, \
    f"draft-40 고유 해시 {len({hashes[i] for i in draft40_cols})}"
log(f"[SELF-CHECK] ✅ draft-40 = 160 문장 / 고유 content_hash 160개 (draft-80 = {len(draft80_cols)})")

R2 = np.random.default_rng(1)
rnd_cols, rnd_lab = [], []
loud_cols, loud_lab = [], []
for c in CLASSES:
    cd = cands[c]
    rp = R2.choice(cd, size=40, replace=False)
    rnd_cols += list(rp); rnd_lab += [CLASSES.index(c)] * 40
    lp = cd[np.argsort(-m_s[cd])][:40]
    loud_cols += list(lp); loud_lab += [CLASSES.index(c)] * 40
rnd_cols = np.asarray(rnd_cols); rnd_lab = np.asarray(rnd_lab, np.int32)
loud_cols = np.asarray(loud_cols); loud_lab = np.asarray(loud_lab, np.int32)

DRAFTS = {"draft40": (draft40_cols, draft40_lab, 40), "draft80": (draft80_cols, draft80_lab, 80),
          "random40": (rnd_cols, rnd_lab, 40), "loud40": (loud_cols, loud_lab, 40)}

# ───────────────────────────── 7) sourcei GT 평가 ─────────────────────────────
import fiftyone as fo                                   # noqa: E402
from fiftyone import ViewField as FF                    # noqa: E402

d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
ds = fo.load_dataset("sourcei")
hids, hemb, hgt, hcam = ds.values(["id", "embedding", "ground_truth.label", "camera"])
assert list(hids) == list(d["ids"]), "sourcei id 순서가 preds.npz 와 다르다"
Fh = np.asarray(hemb, np.float32)
Fh /= np.linalg.norm(Fh, axis=1, keepdims=True)
gt = np.asarray([CLASSES.index(x) for x in hgt], np.int8)
assert (gt == d["gt"]).all()
cam = np.asarray(hcam)
cams = np.unique(cam)
cidx = np.searchsorted(cams, cam)
log(f"sourcei {Fh.shape} / 카메라 {len(cams)} / GT {dict(zip(CLASSES, np.bincount(gt, minlength=4).tolist()))}")

base_pred = d[f"topk__{BASE}"]
base_acc = float((base_pred == gt).mean())
log(f"[SELF-CHECK] {BASE} top-K 정확도 재계산 {base_acc:.4f} (기대 0.706 ± 0.001)")
assert abs(base_acc - 0.706) <= 0.001, base_acc
log("[SELF-CHECK] ✅ 기준선 정확도 0.706 재현")


def macro_f1(g, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = ((p == c) & (g == c)).sum(); fp = ((p == c) & (g != c)).sum(); fn = ((p != c) & (g == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


def tabulate(p):
    """카메라별 (클래스 1..3) × (tp, fp, fn) — 카운트는 카메라에 대해 가법이라 부트스트랩이 정확해진다."""
    T = np.zeros((len(cams), 3, 3), np.float64)
    for k, c in enumerate((1, 2, 3)):
        pc, gc = (p == c), (gt == c)
        np.add.at(T[:, k, 0], cidx[pc & gc], 1)
        np.add.at(T[:, k, 1], cidx[pc & ~gc], 1)
        np.add.at(T[:, k, 2], cidx[~pc & gc], 1)
    return T


def mf1_tab(T):
    tp, fp, fn = T[..., 0], T[..., 1], T[..., 2]
    pr = tp / np.maximum(tp + fp, 1); rc = tp / np.maximum(tp + fn, 1)
    return (2 * pr * rc / np.maximum(pr + rc, 1e-12)).mean(-1)


BR = np.random.default_rng(0)
pick = BR.integers(0, len(cams), size=(NBOOT, len(cams)))
Wm = (pick[:, :, None] == np.arange(len(cams))[None, None, :]).sum(1).astype(np.float64)
idx_by_cam = [np.where(cidx == i)[0] for i in range(len(cams))]

Tb = tabulate(base_pred)
assert abs(mf1_tab(Tb.sum(0)) - macro_f1(gt, base_pred)) < 1e-9, "표 기반 macro-F1 ≠ 직접 계산"
_bt = np.einsum("bc,cij->bij", Wm[:20], Tb)
_direct = np.array([macro_f1(gt[i], base_pred[i]) for i in
                    [np.concatenate([idx_by_cam[j] for j in pick[b]]) for b in range(20)]])
assert np.allclose(mf1_tab(_bt), _direct, atol=1e-9), "부트스트랩 표 근사가 직접 재표집과 불일치"
log("[SELF-CHECK] ✅ 카메라 표 기반 부트스트랩 == 인덱스 재표집(20표본 대조)")


def predict(cols, lab, Fmat, chunk=1000):
    Sc = SENT[np.asarray(cols)]
    out = np.empty(len(Fmat), np.int8)
    for s in range(0, len(Fmat), chunk):
        out[s:s + chunk] = topk_vote(Fmat[s:s + chunk] @ Sc.T, lab, 4, k=RULE_K)
    return out


boot, point, preds_hy = {}, {}, {}
for name, (cols, lab, _k) in DRAFTS.items():
    p = predict(cols, lab, Fh)
    preds_hy[name] = p
    point[name] = dict(acc=float((p == gt).mean()), mf1=macro_f1(gt, p),
                       rec={c: float((p[gt == CLASSES.index(c)] == CLASSES.index(c)).mean()) for c in CLASSES[1:]})
    boot[name] = mf1_tab(np.einsum("bc,cij->bij", Wm, tabulate(p)))
    log(f"{name:<9} acc {point[name]['acc']:.4f} macroF1 {point[name]['mf1']:.4f} "
        + " ".join(f"{c[:4]} {point[name]['rec'][c]:.3f}" for c in CLASSES[1:]))

all_banks = sorted([b for b in d["banks"] if not b.startswith("v2.")], key=vkey)
assert len(all_banks) == 31, len(all_banks)
for b in all_banks:
    p = d[f"topk__{b}"]
    point[b] = dict(acc=float((p == gt).mean()), mf1=macro_f1(gt, p),
                    rec={c: float((p[gt == CLASSES.index(c)] == CLASSES.index(c)).mean()) for c in CLASSES[1:]})
    boot[b] = mf1_tab(np.einsum("bc,cij->bij", Wm, tabulate(p)))
log(f"기준 뱅크 {len(all_banks)}종 부트스트랩 완료 (v2.* 4종 제외 — 이벤트 클래스 없음)")


rec_normal = {n: float((preds_hy[n][gt == 0] == 0).mean()) for n in DRAFTS}
for b in all_banks:
    rec_normal[b] = float((d[f"topk__{b}"][gt == 0] == 0).mean())
log("normal 재현율 " + " ".join(f"{n} {rec_normal[n]:.3f}" for n in list(DRAFTS) + [BASE]))


def delta(a, b):
    dd = boot[a] - boot[b]
    return dict(d=point[a]["mf1"] - point[b]["mf1"], mean=float(dd.mean()),
                lo=float(np.percentile(dd, 2.5)), hi=float(np.percentile(dd, 97.5)),
                p=float((dd > 0).mean()))


# ───────────────────────────── 8) frames 약참조 점검 ─────────────────────────────
fr_res, fr_err = {}, None
try:
    log("frames 표본 적재…")
    frv = fo.load_dataset("frames").match(FF("modality") == "frame")
    fids, ncls_raw = frv.values(["id", "normalized_class"])
    ncls = np.array([x or "none" for x in ncls_raw])
    fire_i = np.where(ncls == "fire")[0]; sm_i = np.where(ncls == "smoke")[0]
    neg_i = np.where(np.isin(ncls, ["none", "person"]))[0]
    FRNG = np.random.default_rng(0)
    sub = np.concatenate([fire_i, sm_i, FRNG.choice(neg_i, size=min(20000, len(neg_i)), replace=False)])
    log(f"frames 표본 {len(sub):,} = fire {len(fire_i):,}(기대 1,578) + smoke {len(sm_i):,}(기대 3,214) "
        f"+ neg {len(sub)-len(fire_i)-len(sm_i):,}")
    sel_ids = [fids[i] for i in sub]
    pos = {s: k for k, s in enumerate(sel_ids)}
    Fm = np.zeros((len(sub), SENT.shape[1]), np.float32)
    filled = np.zeros(len(sub), bool)
    for s in range(0, len(sel_ids), 4000):
        gi2, ge2 = frv.select(sel_ids[s:s + 4000]).values(["id", "image_embedding"])
        for a, b2 in zip(gi2, ge2):
            if b2 is not None:
                Fm[pos[a]] = b2; filled[pos[a]] = True
        log(f"  임베딩 {min(s+4000, len(sel_ids)):,}/{len(sel_ids):,}")
    log(f"임베딩 확보 {int(filled.sum()):,}/{len(sub):,}")
    ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], np.int8)[filled]
    Fm = Fm[filled]
    Fm /= np.linalg.norm(Fm, axis=1, keepdims=True)
    for name, (cols, lab, _k) in list(DRAFTS.items()) + [(BASE, (bcols, bank_lab, None))]:
        p = predict(cols, lab, Fm, chunk=2000)
        fr_res[name] = dict(fire_recall=float((p[ref == 2] == 2).mean()), smoke_recall=float((p[ref == 3] == 3).mean()),
                            fp=float((p[ref == 0] != 0).mean()), fp_fire=float((p[ref == 0] == 2).mean()),
                            firing=float((p != 0).mean()), n=int(len(p)))
        log(f"frames {name:<9} fire재현 {fr_res[name]['fire_recall']:.3f} smoke재현 {fr_res[name]['smoke_recall']:.3f} "
            f"오탐(이벤트) {fr_res[name]['fp']:.3%} 오탐(fire) {fr_res[name]['fp_fire']:.3%} 발화율 {fr_res[name]['firing']:.3%}")
except Exception:
    fr_err = traceback.format_exc()
    log("⚠️ frames 약참조 점검 실패 — 아래 트레이스백 원문:\n" + fr_err)

# ───────────────────────────── 9) 22c CSV ─────────────────────────────
rows22c = []
for name, (_c, _l, k) in DRAFTS.items():
    dv1080 = delta(name, BASE)
    f = fr_res.get(name, {})
    for b in all_banks:
        db = delta(name, b)
        rows22c.append((name, k, b, round(point[name]["acc"], 4), round(point[name]["mf1"], 4),
                        round(point[name]["rec"]["falldown"], 4), round(point[name]["rec"]["fire"], 4),
                        round(point[name]["rec"]["smoke"], 4), round(dv1080["d"], 4),
                        round(db["d"], 4), round(db["mean"], 4), round(db["lo"], 4), round(db["hi"], 4),
                        round(db["p"], 4), round(point[b]["mf1"], 4),
                        "" if not f else round(f["fire_recall"], 4), "" if not f else round(f["smoke_recall"], 4),
                        "" if not f else round(f["fp"], 5), "" if not f else round(f["firing"], 5)))
wcsv(f"{CSVD}/22c_draft_eval.csv",
     ["draft(초안)", "k(클래스당문장수)", "baseline_bank(기준뱅크)", "hy_acc(정확도)", "hy_mf1(macroF1)",
      "hy_recall_falldown(재현율)", "hy_recall_fire(재현율)", "hy_recall_smoke(재현율)",
      "d_mf1_vs_v1080(ΔmacroF1_v1.0.8.0대비)", "d_mf1_vs_baseline(ΔmacroF1_기준뱅크대비)",
      "ci_mean(부트스트랩평균Δ)", "ci_lo(2.5%)", "ci_hi(97.5%)", "p_gt0(PΔ>0)", "baseline_mf1(기준뱅크macroF1)",
      "fr_fire_recall(frames)", "fr_smoke_recall(frames)", "fr_fp(frames오탐율)", "fr_firing_rate(frames발화율)"],
     rows22c)

# ───────────────────────────── 10) 그림 ─────────────────────────────
for f in glob.glob("/workspace/.fonts/*.tt[fc]"):
    fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.color": "#e6e5e1",
                     "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
                     "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e",
                     "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CV = {(c, m, k): v for c, m, k, v in curves}

fig, axes = plt.subplots(2, 2, figsize=(11, 8.2))
for ax, c in zip(axes.ravel(), ORDER):
    series = [("submodular_l0.5", COL["draft"], "-", "서브모듈 탐욕 λ=0.5", 2.2),
              ("submodular_l0", COL["draft"], "--", "서브모듈 탐욕 λ=0", 1.4),
              ("top_by_mean", COL["loud"], "-", "주효과 상위(시끄러운 문장)", 1.6),
              ("random", COL["random"], "-", "무작위(20회 평균)", 1.6),
              ("bank_v1080", COL["base"], "-", f"{BASE} 뱅크(W합 상위)", 1.6)]
    for m, col, ls, lb, lw in series:
        ax.plot(KS, [CV[(c, m, k)] for k in KS], ls, color=col, lw=lw, marker="o", ms=4, label=lb)
    own = cov_extra[c]
    ax.axhline(own["bank_own_cov"], color=COL["base"], ls=":", lw=1.2)
    ax.text(KS[-1], own["bank_own_cov"], f" {BASE} 전량 {own['bank_own_size']:,}문장 {own['bank_own_cov']:.0%}",
            color=COL["base"], fontsize=8, va="bottom", ha="right")
    ax.set_xscale("log"); ax.set_xticks(KS); ax.set_xticklabels([str(k) for k in KS])
    ax.set_xticks([], minor=True)
    ax.set_ylim(0, 1.02)
    ax.set_title(f"{c}  (후보 {own['n_cand']:,}문장 · F(전체)={own['F_all']:.0f})", loc="left", fontsize=11)
    ax.set_xlabel("선택 문장 수 k"); ax.set_ylabel("커버리지 비율 F(S)/F(전체후보)")
axes[0, 0].legend(fontsize=8, frameon=False, loc="lower right")
fig.suptitle("라벨 없이 고른 뱅크의 군집 커버리지 — 시설입지 탐욕 vs 베이스라인 (55군집·9현장)",
             x=0.012, ha="left", fontsize=13)
fig.text(0.012, 0.005, "⚠️ " + LIMIT_NOTE, fontsize=8, color="#52514e")
fig.tight_layout(rect=[0, 0.02, 1, 0.96])
fig.savefig(f"{FIGD}/f24_coverage_curves.png", dpi=160)
plt.close(fig)
log(f"→ {FIGD}/f24_coverage_curves.png")

fig = plt.figure(figsize=(14, 9))
gs = fig.add_gridspec(2, 2, width_ratios=[1, 1.15], height_ratios=[1, 1.25], wspace=0.32, hspace=0.42)
ax1 = fig.add_subplot(gs[0, 0])
names = ["draft40", "draft80", "random40", "loud40"]
lbl = {"draft40": "서브모듈 초안 k=40", "draft80": "서브모듈 초안 k=80",
       "random40": "무작위 40", "loud40": "주효과 상위 40"}
cc = {"draft40": COL["draft"], "draft80": COL["draft"], "random40": COL["random"], "loud40": COL["loud"]}
for i, n in enumerate(names):
    dd = delta(n, BASE)
    ax1.errorbar(dd["d"], i, xerr=[[dd["d"] - dd["lo"]], [dd["hi"] - dd["d"]]], fmt="o", ms=7,
                 color=cc[n], ecolor=cc[n], elinewidth=2, capsize=4,
                 markerfacecolor=("none" if n == "draft80" else cc[n]))
    ax1.text(dd["hi"] + 0.004, i, f"P(Δ>0)={dd['p']:.2f}", va="center", fontsize=8, color="#52514e")
    ax1.text(dd["lo"] - 0.004, i, f"acc {point[n]['acc']:.2f} / F1 {point[n]['mf1']:.3f}",
             va="center", ha="right", fontsize=8, color="#52514e")
ax1.axvline(0, color="#0b0b0b", lw=1)
ax1.set_yticks(range(len(names))); ax1.set_yticklabels([lbl[n] for n in names])
ax1.set_ylim(-0.6, len(names) - 0.4); ax1.invert_yaxis()
ax1.set_xlabel(f"Δ macro-F1 (초안 − {BASE}), 카메라 군집 부트스트랩 95% CI")
ax1.set_xlim(-0.42, 0.30)
ax1.set_title(f"① 초안 vs 기준선 {BASE} — 이벤트 3클래스 macro-F1 차이 "
              f"(기준선 acc {point[BASE]['acc']:.2f} / F1 {point[BASE]['mf1']:.3f})", loc="left", fontsize=11)

ax2 = fig.add_subplot(gs[1, 0])
ev = CLASSES
x = np.arange(len(ev)); wbar = 0.16
for i, n in enumerate(names + [BASE]):
    col = cc.get(n, COL["base"])
    r = [point[n]["rec"].get(c, rec_normal[n]) for c in ev]
    ax2.bar(x + (i - 2) * wbar, r, wbar, color=col, edgecolor="none",
            hatch=("//" if n == "draft80" else None), label=lbl.get(n, f"기준선 {BASE}"))
ax2.set_xticks(x); ax2.set_xticklabels(ev)
ax2.set_ylabel("재현율"); ax2.set_ylim(0, 1)
ax2.legend(fontsize=8, frameon=False, ncol=2)
ax2.set_title("② 클래스별 재현율 (sourcei GT 7,498 프레임) — normal 재현율이 정확도를 좌우한다",
              loc="left", fontsize=11)

ax3 = fig.add_subplot(gs[:, 1])
ys = np.arange(len(all_banks))
win = tie = lose = 0
for i, b in enumerate(all_banks):
    dd = delta("draft40", b)
    if dd["lo"] > 0:
        col = COL["draft"]; win += 1
    elif dd["hi"] < 0:
        col = COL["loud"]; lose += 1
    else:
        col = COL["random"]; tie += 1
    ax3.errorbar(dd["d"], i, xerr=[[dd["d"] - dd["lo"]], [dd["hi"] - dd["d"]]], fmt="o", ms=5,
                 color=col, ecolor=col, elinewidth=1.6, capsize=3)
ax3.axvline(0, color="#0b0b0b", lw=1)
ax3.set_yticks(ys); ax3.set_yticklabels(all_banks, fontsize=8)
ax3.invert_yaxis(); ax3.set_ylim(len(all_banks) - 0.4, -0.6)
ax3.set_xlabel("Δ macro-F1 (서브모듈 초안 k=40 − 기준 뱅크)")
ax3.set_title(f"③ 초안 k=40 vs 전 뱅크 31종 — 파랑 CI>0 승 {win} · 회색 무승부 {tie} · 주황 CI<0 패 {lose}",
              loc="left", fontsize=11)
fig.suptitle("라벨 없이 고른 160문장 초안의 sourcei GT 성능 — 카메라 군집 부트스트랩 2,000회",
             x=0.012, ha="left", fontsize=13)
fig.text(0.012, 0.005, "⚠️ " + LIMIT_NOTE + "  선택에 GT 미사용(무감독).", fontsize=8, color="#52514e")
fig.savefig(f"{FIGD}/f25_draft_vs_baseline.png", dpi=160, bbox_inches="tight")
plt.close(fig)
log(f"→ {FIGD}/f25_draft_vs_baseline.png")

# ───────────────────────────── 11) 요약 JSON ─────────────────────────────
summary = dict(
    generated=time.strftime("%Y-%m-%d %H:%M:%S"),
    coverage_limitation=LIMIT_NOTE, projects=projects, n_groups=len(groups),
    affinity=dict(shape=list(A.shape), nan_share=nan_share, rows=n_rows),
    params=dict(lambda_main_effect=0.5, dup_cos=DUP_COS, agree_min=AGREE_MIN, ks=KS,
                rule="top-K vote", K=RULE_K, n_boot=NBOOT, boot_unit="camera(15)",
                zscore_scope="클래스 후보군 내"),
    candidates={c: int(len(cands[c])) for c in ORDER},
    coverage={c: dict(cov_extra[c], submodular_l05={str(k): runs[(c, 0.5)]["F"][k - 1] / F_all[c] for k in KS},
                      submodular_l0={str(k): runs[(c, 0.0)]["F"][k - 1] / F_all[c] for k in KS},
                      random={str(k): CV[(c, "random", k)] for k in KS},
                      top_by_mean={str(k): CV[(c, "top_by_mean", k)] for k in KS},
                      bank_v1080={str(k): CV[(c, "bank_v1080", k)] for k in KS}) for c in ORDER},
    sourcei=dict(baseline=BASE, baseline_acc=base_acc, baseline_mf1=point[BASE]["mf1"],
                 recall_normal={n: rec_normal[n] for n in list(DRAFTS) + [BASE]},
                 drafts={n: dict(point[n], vs_v1080=delta(n, BASE)) for n in names}),
    vs_all_banks={n: dict(n_banks=len(all_banks),
                          win=int(sum(delta(n, b)["lo"] > 0 for b in all_banks)),
                          tie=int(sum(delta(n, b)["lo"] <= 0 <= delta(n, b)["hi"] for b in all_banks)),
                          lose=int(sum(delta(n, b)["hi"] < 0 for b in all_banks)),
                          per_bank={b: delta(n, b) for b in all_banks}) for n in names},
    frames=dict(result=fr_res, error=fr_err),
    examples={c: [dict(rank=r + 1, gain=round(runs[(c, 0.5)]["gain_raw"][r], 3),
                       text=text[hashes[runs[(c, 0.5)]["sel"][r]]]) for r in range(3)] for c in ORDER},
)
json.dump(summary, open(f"{OUT}/submodular_summary.json", "w"), ensure_ascii=False, indent=1)
log(f"→ {OUT}/submodular_summary.json")

log("=== 클래스별 상위 3문장 (λ=0.5) ===")
for c in ORDER:
    for r in range(3):
        s = runs[(c, 0.5)]["sel"][r]
        log(f"  {c:<9} #{r+1} gain={runs[(c, 0.5)]['gain_raw'][r]:7.3f}  {text[hashes[s]]}")
for n in names:
    dd = delta(n, BASE)
    w = sum(delta(n, b)["lo"] > 0 for b in all_banks); l = sum(delta(n, b)["hi"] < 0 for b in all_banks)
    log(f"{n:<9} Δ vs {BASE} {dd['d']:+.4f} [{dd['lo']:+.4f}, {dd['hi']:+.4f}] P(Δ>0)={dd['p']:.3f} "
        f"| 31뱅크 대비 승 {w} 무 {31-w-l} 패 {l}")
log("DONE")
