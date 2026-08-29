#!/usr/bin/env python3
# ⚠️ 폐기 — 2026-08-27 공유 호스트에서 뱅크 v1.0.12.0(49,140문장)에서 `7498 × 49140`(1.5GB)를
#    한 번에 잡아 OOM(Killed). 뱅크 4개 처리 후 죽어 CSV 를 한 줄도 못 남겼다.
#    후속: `e1c3d3_v2.py` — 프레임 행 청크(1,500) + 뱅크당 체크포인트 + 중복마스크 1회 계산.
#    이 파일은 히스토리로만 남긴다. 실행하지 말 것.
"""E1 프루닝 3컷 · C3 Biclustering · D3 문장 방향 산술 — 한 번의 적재로 셋을 함께 낸다.

공통 1패스: 군집 배정된 프레임 90,084 × 문장 121,614 코사인을 스트리밍해
  · m_s        = 문장별 전역 배경 평균 (주효과)
  · A_k[s,k]   = 문장 × kmeans64 군집 평균  → 특이도 SD = std_k(A_k − rowmean)
를 만든다. 이 둘이 E1 의 두 컷 기준이고 C3 의 입력 행렬이다.

E1: 라벨 없이 (a) 주효과 상위 컷 (b) 특이도 부재 컷 (c) 중복(코사인>0.95) 컷 을 적용하고
    **비열등성**을 카메라 군집 부트스트랩으로 본다. 목표는 성능 향상이 아니라 "몇 %를 지워도
    성능이 안 떨어지나"(유지비 절감)다 — §3 의 유효문장 4% 와 대조.
D3: 문장 방향 산술. s' = normalize(s − μ_normal) 로 문장 자체를 바꿔서 max 풀링한다.
    ⚠️ 정규화 없이 x·(s−μ_n) 를 쓰면 프레임당 상수(x·μ_n)만 빠져 argmax 가 불변이다.
    **정규화가 비선형이라 여기서만 판정이 바뀐다** — 그래서 이 형태로 구현한다.
C3: A_k 의 양의 특이도 행렬에 spectral co-clustering — 문장군 × 군집군 동시 분할.
    "자연 발생 현장군 팩"이 있는지, 그 문장군의 클래스·어휘가 무엇인지 본다.
"""
import os, sys, json, csv, glob, collections
sys.path.insert(0, "/workspace")
os.environ.setdefault("COS_THREADS", "5")
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote, RULE_K
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.cluster import SpectralCoclustering
import fiftyone as fo
from fiftyone import ViewField as F

OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
DSN = "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline"
for f in glob.glob("/workspace/.fonts/*.tt[fc]"): fm.fontManager.addfont(f)
plt.rcParams.update({"font.family": "Noto Sans CJK JP", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
  "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6, "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb",
  "axes.facecolor": "#fcfcfb", "text.color": "#0b0b0b", "axes.labelcolor": "#52514e", "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False})
CLASSES = ["normal", "falldown", "fire", "smoke"]
CC = {"normal": "#8a887f", "falldown": "#eda100", "fire": "#e34948", "smoke": "#4a3aa7"}
RNG = np.random.default_rng(0); T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)

conn = psycopg2.connect(DSN); cur = conn.cursor()
h2c, SENT = load_sentence_vectors(cur)
hashes = [None] * len(h2c)
for h, i in h2c.items(): hashes[i] = h
NS = SENT.shape[0]
log(f"문장 {SENT.shape}")
cur.execute("SELECT content_hash, class_label, count(*) FROM bank_sentences GROUP BY 1,2")
votes = collections.defaultdict(dict)
for h, c, n in cur: votes[h][c] = n
maj = {h: max(sorted(v), key=lambda k: v[k]) for h, v in votes.items()}
agree = {h: max(v.values()) / sum(v.values()) for h, v in votes.items()}
cur.execute("SELECT content_hash, MIN(text) FROM bank_sentences GROUP BY 1")
text = dict(cur.fetchall())
scls = np.array([CLASSES.index(maj[h]) if h in maj and maj[h] in CLASSES else -1 for h in hashes])
log(f"클래스 분포 {collections.Counter(CLASSES[c] if c >= 0 else 'other' for c in scls)}")

# ── 공통 1패스: m_s, A_k ──────────────────────────────────────────────
_cache = os.path.exists(f"{OUT}/Ak_kmeans64.npy") and os.path.exists(f"{OUT}/m_s_bg90k.npy")
cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
e2k = dict(cur.fetchall()); NK = 64
assert len(e2k) == 90084, len(e2k)
cnt = np.bincount(np.fromiter(e2k.values(), np.int64), minlength=NK)
if _cache:
    Ak = np.load(f"{OUT}/Ak_kmeans64.npy"); m_s = np.load(f"{OUT}/m_s_bg90k.npy"); ntot = 90084
    assert Ak.shape == (NS, NK), Ak.shape
    log("1패스 캐시 재사용 (Ak_kmeans64.npy, m_s_bg90k.npy)")
Ak = Ak if _cache else np.zeros((NS, NK), np.float64)
msum = np.zeros(NS, np.float64); ntot = 90084 if _cache else 0
buf_v, buf_k = [], []
if not _cache:
  cnt = np.zeros(NK, np.int64)
if not _cache:
 with conn.cursor(name="fr") as c2:
    c2.itersize = 4000
    c2.execute("SELECT entity_id, embedding::text FROM image_embeddings WHERE entity_type='frame'")
    for eid, vt in c2:
        k = e2k.get(eid)
        if k is None: continue
        buf_v.append(np.fromstring(vt.strip("[]"), sep=",", dtype=np.float32)); buf_k.append(k)
        if len(buf_v) >= 2000:
            X = np.vstack(buf_v); X /= np.linalg.norm(X, axis=1, keepdims=True)
            S = X @ SENT.T
            msum += S.sum(0); ntot += len(buf_k)
            kk = np.asarray(buf_k)
            for k0 in np.unique(kk):
                mm = kk == k0; Ak[:, k0] += S[mm].sum(0); cnt[k0] += int(mm.sum())
            buf_v, buf_k = [], []
            if ntot % 20000 == 0: log(f"  프레임 {ntot:,}/90,084")
if buf_v:
    X = np.vstack(buf_v); X /= np.linalg.norm(X, axis=1, keepdims=True); S = X @ SENT.T
    msum += S.sum(0); ntot += len(buf_k); kk = np.asarray(buf_k)
    for k0 in np.unique(kk):
        mm = kk == k0; Ak[:, k0] += S[mm].sum(0); cnt[k0] += int(mm.sum())
assert ntot == 90084, ntot
if not _cache:
    m_s = (msum / ntot).astype(np.float32)
    Ak = (Ak / np.maximum(cnt, 1)).astype(np.float32)
# m_s 재구성 검증
recon = (Ak * (cnt / ntot)).sum(1)
assert np.abs(recon - m_s).max() < 1e-4, np.abs(recon - m_s).max()
R = Ak - Ak.mean(1, keepdims=True)
spec_sd = R.std(1)
log(f"1패스 완료 — m_s {m_s.mean():.4f}±{m_s.std():.4f}, 특이도SD {spec_sd.mean():.5f} (재구성오차 {np.abs(recon-m_s).max():.2e})")
if not _cache: np.save(f"{OUT}/Ak_kmeans64.npy", Ak); np.save(f"{OUT}/m_s_bg90k.npy", m_s)

# ── sourcei / frames 표본 ────────────────────────────────────────────
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]; ids = list(d["ids"]); cams = np.unique(cam)
dh = fo.load_dataset("sourcei"); hid, hemb = dh.values(["id", "embedding"])
assert hid == ids
FH = np.asarray(hemb, dtype=np.float32); FH /= np.linalg.norm(FH, axis=1, keepdims=True)
ds = fo.load_dataset("frames"); ncls_raw, femb = ds.match(F("modality") == "frame").values(["normalized_class", "image_embedding"])
ncls = np.array([x or "none" for x in ncls_raw])
fi = np.where(ncls == "fire")[0]; si = np.where(ncls == "smoke")[0]; ni = np.where(np.isin(ncls, ["none", "person"]))[0]
sub = np.concatenate([fi, si, RNG.choice(ni, 20000, replace=False)])
FF = np.asarray([femb[i] for i in sub], dtype=np.float32); FF /= np.linalg.norm(FF, axis=1, keepdims=True)
ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], dtype=np.int8)
assert len(sub) == 24792 and len(fi) == 1578
log(f"sourcei {FH.shape} · frames 표본 {FF.shape}")

def macro_f1(t, p, classes=(1, 2, 3)):
    f = []
    for c in classes:
        tp = int(((p == c) & (t == c)).sum()); fp = int(((p == c) & (t != c)).sum()); fn = int(((p != c) & (t == c)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1); f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))

idx_by_cam = {c: np.where(cam == c)[0] for c in cams}
def paired_ci(p1, p0, nboot=2000):
    vals = []
    for _ in range(nboot):
        pick = RNG.choice(cams, size=len(cams), replace=True)
        mm = np.concatenate([idx_by_cam[c] for c in pick])
        vals.append(macro_f1(gt[mm], p1[mm]) - macro_f1(gt[mm], p0[mm]))
    a = np.array(vals); return float(a.mean()), float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5)), float((a > 0).mean())

# ══════════════════════════════════════════════════════════════════
# E1 — 프루닝 3컷 (비열등성)
# ══════════════════════════════════════════════════════════════════
# 전체 뱅크 35종 중 falldown/fire/smoke 를 하나도 안 가진 4종(v2.0.5.* = ['class_5','normal'])은
# sourcei 4클래스 GT 로 채점이 불가능하다 → 리포트 전체가 쓰는 31종과 **같은 집합**을 쓴다.
_m = json.load(open(f"{OUT}/metrics.json"))
BANKS = [str(b) for b in d["banks"] if set(_m["banks"][str(b)]["classes"]) & {"falldown", "fire", "smoke"}]
assert len(BANKS) == 31, len(BANKS)
FOCUS = ["v1.0.8.0", "v1.0.8.1"]              # 상세 패널·D3 변형 비교용
bank_defs = {b["version"]: b for b in load_banks(cur, BANKS)}
BANKS = [b for b in BANKS if b in bank_defs]
log(f"E1/D3 대상 뱅크 {len(BANKS)}종")
e1_rows, e1_best = [], {}
for bank in BANKS:
    rows_ = bank_defs[bank]["rows"]
    cols, lab_names, seen = [], [], set()
    for h, c, _g in rows_:
        if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); lab_names.append(c)
    cols = np.asarray(cols); cs = sorted(set(lab_names))
    lab = np.array([cs.index(c) for c in lab_names], dtype=np.int32)
    to_gt = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], dtype=np.int8)
    log(f"{bank}: 고유문장 {len(cols):,} 클래스 {cs}")
    S_hy = FH @ SENT[cols].T                                       # [7498, n_bank]
    base_pred = to_gt[topk_vote(S_hy, lab, len(cs))]
    base_mf1, base_acc = macro_f1(gt, base_pred), float((base_pred == gt).mean())
    log(f"  기준선 재현 acc {base_acc:.4f} mF1 {base_mf1:.4f} (저장값 {d[f'topk__{bank}'].mean():.0f} 대조)")
    assert abs(base_acc - float((d[f"topk__{bank}"] == gt).mean())) < 1e-6

    ms_b, sd_b = m_s[cols], spec_sd[cols]
    def dedupe_mask(keep):
        """클래스 안에서 코사인>0.95 근접중복 제거 — 주효과 낮은 쪽을 남긴다."""
        out = keep.copy()
        for li in range(len(cs)):
            ii = np.where(keep & (lab == li))[0]
            if len(ii) < 2: continue
            order = ii[np.argsort(ms_b[ii])]                        # 조용한 문장 우선
            V = SENT[cols[order]]
            kept = []
            for j in range(len(order)):
                if kept and float(np.max(V[j] @ V[kept].T)) > 0.95: out[order[j]] = False
                else: kept.append(j)
        return out
    VAR = [("기준선(전량)", None), ("중복컷", "dup")]
    for q in (10, 25, 50, 75):
        VAR += [(f"주효과 상위 {q}% 컷", ("ms", q)), (f"특이도 하위 {q}% 컷", ("sd", q))]
    VAR += [("3컷 동시 (주효과25+특이도25+중복)", ("all", 25)), ("3컷 동시 (주효과50+특이도50+중복)", ("all", 50))]
    for name, spec in VAR:
        keep = np.ones(len(cols), bool)
        if spec == "dup": keep = dedupe_mask(keep)
        elif isinstance(spec, tuple):
            kind, q = spec
            if kind in ("ms", "all"):
                thr = np.percentile(ms_b, 100 - q); keep &= ms_b < thr
            if kind in ("sd", "all"):
                thr2 = np.percentile(sd_b, q); keep &= sd_b > thr2
            if kind == "all": keep = dedupe_mask(keep)
        if keep.sum() < 40: continue
        pred = to_gt[topk_vote(S_hy[:, keep], lab[keep], len(cs))]
        mf1, acc = macro_f1(gt, pred), float((pred == gt).mean())
        mu, lo, hi, pg = paired_ci(pred, base_pred) if name != "기준선(전량)" else (0.0, 0.0, 0.0, 0.5)
        e1_rows.append(dict(bank=bank, variant=name, n_kept=int(keep.sum()), kept_share=round(float(keep.mean()), 4),
                            acc=round(acc, 4), macro_f1=round(mf1, 4), d_mf1=round(mf1 - base_mf1, 4),
                            ci_mean=round(mu, 4), ci_lo=round(lo, 4), ci_hi=round(hi, 4), p_gt0=round(pg, 3),
                            noninferior=("Y" if lo > -0.02 else "N")))
        log(f"  {name:<34} 유지 {keep.sum():>6,} ({keep.mean():.0%})  acc {acc:.3f} mF1 {mf1:.3f} Δ{mf1-base_mf1:+.3f} CI[{lo:+.3f},{hi:+.3f}] 비열등 {'Y' if lo>-0.02 else 'N'}")
        if bank in FOCUS and (name.startswith("3컷") or name == "중복컷"): e1_best[(bank, name)] = (cols[keep], lab[keep], cs, to_gt)
    del S_hy
with open(f"{OUT}/csv/33_pruning.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "variant(프루닝안)", "n_kept(유지문장)", "kept_share(유지비율)", "acc(정확도)",
                                      "macro_f1", "d_mf1(기준선대비Δ)", "ci_mean", "ci_lo(2.5%)", "ci_hi(97.5%)", "p_gt0", "noninferior(CI하한>-0.02)"])
    w.writeheader()
    for r in e1_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log(f"→ csv/33_pruning.csv ({len(e1_rows)}행)")

# 프루닝된 뱅크의 frames 표본 오탐 (유지비 절감이 오탐을 늘리지 않는지)
e1_frames = []
for (bank, name), (cc, ll, cs, tg) in e1_best.items():
    pr = np.empty(len(FF), np.int8)
    for s0 in range(0, len(FF), 2000):
        Sb = FF[s0:s0 + 2000] @ SENT[cc].T
        pr[s0:s0 + 2000] = tg[topk_vote(Sb, ll, len(cs))]
    e1_frames.append(dict(bank=bank, variant=name, n=len(cc),
                          fire_recall=round(float((pr[ref == 2] == 2).mean()), 4),
                          smoke_recall=round(float((pr[ref == 3] == 3).mean()), 4),
                          fp=round(float((pr[ref == 0] > 0).mean()), 4),
                          firing=round(float((pr > 0).mean()), 4)))
    log(f"  frames {bank} {name}: fire {e1_frames[-1]['fire_recall']:.3f} smoke {e1_frames[-1]['smoke_recall']:.3f} 오탐 {e1_frames[-1]['fp']:.3f}")

# ══════════════════════════════════════════════════════════════════
# D3 — 문장 방향 산술
# ══════════════════════════════════════════════════════════════════
d3_rows = []
for bank in BANKS:
    rows_ = bank_defs[bank]["rows"]
    cols, lab_names, seen = [], [], set()
    for h, c, _g in rows_:
        if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); lab_names.append(c)
    cols = np.asarray(cols); cs = sorted(set(lab_names))
    lab = np.array([cs.index(c) for c in lab_names], dtype=np.int32)
    to_gt = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in cs], dtype=np.int8)
    ni_ = cs.index("normal")
    mu_n = SENT[cols[lab == ni_]].mean(0); mu_n /= np.linalg.norm(mu_n)
    mu_g = SENT.mean(0); mu_g /= np.linalg.norm(mu_g)
    variants = {"원본": SENT[cols]}
    for nm, mu in [("normal 중심 제거후 정규화", mu_n), ("전역 문장평균 제거후 정규화", mu_g)]:
        V = SENT[cols] - (SENT[cols] @ mu)[:, None] * mu[None, :]
        nrm = np.linalg.norm(V, axis=1, keepdims=True); V = V / np.maximum(nrm, 1e-8)
        variants[nm] = V.astype(np.float32)
    for nm, V in variants.items():
        Sb = FH @ V.T
        pred = to_gt[topk_vote(Sb, lab, len(cs))]
        mf1, acc = macro_f1(gt, pred), float((pred == gt).mean())
        if nm == "원본": basep = pred; mu2, lo2, hi2, pg2 = 0.0, 0.0, 0.0, 0.5
        else: mu2, lo2, hi2, pg2 = paired_ci(pred, basep)
        if bank in FOCUS:
            prf = np.empty(len(FF), np.int8)
            for s0 in range(0, len(FF), 2000):
                prf[s0:s0 + 2000] = to_gt[topk_vote(FF[s0:s0 + 2000] @ V.T, lab, len(cs))]
            fr_fire, fr_fp = float((prf[ref == 2] == 2).mean()), float((prf[ref == 0] > 0).mean())
        else:
            fr_fire, fr_fp = float("nan"), float("nan")
        d3_rows.append(dict(bank=bank, variant=nm, acc=round(acc, 4), macro_f1=round(mf1, 4),
                            d_mf1=round(mf1 - macro_f1(gt, basep), 4), ci_lo=round(lo2, 4), ci_hi=round(hi2, 4), p_gt0=round(pg2, 3),
                            rec_fall=round(float((pred[gt == 1] == 1).mean()), 4), rec_fire=round(float((pred[gt == 2] == 2).mean()), 4),
                            rec_smoke=round(float((pred[gt == 3] == 3).mean()), 4),
                            fr_fire=round(fr_fire, 4), fr_fp=round(fr_fp, 4)))
        log(f"  D3 {bank} {nm:<26} acc {acc:.3f} mF1 {mf1:.3f} Δ{d3_rows[-1]['d_mf1']:+.3f} CI[{lo2:+.3f},{hi2:+.3f}]  frames fire {d3_rows[-1]['fr_fire']:.3f} 오탐 {d3_rows[-1]['fr_fp']:.3f}")
with open(f"{OUT}/csv/34_direction_arithmetic.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bank(뱅크)", "variant(문장변환)", "acc(정확도)", "macro_f1", "d_mf1(원본대비Δ)", "ci_lo", "ci_hi", "p_gt0",
                                      "rec_fall", "rec_fire", "rec_smoke", "fr_fire(frames)", "fr_fp(frames오탐)"])
    w.writeheader()
    for r in d3_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log(f"→ csv/34_direction_arithmetic.csv ({len(d3_rows)}행)")

# ══════════════════════════════════════════════════════════════════
# C3 — Biclustering (문장군 × 군집군)
# ══════════════════════════════════════════════════════════════════
Z = (R - R.mean(0)) / (R.std(0) + 1e-9)
W = np.clip(Z, 0, None)
act = np.where((W.sum(1) > 0) & (scls >= 0))[0]
log(f"biclustering 입력 {len(act):,} 문장 × {NK} 군집")
NB = 6
co = SpectralCoclustering(n_clusters=NB, random_state=0, svd_method="arpack").fit(W[act])
srow, scol = co.row_labels_, co.column_labels_
cur.execute("SELECT cluster_id, mode() WITHIN GROUP (ORDER BY project) FROM analysis.frame_cluster WHERE method='kmeans64' GROUP BY 1")
k2p = dict(cur.fetchall())
STOP = set("a an the of in on at to with and or is are by for from into near under over as its their this that there it".split())
c3_rows = []
for b in range(NB):
    sm = srow == b; km = np.where(scol == b)[0]
    if sm.sum() == 0: continue
    sidx = act[sm]
    cls_cnt = collections.Counter(CLASSES[c] for c in scls[sidx])
    wc = collections.Counter()
    for j in sidx[:4000]:
        for wd in str(text.get(hashes[j], "")).lower().split():
            wd = "".join(ch for ch in wd if ch.isalpha())
            if wd and wd not in STOP and len(wd) > 2: wc[wd] += 1
    projs = collections.Counter(k2p.get(int(k), "?") for k in km)
    c3_rows.append(dict(bicluster=b, n_sentences=int(sm.sum()), n_clusters=len(km),
                        clusters=" ".join(str(int(k)) for k in km),
                        projects=" ".join(f"{p}×{n}" for p, n in projs.most_common(4)),
                        cls_normal=cls_cnt.get("normal", 0), cls_falldown=cls_cnt.get("falldown", 0),
                        cls_fire=cls_cnt.get("fire", 0), cls_smoke=cls_cnt.get("smoke", 0),
                        top_words=" ".join(w0 for w0, _ in wc.most_common(10)),
                        mean_spec_sd=round(float(spec_sd[sidx].mean()), 5), mean_ms=round(float(m_s[sidx].mean()), 4)))
    log(f"  bicluster {b}: 문장 {sm.sum():,} 군집 {len(km)} ({list(projs.most_common(2))}) 클래스 {dict(cls_cnt)} | {c3_rows[-1]['top_words'][:70]}")
with open(f"{OUT}/csv/35_biclusters.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["bicluster(동시군집)", "n_sentences(문장수)", "n_clusters(프레임군집수)", "clusters(군집id)",
                                      "projects(대표프로젝트)", "cls_normal", "cls_falldown", "cls_fire", "cls_smoke",
                                      "top_words(상위단어)", "mean_spec_sd(평균특이도SD)", "mean_ms(평균배경)"])
    w.writeheader()
    for r in c3_rows: w.writerow(dict(zip(w.fieldnames, r.values())))
log(f"→ csv/35_biclusters.csv ({len(c3_rows)}행)")

# ══════════════════════════════════════════════════════════════════
# 그림
# ══════════════════════════════════════════════════════════════════
NOTE = "카메라 군집 부트스트랩 2,000회 · sourcei GT 7,498/15카메라 · frames 표본 24,792 (SAM3 약참조) · 전체 31 뱅크"
fig, axes = plt.subplots(1, 3, figsize=(23, 7.6), gridspec_kw={"width_ratios": [1.45, 1.05, 0.85]})
ax = axes[0]
r0 = [r for r in e1_rows if r["bank"] == FOCUS[0]]
y = np.arange(len(r0))
ax.barh(y, [r["kept_share"] * 100 for r in r0], color="#c3c2b7", label="유지 문장 비율 %")
ax2 = ax.twiny()
ax2.plot([r["d_mf1"] for r in r0], y, "o", ms=8, color="#eb6834")
for i, r in enumerate(r0):
    ax2.plot([r["ci_lo"], r["ci_hi"]], [i, i], color="#eb6834", lw=1.6, alpha=.6)
ax2.axvline(0, color="#52514e", lw=1); ax2.axvline(-0.02, color="#e34948", ls="--", lw=1)
ax2.text(-0.02, -0.85, "비열등 한계 −0.02", color="#e34948", fontsize=8.5, ha="center")
ax.set_yticks(y); ax.set_yticklabels([f"{r['variant']}  ({r['n_kept']:,}문장)" for r in r0], fontsize=9); ax.invert_yaxis()
ax.set_xlabel("유지 문장 비율 % (회색 막대)"); ax2.set_xlabel("Δ macro-F1 vs 전량 (주황 = 카메라 부트스트랩 평균, 선 = 95% CI)")
ax.set_title(f"① {FOCUS[0]} 프루닝 상세 — 라벨 없이 자른 비율 vs 성능 변화", loc="left", fontsize=11)

ax = axes[1]
for nm, col, mk in [("중복컷", "#8a887f", "o"), ("3컷 동시 (주효과25+특이도25+중복)", "#2a78d6", "s"),
                    ("3컷 동시 (주효과50+특이도50+중복)", "#e34948", "^")]:
    rr = [r for r in e1_rows if r["variant"] == nm]
    if not rr: continue
    ax.scatter([r["kept_share"] * 100 for r in rr], [r["d_mf1"] for r in rr], s=52, marker=mk,
               color=col, alpha=.82, edgecolor="white", lw=.7, label=f"{nm[:22]} (n={len(rr)})")
    ni = sum(1 for r in rr if r["noninferior"] == "Y")
    ax.text(0.02, 0.05 + 0.07 * [nm[:6] for nm in ["중복컷", "3컷 동시 (주효과25", "3컷 동시 (주효과50"]].index(nm[:6]) if False else 0, "", transform=ax.transAxes)
ax.axhline(0, color="#52514e", lw=1); ax.axhline(-0.02, color="#e34948", ls="--", lw=1)
ax.set_xlabel("유지 문장 비율 %"); ax.set_ylabel("Δ macro-F1 vs 전량")
ax.legend(frameon=False, fontsize=8.5, loc="lower right")
nn = {nm: (sum(1 for r in e1_rows if r["variant"] == nm and r["noninferior"] == "Y"), sum(1 for r in e1_rows if r["variant"] == nm))
      for nm in ["중복컷", "3컷 동시 (주효과25+특이도25+중복)", "3컷 동시 (주효과50+특이도50+중복)"]}
ax.set_title("② 전체 31 뱅크 — 비열등(CI 하한 > −0.02) 뱅크 수\n" +
             " · ".join(f"{k[:14]} {v[0]}/{v[1]}" for k, v in nn.items()), loc="left", fontsize=10)

ax = axes[2]
labs = [f"{r['bank']}\n{r['variant'][:14]}" for r in e1_frames]
x = np.arange(len(e1_frames)); w2 = 0.27
for k, (key, lab_, col) in enumerate([("fire_recall", "fire 재현율", "#e34948"), ("smoke_recall", "smoke 재현율", "#4a3aa7"), ("fp", "비화재 오탐", "#8a887f")]):
    v = [r[key] for r in e1_frames]; b_ = ax.bar(x + (k - 1) * w2, v, w2 * 0.92, color=col, label=lab_)
    for bx, vv in zip(b_, v): ax.text(bx.get_x() + bx.get_width() / 2, vv + 0.008, f"{vv:.2f}", ha="center", fontsize=7.5)
ax.set_xticks(x); ax.set_xticklabels(labs, fontsize=7.5); ax.legend(frameon=False, fontsize=8.5)
ax.set_title("③ 프루닝 뱅크의 frames 반응 (오탐이 늘지 않나)", loc="left", fontsize=11)
fig.suptitle("E1 프루닝 3컷 — 목표는 성능 향상이 아니라 **유지비 절감**: 라벨 없이 몇 %를 지워도 성능이 유지되나. §3 의 '유효 문장 4%' 와 대조해 읽는다\n" + NOTE,
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f38_pruning.png", dpi=160); plt.close(fig)
log("saved f38")

fig, axes = plt.subplots(1, 2, figsize=(16, 6.5))
ax = axes[0]
vs = ["normal 중심 제거후 정규화", "전역 문장평균 제거후 정규화"]
bp = ax.boxplot([[r["d_mf1"] for r in d3_rows if r["variant"] == v] for v in vs], vert=True, widths=.5,
                patch_artist=True, medianprops=dict(color="#0b0b0b", lw=1.6))
for pc, c in zip(bp["boxes"], ["#2a78d6", "#1baf7a"]): pc.set_facecolor(c); pc.set_alpha(.55)
for k, v in enumerate(vs):
    yy = [r["d_mf1"] for r in d3_rows if r["variant"] == v]
    ax.scatter(np.full(len(yy), k + 1) + RNG.normal(0, .045, len(yy)), yy, s=22, color="#52514e", alpha=.6, zorder=3)
    win = sum(1 for q in yy if q > 0)
    ax.text(k + 1, max(yy) + .012, f"개선 {win}/{len(yy)} 뱅크\n중앙값 {np.median(yy):+.3f}", ha="center", fontsize=9)
ax.axhline(0, color="#e34948", lw=1.2, ls="--")
ax.set_xticks([1, 2]); ax.set_xticklabels(vs, fontsize=9); ax.set_ylabel("Δ macro-F1 vs 원본 문장 (전체 31 뱅크)")
ax.set_title("D3 문장 방향 산술 — 방향을 빼고 **다시 정규화**하면 판정이 바뀐다", loc="left", fontsize=11)
ax = axes[1]
bc = [r for r in c3_rows]
yb = np.arange(len(bc)); left = np.zeros(len(bc))
for c in CLASSES:
    v = np.array([r[f"cls_{c}"] for r in bc], dtype=float); tot = np.array([r["n_sentences"] for r in bc], dtype=float)
    ax.barh(yb, v / np.maximum(tot, 1), left=left, color=CC[c], label=c); left += v / np.maximum(tot, 1)
ax.set_yticks(yb); ax.set_yticklabels([f"B{r['bicluster']} 문장{r['n_sentences']:,}·군집{r['n_clusters']}\n{r['top_words'][:38]}" for r in bc], fontsize=7.5)
ax.invert_yaxis(); ax.set_xlabel("문장군의 클래스 구성"); ax.legend(frameon=False, ncol=4, fontsize=8.5, loc="lower right")
ax.set_title("C3 Biclustering — 문장군 × 프레임군집군 동시 분할", loc="left", fontsize=11)
fig.suptitle("D3 방향 산술 · C3 동시군집 — 정규화가 비선형이라 방향 제거가 실제로 판정을 바꾼다. "
             "동시군집은 '자연 발생 현장군 팩'이 있는지 본다\n" + NOTE, x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(); fig.savefig(f"{OUT}/fig/f39_direction_bicluster.png", dpi=160); plt.close(fig)
log("saved f39")
json.dump(dict(e1=e1_rows, e1_frames=e1_frames, d3=d3_rows, c3=c3_rows,
               ms_stats=dict(mean=float(m_s.mean()), sd=float(m_s.std())),
               spec_sd_stats=dict(mean=float(spec_sd.mean()), sd=float(spec_sd.std()))),
          open(f"{OUT}/prune_bicluster_direction_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
