#!/usr/bin/env python3
"""남은 분석 ③ 불일치 표집 active learning — 다음에 무엇을 라벨링해야 하나.

두 단계로 나눈다.

(A) **기준 검증** — sourcei GT 로 "불확실성 신호가 오답을 예측하는가"를 먼저 확인한다.
    §설계 진단에서 이미 이상 신호가 있었다: 31뱅크가 **전부 일치한 프레임의 정확도 0.650**,
    **갈리는 프레임 0.737**. 즉 순진한 불일치 표집은 여기서 **거꾸로** 간다.
    신호 4종(뱅크 투표 엔트로피 / 전원일치 / top2 마진 / 분포-IoU 마진)을 각각 오답과 대조해
    쓸 수 있는 기준이 무엇인지 고른다. 못 쓰면 못 쓴다고 보고한다.

(B) **후보 선정** — 검증에서 살아남은 기준으로, 실제 배치 모집단(kmeans64 배정 90,084 프레임에서
    군집 층화 표집 30,000)에 대해 우선순위 목록을 만든다. 군집·프로젝트 균형을 강제해 한 현장이
    후보를 독식하지 않게 한다. 산출물은 사람 라벨링 작업 지시서용 CSV.

CPU 예의: 공유 호스트라 2스레드 + nice 19. 프레임을 1,500행 청크로 흘려 메모리 상한을 잡는다.
"""
import os, sys, json, csv, glob, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
from prompt_cos_db import load_sentence_vectors, load_banks, topk_vote, wave_iou
import numpy as np, psycopg2, matplotlib, time
matplotlib.use("Agg"); import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from sklearn.metrics import roc_auc_score
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
d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
gt, cam = d["gt"], d["camera"]
_m = json.load(open(f"{OUT}/metrics.json"))
BANKS = [str(b) for b in d["banks"] if set(_m["banks"][str(b)]["classes"]) & {"falldown", "fire", "smoke"}]
assert len(BANKS) == 31
log(f"문장 {SENT.shape} · 뱅크 {len(BANKS)}")

# ══════════════════════════════════════════════════════════════════
# (A) 기준 검증 — sourcei GT
# ══════════════════════════════════════════════════════════════════
P = np.stack([d[f"topk__{b}"] for b in BANKS])                       # [31, 7498]
n = P.shape[1]
votes = np.zeros((n, 4))
for c in range(4): votes[:, c] = (P == c).sum(0)
p = votes / votes.sum(1, keepdims=True)
with np.errstate(divide="ignore", invalid="ignore"):
    ent = -np.nansum(np.where(p > 0, p * np.log(p), 0.0), axis=1)     # 뱅크 투표 엔트로피
agree = (P == P[0]).all(0)
ref_pred = d["topk__v1.0.8.1"]
err = (ref_pred != gt).astype(int)

# top2 마진 · 분포-IoU 마진 (기준 뱅크)
per = np.load(f"{OUT}/percls_v1.0.8.1.npy").astype(np.float32)       # [n,4] 클래스 max 코사인
srt = np.sort(per, axis=1)
margin_cos = srt[:, -1] - srt[:, -2]
I = d["iou__v1.0.8.1"].astype(np.float32)                            # [n,3] 이벤트 클래스 IoU
iou_full = np.concatenate([np.ones((n, 1), np.float32), I], axis=1)  # normal 은 자기 자신 IoU=1
srt2 = np.sort(iou_full, axis=1)
margin_iou = srt2[:, 1] - srt2[:, 0]                                 # 가장 작은 두 IoU 의 차

SIG = {
    "vote_entropy":  ("뱅크 투표 엔트로피 (높을수록 불확실)", ent),
    "disagree":      ("31뱅크 불일치 (1=갈림)", (~agree).astype(float)),
    "margin_cos":    ("top2 코사인 마진 역수 (낮은 마진=불확실)", -margin_cos),
    "margin_iou":    ("분포-IoU 마진 역수", -margin_iou),
}
val = []
for key, (nm, s) in SIG.items():
    auc = float(roc_auc_score(err, s))
    q = np.quantile(s, [0.0, .2, .4, .6, .8, 1.0])
    dec = []
    for i in range(5):
        m = (s >= q[i]) & (s <= q[i + 1]) if i == 4 else (s >= q[i]) & (s < q[i + 1])
        dec.append(round(float(err[m].mean()), 4) if m.sum() else None)
    val.append(dict(key=key, signal=nm, auc=round(auc, 4), quintile_err=dec,
                    top20_err=dec[-1], bottom20_err=dec[0],
                    usable=("Y" if auc > 0.55 else "N")))
    log(f"  {nm:<34} AUC(신호→오답) {auc:.4f}  5분위 오답률 {dec}  {'쓸만함' if auc>0.55 else '못 씀'}")
log(f"  참고: 전원일치 프레임 오답률 {err[agree].mean():.4f} · 갈리는 프레임 {err[~agree].mean():.4f} "
    f"(일치 {agree.mean():.1%})")

# 클래스 조건부로도 본다 — normal 이 압도적이라 전체 AUC 가 normal 편향일 수 있다
per_cls = {}
for c in range(4):
    m = gt == c
    if m.sum() < 50: continue
    per_cls[CLASSES[c]] = {key: round(float(roc_auc_score(err[m], s[m])), 4) if len(set(err[m])) > 1 else None
                           for key, (nm, s) in SIG.items()}
    log(f"    GT={CLASSES[c]:<9} (n={int(m.sum()):>5}) " + " ".join(f"{v}" for v in per_cls[CLASSES[c]].values()))

# ══════════════════════════════════════════════════════════════════
# (B) 후보 선정 — 배치 모집단 층화 표집 30,000
# ══════════════════════════════════════════════════════════════════
cur.execute("SELECT entity_id, cluster_id, project FROM analysis.frame_cluster WHERE method='kmeans64'")
fc = cur.fetchall()
by_k = collections.defaultdict(list)
for eid, k, pj in fc: by_k[k].append((eid, k, pj))
TARGET = 30000
per_k = max(1, TARGET // len(by_k))
pool = []
for k, lst in by_k.items():
    take = min(per_k, len(lst))
    idx = RNG.choice(len(lst), take, replace=False)
    pool += [lst[i] for i in idx]
log(f"층화 표집 {len(pool):,} / {len(fc):,} (군집 {len(by_k)}, 군집당 최대 {per_k})")
pool_ids = [x[0] for x in pool]
id2meta = {x[0]: (x[1], x[2]) for x in pool}

# ⚠️ analysis.frame_cluster.entity_id 는 **pgvector 의 UUID** 이지 FiftyOne 샘플 id(ObjectId)가
#    아니다. frames 데이터셋의 `entity_id` 필드가 그 UUID 를 들고 있으므로 그걸로 조인한다
#    (2026-08-27 실측: ds.select(UUID) → bson.errors.InvalidId 로 죽었다).
ds = fo.load_dataset("frames")
view = ds.match(F("modality") == "frame")
all_sid, all_eid, all_cls, all_pj, all_fp = view.values(
    ["id", "entity_id", "normalized_class", "project", "filepath"])       # 문자열만 — 가볍다
want = set(pool_ids)
keep = [i for i, e in enumerate(all_eid) if e in want]
sub_sid = [all_sid[i] for i in keep]
sub_eid = [all_eid[i] for i in keep]
sub_cls = [all_cls[i] or "none" for i in keep]
sub_fp = [all_fp[i] for i in keep]
log(f"FiftyOne 매칭 {len(sub_sid):,} / 층화표본 {len(pool_ids):,}")
emb = ds.select(sub_sid, ordered=True).values("image_embedding")
FP = np.asarray(emb, dtype=np.float32); FP /= np.linalg.norm(FP, axis=1, keepdims=True); del emb
sub_ids = sub_eid                       # 이후 id2meta 조회는 UUID 기준
log(f"임베딩 {FP.shape}")

bank_defs = {b["version"]: b for b in load_banks(cur, BANKS)}
BCOL, BLAB, BCS, BTG = {}, {}, {}, {}
for b in BANKS:
    cols, names, seen = [], [], set()
    for h, c, _g in bank_defs[b]["rows"]:
        if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); names.append(c)
    BCOL[b] = np.asarray(cols); BCS[b] = sorted(set(names))
    BLAB[b] = np.array([BCS[b].index(c) for c in names], np.int32)
    BTG[b] = np.array([CLASSES.index(c) if c in CLASSES else -2 for c in BCS[b]], np.int8)

N = len(FP)
PP = np.empty((len(BANKS), N), np.int8)
ent_p = np.empty(N, np.float32); marg_p = np.empty(N, np.float32); marg_iou_p = np.empty(N, np.float32)
REF = "v1.0.8.1"
CH = 1500
for s0 in range(0, N, CH):
    S = FP[s0:s0 + CH] @ SENT.T                                     # [chunk, 121614]
    for bi, b in enumerate(BANKS):
        PP[bi, s0:s0 + CH] = BTG[b][topk_vote(S[:, BCOL[b]], BLAB[b], len(BCS[b]))]
    Sr = S[:, BCOL[REF]]
    perr = np.stack([np.where(BLAB[REF] == i, Sr, -2.0).max(1) for i in range(len(BCS[REF]))], 1)
    sr = np.sort(perr, axis=1); marg_p[s0:s0 + CH] = sr[:, -1] - sr[:, -2]
    # 분포-IoU 마진도 같은 청크에서 (검증에서 이게 채택 기준이 될 수 있다)
    wi = wave_iou(Sr, {c: np.where(BLAB[REF] == i)[0] for i, c in enumerate(BCS[REF])})
    iouf = np.concatenate([np.ones((Sr.shape[0], 1), np.float32)] +
                          [wi[c][:, None].astype(np.float32) for c in BCS[REF] if c != "normal"], axis=1)
    si = np.sort(iouf, axis=1); marg_iou_p[s0:s0 + CH] = si[:, 1] - si[:, 0]
    del S, Sr, perr, wi, iouf
    if (s0 // CH) % 4 == 0: log(f"  채점 {s0+CH:,}/{N:,}")
v = np.zeros((N, 4))
for c in range(4): v[:, c] = (PP == c).sum(0)
pp = v / v.sum(1, keepdims=True)
with np.errstate(divide="ignore", invalid="ignore"):
    ent_p = -np.nansum(np.where(pp > 0, pp * np.log(pp), 0.0), axis=1)
ref_p = PP[BANKS.index(REF)]
log(f"채점 완료 — 전원일치 {(PP == PP[0]).all(0).mean():.1%} · 엔트로피 평균 {ent_p.mean():.3f}")

# 우선순위 점수: 검증에서 AUC 가 가장 높은 신호를 주로, 마진을 보조로
best = max(val, key=lambda r: r["auc"])
POOL_SIG = {"vote_entropy": lambda: ent_p, "disagree": lambda: (v.max(1) < len(BANKS)).astype(float),
            "margin_cos": lambda: -marg_p, "margin_iou": lambda: -marg_iou_p}
prim = POOL_SIG[best["key"]]()
log(f"채택 기준: {best['signal']} (key={best['key']}, 전체 AUC {best['auc']:.4f})")
log("  ⚠️ 클래스 조건부 AUC 가 서로 반대 방향이다 — 아래 §에서 그대로 보고한다")
rank = np.lexsort((-ent_p, -prim))                                  # 1순위 채택기준, 2순위 투표 엔트로피

# 군집·프로젝트 균형: 군집당 상한을 두고 라운드로빈
CAP_PER_CLUSTER = 12
BUDGET = 2000
chosen, cnt_k = [], collections.Counter()
for i in rank:
    k, pj = id2meta[sub_ids[i]]
    if cnt_k[k] >= CAP_PER_CLUSTER: continue
    chosen.append(i); cnt_k[k] += 1
    if len(chosen) >= BUDGET: break
log(f"후보 {len(chosen):,}개 선정 (군집 {len(cnt_k)}개, 군집당 최대 {CAP_PER_CLUSTER})")

prj = collections.Counter(id2meta[sub_ids[i]][1] for i in chosen)
cls = collections.Counter(sub_cls[i] for i in chosen)
prd = collections.Counter(CLASSES[ref_p[i]] if ref_p[i] >= 0 else "?" for i in chosen)
log(f"  프로젝트 상위: {prj.most_common(6)}")
log(f"  SAM3 약참조 구성: {dict(cls)}")
log(f"  프롬프트 예측 구성: {dict(prd)}")

with open(f"{OUT}/csv/48_al_criterion.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["signal(신호)", "auc(신호→오답)", "usable(AUC>0.55)", "q1_err", "q2_err", "q3_err", "q4_err", "q5_err(최상위20%)"])
    for r in val: w.writerow([r["signal"], r["auc"], r["usable"]] + r["quintile_err"])
    w.writerow([])
    w.writerow(["클래스 조건부 AUC"] + [nm for nm, _v in SIG.values()])
    for c, dd in per_cls.items(): w.writerow([c] + [dd[k] for k in SIG])
with open(f"{OUT}/csv/49_al_candidates.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["rank(우선순위)", "entity_id(pgvector)", "sample_id(FiftyOne)", "cluster_id(kmeans64)", "project(현장)", "sam3_weak_class(약참조)",
                "prompt_pred(프롬프트 예측)", "vote_entropy(뱅크31 투표 엔트로피)", "n_banks_disagree(불일치 뱅크수)",
                "top2_margin(기준뱅크)", "iou_margin(분포IoU 마진)", "filepath"])
    for rk, i in enumerate(chosen, 1):
        k, pj = id2meta[sub_ids[i]]
        nd = int(len(BANKS) - v[i].max())
        w.writerow([rk, sub_ids[i], sub_sid[i], k, pj, sub_cls[i],
                    CLASSES[ref_p[i]] if ref_p[i] >= 0 else "?", round(float(ent_p[i]), 4), nd,
                    round(float(marg_p[i]), 4), round(float(marg_iou_p[i]), 4), sub_fp[i]])
log("→ csv/48_al_criterion.csv, csv/49_al_candidates.csv")

# ── 그림 ────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(21, 6.6), gridspec_kw={"width_ratios": [1.1, 1, 1]})
ax = axes[0]
x = np.arange(5)
for k, r in enumerate(val):
    ax.plot(x, r["quintile_err"], "o-", lw=2, ms=7, label=f"{r['signal'][:26]} (AUC {r['auc']:.3f})",
            color=["#e34948", "#eda100", "#2a78d6", "#1baf7a"][k])
ax.axhline(float(err.mean()), color="#0b0b0b", ls="--", lw=1)
ax.text(0.02, float(err.mean()) + .004, f"전체 오답률 {err.mean():.3f}", fontsize=8.5)
ax.set_xticks(x); ax.set_xticklabels(["1분위\n(가장 확실)", "2분위", "3분위", "4분위", "5분위\n(가장 불확실)"], fontsize=8.5)
ax.set_ylabel("기준 뱅크 v1.0.8.1 오답률")
ax.legend(frameon=False, fontsize=8)
ax.set_title("① 불확실성 신호가 오답을 예측하나 — 우상향이어야 쓸 수 있다", loc="left", fontsize=11)
ax = axes[1]
labs = list(per_cls.keys()); x = np.arange(len(labs)); w2 = 0.2
for k, (key, (nm, _v)) in enumerate(SIG.items()):
    vv = [per_cls[c][key] if per_cls[c][key] is not None else np.nan for c in labs]
    ax.bar(x + (k - 1.5) * w2, vv, w2 * 0.9, color=["#e34948", "#eda100", "#2a78d6", "#1baf7a"][k], label=nm[:22])
ax.axhline(0.5, color="#0b0b0b", ls="--", lw=1)
ax.set_xticks(x); ax.set_xticklabels(labs); ax.set_ylabel("클래스 조건부 AUC")
ax.legend(frameon=False, fontsize=7.6, ncol=2)
ax.set_title("② 클래스 안에서도 성립하나 (0.5 = 무정보)", loc="left", fontsize=11)
ax = axes[2]
top = prj.most_common(8)
ax.barh(range(len(top)), [c for _p, c in top], color="#2a78d6", alpha=.9)
for i, (p_, c_) in enumerate(top): ax.text(c_ + 4, i, f"{c_}", va="center", fontsize=9)
ax.set_yticks(range(len(top))); ax.set_yticklabels([p_[:24] for p_, _c in top], fontsize=8.5); ax.invert_yaxis()
ax.set_xlabel(f"선정 후보 수 (총 {len(chosen):,}, 군집당 상한 {CAP_PER_CLUSTER})")
ax.set_title("③ 후보가 어느 현장에서 나오나", loc="left", fontsize=11)
fig.suptitle("R3 불일치 표집 active learning — 먼저 기준을 검증하고(좌·중) 그 다음 후보를 뽑는다(우)\n"
             f"검증 = sourcei GT 7,498 · 후보 모집단 = kmeans64 배정 90,084 중 군집 층화 {N:,} · 뱅크 31종 투표",
             x=0.01, ha="left", fontsize=11.5)
fig.tight_layout(rect=[0, 0, 1, 0.90]); fig.savefig(f"{OUT}/fig/f48_active_learning.png", dpi=150); plt.close(fig)
log("saved f48")
json.dump(dict(criterion=val, adopted=best["key"], per_class_auc=per_cls, agree_share=float(agree.mean()),
               err_agree=float(err[agree].mean()), err_disagree=float(err[~agree].mean()),
               pool=N, chosen=len(chosen), cap=CAP_PER_CLUSTER,
               projects=dict(prj.most_common()), sam3_mix=dict(cls), pred_mix=dict(prd)),
          open(f"{OUT}/active_learning_summary.json", "w"), ensure_ascii=False, indent=1)
print("DONE")
