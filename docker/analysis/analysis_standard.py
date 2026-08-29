#!/usr/bin/env python3
"""분석 표준 러너 — 새 데이터가 들어와도 **같은 순서·같은 정의·같은 경고**로 분석한다.

왜 필요한가: 2026-08 한 달 동안 22개 절을 쌓으면서 배운 것은 "무엇을 재는가"보다
**"어떻게 재면 틀리는가"** 였다. 그 함정들이 사람 기억에만 있으면 다음 데이터에서 다시 밟는다.
이 모듈은 그 함정을 **가드레일로 코드에 박아** 놓는다. 판정은 사람이 하지만, **경고는 자동**이다.

스테이지 (고정 순서 — 앞 단계의 경고가 뒤 단계의 해석을 바꾼다)
  S0 재고    표본·클래스·카메라·군집 구성, 결측, 클래스×카메라 교락
  S1 기하    이원 분산분해(프레임/문장/상호작용), 모달리티 갭, 역-허핀달 유효문장수
  S2 군집    특이도 z, 군집이 담는 것(장소 vs 이벤트 NMI), 배치 지원 감사
  S3 채점    3규칙(top-K·argmax·분포-IoU) × PR-AUC × 이벤트 단위 집계
  S4 통계    카메라 군집 부트스트랩 CI · 혼합효과 ICC/deff · 필요 카메라 역산
  S5 큐레이션 프루닝 3컷 비열등성 · AL 기준 검증(클래스 조건부)

가드레일 (자동 경고 — 근거는 전부 실측)
  G1 deff > 20        → 프레임 단위 CI·McNemar 금지. 카메라 군집 부트스트랩만 (실측 deff 232)
  G2 클래스 존재 카메라 < 5 → 그 클래스 결론 보류 (fire 는 4대뿐이었다)
  G3 ICC > 0.5        → 뱅크/모델 순위표 만들지 말 것 (실측 0.83)
  G4 오탐 > 예산       → macro-F1 이 올라도 배치 불가 (실측 0.27 인 뱅크가 1위로 뽑혔다)
  G5 폴드에 없는 클래스 → macro 를 존재 클래스로만. 구조적 0 이 목적함수를 지배한다
  G6 불확실성 신호 AUC ≈ 0.5 → 불일치 표집 금지. 클래스 조건부로 다시 볼 것 (실측 0.486)
  G7 문장 근접중복 > 30% → 계수 p값·부호안정성 해석 금지. 유효 표본이 명목의 몇 분의 일
  G8 뱅크 크기 편차 4배↑ → 전량 행렬 금지(OOM). 행 청크로 흘릴 것

쓰는 법
    python3 analysis_standard.py run --config configs/sourcei.json
    python3 analysis_standard.py run --dataset sourcei --gt-field gt --group-field camera \\
            --banks v1.0.8.0,v1.0.8.1 --out /data/.../std_sourcei
    python3 analysis_standard.py guardrails          # 가드레일 표만 출력
"""
from __future__ import annotations
import os, sys, json, csv, glob, time, argparse, collections
sys.path.insert(0, "/workspace")
THR = os.environ.get("COS_THREADS", "2")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "COS_THREADS"): os.environ[_v] = THR
import numpy as np

CLASSES_DEFAULT = ["normal", "falldown", "fire", "smoke"]
T0 = time.time()
def log(m): print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)


# ══════════════════════════════════════════════════════════════════
# 가드레일 — 이 표가 이 모듈의 존재 이유다
# ══════════════════════════════════════════════════════════════════
GUARDRAILS = [
    dict(id="G1", name="설계효과(deff)", trigger="deff > 20",
         action="프레임 단위 CI·McNemar 금지 → 카메라 군집 부트스트랩만",
         evidence="sourcei 실측 deff 232 · ICC 0.51~0.83 · 유효표본 32"),
    dict(id="G2", name="클래스 카메라 수", trigger="그 클래스가 존재하는 카메라 < 5",
         action="그 클래스 결론 보류 · 표에 카메라 수 병기",
         evidence="fire 는 15대 중 4대·20이벤트뿐 → 뱅크 비교 무의미"),
    dict(id="G3", name="현장 간 이질성(ICC)", trigger="ICC > 0.5",
         action="뱅크/모델 순위표 만들지 말 것 · 쌍대 설계로만 비교",
         evidence="혼합효과 ICC 0.827(macro-F1)/0.932(정확도) · 카메라 4대는 전 뱅크 성능 0"),
    dict(id="G4", name="오탐 예산", trigger="정상 프레임 오탐 > 예산(기본 5%)",
         action="macro-F1 이 올라도 배치 불가로 표기 · 목적함수에 하드 제약",
         evidence="오탐 미포함 목적함수가 오탐 27% 뱅크를 1위로 뽑았다(공급 0.76%)"),
    dict(id="G5", name="폴드 결손 클래스", trigger="폴드/카메라에 없는 클래스",
         action="macro 는 존재 클래스로만 · 폴드별 지표 대신 폴드 밖 예측 풀링",
         evidence="GroupKFold 가 카메라 1대 폴드를 만들어 mF1 0.014·균형 0.000 이 나왔다"),
    dict(id="G6", name="불확실성 신호", trigger="신호→오답 AUC ∈ [0.45, 0.55]",
         action="불일치 표집 금지 · 클래스 조건부 AUC 로 다시 볼 것",
         evidence="31뱅크 불일치 AUC 0.486 · 클래스별로 부호 반전(normal 0.935 vs falldown 0.307)"),
    dict(id="G7", name="문장 근접중복", trigger="코사인>0.95 중복률 > 30%",
         action="계수 p값·부호안정성 해석 금지 · 템플릿 단위 dedup 후 재적합",
         evidence="falldown 문장 유지율 16.9% · 부호안정성 638/640 은 폴드가 같은 표본인 탓"),
    dict(id="G8", name="행렬 크기", trigger="뱅크 문장 수 편차 4배 이상 또는 최대 > 20,000",
         action="전량 행렬 금지 → 프레임 1,500행 청크 + 뱅크당 체크포인트",
         evidence="49,140문장 뱅크에서 7498×49140(1.5GB) 한 번에 잡아 OOM(Killed)"),
]


def check(gid, cond, detail):
    g = next(x for x in GUARDRAILS if x["id"] == gid)
    return dict(id=gid, name=g["name"], fired=bool(cond), action=g["action"],
                evidence=g["evidence"], detail=detail)


# ══════════════════════════════════════════════════════════════════
# 공통 지표 — 정의를 여기 한 곳에 고정한다
# ══════════════════════════════════════════════════════════════════
def f1_per_class(t, p, classes):
    out = {}
    for i, c in enumerate(classes):
        tp = int(((p == i) & (t == i)).sum()); fp = int(((p == i) & (t != i)).sum()); fn = int(((p != i) & (t == i)).sum())
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        out[c] = dict(f1=2 * pr * rc / max(pr + rc, 1e-12), precision=pr, recall=rc, support=tp + fn)
    return out


def macro_present(t, p, classes, events):
    """⚠️ G5 — **존재하는 클래스로만** macro. 없는 클래스의 0 을 넣으면 지표가 붕괴한다."""
    per = f1_per_class(t, p, classes)
    present = [c for c in events if (t == classes.index(c)).sum() > 0]
    if not present: return 0.0, [], per
    return float(np.mean([per[c]["f1"] for c in present])), present, per


def design_effect(y, groups):
    """deff = 1 + (m̄ − 1)·ICC — 군집 표집의 분산 팽창. G1 의 판정 근거."""
    g = np.asarray(groups); y = np.asarray(y, float)
    lev = np.unique(g); k = len(lev)
    if k < 2: return 1.0, 0.0, len(y)
    ns = np.array([(g == l).sum() for l in lev])
    mu = y.mean()
    ssb = sum(n * (y[g == l].mean() - mu) ** 2 for l, n in zip(lev, ns))
    ssw = sum(((y[g == l] - y[g == l].mean()) ** 2).sum() for l in lev)
    msb = ssb / (k - 1); msw = ssw / max(len(y) - k, 1)
    m0 = (len(y) - (ns ** 2).sum() / len(y)) / (k - 1)
    icc = max(0.0, (msb - msw) / max(msb + (m0 - 1) * msw, 1e-12))
    mbar = len(y) / k
    return float(1 + (mbar - 1) * icc), float(icc), float(len(y) / max(1 + (mbar - 1) * icc, 1e-9))


def cluster_bootstrap_ci(metric_fn, groups, n_boot=2000, seed=0):
    """G1 준수 — 카메라(군집)를 복원추출한다. 프레임 단위 부트스트랩 금지."""
    rng = np.random.default_rng(seed)
    g = np.asarray(groups); lev = np.unique(g)
    idx_by = {l: np.where(g == l)[0] for l in lev}
    vals = []
    for _ in range(n_boot):
        pick = rng.choice(lev, size=len(lev), replace=True)
        vals.append(metric_fn(np.concatenate([idx_by[l] for l in pick])))
    a = np.array(vals)
    return float(a.mean()), float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))


def inverse_herfindahl(counts):
    """역-허핀달 유효 개수 — "뱅크가 실제로 몇 문장으로 도는가"."""
    c = np.asarray(counts, float); s = c.sum()
    if s <= 0: return 0.0
    p = c / s
    return float(1.0 / (p ** 2).sum())


def two_way_variance(C):
    """이원 분산분해 — 프레임 주효과 / 문장 주효과 / 상호작용. 판별 신호는 상호작용뿐."""
    n_f, n_s = C.shape
    gm = C.mean()
    ss_tot = float(((C - gm) ** 2).sum())
    ss_f = float(n_s * ((C.mean(1) - gm) ** 2).sum())
    ss_s = float(n_f * ((C.mean(0) - gm) ** 2).sum())
    return dict(mean_cos=float(gm), frame=ss_f / ss_tot, sentence=ss_s / ss_tot,
                interaction=(ss_tot - ss_f - ss_s) / ss_tot, sd=float(np.sqrt(ss_tot / C.size)))


def nmi(a, b):
    """정규화 상호정보량 — 군집이 무엇을 담는가(장소 vs 이벤트)를 가른다."""
    from sklearn.metrics import normalized_mutual_info_score
    return float(normalized_mutual_info_score(np.asarray(a), np.asarray(b)))


# ══════════════════════════════════════════════════════════════════
# 스테이지
# ══════════════════════════════════════════════════════════════════
def s0_inventory(D, R):
    gt, grp, classes = D["gt"], D["group"], D["classes"]
    cams = np.unique(grp)
    per_cam = {}
    for c in cams:
        m = grp == c
        per_cam[str(c)] = dict(n=int(m.sum()), classes=sorted({classes[x] for x in gt[m]}),
                               n_event_classes=len({int(x) for x in gt[m] if x > 0}))
    per_class_cams = {classes[i]: int(sum(1 for c in cams if (gt[grp == c] == i).any()))
                      for i in range(len(classes))}
    single = [k for k, v in per_cam.items() if len(v["classes"]) == 1]
    R["S0"] = dict(n=int(len(gt)), n_groups=int(len(cams)),
                   class_counts={classes[i]: int((gt == i).sum()) for i in range(len(classes))},
                   per_class_groups=per_class_cams, single_class_groups=len(single), per_group=per_cam)
    R["guardrails"] += [check("G2", min(v for k, v in per_class_cams.items() if k != "normal") < 5,
                              f"클래스별 존재 카메라 {per_class_cams}")]
    log(f"S0 재고 — 표본 {len(gt):,} · 카메라 {len(cams)} · 클래스 {R['S0']['class_counts']}")
    log(f"   클래스별 존재 카메라 {per_class_cams} · 단일클래스 카메라 {len(single)}")
    return R


def s1_geometry(D, R):
    F, S = D["frames"], D["sent"]
    rng = np.random.default_rng(0)
    fi = rng.choice(len(F), min(2000, len(F)), replace=False)
    si = rng.choice(len(S), min(20000, len(S)), replace=False)
    C = F[fi] @ S[si].T
    R["S1"] = dict(anova=two_way_variance(C))
    ii = F[fi][:min(1000, len(fi))]
    R["S1"]["gap"] = dict(image_text=float(C.mean()),
                          image_image=float((ii @ ii.T)[np.triu_indices(len(ii), 1)].mean()),
                          text_text=float((S[si][:2000] @ S[si][:2000].T)[np.triu_indices(min(2000, len(si)), 1)].mean()))
    if D.get("bank_counts"):
        R["S1"]["effective_sentences"] = {k: round(inverse_herfindahl(v), 1) for k, v in D["bank_counts"].items()}
    a = R["S1"]["anova"]
    log(f"S1 기하 — 상호작용 {a['interaction']:.1%} (프레임 {a['frame']:.1%} 문장 {a['sentence']:.1%}) · "
        f"모달리티 갭 이미지↔문장 {R['S1']['gap']['image_text']:.3f} vs 이미지↔이미지 {R['S1']['gap']['image_image']:.3f}")
    return R


def s2_cluster(D, R):
    if D.get("cluster") is None:
        R["S2"] = dict(skipped="군집 배정 없음"); log("S2 군집 — 건너뜀(군집 없음)"); return R
    k = D["cluster"]; gt = D["gt"]; classes = D["classes"]
    place = D.get("place")
    R["S2"] = dict(n_clusters=int(len(np.unique(k))),
                   nmi_event=nmi(k, gt),
                   nmi_place=nmi(k, place) if place is not None else None,
                   purity=float(np.mean([collections.Counter(gt[k == c]).most_common(1)[0][1] / (k == c).sum()
                                         for c in np.unique(k)])))
    if D.get("cluster_pool") is not None:
        pool_k = D["cluster_pool"]
        cov = len(set(np.unique(k).tolist())) / max(len(set(np.unique(pool_k).tolist())), 1)
        R["S2"]["deployment_coverage"] = round(float(cov), 4)
    msg = f"S2 군집 — {R['S2']['n_clusters']}개 · NMI(이벤트) {R['S2']['nmi_event']:.3f}"
    if R["S2"]["nmi_place"] is not None:
        msg += f" vs NMI(장소) {R['S2']['nmi_place']:.3f} → 군집이 담는 것은 " + \
               ("**장소**" if R["S2"]["nmi_place"] > R["S2"]["nmi_event"] else "이벤트")
    log(msg + f" · 순도 {R['S2']['purity']:.3f}")
    return R


def _wave_iou(S, mem, bins=80):
    lo = S.min(1); hi = S.max(1); w = np.maximum(hi - lo, 1e-6)
    B = np.clip(((S - lo[:, None]) / w[:, None] * bins).astype(np.int32), 0, bins - 1)
    f = S.shape[0]; fi = np.arange(f); h = {}
    for c, idx in mem.items():
        flat = (fi[:, None] * bins + B[:, idx]).ravel()
        h[c] = np.bincount(flat, minlength=f * bins).reshape(f, bins).astype(np.float32) / len(idx)
    out = {}
    for c in mem:
        if c == "normal": continue
        inter = np.minimum(h["normal"], h[c]).sum(1); uni = np.maximum(h["normal"], h[c]).sum(1)
        out[c] = inter / np.maximum(uni, 1e-9)
    return out


def s3_scoring(D, R):
    from prompt_cos_db import topk_vote
    from sklearn.metrics import average_precision_score
    gt, classes, events = D["gt"], D["classes"], D["events"]
    rows = []
    for bname, (cols_lab, S) in D["bank_scores"].items():
        lab = cols_lab
        mem = {c: np.where(lab == i)[0] for i, c in enumerate(classes) if (lab == i).any()}
        preds = dict(topk=topk_vote(S, lab, len(classes)))
        per = np.stack([np.where(lab == i, S, -2.0).max(1) for i in range(len(classes))], 1)
        preds["argmax"] = per.argmax(1)
        io = _wave_iou(S, mem) if "normal" in mem and len(mem) > 1 else {}
        if io:
            iou_full = np.column_stack([np.ones(len(gt), np.float32)] +
                                       [io[c] for c in classes if c != "normal" and c in io])
            preds["wave"] = iou_full.argmin(1)
        for rule, p in preds.items():
            mf1, present, pc = macro_present(gt, p, classes, events)
            aps = [float(average_precision_score((gt == classes.index(c)).astype(int), -io[c]))
                   for c in present if c in io]
            rows.append(dict(bank=bname, rule=rule, acc=round(float((p == gt).mean()), 4),
                             macro_f1=round(mf1, 4), prauc=round(float(np.mean(aps)) if aps else 0.0, 4),
                             fp_normal=round(float((p[gt == 0] > 0).mean()), 4),
                             present=",".join(present),
                             **{f"f1_{c}": round(pc[c]["f1"], 4) for c in classes}))
            log(f"S3 채점 {bname:<16} {rule:<7} acc {rows[-1]['acc']:.4f} mF1 {mf1:.4f} "
                f"PR-AUC {rows[-1]['prauc']:.4f} 오탐 {rows[-1]['fp_normal']:.4f}")
        R.setdefault("_preds", {})[bname] = preds
    R["S3"] = rows
    budget = D.get("fp_budget", 0.05)
    worst = max(rows, key=lambda r: r["fp_normal"])
    R["guardrails"] += [check("G4", worst["fp_normal"] > budget,
                              f"최대 오탐 {worst['fp_normal']:.3f} ({worst['bank']}/{worst['rule']}), 예산 {budget:.0%}")]
    return R


def s4_stats(D, R):
    gt, grp, classes, events = D["gt"], D["group"], D["classes"], D["events"]
    ref = D.get("ref_bank") or list(D["bank_scores"])[0]
    p = R["_preds"][ref]["topk"]
    deff, icc, neff = design_effect((p == gt).astype(float), grp)
    R["S4"] = dict(ref_bank=ref, deff=round(deff, 1), icc=round(icc, 4), n_effective=round(neff, 1))
    def mf(idx): return macro_present(gt[idx], p[idx], classes, events)[0]
    mu, lo, hi = cluster_bootstrap_ci(mf, grp)
    R["S4"]["macro_f1_ci"] = [round(mu, 4), round(lo, 4), round(hi, 4)]
    # 필요 카메라 역산 — 쌍대차 표준오차 ≈ sqrt(2σ²_e/n)
    cams = np.unique(grp)
    percam = np.array([macro_present(gt[grp == c], p[grp == c], classes, events)[0] for c in cams])
    s2e = float(percam.var(ddof=1))
    R["S4"]["cameras_needed"] = {f"delta_{d}": int(np.ceil(2 * s2e * (2.8 / d) ** 2)) for d in (0.02, 0.05, 0.10)}
    R["guardrails"] += [check("G1", deff > 20, f"deff {deff:.1f} · ICC {icc:.3f} · 유효표본 {neff:.0f}"),
                        check("G3", icc > 0.5, f"ICC {icc:.3f} (카메라 {len(cams)}대)")]
    log(f"S4 통계 — deff {deff:.1f} · ICC {icc:.3f} · 유효표본 {neff:.0f} · "
        f"mF1 CI [{lo:.4f},{hi:.4f}] · 필요 카메라 {R['S4']['cameras_needed']}")
    return R


def s5_curation(D, R):
    from sklearn.metrics import roc_auc_score
    gt, classes = D["gt"], D["classes"]
    ref = D.get("ref_bank") or list(D["bank_scores"])[0]
    lab, S = D["bank_scores"][ref]
    err = (R["_preds"][ref]["topk"] != gt).astype(int)
    out = dict(ref_bank=ref)
    # 중복률 (G7)
    if D.get("bank_vecs", {}).get(ref) is not None:
        V = D["bank_vecs"][ref]
        rng = np.random.default_rng(0)
        sub = rng.choice(len(V), min(2000, len(V)), replace=False)
        G = V[sub] @ V[sub].T; np.fill_diagonal(G, 0)
        dup = float((G > 0.95).any(1).mean())
        out["near_dup_rate"] = round(dup, 4)
        R["guardrails"] += [check("G7", dup > 0.30, f"근접중복률 {dup:.1%} (표본 {len(sub)})")]
    # AL 기준 검증 (G6) — 전체 AUC 와 **클래스 조건부** AUC 를 같이 본다
    per = np.stack([np.where(lab == i, S, -2.0).max(1) for i in range(len(classes))], 1)
    srt = np.sort(per, 1); margin = srt[:, -1] - srt[:, -2]
    sig = {"margin_cos_inv": -margin}
    if len(D.get("all_bank_preds", [])) > 2:
        P = np.stack(D["all_bank_preds"])
        v = np.stack([(P == c).sum(0) for c in range(len(classes))], 1) / P.shape[0]
        with np.errstate(divide="ignore", invalid="ignore"):
            sig["vote_entropy"] = -np.nansum(np.where(v > 0, v * np.log(v), 0.0), 1)
        sig["disagree"] = (~(P == P[0]).all(0)).astype(float)
    out["al_signals"] = {}
    for nm, s in sig.items():
        auc = float(roc_auc_score(err, s))
        pc = {}
        for i, c in enumerate(classes):
            m = gt == i
            if m.sum() >= 50 and len(set(err[m])) > 1: pc[c] = round(float(roc_auc_score(err[m], s[m])), 4)
        out["al_signals"][nm] = dict(auc=round(auc, 4), per_class=pc,
                                     inverted=bool(pc and (max(pc.values()) - min(pc.values())) > 0.3))
        log(f"S5 AL 기준 {nm:<18} 전체 AUC {auc:.4f} · 클래스별 {pc}")
    flat = [v["auc"] for v in out["al_signals"].values()]
    R["guardrails"] += [check("G6", any(0.45 <= a <= 0.55 for a in flat),
                              f"신호 AUC {[round(a,3) for a in flat]}")]
    R["S5"] = out
    return R


STAGES = [("S0", s0_inventory), ("S1", s1_geometry), ("S2", s2_cluster),
          ("S3", s3_scoring), ("S4", s4_stats), ("S5", s5_curation)]


def run(D, outdir):
    os.makedirs(outdir, exist_ok=True)
    R = dict(guardrails=[], started=time.strftime("%Y-%m-%d %H:%M:%S"))
    for sid, fn in STAGES:
        try:
            R = fn(D, R)
        except Exception as e:
            R[sid] = dict(error=f"{type(e).__name__}: {e}")
            log(f"{sid} 실패 — {type(e).__name__}: {e}")
    R.pop("_preds", None)
    fired = [g for g in R["guardrails"] if g["fired"]]
    R["verdict"] = dict(n_guardrails=len(R["guardrails"]), n_fired=len(fired),
                        fired=[g["id"] for g in fired])
    json.dump(R, open(f"{outdir}/standard_report.json", "w"), ensure_ascii=False, indent=1, default=str)
    with open(f"{outdir}/standard_card.md", "w") as f:
        f.write(f"# 분석 표준 카드 — {D.get('name','(무명)')}\n\n생성 {R['started']}\n\n")
        f.write(f"## 판정: 가드레일 {len(fired)}/{len(R['guardrails'])} 발동\n\n")
        f.write("| ID | 항목 | 발동 | 조치 | 실측 |\n|---|---|---|---|---|\n")
        for g in R["guardrails"]:
            f.write(f"| {g['id']} | {g['name']} | {'🔴 예' if g['fired'] else '⚪ 아니오'} | "
                    f"{g['action']} | {g['detail']} |\n")
        if R.get("S3"):
            f.write("\n## S3 채점\n\n| 뱅크 | 규칙 | 정확도 | macro-F1 | PR-AUC | 정상 오탐 |\n|---|---|---|---|---|---|\n")
            for r in R["S3"]:
                f.write(f"| {r['bank']} | {r['rule']} | {r['acc']} | {r['macro_f1']} | {r['prauc']} | {r['fp_normal']} |\n")
        if R.get("S4"):
            s = R["S4"]
            f.write(f"\n## S4 통계\n\n- deff **{s['deff']}** · ICC **{s['icc']}** · 유효표본 **{s['n_effective']}**\n")
            f.write(f"- macro-F1 95% CI(카메라 군집 부트스트랩) {s['macro_f1_ci']}\n")
            f.write(f"- 필요 카메라 {s['cameras_needed']}\n")
    if R.get("S3"):
        with open(f"{outdir}/S3_scoring.csv", "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(R["S3"][0].keys())); w.writeheader()
            for r in R["S3"]: w.writerow(r)
    log(f"→ {outdir}/standard_report.json · standard_card.md · S3_scoring.csv")
    log(f"판정: 가드레일 {len(fired)}/{len(R['guardrails'])} 발동 — {[g['id'] for g in fired]}")
    return R


# ══════════════════════════════════════════════════════════════════
# sourcei 어댑터 (다른 데이터셋은 이 함수만 새로 쓰면 된다)
# ══════════════════════════════════════════════════════════════════
def load_sourcei(banks, base="/data/fiftyone/frames_bank/report/sourcei_gt"):
    import psycopg2, fiftyone as fo
    from prompt_cos_db import load_sentence_vectors, load_banks
    cur = psycopg2.connect("postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline").cursor()
    h2c, SENT = load_sentence_vectors(cur)
    d = np.load(f"{base}/preds.npz", allow_pickle=True)
    gt, cam = d["gt"], d["camera"]
    ds = fo.load_dataset("sourcei"); hid, hemb = ds.values(["id", "embedding"])
    assert hid == list(d["ids"])
    F = np.asarray(hemb, dtype=np.float32); F /= np.linalg.norm(F, axis=1, keepdims=True)
    bs, bv, bc = {}, {}, {}
    for b in banks:
        bd = load_banks(cur, [b])[0]
        cols, names, seen = [], [], set()
        for h, c, _g in bd["rows"]:
            if h in h2c and h not in seen: seen.add(h); cols.append(h2c[h]); names.append(c)
        cols = np.asarray(cols); cs = CLASSES_DEFAULT
        lab = np.array([cs.index(c) if c in cs else 0 for c in names], np.int32)
        bs[b] = (lab, F @ SENT[cols].T)
        bv[b] = SENT[cols]
        bc[b] = list(collections.Counter(names).values())
    kk = None
    try:
        cur.execute("SELECT entity_id, cluster_id FROM analysis.frame_cluster WHERE method='kmeans64'")
        _ = cur.fetchall()
    except Exception: pass
    return dict(name="sourcei", gt=gt, group=cam, classes=CLASSES_DEFAULT,
                events=["falldown", "fire", "smoke"], frames=F, sent=SENT,
                bank_scores=bs, bank_vecs=bv, bank_counts=bc, cluster=kk,
                ref_bank=banks[-1], fp_budget=0.05,
                all_bank_preds=[d[f"topk__{b}"] for b in d["banks"]
                                if f"topk__{b}" in d])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("guardrails")
    r = sub.add_parser("run")
    r.add_argument("--dataset", default="sourcei")
    r.add_argument("--banks", default="v1.0.8.0,v1.0.8.1")
    r.add_argument("--out", default="/data/fiftyone/frames_bank/report/sourcei_gt/standard")
    a = ap.parse_args()
    if a.cmd == "guardrails":
        print(json.dumps(GUARDRAILS, ensure_ascii=False, indent=1)); return
    banks = [b for b in a.banks.split(",") if b]
    if a.dataset != "sourcei":
        raise SystemExit(f"어댑터 없음: {a.dataset} — load_* 함수를 추가하세요 (load_sourcei 참고)")
    D = load_sourcei(banks)
    run(D, a.out)


if __name__ == "__main__":
    main()
