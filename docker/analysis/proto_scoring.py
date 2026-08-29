#!/usr/bin/env python3
"""A1 — 클래스 프로토타입(centroid) 채점 vs top-K 다수결.

질문: "대형 뱅크일수록 너무 많이 발화하고 sourcei GT 에서 가장 나쁘다"는 병리
(v1.0.12.0/v1.0.4.2/v1.0.3.2 등)가, 수천 문장에 대한 max-pooling(top-K=10 다수결이
사실상 상위 K개 문장의 코사인 최댓값에 좌우됨) 때문인가? 문장을 평균 내는 프로토타입
채점으로 바꾸면 이 병리가 줄어드는지를 본다.

규칙 3종 (뱅크별):
  · topK   — 기존 제품 규칙. prompt_cos_db.topk_vote(K=10). sourcei 는 sourcei_gt_rules.py
             가 이미 낸 preds.npz 를 재사용(재채점 안 함), frames 표본은 여기서 새로 낸다.
  · P      — 프로토타입 argmax. 클래스 c 문장집합 S_c(뱅크 내 content_hash 기준, 이미
             (bank,hash,class) 로 GROUP BY 돼 있어 자연히 중복없음) → μ_c=normalize(mean
             (SENT[S_c])), pred=argmax_c x·μ_c (뱅크의 전 클래스 대상, GT 4종 밖은 -2).
  · PC     — 프로토타입 대조(라벨 무관, threshold-free). score_e = x·(μ_e − μ_normal)
             (e = normal 제외 전 클래스), max score_e > 0 이면 그 이벤트, 아니면 normal.

정합도(coherence) R_c = ||mean(SENT[S_c])|| ∈ [0,1] — 문장들이 방향적으로 얼마나
뭉쳐있는가(평균 결과 벡터 길이). 1 = 완전히 같은 방향, 0 = 서로 상쇄.

sourcei GT(7,498프레임/15카메라)는 카메라를 재표집 단위로 하는 **paired 카메라군집
부트스트랩**(2,000회, sourcei_cluster_ci.py 와 같은 기법)으로 Δ=metric(P)-metric(topK)
CI 를 낸다. camera-cluster 부트스트랩은 통계량이 개수의 선형함수(tp/fp/fn/correct/n)라는
성질을 이용해 카메라별 집계를 2,000×15 곱연산 한 번으로 벡터화한다(문장 전체를 매
리샘플마다 다시 훑지 않음) — 결과는 "고른 카메라의 프레임을 이어붙여 채점"과 수학적으로
동일하다.

frames 표본(24,792 = SAM3 fire 전부 1,578 + smoke 전부 3,214 + 비화재 20,000, seed 0,
frames_fire_banks.py 와 동일 표본)은 GT 가 없는 대신 표본 크기가 커서 "발화율(firing
rate)이 문장 수와 상관되는가"를 보는 용도다 — 이게 이 분석의 핵심 검정이다.

출력: csv/21_prototype_vs_topk.csv, proto_summary.json, fig/f22_*.png, fig/f23_*.png
"""
import glob
import json
import os
import sys
import time

_T = os.environ.get("COS_THREADS", "6")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, _T)

import numpy as np  # noqa: E402
import psycopg2  # noqa: E402
from scipy.stats import spearmanr  # noqa: E402
import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.font_manager as fm  # noqa: E402

sys.path.insert(0, "/workspace")
from prompt_cos_db import load_banks, load_sentence_vectors, topk_vote, RULE_K, _topk_selfcheck  # noqa: E402

DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CSV_DIR = f"{OUT}/csv"
FIG_DIR = f"{OUT}/fig"
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)
CLASSES = ["normal", "falldown", "fire", "smoke"]
CHUNK = 1000
N_BOOT = 50 if os.environ.get("PROTO_SMOKE") == "1" else 2000
SMOKE = os.environ.get("PROTO_SMOKE") == "1"
RC_TOPK, RC_P, RC_PC = "#eb6834", "#4a3aa7", "#4a3aa7"
ANNOT_BANKS = {"v1.0.8.0", "v1.0.8.1", "v1.0.8.4", "v1.0.12.0", "v1.0.4.2", "v1.0.3.2"}


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def vkey(b: str) -> tuple:
    return tuple(int(x) for x in b.lstrip("vV").split("."))


def r4(x):
    return None if x is None else round(float(x), 4)


# ─────────────────────────────── 뱅크 준비 ───────────────────────────────

def prepare_bank(b: dict, h2c: dict):
    missing = [h for h, _c, _g in b["rows"] if h not in h2c]
    if missing:
        return None, f"벡터 없는 문장 {len(missing)}건"
    cls_cols_global: dict[str, list[int]] = {}
    cls_local: dict[str, list[int]] = {}
    gcols: list[int] = []
    for chash, cls, _g in b["rows"]:
        col = h2c[chash]
        cls_cols_global.setdefault(cls, []).append(col)
        cls_local.setdefault(cls, []).append(len(gcols))
        gcols.append(col)
    if "normal" not in cls_cols_global or "fire" not in cls_cols_global:
        return None, "normal/fire 클래스 요건 미달"
    cs = sorted(cls_local)
    events = [c for c in cs if c != "normal"]
    idx_normal = cs.index("normal")
    lab = np.empty(len(gcols), dtype=np.int32)
    for ci, c in enumerate(cs):
        lab[np.asarray(cls_local[c], dtype=np.int64)] = ci
    to_gt = np.asarray([CLASSES.index(c) if c in CLASSES else -2 for c in cs], dtype=np.int8)
    ev_to_gt = np.asarray([CLASSES.index(c) if c in CLASSES else -2 for c in events], dtype=np.int8)
    return dict(
        version=b["version"], n_sent=len(b["rows"]), cs=cs, events=events,
        idx_normal=idx_normal, lab=lab, to_gt=to_gt, ev_to_gt=ev_to_gt,
        gcols=np.asarray(gcols, dtype=np.int64),
        cls_cols_global={c: np.asarray(v, dtype=np.int64) for c, v in cls_cols_global.items()},
    ), None


def compute_prototypes(p: dict, SENT: np.ndarray) -> None:
    MU, COH = {}, {}
    for c, cols in p["cls_cols_global"].items():
        v = SENT[cols].mean(axis=0)
        coh = float(np.linalg.norm(v))
        MU[c] = (v / max(coh, 1e-12)).astype(np.float32)
        COH[c] = coh
    p["MU"], p["COH"] = MU, COH
    p["mu_mat"] = np.stack([MU[c] for c in p["cs"]], axis=0).astype(np.float32)
    p["diff_mat"] = np.stack([MU[e] - MU["normal"] for e in p["events"]], axis=0).astype(np.float32)


# ─────────────────────────────── 지표 함수 ───────────────────────────────

def macro_f1(t: np.ndarray, pr: np.ndarray, classes=(1, 2, 3)) -> float:
    fs = []
    for c in classes:
        tp = int(((pr == c) & (t == c)).sum())
        fp = int(((pr == c) & (t != c)).sum())
        fn = int(((pr != c) & (t == c)).sum())
        prec = tp / max(tp + fp, 1)
        rec = tp / max(tp + fn, 1)
        fs.append(2 * prec * rec / max(prec + rec, 1e-12))
    return float(np.mean(fs))


def recall_c(t: np.ndarray, pr: np.ndarray, c: int):
    m = t == c
    return float((pr[m] == c).mean()) if m.sum() else None


def rate(mask: np.ndarray, predref: np.ndarray, target: list):
    return float(np.isin(predref[mask], target).mean()) if mask.sum() else None


# ─────────────────────────────── 데이터 적재 ───────────────────────────────

def load_sourcei():
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    ids, emb, gt, cam = ds.values(["id", "embedding", "ground_truth.label", "camera"])
    F = np.asarray(emb, dtype=np.float32)
    F /= np.linalg.norm(F, axis=1, keepdims=True)
    gt_i = np.asarray([CLASSES.index(g) for g in gt], dtype=np.int8)
    return list(ids), F, gt_i, np.asarray(cam)


def load_frames_sample():
    import fiftyone as fo
    from fiftyone import ViewField as VF
    ds = fo.load_dataset("frames")
    fr = ds.match(VF("modality") == "frame")
    ncls_raw, emb = fr.values(["normalized_class", "image_embedding"])
    ncls = np.array([x or "none" for x in ncls_raw])
    fire_idx = np.where(ncls == "fire")[0]
    sm_idx = np.where(ncls == "smoke")[0]
    neg_idx = np.where(np.isin(ncls, ["none", "person"]))[0]
    rng = np.random.default_rng(0)   # 반드시 이 함수 안에서 새로 생성 — frames_fire_banks.py 와 동일 표본 재현
    sub = np.concatenate([fire_idx, sm_idx,
                          rng.choice(neg_idx, size=min(20000, len(neg_idx)), replace=False)])
    ref = np.array([{"fire": 2, "smoke": 3}.get(ncls[i], 0) for i in sub], dtype=np.int8)
    Fm = np.asarray([emb[i] for i in sub], dtype=np.float32)
    Fm /= np.linalg.norm(Fm, axis=1, keepdims=True)
    return sub, ref, Fm, len(fire_idx)


# ─────────────────────────────── 메인 ───────────────────────────────

def main():
    t0 = time.time()
    _topk_selfcheck()
    log(f"K={RULE_K} chunk={CHUNK} n_boot={N_BOOT} smoke={SMOKE}")

    # 1) 뱅크 + 문장벡터 -----------------------------------------------------
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    log("문장 벡터 적재…")
    h2c, SENT = load_sentence_vectors(cur)
    log(f"  고유 문장벡터 {SENT.shape}")
    only = None
    if os.environ.get("PROTO_BANKS"):
        only = [x.strip() for x in os.environ["PROTO_BANKS"].split(",") if x.strip()]
    banks_raw = load_banks(cur, only)
    cur.close(); conn.close()

    prepared, skipped = [], []
    for b in banks_raw:
        if b["version"].startswith("v2."):
            skipped.append((b["version"], "v2.* 접두 — normal+class_5 뿐, 이벤트 클래스 없음"))
            continue
        p, why = prepare_bank(b, h2c)
        if p is None:
            skipped.append((b["version"], why))
            continue
        compute_prototypes(p, SENT)
        prepared.append(p)
    for v, why in skipped:
        log(f"  skip {v}: {why}")
    prepared.sort(key=lambda p: vkey(p["version"]))
    log(f"대상 뱅크 {len(prepared)} (skip {len(skipped)})")
    if not SMOKE:
        assert len(prepared) == 31, f"뱅크 수 31 기대, 실제 {len(prepared)}"

    # 자가검증: 프로토타입 단위노름 + coherence ∈ [0,1] ------------------------
    coherence_below_half = []
    coh_range = {c: [1.0, 0.0] for c in CLASSES}
    for p in prepared:
        for c, mu in p["MU"].items():
            n = float(np.linalg.norm(mu))
            assert abs(n - 1.0) < 1e-3, f"{p['version']} {c} 프로토타입 노름 {n} != 1"
        for c, coh in p["COH"].items():
            assert -1e-6 <= coh <= 1 + 1e-4, f"{p['version']} {c} coherence {coh} ∉ [0,1]"
            if c in coh_range:
                coh_range[c][0] = min(coh_range[c][0], coh)
                coh_range[c][1] = max(coh_range[c][1], coh)
            if coh < 0.5:
                coherence_below_half.append((p["version"], c, r4(coh)))
    log("자가검증 통과: 프로토타입 단위노름 + coherence∈[0,1]")
    log(f"  coherence 범위(클래스별): {json.dumps({c: [r4(v[0]), r4(v[1])] for c, v in coh_range.items()})}")
    if coherence_below_half:
        log(f"  coherence<0.5 인 (뱅크,클래스) {len(coherence_below_half)}건 — 상위 5: {coherence_below_half[:5]}")

    # 2) sourcei GT ----------------------------------------------------------
    log("sourcei GT 적재…")
    ids, F_hy, gt_hy, cam_hy = load_sourcei()
    d = np.load(f"{OUT}/preds.npz", allow_pickle=True)
    assert list(d["ids"]) == ids, "sourcei ids 정렬 불일치 — preds.npz(sourcei_gt_rules.py) 재실행 필요"
    acc_check = float((d["topk__v1.0.8.0"] == d["gt"]).mean())
    assert abs(acc_check - 0.706) < 0.001, f"v1.0.8.0 top-K sourcei acc 자가검증 실패: {acc_check}"
    log(f"자가검증 통과: v1.0.8.0 top-K sourcei acc = {acc_check:.4f} (기대 0.706±0.001), "
        f"ids 정렬 {len(ids):,} 일치")

    cams = np.unique(cam_hy)
    n_cam = len(cams)
    idx_by_cam = {c: np.where(cam_hy == c)[0] for c in cams}
    log(f"카메라 {n_cam}종, 프레임 {len(gt_hy):,}")

    BOOT_RNG = np.random.default_rng(0)
    MULT = np.zeros((N_BOOT, n_cam), dtype=np.float64)
    for r in range(N_BOOT):
        pick = BOOT_RNG.integers(0, n_cam, size=n_cam)
        MULT[r] = np.bincount(pick, minlength=n_cam)

    def percam_counts(pred: np.ndarray, classes=(1, 2, 3)):
        tp = np.zeros((n_cam, len(classes)))
        fp = np.zeros((n_cam, len(classes)))
        fn = np.zeros((n_cam, len(classes)))
        correct = np.zeros(n_cam)
        n = np.zeros(n_cam)
        for i, c in enumerate(cams):
            idx = idx_by_cam[c]
            t, pv = gt_hy[idx], pred[idx]
            n[i] = len(idx)
            correct[i] = (pv == t).sum()
            for j, cls in enumerate(classes):
                tp[i, j] = ((pv == cls) & (t == cls)).sum()
                fp[i, j] = ((pv == cls) & (t != cls)).sum()
                fn[i, j] = ((pv != cls) & (t == cls)).sum()
        return tp, fp, fn, correct, n

    def bootstrap_delta(pred_a: np.ndarray, pred_b: np.ndarray):
        """paired 카메라군집 부트스트랩: Δ=metric(a)-metric(b). 반환: (mf1 통계4, acc 통계4)."""
        tpA, fpA, fnA, corA, n = percam_counts(pred_a)
        tpB, fpB, fnB, corB, _ = percam_counts(pred_b)
        TP_A, FP_A, FN_A = MULT @ tpA, MULT @ fpA, MULT @ fnA
        TP_B, FP_B, FN_B = MULT @ tpB, MULT @ fpB, MULT @ fnB

        def mf1(TP, FP, FN):
            PR = TP / np.maximum(TP + FP, 1)
            RC = TP / np.maximum(TP + FN, 1)
            F1 = 2 * PR * RC / np.maximum(PR + RC, 1e-12)
            return F1.mean(axis=1)

        MF1_A, MF1_B = mf1(TP_A, FP_A, FN_A), mf1(TP_B, FP_B, FN_B)
        COR_A, COR_B, N = MULT @ corA, MULT @ corB, MULT @ n
        ACC_A, ACC_B = COR_A / N, COR_B / N
        d_mf1, d_acc = MF1_A - MF1_B, ACC_A - ACC_B

        def summarize(dd):
            return (float(dd.mean()), float(np.percentile(dd, 2.5)),
                    float(np.percentile(dd, 97.5)), float((dd > 0).mean()))

        return summarize(d_mf1), summarize(d_acc)

    for p in prepared:
        v = p["version"]
        scores = F_hy @ p["mu_mat"].T
        p_local = scores.argmax(axis=1)
        p["P_hy"] = p["to_gt"][p_local]
        esc = F_hy @ p["diff_mat"].T
        beste, bestv = esc.argmax(axis=1), esc.max(axis=1)
        fired = bestv > 0
        p["PC_hy"] = np.where(fired, p["ev_to_gt"][beste], 0).astype(np.int8)
        key = f"topk__{v}"
        assert key in d.files, f"preds.npz 에 {key} 없음"
        p["topk_hy"] = d[key]
    log(f"sourcei P/PC 채점 완료 ({time.time() - t0:.0f}s)")

    # 3) frames 표본 (fresh top-K + P + PC) -----------------------------------
    log("frames 표본 적재…")
    sub, ref, Fm, n_fire_total = load_frames_sample()
    log(f"  표본 {len(sub):,} = fire {n_fire_total:,} + smoke {int((ref == 3).sum()):,} "
        f"+ neg {int((ref == 0).sum()):,}")
    if SMOKE:
        sub, ref, Fm = sub[:3000], ref[:3000], Fm[:3000]
        log(f"  [SMOKE] frames 표본을 {len(sub):,} 로 절단")
    else:
        assert len(sub) == 24792, f"frames 표본 크기 24,792 기대, 실제 {len(sub)}"
        assert n_fire_total == 1578, f"fire 카운트 1,578 기대, 실제 {n_fire_total}"
        log("자가검증 통과: frames 표본 24,792 / fire 1,578")

    n = len(sub)
    STORE = {p["version"]: dict(
        topk_ref=np.empty(n, dtype=np.int8), topk_ev=np.empty(n, dtype=bool),
        P_ref=np.empty(n, dtype=np.int8), P_ev=np.empty(n, dtype=bool),
        PC_ref=np.empty(n, dtype=np.int8), PC_ev=np.empty(n, dtype=bool),
    ) for p in prepared}

    t1 = time.time()
    for s in range(0, n, CHUNK):
        e = min(s + CHUNK, n)
        Fc = Fm[s:e]
        S = Fc @ SENT.T
        for p in prepared:
            v = p["version"]
            st = STORE[v]
            Sb = S[:, p["gcols"]]
            tk_local = topk_vote(Sb, p["lab"], len(p["cs"]))
            st["topk_ref"][s:e] = p["to_gt"][tk_local]
            st["topk_ev"][s:e] = tk_local != p["idx_normal"]

            pscores = Fc @ p["mu_mat"].T
            p_local = pscores.argmax(axis=1)
            st["P_ref"][s:e] = p["to_gt"][p_local]
            st["P_ev"][s:e] = p_local != p["idx_normal"]

            esc = Fc @ p["diff_mat"].T
            beste, bestv = esc.argmax(axis=1), esc.max(axis=1)
            fired = bestv > 0
            st["PC_ref"][s:e] = np.where(fired, p["ev_to_gt"][beste], 0)
            st["PC_ev"][s:e] = fired
        del S
        log(f"  frames {e:,}/{n:,} ({time.time() - t1:.0f}s)")
    log(f"frames 채점 완료 ({time.time() - t1:.0f}s)")

    isf, iss, isn = ref == 2, ref == 3, ref == 0

    # 4) 뱅크별 표(row) 조립 ---------------------------------------------------
    rows = {}
    for p in prepared:
        v = p["version"]
        row = {"bank": v, "n_sentences": p["n_sent"], "n_classes": len(p["cs"])}
        for c in CLASSES:
            row[f"coherence_{c}"] = r4(p["COH"].get(c))

        for rule, pred_hy in (("topk", p["topk_hy"]), ("P", p["P_hy"]), ("PC", p["PC_hy"])):
            row[f"hy_acc_{rule}"] = r4(float((pred_hy == gt_hy).mean()))
            row[f"hy_mf1_{rule}"] = r4(macro_f1(gt_hy, pred_hy))
            for cname, cidx in (("falldown", 1), ("fire", 2), ("smoke", 3)):
                row[f"hy_recall_{cname}_{rule}"] = r4(recall_c(gt_hy, pred_hy, cidx))

        (mmean, mlo, mhi, mp), (amean, alo, ahi, ap) = bootstrap_delta(p["P_hy"], p["topk_hy"])
        row["d_mf1_P_minus_topk"] = r4(mmean)
        row["d_mf1_ci_lo"] = r4(mlo)
        row["d_mf1_ci_hi"] = r4(mhi)
        row["p_gt0"] = r4(mp)
        row["d_acc_P_minus_topk"] = r4(amean)
        row["d_acc_P_ci_lo"] = r4(alo)
        row["d_acc_P_ci_hi"] = r4(ahi)
        row["p_acc_P_gt0"] = r4(ap)

        (mmean2, mlo2, mhi2, mp2), (amean2, alo2, ahi2, ap2) = bootstrap_delta(p["PC_hy"], p["topk_hy"])
        row["d_mf1_PC_minus_topk"] = r4(mmean2)
        row["d_mf1_PC_ci_lo"] = r4(mlo2)
        row["d_mf1_PC_ci_hi"] = r4(mhi2)
        row["p_PC_gt0"] = r4(mp2)
        row["d_acc_PC_minus_topk"] = r4(amean2)
        row["d_acc_PC_ci_lo"] = r4(alo2)
        row["d_acc_PC_ci_hi"] = r4(ahi2)
        row["p_acc_PC_gt0"] = r4(ap2)

        st = STORE[v]
        for rule in ("topk", "P", "PC"):
            predref, ev = st[f"{rule}_ref"], st[f"{rule}_ev"]
            row[f"fr_fire_recall_{rule}"] = r4(rate(isf, predref, [2]))
            row[f"fr_smoke_recall_{rule}"] = r4(rate(iss, predref, [3]))
            row[f"fr_fp_{rule}"] = r4(rate(isn, predref, [2, 3]))
            row[f"fr_firing_rate_{rule}"] = r4(float(ev.mean()))
        rows[v] = row
    log(f"뱅크별 표 조립 완료 ({time.time() - t0:.0f}s)")

    # 5) CSV -------------------------------------------------------------------
    fieldnames_key = [
        "bank", "n_sentences", "n_classes",
        "coherence_normal", "coherence_falldown", "coherence_fire", "coherence_smoke",
        "hy_acc_topk", "hy_acc_P", "hy_acc_PC", "hy_mf1_topk", "hy_mf1_P", "hy_mf1_PC",
        "d_mf1_P_minus_topk", "d_mf1_ci_lo", "d_mf1_ci_hi", "p_gt0",
        "fr_fire_recall_topk", "fr_fire_recall_P", "fr_fire_recall_PC",
        "fr_smoke_recall_topk", "fr_smoke_recall_P", "fr_smoke_recall_PC",
        "fr_fp_topk", "fr_fp_P", "fr_fp_PC",
        "fr_firing_rate_topk", "fr_firing_rate_P", "fr_firing_rate_PC",
        # 확장(방법론 §Metrics/Report 요구사항 커버 — 명세의 "정확 이름" 집합에 추가):
        "d_mf1_PC_minus_topk", "d_mf1_PC_ci_lo", "d_mf1_PC_ci_hi", "p_PC_gt0",
        "d_acc_P_minus_topk", "d_acc_P_ci_lo", "d_acc_P_ci_hi", "p_acc_P_gt0",
        "d_acc_PC_minus_topk", "d_acc_PC_ci_lo", "d_acc_PC_ci_hi", "p_acc_PC_gt0",
        "hy_recall_falldown_topk", "hy_recall_falldown_P", "hy_recall_falldown_PC",
        "hy_recall_fire_topk", "hy_recall_fire_P", "hy_recall_fire_PC",
        "hy_recall_smoke_topk", "hy_recall_smoke_P", "hy_recall_smoke_PC",
    ]
    kr = {
        "bank": "뱅크", "n_sentences": "문장수", "n_classes": "클래스수",
        "coherence_normal": "정합도", "coherence_falldown": "정합도",
        "coherence_fire": "정합도", "coherence_smoke": "정합도",
        "hy_acc_topk": "sourcei정확도_topK", "hy_acc_P": "sourcei정확도_P", "hy_acc_PC": "sourcei정확도_PC",
        "hy_mf1_topk": "sourceimacroF1_topK", "hy_mf1_P": "sourceimacroF1_P", "hy_mf1_PC": "sourceimacroF1_PC",
        "d_mf1_P_minus_topk": "P-topK평균Δ부트스트랩", "d_mf1_ci_lo": "95%CI하한", "d_mf1_ci_hi": "95%CI상한",
        "p_gt0": "P(Δ>0)",
        "fr_fire_recall_topk": "frames표본fire재현율_topK", "fr_fire_recall_P": "frames표본fire재현율_P",
        "fr_fire_recall_PC": "frames표본fire재현율_PC",
        "fr_smoke_recall_topk": "frames표본smoke재현율_topK", "fr_smoke_recall_P": "frames표본smoke재현율_P",
        "fr_smoke_recall_PC": "frames표본smoke재현율_PC",
        "fr_fp_topk": "frames표본오탐율_topK", "fr_fp_P": "frames표본오탐율_P", "fr_fp_PC": "frames표본오탐율_PC",
        "fr_firing_rate_topk": "frames표본전체발화율_topK", "fr_firing_rate_P": "frames표본전체발화율_P",
        "fr_firing_rate_PC": "frames표본전체발화율_PC",
        "d_mf1_PC_minus_topk": "PC-topK평균Δ부트스트랩", "d_mf1_PC_ci_lo": "95%CI하한", "d_mf1_PC_ci_hi": "95%CI상한",
        "p_PC_gt0": "P(Δ>0)_PC",
        "d_acc_P_minus_topk": "정확도P-topK평균Δ", "d_acc_P_ci_lo": "95%CI하한", "d_acc_P_ci_hi": "95%CI상한",
        "p_acc_P_gt0": "P(Δacc>0)_P",
        "d_acc_PC_minus_topk": "정확도PC-topK평균Δ", "d_acc_PC_ci_lo": "95%CI하한", "d_acc_PC_ci_hi": "95%CI상한",
        "p_acc_PC_gt0": "P(Δacc>0)_PC",
        "hy_recall_falldown_topk": "sourcei재현율falldown_topK", "hy_recall_falldown_P": "sourcei재현율falldown_P",
        "hy_recall_falldown_PC": "sourcei재현율falldown_PC",
        "hy_recall_fire_topk": "sourcei재현율fire_topK", "hy_recall_fire_P": "sourcei재현율fire_P",
        "hy_recall_fire_PC": "sourcei재현율fire_PC",
        "hy_recall_smoke_topk": "sourcei재현율smoke_topK", "hy_recall_smoke_P": "sourcei재현율smoke_P",
        "hy_recall_smoke_PC": "sourcei재현율smoke_PC",
    }
    header = [f"{k}({kr[k]})" for k in fieldnames_key]
    banks_sorted = sorted(rows, key=vkey)
    csv_path = f"{CSV_DIR}/21_prototype_vs_topk.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        f.write(",".join(header) + "\n")
        for v in banks_sorted:
            row = rows[v]
            vals = ["" if row.get(k) is None else str(row.get(k)) for k in fieldnames_key]
            f.write(",".join(vals) + "\n")
    log(f"→ {csv_path} ({len(banks_sorted)}행)")

    # 6) Spearman 상관 -----------------------------------------------------------
    nsent = np.array([rows[b]["n_sentences"] for b in banks_sorted], dtype=float)
    spearman_nsent_firing, spearman_firing_mf1 = {}, {}
    for rule in ("topk", "P", "PC"):
        firing = np.array([rows[b][f"fr_firing_rate_{rule}"] for b in banks_sorted], dtype=float)
        mf1v = np.array([rows[b][f"hy_mf1_{rule}"] for b in banks_sorted], dtype=float)
        rho1, p1 = spearmanr(nsent, firing)
        rho2, p2 = spearmanr(firing, mf1v)
        spearman_nsent_firing[rule] = {"rho": r4(rho1), "p": r4(p1)}
        spearman_firing_mf1[rule] = {"rho": r4(rho2), "p": r4(p2)}
        log(f"  Spearman[{rule}] n_sentences↔firing_rate ρ={rho1:.3f}(p={p1:.3g})  "
            f"firing_rate↔macroF1 ρ={rho2:.3f}(p={p2:.3g})")

    # 7) proto_summary.json -------------------------------------------------------
    d_mf1_P = np.array([rows[b]["d_mf1_P_minus_topk"] for b in banks_sorted])
    ci_lo_P = np.array([rows[b]["d_mf1_ci_lo"] for b in banks_sorted])
    ci_hi_P = np.array([rows[b]["d_mf1_ci_hi"] for b in banks_sorted])
    d_mf1_PC = np.array([rows[b]["d_mf1_PC_minus_topk"] for b in banks_sorted])
    ci_lo_PC = np.array([rows[b]["d_mf1_PC_ci_lo"] for b in banks_sorted])
    ci_hi_PC = np.array([rows[b]["d_mf1_PC_ci_hi"] for b in banks_sorted])

    summary = {
        "n_banks": len(banks_sorted), "n_boot": N_BOOT, "rule_k": RULE_K,
        "skipped_banks": skipped,
        "spearman_nsentences_vs_firing_rate": spearman_nsent_firing,
        "spearman_firing_rate_vs_hy_macro_f1": spearman_firing_mf1,
        "mean_delta_mf1_P_minus_topk": r4(float(d_mf1_P.mean())),
        "mean_delta_mf1_PC_minus_topk": r4(float(d_mf1_PC.mean())),
        "count_P_beats_topk_mf1_point": int((d_mf1_P > 0).sum()),
        "count_P_ci_excludes_zero": int(((ci_lo_P > 0) | (ci_hi_P < 0)).sum()),
        "count_PC_beats_topk_mf1_point": int((d_mf1_PC > 0).sum()),
        "count_PC_ci_excludes_zero": int(((ci_lo_PC > 0) | (ci_hi_PC < 0)).sum()),
        "banks_P_significantly_better": [b for b in banks_sorted if rows[b]["d_mf1_ci_lo"] > 0],
        "banks_P_significantly_worse": [b for b in banks_sorted if rows[b]["d_mf1_ci_hi"] < 0],
        "banks_PC_significantly_better": [b for b in banks_sorted if rows[b]["d_mf1_PC_ci_lo"] > 0],
        "banks_PC_significantly_worse": [b for b in banks_sorted if rows[b]["d_mf1_PC_ci_hi"] < 0],
        "coherence_range_by_class": {c: [r4(v[0]), r4(v[1])] for c, v in coh_range.items()},
        "coherence_below_0.5": coherence_below_half,
        "self_checks": {
            "v1080_topk_sourcei_acc": r4(acc_check), "sourcei_ids_match": True,
            "frames_sample_n": int(len(sub)), "frames_sample_fire_n": int(n_fire_total),
            "prototype_unit_norm": True, "coherence_in_0_1": True,
        },
    }
    with open(f"{OUT}/proto_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    log(f"→ {OUT}/proto_summary.json")

    # 8) 그림 ----------------------------------------------------------------
    for fpath in glob.glob("/workspace/.fonts/*.tt[fc]"):
        fm.fontManager.addfont(fpath)
    plt.rcParams.update({
        "font.family": "Noto Sans CJK JP", "font.size": 11,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": "#e6e5e1", "grid.linewidth": 0.6,
        "axes.edgecolor": "#c3c2b7", "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb",
        "text.color": "#0b0b0b", "axes.labelcolor": "#52514e",
        "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.unicode_minus": False,
    })

    Y = np.arange(len(banks_sorted))
    fig, axes = plt.subplots(1, 2, figsize=(16, 10))
    ax = axes[0]
    for rule, marker, mfc in (("topk", "o", RC_TOPK), ("P", "s", RC_P), ("PC", "^", "none")):
        xs = [rows[b][f"hy_mf1_{rule}"] for b in banks_sorted]
        color = RC_TOPK if rule == "topk" else RC_P
        name = {"topk": "top-K(K=10)", "P": "프로토타입 argmax(P)", "PC": "프로토타입 대조(PC)"}[rule]
        ax.plot(xs, Y, marker, ms=7, color=color, mfc=mfc, mec=color, mew=1.3, label=name)
    ax.set_yticks(Y); ax.set_yticklabels(banks_sorted, fontsize=8.5); ax.invert_yaxis()
    ax.set_xlabel("sourcei GT macro-F1 (이벤트 3클래스: falldown/fire/smoke)")
    ax.legend(frameon=False, fontsize=9, loc="lower right")
    ax.set_title("규칙별 sourcei macro-F1 (뱅크 31종, 버전순)", loc="left", fontsize=11)

    ax = axes[1]
    for rule, color, dy, marker, mfc in (("P", RC_P, -0.12, "o", RC_P), ("PC", RC_PC, 0.12, "^", "none")):
        dd = np.array([rows[b][f"d_mf1_{rule}_minus_topk" if rule == "PC" else "d_mf1_P_minus_topk"]
                       for b in banks_sorted])
        lo = np.array([rows[b][f"d_mf1_{rule}_ci_lo" if rule == "PC" else "d_mf1_ci_lo"] for b in banks_sorted])
        hi = np.array([rows[b][f"d_mf1_{rule}_ci_hi" if rule == "PC" else "d_mf1_ci_hi"] for b in banks_sorted])
        xerr = np.vstack([dd - lo, hi - dd])
        ax.errorbar(dd, Y + dy, xerr=xerr, fmt=marker, ms=5, color=color, mfc=mfc, mec=color,
                    ecolor=color, elinewidth=1.1, capsize=2, alpha=0.9,
                    label=f"{rule} − top-K (95% CI, 카메라군집 부트스트랩 n={N_BOOT})")
    ax.axvline(0, color="#52514e", lw=1, ls="--")
    ax.set_yticks(Y); ax.set_yticklabels([]); ax.invert_yaxis()
    ax.set_xlabel("Δ macro-F1 (프로토타입 − top-K)")
    ax.legend(frameon=False, fontsize=8.5, loc="lower right")
    ax.set_title("Paired 카메라군집 부트스트랩 Δ + 95% CI", loc="left", fontsize=11)
    fig.suptitle(f"프로토타입(centroid) 채점 vs top-K 다수결 — sourcei GT {len(gt_hy):,}프레임/{n_cam}카메라, "
                 f"뱅크 {len(banks_sorted)}종", x=0.01, ha="left", fontsize=12.5)
    fig.tight_layout()
    fig.savefig(f"{FIG_DIR}/f22_prototype_vs_topk_paired.png", dpi=160)
    plt.close(fig)
    log("→ fig/f22_prototype_vs_topk_paired.png")

    fig, faxes = plt.subplots(1, 3, figsize=(18, 6.8))
    for ax, rule in zip(faxes, ["topk", "P", "PC"]):
        xs = np.array([rows[b]["n_sentences"] for b in banks_sorted], dtype=float)
        ys = np.array([rows[b][f"fr_firing_rate_{rule}"] for b in banks_sorted], dtype=float)
        color = RC_TOPK if rule == "topk" else RC_P
        rho, pval = spearman_nsent_firing[rule]["rho"], spearman_nsent_firing[rule]["p"]
        ax.scatter(xs, ys, s=48, color=color, edgecolor="#fcfcfb", lw=0.8, alpha=0.9)
        for b, x, y in zip(banks_sorted, xs, ys):
            if b in ANNOT_BANKS:
                ax.annotate(b, (x, y), textcoords="offset points", xytext=(5, 3), fontsize=7.5, color="#52514e")
        ax.set_xscale("log")
        ax.set_xlabel("뱅크 문장 수 (log)")
        if rule == "topk":
            ax.set_ylabel("frames 표본(24,792) 발화율 — non-normal 예측 비율")
        name = {"topk": "top-K(K=10)", "P": "프로토타입 argmax", "PC": "프로토타입 대조"}[rule]
        ax.set_title(f"{name}\nSpearman ρ={rho:.3f} (p={pval:.3g})", loc="left", fontsize=10.5)
    fig.suptitle("뱅크 크기(문장수) ↔ frames 표본 발화율 — 규칙별 (SAM3 약참조, GT 아님)",
                 x=0.01, ha="left", fontsize=12.5)
    fig.tight_layout()
    fig.savefig(f"{FIG_DIR}/f23_banksize_vs_firing.png", dpi=160)
    plt.close(fig)
    log("→ fig/f23_banksize_vs_firing.png")

    log(f"총 소요 {time.time() - t0:.0f}s")
    print("DONE")


if __name__ == "__main__":
    main()
