#!/usr/bin/env python3
"""B1 캡션→프레임 검색 감사 + B3 CCA/Procrustes 정렬 — sourcei 캡션 임베딩.

무엇을 묻는가:
  B1 · Gemini 가 쓴 한국어 캡션의 텍스트 임베딩으로 그 캡션이 붙은 프레임을
       찾아낼 수 있는가. 찾아내지 못하면 그 캡션은 프레임 단위 라벨로 쓸 수 없다.
       하드네거티브는 **같은 카메라의 다른 캡션 프레임**이다 — 카메라가 다르면
       장소 어휘만으로 맞춰지므로 그건 접지(grounding)의 증거가 아니다.
  B3 · 학습된 선형 사상(직교 Procrustes / CCA)이 이미지-텍스트 갭을 닫는가,
       그리고 그게 무학습 뱅크 채점(top-K 투표)을 실제로 개선하는가.

계약 (어기면 조용한 오답):
  · 페어 키 = frame.caption.strip() == image_embeddings.text_content.strip()
    (entity_type='caption', raw_files.raw_key LIKE 'sourcei%'). 벡터는 텍스트의
    함수라 같은 텍스트의 여러 caption 행은 같은 벡터 → 텍스트로 dedupe 한다.
  · 캡션은 독립 표본이 아니다 (한 캡션 = 한 이벤트의 여러 프레임, 한 카메라에
    여러 캡션). 집계는 **캡션 → 카메라 → 전체** 2단 macro 로만 한다.
  · B3 분할은 카메라 그룹 (GroupKFold) — 프레임 무작위 분할은 같은 이벤트가
    train/test 양쪽에 들어가 누출이다.
  · KR/EN 일치 검사는 **현 데이터로 불가능**하다. 영어 캡션 임베딩이 없다
    (caption_en 은 다른 데이터셋 264장만). 만들어내지 않고 그렇게 기록한다.

사용법: nohup python3 /workspace/caption_retrieval_cca.py > <log> 2>&1 &
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
import time
import warnings

# BLAS 스레드 캡은 numpy import 보다 앞이어야 먹는다.
_THREADS = os.environ.get("COS_THREADS", "4")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, _THREADS)

import numpy as np  # noqa: E402
import psycopg2  # noqa: E402
import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.font_manager as fm  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from scipy.linalg import orthogonal_procrustes  # noqa: E402
from scipy.stats import rankdata  # noqa: E402
from sklearn.cross_decomposition import CCA  # noqa: E402
from sklearn.model_selection import GroupKFold  # noqa: E402

sys.path.insert(0, "/workspace")

DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
OUT = os.environ.get("CRC_OUT", "/data/fiftyone/frames_bank/report/sourcei_gt")
CSV = os.path.join(OUT, "csv")
FIG = os.path.join(OUT, "fig")
DATASET = os.environ.get("CRC_DATASET", "sourcei")
BANK = os.environ.get("CRC_BANK", "v1.0.8.0")
EVENT_CLASSES = ("falldown", "fire", "smoke")
BG = "#fcfcfb"
C_RAW, C_PROC, C_CCA, C_CHANCE = "#2a78d6", "#eb6834", "#1baf7a", "#c3c2b7"
EK_COLORS = {
    "normal": "#c3c2b7", "falldown": "#2a78d6", "fire": "#eb6834",
    "smoke": "#1baf7a", "near_miss": "#8a6fbf", "drop": "#d4a017",
    "other": "#7a7a72", "violence": "#b0356b", "unknown": "#9a9a92",
}
KR_EN_NOTE = ("KR/EN 캡션 일치 검사는 현 데이터로 불가능 — 한국어 캡션 임베딩만 존재하고 "
              "영어 캡션(caption_en)은 다른 데이터셋 264장에만 있어 대규모 영어 캡션 "
              "임베딩이 없다. 가능하게 하려면 sourcei labels 의 caption_en 을 "
              "embedding-service /embed_text 로 임베딩해 image_embeddings 에 적재해야 한다.")


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def setup_font() -> None:
    for f in glob.glob("/workspace/.fonts/*.tt[fc]"):
        try:
            fm.fontManager.addfont(f)
        except Exception as e:  # noqa: BLE001
            log(f"  font skip {f}: {e}")
    plt.rcParams["font.family"] = "Noto Sans CJK JP"
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.facecolor"] = BG
    plt.rcParams["axes.facecolor"] = BG
    plt.rcParams["savefig.facecolor"] = BG


def bare(ax) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def l2(M: np.ndarray) -> np.ndarray:
    M = np.asarray(M, dtype=np.float32)
    n = np.linalg.norm(M, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return M / n


def write_csv(path: str, header: list[str], rows: list[list]) -> None:
    import csv as _csv
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = _csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    log(f"Wrote {path} ({len(rows)} rows)")


def fnum(x, nd=4):
    if x is None:
        return ""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x)
    if not np.isfinite(v):
        return ""
    return round(v, nd)


# ───────────────────────────── 1. 적재 + 페어링 ─────────────────────────────

def ap_multi(scores: np.ndarray, is_pos: np.ndarray) -> float:
    """다중 정답 average precision."""
    order = np.argsort(-scores, kind="stable")
    y = is_pos[order]
    if not y.any():
        return float("nan")
    cum = np.cumsum(y)
    prec = cum / np.arange(1, len(y) + 1, dtype=np.float64)
    return float(prec[y].mean())


def load_frames():
    import fiftyone as fo
    ds = fo.load_dataset(DATASET)
    n = len(ds)
    log(f"FiftyOne '{DATASET}': {n} frames")
    assert n == 7498, f"SELF-CHECK FAIL: sourcei frames == {n}, expected 7498"
    log("SELF-CHECK OK: sourcei frames == 7,498")
    fields = ["camera", "caption", "embedding", "ground_truth.label",
              "event_kind.label", "gt_source.label", "src_video", "event_index", "t_sec"]
    cam, cap, emb, gt, ek, gs, sv, evi, tsec = ds.values(fields)
    n_no_emb = sum(1 for e in emb if e is None or len(e) == 0)
    log(f"  frames without embedding: {n_no_emb}")
    F = np.zeros((n, 1024), dtype=np.float32)
    ok = np.zeros(n, dtype=bool)
    for i, e in enumerate(emb):
        if e is not None and len(e) == 1024:
            F[i] = np.asarray(e, dtype=np.float32)
            ok[i] = True
    del emb
    return dict(n=n, camera=[str(c) if c else "" for c in cam],
                caption=[(str(c).strip() if c else "") for c in cap],
                F=F, has_emb=ok,
                gt=[str(x) if x else "" for x in gt],
                ek=[str(x) if x else "" for x in ek],
                gs=[str(x) if x else "" for x in gs],
                src=[str(x) if x else "" for x in sv],
                evi=list(evi), tsec=list(tsec))


def load_caption_embeddings():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("""
      SELECT ie.entity_id, ie.asset_id, ie.text_content, ie.embedding::text, r.original_name,
             split_part(r.raw_key,'/',1) AS project
      FROM image_embeddings ie JOIN raw_files r USING(asset_id)
      WHERE ie.entity_type='caption' AND r.raw_key LIKE 'sourcei%'
    """)
    rows = cur.fetchall()
    log(f"caption-embedding rows for sourcei%: {len(rows)}")
    by_text: dict[str, dict] = {}
    proj_rows: dict[str, int] = {}
    for eid, aid, txt, vtxt, orig, proj in rows:
        proj_rows[proj] = proj_rows.get(proj, 0) + 1
        if not txt:
            continue
        t = txt.strip()
        v = np.fromstring(vtxt.strip("[]"), sep=",", dtype=np.float32)
        d = by_text.setdefault(t, {"vec": v, "assets": set(), "names": set(), "n_rows": 0,
                                   "entity_ids": set()})
        d["assets"].add(str(aid))
        d["names"].add(str(orig))
        d["entity_ids"].add(str(eid))
        d["n_rows"] += 1
        # 같은 텍스트의 벡터는 같아야 한다 (벡터 = 텍스트의 함수). 실측 확인.
        dv = float(np.abs(d["vec"] - v).max())
        if dv > 1e-5:
            d["vec_mismatch"] = max(d.get("vec_mismatch", 0.0), dv)
    mism = {t: d["vec_mismatch"] for t, d in by_text.items() if "vec_mismatch" in d}
    log(f"  rows per project: {proj_rows}")
    log(f"  distinct caption texts: {len(by_text)}; "
        f"same-text vector mismatches (>1e-5): {len(mism)}"
        + (f" max={max(mism.values()):.2e}" if mism else ""))
    cur.close()
    conn.close()
    return by_text, proj_rows


def norm_key(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]", "", s)


def build_pairs(fr, by_text):
    n = fr["n"]
    caps = fr["caption"]
    has_cap = np.array([bool(c) for c in caps])
    db_texts = set(by_text)
    dbn: dict[str, str] = {}
    for t in db_texts:
        dbn.setdefault(norm_key(t), t)
    exact = np.array([bool(c) and c in db_texts for c in caps])
    loose = np.array([bool(c) and (c in db_texts or norm_key(c) in dbn) for c in caps])
    matched_text = [(c if c in db_texts else dbn.get(norm_key(c), "")) for c in caps]
    funnel = {
        "n_frames_total": int(n),
        "n_frames_with_embedding": int(fr["has_emb"].sum()),
        "n_frames_with_caption_string": int(has_cap.sum()),
        "n_frames_caption_exact_match": int(exact.sum()),
        "n_frames_caption_loose_match": int(loose.sum()),
        "n_distinct_captions_in_dataset": len({c for c in caps if c}),
        "n_distinct_captions_in_db": len(db_texts),
        "n_distinct_captions_matched": len({matched_text[i] for i in range(n)
                                            if loose[i] and matched_text[i]}),
        "n_db_captions_unused_by_frames": len(db_texts - {c for c in caps if c}),
        "kr_en_agreement": "INFEASIBLE — " + KR_EN_NOTE,
    }
    use = loose & fr["has_emb"]
    funnel["n_pairs_final"] = int(use.sum())
    log("PAIRING FUNNEL")
    for k, v in funnel.items():
        log(f"  {k}: {v}")
    per_cam: dict[str, dict] = {}
    for i in range(n):
        d = per_cam.setdefault(fr["camera"][i], {"frames": 0, "with_caption": 0,
                                                 "matched": 0, "captions": set()})
        d["frames"] += 1
        if has_cap[i]:
            d["with_caption"] += 1
        if use[i]:
            d["matched"] += 1
            d["captions"].add(matched_text[i])
    log("PAIRING FUNNEL per camera (frames / with_caption / matched / distinct_captions)")
    for c in sorted(per_cam):
        d = per_cam[c]
        log(f"  {c:52s} {d['frames']:5d} {d['with_caption']:5d} "
            f"{d['matched']:5d} {len(d['captions']):4d}")
    funnel["per_camera"] = {c: {"frames": d["frames"], "with_caption": d["with_caption"],
                                "matched": d["matched"],
                                "distinct_captions": len(d["captions"])}
                            for c, d in per_cam.items()}
    ek_cnt: dict[str, int] = {}
    gs_cnt: dict[str, int] = {}
    for i in range(n):
        if use[i]:
            ek_cnt[fr["ek"][i]] = ek_cnt.get(fr["ek"][i], 0) + 1
            gs_cnt[fr["gs"][i]] = gs_cnt.get(fr["gs"][i], 0) + 1
    funnel["matched_by_event_kind"] = ek_cnt
    funnel["matched_by_gt_source"] = gs_cnt
    log(f"  matched by event_kind: {ek_cnt}")
    log(f"  matched by gt_source:  {gs_cnt}")
    gt_in: dict[str, int] = {}
    gt_out: dict[str, int] = {}
    for i in range(n):
        d = gt_in if use[i] else gt_out
        d[fr["gt"][i]] = d.get(fr["gt"][i], 0) + 1
    funnel["ground_truth_paired"] = gt_in
    funnel["ground_truth_unpaired_excluded"] = gt_out
    log(f"  ground_truth of PAIRED frames (= B3 학습/평가 모집단): {gt_in}")
    log(f"  ground_truth of UNPAIRED (excluded) frames: {gt_out}")
    log("  ⚠ 캡션 없는 프레임은 전부 camera='v3_unknown'(sourcei_v3) 이고 "
        "sourcei_v3 에는 caption 임베딩이 0행이다 → fire/smoke 다수가 B3 모집단 밖이다.")
    return use, matched_text, funnel


# ───────────────────────────── 2. B1 검색 ─────────────────────────────

def majority(vals: list[str]) -> str:
    c: dict[str, int] = {}
    for v in vals:
        c[v] = c.get(v, 0) + 1
    return max(c.items(), key=lambda kv: (kv[1], kv[0]))[0] if c else ""


def b1_retrieval(fr, use, matched_text, by_text):
    n = fr["n"]
    Fn = l2(fr["F"].copy())
    cams = np.array(fr["camera"])
    caps_eff = np.array([matched_text[i] if use[i] else "" for i in range(n)], dtype=object)
    texts = sorted({matched_text[i] for i in range(n) if use[i]})
    log(f"B1: distinct matched captions = {len(texts)}")
    T = l2(np.vstack([by_text[t]["vec"] for t in texts]))
    S_txt = T @ Fn.T                                   # (n_cap, n_frames)
    rows = []
    max_same_cam_neg = 0
    n_fallback = 0
    for ci, t in enumerate(texts):
        pos = np.where(caps_eff == t)[0]
        pcams = sorted(set(cams[pos].tolist()))
        same_cam = np.isin(cams, pcams)
        neg = np.where(same_cam & (caps_eff != t))[0]
        fallback = len(neg) < 50
        max_same_cam_neg = max(max_same_cam_neg, len(neg))
        n_same_cam_neg = len(neg)
        if fallback:
            neg = np.setdiff1d(np.arange(n), pos)
            n_fallback += 1
        cand = np.concatenate([pos, neg])
        is_pos = np.zeros(len(cand), dtype=bool)
        is_pos[: len(pos)] = True
        sc = S_txt[ci, cand]
        rk = rankdata(-sc, method="average")
        pct = (rk[is_pos] - 1.0) / max(len(cand) - 1, 1)
        order = np.argsort(-sc, kind="stable")
        y = is_pos[order]
        top_hit = {k: bool(y[:k].any()) for k in (1, 5, 10)}
        ap = ap_multi(sc, is_pos)
        chance = len(pos) / float(len(pos) + len(neg))
        bp = float(sc[is_pos].max())
        bn = float(sc[~is_pos].max()) if (~is_pos).any() else float("nan")
        # ── 단일-정답 LOO 변형: 텍스트 질의 vs 이미지-평균 질의 (동일 후보집합) ──
        cand1 = np.concatenate([[-1], neg])            # -1 = 자리표시자(해당 positive)
        txt_r, img_r = [], []
        if len(pos) >= 2:
            Sp = Fn[pos].sum(axis=0)
            Q = (Sp[None, :] - Fn[pos]) / (len(pos) - 1)
            Q = l2(Q)
            Simg = Q @ Fn[neg].T                       # (n_pos, n_neg)
            Sself = np.einsum("ij,ij->i", Q, Fn[pos])  # 자기 자신과의 코사인
            neg_txt = S_txt[ci, neg]
            for pi in range(len(pos)):
                r_t = 1 + int((neg_txt > S_txt[ci, pos[pi]]).sum())
                r_i = 1 + int((Simg[pi] > Sself[pi]).sum())
                txt_r.append(r_t)
                img_r.append(r_i)
            del Simg, Q
        elif len(pos) == 1:
            neg_txt = S_txt[ci, neg]
            txt_r.append(1 + int((neg_txt > S_txt[ci, pos[0]]).sum()))
        txt_r = np.asarray(txt_r, dtype=np.float64)
        img_r = np.asarray(img_r, dtype=np.float64)

        def _rk(arr, k):
            return float((arr <= k).mean()) if arr.size else float("nan")

        n_c1 = len(cand1)
        rows.append(dict(
            caption=t, n_pos=int(len(pos)), n_neg=int(len(neg)),
            n_same_camera_neg=int(n_same_cam_neg), fallback_all_frames=bool(fallback),
            cameras="|".join(pcams), n_cameras=len(pcams), camera_major=majority(cams[pos].tolist()),
            event_kind_major=majority([fr["ek"][i] for i in pos]),
            gt_source_major=majority([fr["gs"][i] for i in pos]),
            gt_major=majority([fr["gt"][i] for i in pos]),
            recall_at_1=float(top_hit[1]), recall_at_5=float(top_hit[5]),
            recall_at_10=float(top_hit[10]),
            AP=ap, chance_AP=chance, AP_over_chance=(ap / chance if chance > 0 else float("nan")),
            best_pos_cos=bp, best_neg_cos=bn, margin=bp - bn,
            mean_pos_percentile=float(pct.mean()),
            median_pos_percentile=float(np.median(pct)),
            loo_txt_recall_at_1=_rk(txt_r, 1), loo_txt_recall_at_5=_rk(txt_r, 5),
            loo_txt_recall_at_10=_rk(txt_r, 10),
            loo_txt_mrr=float((1.0 / txt_r).mean()) if txt_r.size else float("nan"),
            loo_img_recall_at_1=_rk(img_r, 1), loo_img_recall_at_5=_rk(img_r, 5),
            loo_img_recall_at_10=_rk(img_r, 10),
            loo_img_mrr=float((1.0 / img_r).mean()) if img_r.size else float("nan"),
            loo_chance_recall_at_5=5.0 / n_c1,
            loo_chance_mrr=float(np.sum(1.0 / np.arange(1, n_c1 + 1)) / n_c1),
        ))
    log(f"B1: max same-camera hard negatives over captions = {max_same_cam_neg}; "
        f"captions needing fallback (<50) = {n_fallback}/{len(texts)}")
    assert max_same_cam_neg >= 50, (
        "SELF-CHECK FAIL: no caption has >= 50 same-camera hard negatives "
        f"(max={max_same_cam_neg}) — hard-negative design impossible")
    log("SELF-CHECK OK: at least one caption has >= 50 same-camera hard negatives "
        f"(max={max_same_cam_neg})")
    return rows, texts, T, Fn, S_txt


METRICS_1 = ["recall_at_1", "recall_at_5", "recall_at_10", "AP", "chance_AP",
             "AP_over_chance", "margin", "best_pos_cos", "best_neg_cos",
             "mean_pos_percentile", "loo_txt_recall_at_5", "loo_txt_mrr",
             "loo_img_recall_at_5", "loo_img_mrr", "loo_chance_recall_at_5",
             "loo_chance_mrr"]


def group_macro(rows: list[dict], key: str):
    g: dict[str, list[dict]] = {}
    for r in rows:
        g.setdefault(r[key], []).append(r)
    out = []
    for k in sorted(g):
        rs = g[k]
        d = {key: k, "n_captions": len(rs), "n_pos_frames": sum(r["n_pos"] for r in rs),
             "n_fallback_captions": sum(1 for r in rs if r["fallback_all_frames"])}
        for m in METRICS_1:
            v = np.array([r[m] for r in rs], dtype=np.float64)
            v = v[np.isfinite(v)]
            d[m] = float(v.mean()) if v.size else float("nan")
        out.append(d)
    return out


def macro_of(per_group: list[dict]):
    d = {"n_groups": len(per_group),
         "n_captions": sum(g["n_captions"] for g in per_group)}
    for m in METRICS_1:
        v = np.array([g[m] for g in per_group], dtype=np.float64)
        v = v[np.isfinite(v)]
        d[m] = float(v.mean()) if v.size else float("nan")
    return d


def pooled_of(rows: list[dict]):
    d = {"n_groups": 1, "n_captions": len(rows)}
    for m in METRICS_1:
        v = np.array([r[m] for r in rows], dtype=np.float64)
        v = v[np.isfinite(v)]
        d[m] = float(v.mean()) if v.size else float("nan")
    return d


# ───────────────────────────── 3. B3 정렬 ─────────────────────────────

def cca_y(cca, Y: np.ndarray, block: int = 4096) -> np.ndarray:
    """CCA 의 y-side 투영 — 공식 transform(X, Y) 을 dummy X 로 호출."""
    outs = []
    p = cca.n_features_in_
    for s in range(0, Y.shape[0], block):
        yb = np.asarray(Y[s:s + block], dtype=np.float64)
        xd = np.zeros((yb.shape[0], p), dtype=np.float64)
        outs.append(cca.transform(xd, yb)[1])
    return np.vstack(outs).astype(np.float32)


def retrieval_macro(Fr: np.ndarray, Tr: np.ndarray, idx: np.ndarray,
                    caps_eff: np.ndarray, cams: np.ndarray, texts: list[str]):
    """주어진 표현으로 idx 부분집합 안에서 B1 AP/recall@5 를 재계산 → 카메라 macro."""
    sub_caps = caps_eff[idx]
    sub_cams = cams[idx]
    Fs = l2(Fr[idx].copy())
    t2i = {t: i for i, t in enumerate(texts)}
    per_cam: dict[str, list[tuple[float, float]]] = {}
    for t in sorted(set(sub_caps.tolist())):
        if not t:
            continue
        pos = np.where(sub_caps == t)[0]
        pcams = sorted(set(sub_cams[pos].tolist()))
        same = np.isin(sub_cams, pcams)
        neg = np.where(same & (sub_caps != t))[0]
        if len(neg) < 50:
            neg = np.setdiff1d(np.arange(len(idx)), pos)
        if len(neg) == 0:
            continue
        cand = np.concatenate([pos, neg])
        ip = np.zeros(len(cand), dtype=bool)
        ip[: len(pos)] = True
        q = Tr[t2i[t]]
        q = q / max(float(np.linalg.norm(q)), 1e-12)
        sc = Fs[cand] @ q
        order = np.argsort(-sc, kind="stable")
        r5 = float(ip[order][:5].any())
        per_cam.setdefault(majority(sub_cams[pos].tolist()), []).append(
            (ap_multi(sc, ip), r5))
    if not per_cam:
        return float("nan"), float("nan"), 0
    aps = [float(np.nanmean([x[0] for x in v])) for v in per_cam.values()]
    r5s = [float(np.nanmean([x[1] for x in v])) for v in per_cam.values()]
    ncap = sum(len(v) for v in per_cam.values())
    return float(np.nanmean(aps)), float(np.nanmean(r5s)), ncap


def load_bank():
    from prompt_cos_db import RULE_K, load_banks, load_sentence_vectors
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    log(f"bank {BANK}: loading sentence vectors (all prompt vectors, then slicing)…")
    h2c, SENT = load_sentence_vectors(cur)
    log(f"  prompt vectors: {SENT.shape}")
    banks = load_banks(cur, [BANK])
    cur.close()
    conn.close()
    assert len(banks) == 1, f"bank {BANK} not found as db_backed (got {len(banks)})"
    b = banks[0]
    missing = [h for h, _c, _g in b["rows"] if h not in h2c]
    assert not missing, f"bank {BANK}: {len(missing)} sentences without vectors"
    classes = sorted({c for _h, c, _g in b["rows"]})
    cols, lab = [], []
    for chash, cls, _g in b["rows"]:
        cols.append(h2c[chash])
        lab.append(classes.index(cls))
    SB = np.ascontiguousarray(SENT[np.asarray(cols, dtype=np.int64)])
    del SENT, h2c
    SB = l2(SB)
    lab = np.asarray(lab, dtype=np.int32)
    log(f"  bank {BANK}: {SB.shape[0]} sentence columns, classes={classes}, RULE_K={RULE_K}")
    return SB, lab, classes, RULE_K


def zeroshot(Fr: np.ndarray, SB: np.ndarray, lab: np.ndarray, classes: list[str],
             k: int, gt: list[str], idx: np.ndarray):
    from prompt_cos_db import topk_vote
    Fs = l2(Fr[idx].copy())
    out_pred = np.empty(len(idx), dtype=np.int64)
    step = 2000
    for s in range(0, len(idx), step):
        Sb = Fs[s:s + step] @ SB.T
        out_pred[s:s + step] = topk_vote(Sb, lab, len(classes), k=k)
        del Sb
    pred = [classes[p] for p in out_pred]
    truth = [gt[i] for i in idx]
    acc = float(np.mean([p == t for p, t in zip(pred, truth)]))
    f1s = {}
    for c in EVENT_CLASSES:
        tp = sum(1 for p, t in zip(pred, truth) if p == c and t == c)
        fp = sum(1 for p, t in zip(pred, truth) if p == c and t != c)
        fn = sum(1 for p, t in zip(pred, truth) if p != c and t == c)
        pr = tp / (tp + fp) if tp + fp else 0.0
        rc = tp / (tp + fn) if tp + fn else 0.0
        f1s[c] = 2 * pr * rc / (pr + rc) if pr + rc else 0.0
    return acc, float(np.mean(list(f1s.values()))), f1s


def b3_alignment(fr, use, matched_text, by_text, texts, Fn):
    n = fr["n"]
    cams = np.array(fr["camera"])
    caps_eff = np.array([matched_text[i] if use[i] else "" for i in range(n)], dtype=object)
    pair_idx = np.where(use)[0]
    t2i = {t: i for i, t in enumerate(texts)}
    T = l2(np.vstack([by_text[t]["vec"] for t in texts]))
    Ximg = Fn[pair_idx]                                    # 이미 L2 정규화
    Xtxt = T[np.asarray([t2i[caps_eff[i]] for i in pair_idx])]
    groups = cams[pair_idx]
    ucam = sorted(set(groups.tolist()))
    log(f"B3: pairs={len(pair_idx)}, distinct captions={len(texts)}, cameras={len(ucam)}")
    n_splits = 3 if len(ucam) >= 3 else len(ucam)
    log(f"B3: GroupKFold(n_splits={n_splits}) on camera")
    SB, lab, classes, RK = load_bank()
    gkf = GroupKFold(n_splits=n_splits)
    csv_rows, summary_folds = [], []
    warn_leak = []
    for fold, (tr, te) in enumerate(gkf.split(Ximg, groups=groups)):
        gi_tr, gi_te = pair_idx[tr], pair_idx[te]
        ncap_tr = len({caps_eff[i] for i in gi_tr})
        ncap_te = len({caps_eff[i] for i in gi_te})
        cams_tr = sorted(set(groups[tr].tolist()))
        cams_te = sorted(set(groups[te].tolist()))
        log(f"── fold {fold}: train {len(tr)} pairs / {ncap_tr} captions / "
            f"{len(cams_tr)} cams | test {len(te)} pairs / {ncap_te} captions / "
            f"{len(cams_te)} cams")
        log(f"   test cameras: {cams_te}")
        gt_cnt = {}
        for sp, gidx in (("train", gi_tr), ("test", gi_te)):
            c = {}
            for i in gidx:
                c[fr["gt"][i]] = c.get(fr["gt"][i], 0) + 1
            gt_cnt[sp] = c
            log(f"   {sp} ground_truth: {c}")
        A = np.asarray(Ximg[tr], dtype=np.float64)
        B = np.asarray(Xtxt[tr], dtype=np.float64)
        R, _ss = orthogonal_procrustes(A, B)
        orth = float(np.linalg.norm(R.T @ R - np.eye(R.shape[0]), ord="fro"))
        log(f"   Procrustes ||R^T R - I||_F = {orth:.3e}")
        assert orth < 1e-3, f"SELF-CHECK FAIL: Procrustes R not orthogonal ({orth:.3e})"
        log("   SELF-CHECK OK: Procrustes R orthogonal (< 1e-3)")
        rng = np.random.default_rng(1234 + fold)
        perm = rng.permutation(len(tr))
        while len(tr) > 1 and (perm == np.arange(len(tr))).all():
            perm = rng.permutation(len(tr))
        Rs, _ = orthogonal_procrustes(A, np.asarray(Xtxt[tr][perm], dtype=np.float64))
        ncomp = int(min(64, max(len(tr) // 10, 1), 256, max(ncap_tr - 1, 1)))
        log(f"   CCA n_components = min(64, n_train//10={len(tr)//10}, 256, "
            f"n_distinct_captions_train-1={ncap_tr-1}) = {ncomp}")
        cca = CCA(n_components=ncomp, max_iter=1000)
        t0 = time.time()
        with warnings.catch_warnings(record=True) as wl:
            warnings.simplefilter("always")
            cca.fit(np.asarray(Ximg[tr], dtype=np.float64),
                    np.asarray(Xtxt[tr], dtype=np.float64))
        log(f"   CCA fitted in {time.time()-t0:.1f}s (n_iter head={list(cca.n_iter_)[:5]}, "
            f"n_iter len={len(cca.n_iter_)})")
        seen = set()
        for w in wl:
            msg = f"{w.category.__name__}: {w.message}"
            if msg not in seen:
                seen.add(msg)
                log(f"   CCA warning: {msg}")
        nz = int((np.abs(cca.x_rotations_).sum(axis=0) > 0).sum())
        log(f"   CCA non-degenerate components: {nz}/{ncomp}")
        reps = {}
        Fimg_R = l2(Fn @ R.astype(np.float32))
        reps["procrustes"] = (Fimg_R, T)
        reps["shuffled"] = (l2(Fn @ Rs.astype(np.float32)), T)
        reps["raw"] = (Fn, T)
        Fc = l2(cca.transform(np.asarray(Fn, dtype=np.float64)).astype(np.float32))
        Tc = l2(cca_y(cca, T))
        reps["cca"] = (Fc, Tc)
        SB_c = l2(cca_y(cca, SB))
        for meth in ("raw", "procrustes", "cca", "shuffled"):
            Frep, Trep = reps[meth]
            sbank = SB_c if meth == "cca" else SB
            rec = {"fold": fold, "method": meth, "n_train_pairs": int(len(tr)),
                   "n_test_pairs": int(len(te)),
                   "n_distinct_captions_train": ncap_tr,
                   "n_distinct_captions_test": ncap_te,
                   "n_cameras_train": len(cams_tr), "n_cameras_test": len(cams_te),
                   "cca_n_components": ncomp if meth == "cca" else "",
                   "test_cameras": "|".join(cams_te)}
            for sp in ("train", "test"):
                for c in ("normal",) + EVENT_CLASSES:
                    rec[f"n_gt_{c}_{sp}"] = int(gt_cnt[sp].get(c, 0))
            for split, gidx in (("test", gi_te), ("train", gi_tr)):
                ti = np.asarray([t2i[caps_eff[i]] for i in gidx])
                mc = float(np.einsum("ij,ij->i", Frep[gidx], Trep[ti]).mean())
                ap, r5, ncap = retrieval_macro(Frep, Trep, gidx, caps_eff, cams, texts)
                acc, mf1, per = zeroshot(Frep, sbank, lab, classes, RK, fr["gt"], gidx)
                rec[f"mean_cos_{split}"] = mc
                rec[f"retrieval_AP_{split}"] = ap
                rec[f"retrieval_recall5_{split}"] = r5
                rec[f"retrieval_n_captions_{split}"] = ncap
                rec[f"zeroshot_macro_f1_{split}"] = mf1
                rec[f"zeroshot_acc_{split}"] = acc
                for c in EVENT_CLASSES:
                    rec[f"zeroshot_f1_{c}_{split}"] = per[c]
            csv_rows.append(rec)
            summary_folds.append(rec)
            log(f"   {meth:10s} test: cos={rec['mean_cos_test']:.4f} "
                f"AP={rec['retrieval_AP_test']:.4f} R@5={rec['retrieval_recall5_test']:.4f} "
                f"mF1={rec['zeroshot_macro_f1_test']:.4f} acc={rec['zeroshot_acc_test']:.4f} "
                f"| train: cos={rec['mean_cos_train']:.4f} "
                f"AP={rec['retrieval_AP_train']:.4f} mF1={rec['zeroshot_macro_f1_train']:.4f}")
        raw_cos = [r for r in csv_rows if r["fold"] == fold and r["method"] == "raw"][0]["mean_cos_test"]
        sh_cos = [r for r in csv_rows if r["fold"] == fold and r["method"] == "shuffled"][0]["mean_cos_test"]
        if sh_cos > raw_cos + 0.01:
            msg = (f"LEAKAGE WARNING fold {fold}: shuffled-pairs test mean_cos "
                   f"{sh_cos:.4f} exceeds raw {raw_cos:.4f} by "
                   f"{sh_cos-raw_cos:.4f} (> 0.01)")
            log(msg)
            warn_leak.append(msg)
        else:
            log(f"   SELF-CHECK OK fold {fold}: shuffled test mean_cos {sh_cos:.4f} "
                f"<= raw {raw_cos:.4f} + 0.01")
        del reps, Fc, Tc, SB_c, Fimg_R, A, B
    return csv_rows, warn_leak, classes, RK


# ───────────────────────────── 4. 그림 ─────────────────────────────

def fig_b1(rows, per_cam, overall, path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15.5, 6.2))
    eks = sorted({r["event_kind_major"] for r in rows})
    for ek in eks:
        rs = [r for r in rows if r["event_kind_major"] == ek]
        ax1.scatter([r["chance_AP"] for r in rs], [r["AP"] for r in rs],
                    s=[min(14 + 0.35 * r["n_pos"], 170) for r in rs],
                    c=EK_COLORS.get(ek, "#7a7a72"), alpha=0.82,
                    edgecolors="white", linewidths=0.6,
                    label=f"{ek} ({len(rs)})", zorder=3)
    lo = min(min(r["chance_AP"] for r in rows), min(r["AP"] for r in rows))
    hi = max(max(r["chance_AP"] for r in rows), max(r["AP"] for r in rows))
    pad = 0.05 * (hi - lo + 1e-9)
    ax1.plot([lo - pad, hi + pad], [lo - pad, hi + pad], color=C_CHANCE, lw=1.6,
             ls="--", zorder=2, label="y=x (무작위 수준)")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("무작위 기대 AP (= 양성비율)")
    ax1.set_ylabel("실측 AP (캡션 텍스트 질의)")
    n_above = sum(1 for r in rows if r["AP"] > r["chance_AP"])
    ratio_of_means = overall["AP"] / overall["chance_AP"] if overall["chance_AP"] else float("nan")
    ax1.set_title(f"캡션 {len(rows)}개 중 {n_above}개만 무작위보다 위\n"
                  f"카메라 macro AP {overall['AP']:.3f} vs 무작위 {overall['chance_AP']:.3f} "
                  f"(평균비 {ratio_of_means:.2f}배 · 캡션별 비 평균 {overall['AP_over_chance']:.2f}배)\n"
                  "점 크기 = 양성 프레임 수, 색 = 이벤트 종류(다수결)", loc="left", fontsize=10.5)
    ax1.legend(fontsize=8, frameon=False, ncol=2, loc="lower right")
    ax1.grid(alpha=0.18, which="both")
    bare(ax1)

    cams = [g["camera_major"] for g in per_cam]
    x = np.arange(len(cams))
    w = 0.38
    tv = [g["loo_txt_recall_at_5"] for g in per_cam]
    iv = [g["loo_img_recall_at_5"] for g in per_cam]
    cv = [g["loo_chance_recall_at_5"] for g in per_cam]
    ax2.bar(x - w / 2, tv, w, color=C_RAW, label="텍스트 질의 (캡션 임베딩)", zorder=3)
    ax2.bar(x + w / 2, iv, w, color=C_CCA, label="이미지-평균 질의 (LOO)", zorder=3)
    ax2.hlines(cv, x - w, x + w, color=C_CHANCE, lw=2.2, zorder=4, label="무작위 기대")
    ax2.set_xticks(x)
    ax2.set_xticklabels([c[:30] for c in cams], rotation=55, ha="right", fontsize=7)
    ax2.set_ylabel("Recall@5 (단일 정답, 동일 후보집합)")
    ax2.set_ylim(0, 1.02)
    mt = float(np.nanmean(tv))
    mi = float(np.nanmean([v for v in iv if np.isfinite(v)]))
    ax2.set_title(f"모달리티 갭 — 텍스트 질의 Recall@5 {mt:.3f}\nvs 이미지-평균 질의 {mi:.3f} (카메라 macro)\n"
                  "같은 후보집합·같은 단일 정답 기준", loc="left", fontsize=10.5)
    ax2.legend(fontsize=8.5, frameon=False)
    ax2.grid(axis="y", alpha=0.18)
    bare(ax2)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)
    log(f"Wrote {path}")


def fig_b3(rows, path, verdict):
    meths = ["raw", "procrustes", "cca", "shuffled"]
    labels = {"raw": "raw (원본)", "procrustes": "Procrustes", "cca": "CCA",
              "shuffled": "shuffled (대조)"}
    cols = {"raw": C_RAW, "procrustes": C_PROC, "cca": C_CCA, "shuffled": C_CHANCE}
    panels = [("mean_cos_test", "테스트 폴드 평균 코사인 (이미지↔캡션)\n※ CCA 값은 정규상관공간(64차원) 코사인 — raw 1024차원과 직접 비교 불가"),
              ("retrieval_AP_test", "테스트 폴드 검색 AP (카메라 macro)"),
              ("zeroshot_macro_f1_test", f"무학습 뱅크 {BANK} 이벤트 macro-F1")]
    fig, axes = plt.subplots(1, 3, figsize=(16, 5.6))
    for ax, (key, title) in zip(axes, panels):
        vals, pts = [], []
        for m in meths:
            v = [r[key] for r in rows if r["method"] == m and np.isfinite(r[key])]
            vals.append(float(np.mean(v)) if v else float("nan"))
            pts.append(v)
        x = np.arange(len(meths))
        ax.bar(x, vals, 0.62, color=[cols[m] for m in meths], zorder=3,
               edgecolor="white", linewidth=0.8)
        for i, v in enumerate(pts):
            ax.scatter([i] * len(v), v, s=34, color="#2b2b28", zorder=5,
                       alpha=0.85, linewidths=0)
        span = max([max(v) for v in pts if v] + [max(vals)] + [1e-9])
        for i, v in enumerate(vals):
            if np.isfinite(v):
                top = max([v] + (pts[i] if pts[i] else []))
                ax.text(i, top + 0.035 * span, f"{v:.3f}", ha="center",
                        va="bottom", fontsize=9.5)
        ax.margins(y=0.14)
        ax.axhline(0, color="#8a8a82", lw=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels([labels[m] for m in meths], rotation=22, ha="right", fontsize=9)
        ax.set_title(title, loc="left", fontsize=10.5)
        ax.grid(axis="y", alpha=0.18)
        bare(ax)
    fig.suptitle(verdict, x=0.006, ha="left", fontsize=12.5)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(path, dpi=140)
    plt.close(fig)
    log(f"Wrote {path}")


# ───────────────────────────── main ─────────────────────────────

def main() -> None:
    t00 = time.time()
    os.makedirs(CSV, exist_ok=True)
    os.makedirs(FIG, exist_ok=True)
    setup_font()
    log(f"threads={_THREADS} out={OUT}")
    log("NOTE: " + KR_EN_NOTE)

    fr = load_frames()
    by_text, proj_rows = load_caption_embeddings()
    use, matched_text, funnel = build_pairs(fr, by_text)
    funnel["caption_rows_per_project"] = proj_rows

    # ── B1 ──
    rows, texts, T, Fn, S_txt = b1_retrieval(fr, use, matched_text, by_text)
    hdr = ["caption(캡션)", "n_pos(양성프레임)", "n_neg(음성프레임)",
           "n_same_camera_neg(동일카메라음성)", "fallback_all_frames(전체폴백)",
           "cameras(카메라)", "n_cameras(카메라수)", "camera_major(대표카메라)",
           "event_kind_major(이벤트종류)", "gt_source_major(GT출처)", "gt_major(GT클래스)",
           "recall_at_1(재현율@1)", "recall_at_5(재현율@5)", "recall_at_10(재현율@10)",
           "AP(평균정밀도)", "chance_AP(무작위AP)", "AP_over_chance(무작위대비배수)",
           "best_pos_cos(최고양성코사인)", "best_neg_cos(최고음성코사인)", "margin(마진)",
           "mean_pos_percentile(양성평균백분위)", "median_pos_percentile(양성중앙백분위)",
           "loo_txt_recall_at_1(단일정답텍스트@1)", "loo_txt_recall_at_5(단일정답텍스트@5)",
           "loo_txt_recall_at_10(단일정답텍스트@10)", "loo_txt_mrr(텍스트MRR)",
           "loo_img_recall_at_1(이미지평균@1)", "loo_img_recall_at_5(이미지평균@5)",
           "loo_img_recall_at_10(이미지평균@10)", "loo_img_mrr(이미지평균MRR)",
           "loo_chance_recall_at_5(무작위@5)", "loo_chance_mrr(무작위MRR)"]
    keys = ["caption", "n_pos", "n_neg", "n_same_camera_neg", "fallback_all_frames",
            "cameras", "n_cameras", "camera_major", "event_kind_major", "gt_source_major",
            "gt_major", "recall_at_1", "recall_at_5", "recall_at_10", "AP", "chance_AP",
            "AP_over_chance", "best_pos_cos", "best_neg_cos", "margin",
            "mean_pos_percentile", "median_pos_percentile",
            "loo_txt_recall_at_1", "loo_txt_recall_at_5", "loo_txt_recall_at_10",
            "loo_txt_mrr", "loo_img_recall_at_1", "loo_img_recall_at_5",
            "loo_img_recall_at_10", "loo_img_mrr", "loo_chance_recall_at_5",
            "loo_chance_mrr"]
    write_csv(os.path.join(CSV, "26_caption_retrieval.csv"), hdr,
              [[r[k] if isinstance(r[k], (str, bool, int)) else fnum(r[k]) for k in keys]
               for r in sorted(rows, key=lambda r: -r["n_pos"])])

    per_cam = group_macro(rows, "camera_major")
    per_ek = group_macro(rows, "event_kind_major")
    per_gs = group_macro(rows, "gt_source_major")
    overall_cam_macro = macro_of(per_cam)
    overall_pooled = pooled_of(rows)

    def _grp_rows(gl, kname, gkey):
        out = []
        for g in gl:
            out.append([kname, g[gkey], g["n_captions"], g["n_pos_frames"],
                        g["n_fallback_captions"]] + [fnum(g[m]) for m in METRICS_1])
        return out
    ghdr = (["group_kind(집계축)", "group(그룹)", "n_captions(캡션수)",
             "n_pos_frames(양성프레임합)", "n_fallback_captions(폴백캡션수)"]
            + [f"{m}(평균)" for m in METRICS_1])
    grows = (_grp_rows(per_cam, "camera", "camera_major")
             + _grp_rows(per_ek, "event_kind", "event_kind_major")
             + _grp_rows(per_gs, "gt_source", "gt_source_major"))
    grows.append(["OVERALL_camera_macro", "ALL", overall_cam_macro["n_captions"],
                  sum(r["n_pos"] for r in rows),
                  sum(1 for r in rows if r["fallback_all_frames"])]
                 + [fnum(overall_cam_macro[m]) for m in METRICS_1])
    grows.append(["OVERALL_caption_pooled", "ALL", overall_pooled["n_captions"],
                  sum(r["n_pos"] for r in rows),
                  sum(1 for r in rows if r["fallback_all_frames"])]
                 + [fnum(overall_pooled[m]) for m in METRICS_1])
    write_csv(os.path.join(CSV, "26b_retrieval_by_camera.csv"), ghdr, grows)

    log("B1 OVERALL (카메라 macro): "
        f"R@1={overall_cam_macro['recall_at_1']:.3f} R@5={overall_cam_macro['recall_at_5']:.3f} "
        f"R@10={overall_cam_macro['recall_at_10']:.3f} AP={overall_cam_macro['AP']:.4f} "
        f"chance={overall_cam_macro['chance_AP']:.4f} "
        f"({overall_cam_macro['AP_over_chance']:.2f}x) "
        f"margin={overall_cam_macro['margin']:.4f} "
        f"pos_pct={overall_cam_macro['mean_pos_percentile']:.4f}")
    log("B1 OVERALL (캡션 pooled): "
        f"R@5={overall_pooled['recall_at_5']:.3f} AP={overall_pooled['AP']:.4f} "
        f"chance={overall_pooled['chance_AP']:.4f}")
    log(f"B1 LOO single-positive (카메라 macro): txt R@5="
        f"{overall_cam_macro['loo_txt_recall_at_5']:.3f} MRR={overall_cam_macro['loo_txt_mrr']:.4f}"
        f" | img-mean R@5={overall_cam_macro['loo_img_recall_at_5']:.3f} "
        f"MRR={overall_cam_macro['loo_img_mrr']:.4f}"
        f" | chance R@5={overall_cam_macro['loo_chance_recall_at_5']:.4f} "
        f"MRR={overall_cam_macro['loo_chance_mrr']:.4f}")
    for name, gl, gkey in (("event_kind", per_ek, "event_kind_major"),
                           ("gt_source", per_gs, "gt_source_major")):
        for g in gl:
            log(f"  B1 by {name}={g[gkey]:12s} ncap={g['n_captions']:3d} "
                f"npos={g['n_pos_frames']:5d} R@5={g['recall_at_5']:.3f} "
                f"AP={g['AP']:.4f} chance={g['chance_AP']:.4f} "
                f"({g['AP_over_chance']:.2f}x) txtR@5(LOO)={g['loo_txt_recall_at_5']:.3f} "
                f"imgR@5(LOO)={g['loo_img_recall_at_5']:.3f}")
    for g in per_cam:
        log(f"  B1 camera={g['camera_major'][:44]:46s} ncap={g['n_captions']:3d} "
            f"npos={g['n_pos_frames']:5d} R@5={g['recall_at_5']:.3f} AP={g['AP']:.4f} "
            f"chance={g['chance_AP']:.4f} txtR@5={g['loo_txt_recall_at_5']:.3f} "
            f"imgR@5={g['loo_img_recall_at_5']:.3f} fallback={g['n_fallback_captions']}")
    fig_b1(rows, per_cam, overall_cam_macro, os.path.join(FIG, "f30_caption_retrieval.png"))
    del S_txt, T

    # ── B3 ──
    a_rows, warn_leak, classes, RK = b3_alignment(fr, use, matched_text, by_text, texts, Fn)
    akeys = ["fold", "method", "n_train_pairs", "n_test_pairs",
             "n_distinct_captions_train", "n_distinct_captions_test",
             "n_cameras_train", "n_cameras_test", "cca_n_components", "test_cameras",
             "mean_cos_test", "retrieval_AP_test", "retrieval_recall5_test",
             "retrieval_n_captions_test", "zeroshot_macro_f1_test", "zeroshot_acc_test",
             "zeroshot_f1_falldown_test", "zeroshot_f1_fire_test", "zeroshot_f1_smoke_test",
             "mean_cos_train", "retrieval_AP_train", "retrieval_recall5_train",
             "retrieval_n_captions_train", "zeroshot_macro_f1_train", "zeroshot_acc_train",
             "zeroshot_f1_falldown_train", "zeroshot_f1_fire_train", "zeroshot_f1_smoke_train",
             "n_gt_normal_test", "n_gt_falldown_test", "n_gt_fire_test", "n_gt_smoke_test",
             "n_gt_normal_train", "n_gt_falldown_train", "n_gt_fire_train", "n_gt_smoke_train"]
    ahdr = ["fold(폴드)", "method(방법)", "n_train_pairs(학습페어)", "n_test_pairs(테스트페어)",
            "n_distinct_captions_train(학습고유캡션)", "n_distinct_captions_test(테스트고유캡션)",
            "n_cameras_train(학습카메라)", "n_cameras_test(테스트카메라)",
            "cca_n_components(CCA성분수)", "test_cameras(테스트카메라목록)",
            "mean_cos_test(테스트평균코사인)", "retrieval_AP_test(테스트검색AP)",
            "retrieval_recall5_test(테스트검색R@5)", "retrieval_n_captions_test(테스트캡션수)",
            "zeroshot_macro_f1_test(테스트무학습macroF1)", "zeroshot_acc_test(테스트무학습정확도)",
            "zeroshot_f1_falldown_test(테스트F1_falldown)", "zeroshot_f1_fire_test(테스트F1_fire)",
            "zeroshot_f1_smoke_test(테스트F1_smoke)",
            "mean_cos_train(학습평균코사인)", "retrieval_AP_train(학습검색AP)",
            "retrieval_recall5_train(학습검색R@5)", "retrieval_n_captions_train(학습캡션수)",
            "zeroshot_macro_f1_train(학습무학습macroF1)", "zeroshot_acc_train(학습무학습정확도)",
            "zeroshot_f1_falldown_train(학습F1_falldown)", "zeroshot_f1_fire_train(학습F1_fire)",
            "zeroshot_f1_smoke_train(학습F1_smoke)",
            "n_gt_normal_test(테스트GT_normal)", "n_gt_falldown_test(테스트GT_falldown)",
            "n_gt_fire_test(테스트GT_fire)", "n_gt_smoke_test(테스트GT_smoke)",
            "n_gt_normal_train(학습GT_normal)", "n_gt_falldown_train(학습GT_falldown)",
            "n_gt_fire_train(학습GT_fire)", "n_gt_smoke_train(학습GT_smoke)"]
    write_csv(os.path.join(CSV, "27_alignment_eval.csv"), ahdr,
              [[r[k] if isinstance(r[k], (str, int)) else fnum(r[k]) for k in akeys]
               for r in a_rows])

    def mm(meth, key):
        v = [r[key] for r in a_rows if r["method"] == meth and np.isfinite(r[key])]
        return float(np.mean(v)) if v else float("nan")
    meth_mean = {m: {k: mm(m, k) for k in
                     ("mean_cos_test", "retrieval_AP_test", "retrieval_recall5_test",
                      "zeroshot_macro_f1_test", "zeroshot_acc_test",
                      "mean_cos_train", "retrieval_AP_train",
                      "zeroshot_macro_f1_train", "zeroshot_acc_train")}
                 for m in ("raw", "procrustes", "cca", "shuffled")}
    log("B3 fold-mean (test):  [주의: 모집단은 캡션-페어 프레임 5,692장뿐 — "
        "fire/smoke 대부분이 v3_unknown 에 있어 이 평가 밖이다]")
    for m, d in meth_mean.items():
        log(f"  {m:11s} cos={d['mean_cos_test']:.4f} AP={d['retrieval_AP_test']:.4f} "
            f"R@5={d['retrieval_recall5_test']:.4f} mF1={d['zeroshot_macro_f1_test']:.4f} "
            f"acc={d['zeroshot_acc_test']:.4f} || train cos={d['mean_cos_train']:.4f} "
            f"AP={d['retrieval_AP_train']:.4f} mF1={d['zeroshot_macro_f1_train']:.4f}")
    r0 = meth_mean["raw"]
    best = max(("procrustes", "cca"), key=lambda m: meth_mean[m]["zeroshot_macro_f1_test"])
    dz = meth_mean[best]["zeroshot_macro_f1_test"] - r0["zeroshot_macro_f1_test"]
    verdict = (f"정렬 판정: 코사인은 raw {r0['mean_cos_test']:.3f} → "
               f"Procrustes {meth_mean['procrustes']['mean_cos_test']:.3f} / "
               f"CCA {meth_mean['cca']['mean_cos_test']:.3f} 로 닫히지만, "
               f"무학습 뱅크 macro-F1 은 raw {r0['zeroshot_macro_f1_test']:.3f} → "
               f"{best} {meth_mean[best]['zeroshot_macro_f1_test']:.3f} "
               f"({dz:+.3f}) — 카메라 홀드아웃 {a_rows[0]['n_cameras_test']}대·"
               f"고유 캡션 {a_rows[0]['n_distinct_captions_train']}개 기준")
    fig_b3(a_rows, os.path.join(FIG, "f31_alignment.png"), verdict)

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "dataset": DATASET, "bank": BANK, "rule_k": RK, "bank_classes": classes,
        "threads": _THREADS,
        "pairing_funnel": funnel,
        "kr_en_agreement_infeasible": KR_EN_NOTE,
        "b1": {
            "overall_camera_macro": overall_cam_macro,
            "overall_caption_pooled": overall_pooled,
            "per_camera": per_cam, "per_event_kind": per_ek, "per_gt_source": per_gs,
            "n_captions": len(rows),
            "n_captions_fallback_negatives": sum(1 for r in rows if r["fallback_all_frames"]),
            "max_same_camera_negatives": max(r["n_same_camera_neg"] for r in rows),
        },
        "b3": {"folds": a_rows, "method_fold_means": meth_mean,
               "verdict": verdict, "leakage_warnings": warn_leak},
        "outputs": {
            "csv": ["csv/26_caption_retrieval.csv", "csv/26b_retrieval_by_camera.csv",
                    "csv/27_alignment_eval.csv"],
            "fig": ["fig/f30_caption_retrieval.png", "fig/f31_alignment.png"],
        },
    }
    sp = os.path.join(OUT, "alignment_summary.json")
    with open(sp, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2, default=str)
    log(f"Wrote {sp}")
    log(f"total {time.time()-t00:.0f}s")
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
