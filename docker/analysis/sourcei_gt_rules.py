#!/usr/bin/env python3
"""sourcei(GT 보유) 프레임 × 전 텍스트 뱅크 × 3규칙 채점 → GT 대비 지표.

prompt_cos_db.py(stage_topk) 와 **같은 커널**(topk_vote / wave_iou / argmax, 같은
K·bins·thr)을 GT 가 있는 sourcei 7,498 프레임에 적용한다. 차이는 입력만이다:
프레임 벡터를 pgvector 가 아니라 FiftyOne `sourcei.embedding` 에서 읽는다
(sourcei 프레임은 image_embeddings 에 없다 — 실측 0행).

출력(OUT_DIR):
  preds.npz     — pred[rule][bank] int8 프레임별 예측(클래스 인덱스, CLASSES 순), gt, camera,
                  gt_source, iou[bank] (f×3 float16)  → 재채점 없이 후속 분석 가능
  metrics.json  — 뱅크×규칙: acc, macro-F1, per-class P/R/F1, 발화수, 규칙 일치율
"""
import json, os, sys, time
_T = os.environ.get("COS_THREADS", "6")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, _T)
import numpy as np
import psycopg2

sys.path.insert(0, "/workspace")
from prompt_cos_db import (load_banks, load_sentence_vectors, topk_vote, wave_iou,  # noqa: E402
                           WAVE_THR, RULE_K, WAVE_BINS, _topk_selfcheck)

DSN = os.environ.get("DATAOPS_POSTGRES_DSN",
                     "postgresql://airflow:airflow@docker-postgres-1:5432/vlm_pipeline")
OUT_DIR = os.environ.get("OUT_DIR", "/data/fiftyone/frames_bank/report/sourcei_gt")
CLASSES = ["normal", "falldown", "fire", "smoke"]   # GT 라벨 공간. 뱅크의 smoking/class_N 은 normal 로 접지 않고 "기타(-2)"
CHUNK = 1000


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def load_frames():
    import fiftyone as fo
    ds = fo.load_dataset("sourcei")
    ids, emb, gt, cam, src, unit = ds.values(["id", "embedding", "ground_truth.label", "camera",
                                              "gt_source.label", "source_unit.label"])
    F = np.asarray(emb, dtype=np.float32)
    F /= np.linalg.norm(F, axis=1, keepdims=True)
    return ids, F, np.asarray(gt), np.asarray(cam), np.asarray(src), np.asarray(unit)


def prf(gt_i, pred_i, n_cls):
    out = {}
    for c in range(n_cls):
        tp = int(((pred_i == c) & (gt_i == c)).sum())
        fp = int(((pred_i == c) & (gt_i != c)).sum())
        fn = int(((pred_i != c) & (gt_i == c)).sum())
        p = tp / max(tp + fp, 1); r = tp / max(tp + fn, 1)
        out[CLASSES[c]] = {"tp": tp, "fp": fp, "fn": fn, "p": p, "r": r,
                           "f1": 2 * p * r / max(p + r, 1e-12), "n_pred": tp + fp, "n_gt": tp + fn}
    return out


def main():
    _topk_selfcheck()
    os.makedirs(OUT_DIR, exist_ok=True)
    t0 = time.time()
    ids, F, gt, cam, src, unit = load_frames()
    gt_i = np.asarray([CLASSES.index(g) for g in gt], dtype=np.int8)
    log(f"프레임 {len(ids):,}, GT 분포 {dict(zip(*np.unique(gt, return_counts=True)))}")

    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    h2c, SENT = load_sentence_vectors(cur)
    banks = load_banks(cur, None)
    log(f"문장 벡터 {SENT.shape}, db_backed 뱅크 {len(banks)}")

    prepared, skipped = [], []
    for b in banks:
        if any(h not in h2c for h, _c, _g in b["rows"]):
            skipped.append((b["version"], "벡터 없는 문장")); continue
        cls_local, gcols = {}, []
        for chash, cls, _g in b["rows"]:
            cls_local.setdefault(cls, []).append(len(gcols)); gcols.append(h2c[chash])
        if "normal" not in cls_local or len(cls_local) < 2:
            skipped.append((b["version"], "normal/이벤트 요건 미달")); continue
        cs = sorted(cls_local); ev = [c for c in cs if c != "normal"]
        lab = np.empty(len(gcols), dtype=np.int32)
        for ci, c in enumerate(cs):
            lab[np.asarray(cls_local[c])] = ci
        # 뱅크-로컬 클래스 → GT 클래스 인덱스 (GT 공간 밖 클래스는 -2)
        to_gt = np.asarray([CLASSES.index(c) if c in CLASSES else -2 for c in cs], dtype=np.int8)
        ev_to_gt = np.asarray([CLASSES.index(c) if c in CLASSES else -2 for c in ev], dtype=np.int8)
        prepared.append(dict(version=b["version"], classes=cs, events=ev, lab=lab, to_gt=to_gt,
                             ev_to_gt=ev_to_gt, gcols=np.asarray(gcols),
                             members={c: np.asarray(v) for c, v in cls_local.items()}))
    for v, w in skipped:
        log(f"  skip {v}: {w}")
    log(f"대상 뱅크 {len(prepared)} (K={RULE_K}, bins={WAVE_BINS}, thr={WAVE_THR})")

    n = len(ids)
    P = {r: {p["version"]: np.empty(n, dtype=np.int8) for p in prepared} for r in ("argmax", "topk", "wave")}
    IOU = {p["version"]: np.full((n, 3), np.nan, dtype=np.float16) for p in prepared}
    MARGIN = {p["version"]: np.empty(n, dtype=np.float16) for p in prepared}   # argmax 1등-2등
    for s in range(0, n, CHUNK):
        S = F[s:s + CHUNK] @ SENT.T
        for p in prepared:
            Sb = S[:, p["gcols"]]
            v = p["version"]
            # top-K
            P["topk"][v][s:s + CHUNK] = p["to_gt"][topk_vote(Sb, p["lab"], len(p["classes"]))]
            # wave (분포-IoU): 발화 중 IoU 최저, 무발화=normal
            iou = wave_iou(Sb, p["members"])
            I = np.stack([iou[c] for c in p["events"]], axis=1)
            w = np.where((I < WAVE_THR).any(axis=1), p["ev_to_gt"][I.argmin(axis=1)], 0)
            P["wave"][v][s:s + CHUNK] = w
            for j, c in enumerate(p["events"]):
                if c in CLASSES:
                    IOU[v][s:s + CHUNK, CLASSES.index(c) - 1] = I[:, j]
            # argmax(클래스별 max 코사인)
            per = np.stack([Sb[:, p["members"][c]].max(axis=1) for c in p["classes"]], axis=1)
            P["argmax"][v][s:s + CHUNK] = p["to_gt"][per.argmax(axis=1)]
            part = np.partition(per, -2, axis=1)
            MARGIN[v][s:s + CHUNK] = part[:, -1] - part[:, -2]
        log(f"  {min(s + CHUNK, n):,}/{n:,} ({time.time() - t0:.0f}s)")

    # 지표
    metrics = {"n": n, "classes": CLASSES, "K": RULE_K, "bins": WAVE_BINS, "thr": WAVE_THR,
               "skipped": skipped, "banks": {}}
    for p in prepared:
        v = p["version"]; row = {"classes": p["classes"], "n_sent": int(len(p["gcols"])), "rules": {}}
        for r in ("argmax", "topk", "wave"):
            pr = P[r][v]
            pc = prf(gt_i, pr, 4)
            ev_f1 = [pc[c]["f1"] for c in CLASSES[1:]]
            row["rules"][r] = {"acc": float((pr == gt_i).mean()), "macro_f1_ev": float(np.mean(ev_f1)),
                               "macro_f1_all": float(np.mean([pc[c]["f1"] for c in CLASSES])),
                               "n_other": int((pr == -2).sum()), "per_class": pc,
                               "per_gt_source": {g: float((pr[src == g] == gt_i[src == g]).mean())
                                                 for g in np.unique(src)},
                               "per_unit": {u: float((pr[unit == u] == gt_i[unit == u]).mean())
                                            for u in np.unique(unit)}}
        a, t, w = P["argmax"][v], P["topk"][v], P["wave"][v]
        row["agree"] = {"tw": float((t == w).mean()), "ta": float((t == a).mean()), "wa": float((w == a).mean())}
        metrics["banks"][v] = row
    with open(f"{OUT_DIR}/metrics.json", "w") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=1)
    np.savez_compressed(f"{OUT_DIR}/preds.npz", ids=np.asarray(ids), gt=gt_i, camera=cam, gt_source=src,
                        unit=unit, banks=np.asarray([p["version"] for p in prepared]),
                        **{f"{r}__{v}": P[r][v] for r in P for v in P[r]},
                        **{f"iou__{v}": IOU[v] for v in IOU}, **{f"margin__{v}": MARGIN[v] for v in MARGIN})
    log(f"완료 {time.time() - t0:.0f}s → {OUT_DIR}")
    for v, row in metrics["banks"].items():
        print(v, " ".join(f"{r}:acc={m['acc']:.3f}/mF1={m['macro_f1_ev']:.3f}" for r, m in row["rules"].items()))


if __name__ == "__main__":
    main()
