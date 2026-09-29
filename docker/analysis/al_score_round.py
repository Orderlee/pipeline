#!/usr/bin/env python3
"""⑤ 라운드 채점 — 이 라운드가 **실제로 획득한 라벨**이 같은 수의 무작위보다 나았나.

    docker exec docker-dagster-daemon-1 python3 /tmp/al_score_round.py --round <round_id>

⚠️ **범위를 정직하게 둔다.** `defs/train/eval.py` 의 SAM3/PE-Core 채점부는 아직
`NotImplementedError` 라 "AL 이 다운스트림 모델을 개선했다"는 여기서 증명할 수 없다.
이 스크립트가 재는 것은 **선형 프로브 수준의 라벨 효율**이다 — 그건 시뮬레이션이 아니라
사람이 실제로 확정한 라벨로 재므로, 지금 가능한 것 중 가장 실제에 가깝다.

방법: 기존 GT(베이스) 로 학습한 프로브를 기준선으로 두고
  (A) 베이스 + 이 라운드가 획득한 N장
  (B) 베이스 + 같은 풀에서 무작위로 뽑은 N장 (여러 시드)
를 **동일 홀드아웃**에서 비교한다. (B) 의 라벨은 이미 확정된 것 중에서만 뽑는다 —
없는 라벨을 지어내지 않기 위해서다. 그래서 라운드가 회수된(labeled) 뒤에만 돌아간다.
"""
from __future__ import annotations

import argparse
import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "6")

import numpy as np
import psycopg2
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
# 자기학습 금지 게이트 — 여기서 로딩하는 것(베이스 GT 코호트 + 회수된 풀 프레임, 둘 다
# cls IS NOT NULL 인 "이미 라벨된" 행)은 전부 학습/채점에 쓰이는 GT 경로다. 풀의 미라벨
# 후보를 긁는 쿼리가 아니므로 게이트를 무조건 건다.
LABEL_SOURCES = [s for s in os.environ.get("LABEL_SOURCES", "human,derived").split(",") if s]

Q = """
SELECT f.cohort, f.frame_key, f.cls, f.group_key, e.embedding::text
FROM al_frames f
JOIN image_embeddings e ON e.entity_type='al_frame' AND e.entity_id = f.cohort||'/'||f.frame_key
WHERE f.cohort = ANY(%s) AND f.cls IS NOT NULL AND NOT f.ambiguous AND f.label_source = ANY(%s)
"""


def _report_label_source_gate(cur, cohorts):
    cur.execute("SELECT count(*) FROM al_frames WHERE cohort = ANY(%s) AND cls IS NOT NULL AND NOT ambiguous",
                (list(cohorts),))
    total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM al_frames WHERE cohort = ANY(%s) AND cls IS NOT NULL AND NOT ambiguous "
                "AND label_source = ANY(%s)", (list(cohorts), LABEL_SOURCES))
    kept = cur.fetchone()[0]
    print(f"[LABEL_SOURCES gate] {','.join(LABEL_SOURCES)} · {total:,}행 중 {total - kept:,}행 제외 → {kept:,}행 사용")


def f1(t, p, c):
    tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
    pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
    return 2 * pr * rc / max(pr + rc, 1e-12)


def macro(t, p, labels):
    return float(np.mean([f1(t, p, c) for c in labels]))


def fit(X, y):
    return LogisticRegression(max_iter=3000, C=1.0, class_weight="balanced").fit(X, y)


def load(cur, cohorts):
    cur.execute(Q, (list(cohorts), LABEL_SOURCES))
    co, fk, y, g, X = [], [], [], [], []
    for c, k, cls, grp, emb in cur:
        co.append(c); fk.append(k); y.append(cls); g.append(grp or "?")
        X.append(np.fromstring(emb.strip("[]"), sep=","))
    A = np.asarray(X, dtype=np.float32)
    A /= np.maximum(np.linalg.norm(A, axis=1, keepdims=True), 1e-12)
    return np.array(co), np.array(fk), np.array(y), np.array(g), A


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--round", required=True)
    ap.add_argument("--reps", type=int, default=20)
    args = ap.parse_args()

    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT pool_cohort, gt_cohorts, strategy, status FROM al_rounds WHERE round_id=%s", (args.round,))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"round 없음: {args.round}")
    pool, gts, strategy, status = row
    cur.execute("SELECT frame_key FROM al_selections WHERE round_id=%s AND labeled_cls IS NOT NULL", (args.round,))
    acquired = {r[0] for r in cur.fetchall()}
    print(f"round={args.round} · strategy={strategy} · status={status} · 획득 라벨 {len(acquired)}")
    if not acquired:
        raise SystemExit("이 라운드는 아직 회수되지 않았다 — 먼저 al_harvest.py --apply")

    _report_label_source_gate(cur, list(gts) + [pool])
    co, fk, y, g, X = load(cur, list(gts) + [pool])
    cur.close(); conn.close()
    is_pool = co == pool
    is_acq = is_pool & np.isin(fk, list(acquired))
    base = ~is_pool
    # 풀 안의 '확정된' 라벨 중 이번 라운드가 고르지 않은 것 = 무작위 대조군 후보
    other = is_pool & ~is_acq
    labels = sorted(set(y.tolist()))
    print(f"베이스 GT {base.sum():,} · 획득 {is_acq.sum()} · 대조군 후보 {other.sum()}")
    if other.sum() < is_acq.sum():
        print(f"⚠️ 대조군 후보({other.sum()})가 획득분({is_acq.sum()})보다 적다 — "
              "무작위 비교가 약해진다. 풀에서 무작위 N장을 따로 라벨해야 정확하다.")

    folds = [(tr, te) for tr, te in GroupKFold(n_splits=5).split(X[base], y[base], groups=g[base])
             if len(set(y[base][te])) >= 2]
    Xb, yb, gb = X[base], y[base], g[base]
    rng = np.random.default_rng(0)
    acq_idx = np.where(is_acq)[0]
    oth_idx = np.where(other)[0]
    n = len(acq_idx)

    res = {"베이스만": [], f"+획득 {n} ({strategy})": [], f"+무작위 {n}": []}
    for tr, te in folds:
        Xte, yte = Xb[te], yb[te]
        m0 = fit(Xb[tr], yb[tr]); res["베이스만"].append(macro(yte, m0.predict(Xte), labels))
        Xa = np.vstack([Xb[tr], X[acq_idx]]); ya = np.concatenate([yb[tr], y[acq_idx]])
        res[f"+획득 {n} ({strategy})"].append(macro(yte, fit(Xa, ya).predict(Xte), labels))
        r = []
        for _ in range(args.reps):
            if len(oth_idx) < n:
                break
            pick = rng.choice(oth_idx, size=n, replace=False)
            Xr = np.vstack([Xb[tr], X[pick]]); yr = np.concatenate([yb[tr], y[pick]])
            r.append(macro(yte, fit(Xr, yr).predict(Xte), labels))
        res[f"+무작위 {n}"].append(float(np.mean(r)) if r else float("nan"))

    print(f"\n=== 라운드 효과 (그룹 홀드아웃 {len(folds)}폴드, 무작위 {args.reps}시드 평균) ===")
    for k, v in res.items():
        a = np.asarray(v, dtype=float)
        print(f"  {k:<28} macro-F1 {np.nanmean(a):.4f} ±{np.nanstd(a):.4f}")
    acq = np.asarray(res[f"+획득 {n} ({strategy})"], dtype=float)
    rnd = np.asarray(res[f"+무작위 {n}"], dtype=float)
    b = np.asarray(res["베이스만"], dtype=float)
    d = acq - rnd
    if not np.isnan(d).all():
        se = np.nanstd(d, ddof=1) / np.sqrt(np.sum(~np.isnan(d)))
        lo, hi = np.nanmean(d) - 1.96 * se, np.nanmean(d) + 1.96 * se
        print(f"\n  획득 − 무작위 Δ {np.nanmean(d):+.4f} · 95%CI [{lo:+.4f}, {hi:+.4f}] "
              f"→ {'유의' if lo > 0 else ('유의(음수)' if hi < 0 else '유의차 없음')}")
    print(f"  획득 − 베이스 Δ {np.nanmean(acq - b):+.4f}  (라벨 {n}장 추가의 절대 효과)")
    print("\n⚠️ 이 수치는 **선형 프로브 수준**이다. SAM3/PE-Core 다운스트림 효과는 "
          "defs/train/eval.py 채점부가 구현될 때까지 측정 불가.")


if __name__ == "__main__":
    main()
