#!/usr/bin/env python3
"""AL 선택기 — 사람 GT 로 학습 → **진짜 미라벨 풀**을 점수 매겨 라벨 대상 N장을 낸다.

카테고리별로 스크립트를 복제하지 않는다. GT/POOL 코호트를 env 로 받는다:
    GT_COHORTS=cohorta_falldown_gt POOL_COHORT=cohorta_outdoor_fall_pool TOPN=300 \\
      python3 /workspace/al_select.py

⚠️ 선행조건: migration 031(`al_frames.eval_holdout`). 미적용 상태에서 풀 경로를 돌리면
   `column "eval_holdout" does not exist` 로 멈춘다 — 봉인 없이 선별이 도는 것을 막는
   의도된 실패다. 적용 확인:
     docker exec docker-postgres-1 psql -U airflow -d vlm_pipeline \
       -tAc "SELECT count(*) FROM _pg_migrations WHERE name='031_al_eval_holdout.sql';"

지금까지의 E4 는 '라벨을 숨긴 시뮬레이션'이었다. 여기서는 둘을 분리한다:

  [측정]  cohort=cohorta_falldown_gt (사람 bbox GT 6,897장) 안에서 라벨을 숨기고
          margin/coverage/random 을 라운드로 비교 → **같은 도메인·진짜 사람 GT** 로 채점
  [산출]  cohort=cohorta_outdoor_fall_pool (사람 검수 0, MinIO 복원 완료 3,052장) 에
          학습된 모델을 적용 → 상위 N장을 CSV 로 낸다. 이게 실제로 라벨러에게 갈 목록이다.

⚠️ 홀드아웃은 **영상 단위**(group_key). 같은 영상 프레임은 독립이 아니다.
⚠️ 미라벨 풀에는 정답이 없다 — 산출물의 '정확도'를 주장하지 않는다. 측정은 GT 쪽에서만.
"""
import csv
import datetime as _dt
import json as _json
import os
import sys
from collections import Counter

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "6")

import numpy as np
import psycopg2
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")
GT_COHORTS = [c for c in os.environ.get("GT_COHORTS", "cohorta_falldown_gt").split(",") if c]
POOL = os.environ.get("POOL_COHORT", "cohorta_outdoor_fall_pool")
TAG = os.environ.get("TAG", "_".join(GT_COHORTS)[:40])
TOPN = int(os.environ.get("TOPN", "300"))
OUT = os.environ.get("OUT", "")
# 자기학습 금지 게이트 — GT(학습/채점) 표본에만 적용한다. 기본은 사람 확정 + 사람 GT 구간의
# 프레임 투영(파생)만. 'model'(탐지기/캡션 파생)·'unknown' 은 기본 제외.
# ⚠️ POOL 쿼리에는 절대 걸면 안 된다 — 미라벨 풀은 label_source='unlabeled' 라 게이트를 걸면
#    풀이 0건이 되어 선별이 죽는다 (아래 Q_POOL 은 별도 쿼리로 분리해 둠).
LABEL_SOURCES = [s for s in os.environ.get("LABEL_SOURCES", "human,derived").split(",") if s]

Q_GT = """
SELECT f.frame_key, f.cls, f.group_key, f.media_uri, f.extra::text, e.embedding::text
FROM al_frames f
JOIN image_embeddings e ON e.entity_type='al_frame' AND e.entity_id = f.cohort||'/'||f.frame_key
WHERE f.cohort = ANY(%s) AND NOT f.ambiguous AND f.label_source = ANY(%s)
"""

# 풀 쿼리 — label_source 게이트 없음(위 주석). AL 이 봉인된 평가 홀드아웃(migration 031,
# 아직 prod 미적용)을 선택하지 못하게 eval_holdout 만 제외한다. 컬럼이 없으면 여기서
# 그대로 실패한다 — 방어하지 않는다(이 repo 의 반복 결함 형태가 "부재에 기댄 안전").
Q_POOL = """
SELECT f.frame_key, f.cls, f.group_key, f.media_uri, f.extra::text, e.embedding::text
FROM al_frames f
JOIN image_embeddings e ON e.entity_type='al_frame' AND e.entity_id = f.cohort||'/'||f.frame_key
WHERE f.cohort = ANY(%s) AND NOT f.ambiguous AND NOT f.eval_holdout
"""


def _report_label_source_gate(cohorts):
    """GT 코호트에 게이트를 적용하기 전, 무엇이 왜 몇 행 빠지는지 stdout 에 남긴다.
    조용히 걸리면 다음 사람이 수치 차이의 원인을 못 찾는다."""
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT count(*) FROM al_frames WHERE cohort = ANY(%s) AND NOT ambiguous", (list(cohorts),))
    total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM al_frames WHERE cohort = ANY(%s) AND NOT ambiguous AND label_source = ANY(%s)",
                (list(cohorts), LABEL_SOURCES))
    kept = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"[LABEL_SOURCES gate] GT={','.join(LABEL_SOURCES)} · 코호트 {total:,}행 중 "
          f"{total - kept:,}행 제외(model/unknown 등) → {kept:,}행 사용")


def load(cohorts, gt=True):
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    if gt:
        cur.execute(Q_GT, (list(cohorts), LABEL_SOURCES))
    else:
        cur.execute(Q_POOL, (list(cohorts),))
    k, y, g, u, x, ex = [], [], [], [], [], []
    for fk, cls, grp, uri, extra, emb in cur:
        k.append(fk); y.append(cls); g.append(grp or "?"); u.append(uri); ex.append(extra)
        x.append(np.fromstring(emb.strip("[]"), sep=","))
    cur.close(); conn.close()
    X = np.asarray(x, dtype=np.float32)
    X /= np.maximum(np.linalg.norm(X, axis=1, keepdims=True), 1e-12)
    return np.array(k), np.array(y), np.array(g), np.array(u), np.array(ex), X


def f1(t, p, c):
    tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
    pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
    return 2 * pr * rc / max(pr + rc, 1e-12)


def fit(X, y):
    return LogisticRegression(max_iter=3000, C=1.0, class_weight="balanced").fit(X, y)


def simulate(X, y, g, seed_n=12, step=32, rounds=8, reps=5):
    labels = sorted(set(y))
    folds = [(tr, te) for tr, te in GroupKFold(n_splits=5).split(X, y, groups=g)
             if len(set(y[te])) >= 2 and len(set(y[tr])) >= 2]
    runs = {s: [] for s in ("random", "margin", "coverage")}
    for tr_all, te in folds:
        for rep in range(reps):
            rng = np.random.default_rng(100 + rep)
            seed = np.concatenate([rng.choice(tr_all[y[tr_all] == c], size=min(seed_n, (y[tr_all] == c).sum()),
                                              replace=False) for c in labels if (y[tr_all] == c).sum()])
            for strat in runs:
                lab = seed.copy(); curve = []
                for r in range(rounds + 1):
                    m = fit(X[lab], y[lab])
                    pr = m.predict(X[te])
                    curve.append(float(np.mean([f1(y[te], pr, c) for c in labels])))
                    if r == rounds: break
                    pool = np.setdiff1d(tr_all, lab)
                    if len(pool) == 0:
                        curve.extend([curve[-1]] * (rounds - r)); break
                    if strat == "random":
                        add = rng.choice(pool, size=min(step, len(pool)), replace=False)
                    elif strat == "margin":
                        P = m.predict_proba(X[pool]); s = np.sort(P, axis=1)
                        add = pool[np.argsort(s[:, -1] - s[:, -2])[:step]]
                    else:
                        add = pool[np.argsort((X[pool] @ X[lab].T).max(axis=1))[:step]]
                    lab = np.concatenate([lab, add])
                runs[strat].append(curve)
    return {s: np.asarray(v) for s, v in runs.items()}, [seed_n * len(labels) + step * r for r in range(rounds + 1)]


def main():
    _report_label_source_gate(GT_COHORTS)
    k, y, g, u, ex, X = load(GT_COHORTS, gt=True)
    print(f"[GT {','.join(GT_COHORTS)}] {len(X):,} · {dict(Counter(y))} · 영상 {len(set(g))}")
    res, budgets = simulate(X, y, g)
    print(f"\n=== 측정: 사람 GT 안에서 AL vs random (영상 홀드아웃, 실행 {len(res['random'])}회) ===")
    print(f"{'라벨수':>7}" + "".join(f"{s:>12}" for s in res))
    for i, b in enumerate(budgets):
        print(f"{b:>7}" + "".join(f"{res[s][:, i].mean():>12.3f}" for s in res))
    n = len(res["random"])
    for s in ("margin", "coverage"):
        d = (res[s] - res["random"])[:, -1]
        se = d.std(ddof=1) / np.sqrt(n)
        lo, hi = d.mean() - 1.96 * se, d.mean() + 1.96 * se
        print(f"  {s:<9} Δ {d.mean():+.3f} · 95%CI [{lo:+.3f}, {hi:+.3f}] · 이긴 실행 {(d>0).sum()}/{n} "
              f"→ {'유의' if lo > 0 else ('유의(음수)' if hi < 0 else '유의차 없음')}")
        tgt = res["random"][:, -1].mean()
        hit = next((i for i, v in enumerate(res[s].mean(axis=0)) if v >= tgt), None)
        if hit is not None and res["random"].mean(axis=0)[0] < tgt:
            print(f"            random 최종({tgt:.3f}) 도달 라벨수 {budgets[hit]} / {budgets[-1]} "
                  f"→ 절감 {100*(1-budgets[hit]/budgets[-1]):.0f}%")

    # ── 산출: 진짜 미라벨 풀 채점 ────────────────────────────────────────────
    pk, _, pg, pu, pex, PX = load([POOL], gt=False)
    print(f"\n[POOL] {len(PX):,} 프레임 · 영상 {len(set(pg))} (라벨 0)")
    model = fit(X, y)
    P = model.predict_proba(PX)
    srt = np.sort(P, axis=1)
    margin = srt[:, -1] - srt[:, -2]
    pred = model.classes_[P.argmax(1)]
    cov = (PX @ X.T).max(axis=1)          # GT 와의 최대 유사도 = 이미 덮인 정도
    # ⚠️ margin 이 **작을수록** 모델이 헷갈린다. 예전 코드의 (-margin).argsort() 는
    #    확신하는 순이라 정확히 반대를 골랐다(선택분 margin 중앙 0.965 vs 풀 0.811 로 들통).
    strategy = os.environ.get("STRATEGY", "margin")
    if strategy == "margin":
        score = margin.argsort()                      # 작은 것부터 = 가장 헷갈리는 것
    elif strategy == "coverage":
        score = cov.argsort()                         # GT 와 가장 안 닮은 것부터
    else:
        score = np.random.default_rng(0).permutation(len(margin))
    print(f"  예측 분포: {dict(Counter(pred))}")
    print(f"  margin: 중앙 {np.median(margin):.3f} · GT 최대유사도 중앙 {np.median(cov):.3f}")
    top = score[:TOPN]
    # ── 라운드 기록 (migration 028) ─────────────────────────────────────────
    # CSV 만 남기면 "어느 라운드에서 어떤 모델로 왜 골랐나"가 사라진다. DB 에 남겨야
    # 나중에 LS 확정 결과를 이 선별에 되돌려 붙이고(①) 라운드 효과를 채점할 수 있다(⑤).
    rid = f"{POOL}__{strategy}__{_dt.datetime.now():%Y%m%d%H%M}"
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("""INSERT INTO al_rounds
        (round_id, pool_cohort, gt_cohorts, strategy, model_desc, n_pool, n_selected, status)
        VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,'selected')
        ON CONFLICT (round_id) DO UPDATE SET n_selected=EXCLUDED.n_selected""",
        (rid, POOL, GT_COHORTS, strategy,
         _json.dumps({"classes": sorted(set(y.tolist())), "n_gt": int(len(X)),
                      "gt_groups": int(len(set(g.tolist()))), "model": "logreg_C1_balanced",
                      "embed": "PE-Core-L14-336"}, ensure_ascii=False),
         int(len(PX)), int(min(TOPN, len(score)))))
    from psycopg2.extras import execute_values as _ev
    _ev(cur, """INSERT INTO al_selections (round_id, cohort, frame_key, rank, score, pred)
                VALUES %s ON CONFLICT (round_id, cohort, frame_key) DO UPDATE
                SET rank=EXCLUDED.rank, score=EXCLUDED.score, pred=EXCLUDED.pred""",
        [(rid, POOL, pk[i], r, float(margin[i] if strategy == "margin" else cov[i]), str(pred[i]))
         for r, i in enumerate(top, 1)], page_size=500)
    conn.commit(); cur.close(); conn.close()
    print(f"  라운드 기록: {rid} (al_rounds/al_selections)")

    out = OUT or f"/data/fiftyone/frames_bank/report/al_pick/{POOL}__{strategy}.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["round_id", "rank", "frame_key", "media_uri", "pred", "margin",
                    "gt_max_cos", "video", "sam3_boxes"])
        for r, i in enumerate(score[:TOPN], 1):
            import json as _j
            sb = _j.loads(pex[i]).get("sam3_boxes") if pex[i] else None
            w.writerow([rid, r, pk[i], pu[i], pred[i], f"{margin[i]:.4f}", f"{cov[i]:.4f}", pg[i], sb])
    print(f"  전략={strategy} · 상위 {TOPN}장 → {out}")
    print(f"  선택분: 예측 {dict(Counter(pred[top]))} · 영상 {len(set(pg[top]))}/{len(set(pg))} "
          f"· margin 중앙 {np.median(margin[top]):.3f}")



if __name__ == "__main__":
    main()
