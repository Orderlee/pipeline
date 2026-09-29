#!/usr/bin/env python3
"""E4 — 능동학습이 라벨을 실제로 아끼나. margin vs coverage vs random.

P2/P3(학습곡선·margin AUC)는 대리지표다. AL 의 값어치는 **같은 라벨 수로 더 좋은 모델**
이냐로만 정해진다. 그래서 라운드를 실제로 돌린다:
  씨앗 k장 → 학습 → 선택전략으로 n장 추가 → 재학습 → ... → 라벨수 대비 macro-F1 곡선

전략 3종:
  random   기준선. 이걸 못 이기면 AL 은 값어치가 없다
  margin   top1-top2 확률차가 작은 것부터 (불확실성)
  coverage 이미 라벨된 것에서 가장 먼 것부터 (ProbCover 식 다양성)

⚠️ 평가는 **그룹 홀드아웃 테스트셋 고정**. 후보 풀도 train 그룹 안에서만 뽑는다 —
   테스트 그룹에서 뽑으면 라벨을 훔치는 것이다.
⚠️ 시드 여러 개로 반복해 밴드를 낸다. 한 번 돌린 곡선의 교차는 잡음이다.
"""
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
COHORT = os.environ.get("COHORT", "sitej_certbody")
SEED_N = int(os.environ.get("SEED_N", "12"))     # 클래스당 초기 라벨
STEP = int(os.environ.get("STEP", "24"))          # 라운드당 추가
ROUNDS = int(os.environ.get("ROUNDS", "8"))
REPS = int(os.environ.get("REPS", "5"))
# 자기학습 금지 게이트 — 이 스크립트는 전체가 GT 시뮬레이션(사람이 이미 확정한 라벨 안에서
# 라벨을 숨기고 전략을 비교)이라 유일한 쿼리가 곧 GT 로딩 경로다. 풀 쿼리는 없다.
LABEL_SOURCES = [s for s in os.environ.get("LABEL_SOURCES", "human,derived").split(",") if s]

SQL = """
SELECT f.cls, f.group_key, e.embedding::text
FROM al_frames f
JOIN image_embeddings e ON e.entity_type='al_frame' AND e.entity_id = f.cohort||'/'||f.frame_key
WHERE f.cohort=%s AND NOT f.ambiguous AND f.cls IS NOT NULL AND f.label_source = ANY(%s)
"""


def _report_label_source_gate(cohort):
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT count(*) FROM al_frames WHERE cohort=%s AND NOT ambiguous AND cls IS NOT NULL", (cohort,))
    total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM al_frames WHERE cohort=%s AND NOT ambiguous AND cls IS NOT NULL "
                "AND label_source = ANY(%s)", (cohort, LABEL_SOURCES))
    kept = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"[LABEL_SOURCES gate] {','.join(LABEL_SOURCES)} · {cohort} {total:,}행 중 "
          f"{total - kept:,}행 제외(model/unknown 등) → {kept:,}행 사용")


def macro_f1(t, p, labels):
    f = []
    for c in labels:
        tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


def fit(X, y):
    return LogisticRegression(max_iter=2000, C=1.0, class_weight="balanced").fit(X, y)


def pick(strategy, model, Xp, pool, labeled_idx, X, n, rng):
    if strategy == "random":
        return rng.choice(pool, size=min(n, len(pool)), replace=False)
    if strategy == "margin":
        P = model.predict_proba(Xp)
        s = np.sort(P, axis=1)
        m = s[:, -1] - s[:, -2]
        return pool[np.argsort(m)[:n]]
    if strategy == "coverage":
        # 라벨된 것들과의 최대 유사도가 가장 낮은 것 = 가장 안 덮인 것
        sim = (Xp @ X[labeled_idx].T).max(axis=1)
        return pool[np.argsort(sim)[:n]]
    raise ValueError(strategy)


def main():
    _report_label_source_gate(COHORT)
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute(SQL, (COHORT, LABEL_SOURCES))
    cls, grp, vecs = [], [], []
    for k, g, emb in cur:
        cls.append(k); grp.append(g or "?"); vecs.append(np.fromstring(emb.strip("[]"), sep=","))
    cur.close(); conn.close()
    X = np.asarray(vecs, dtype=np.float32); X /= np.maximum(np.linalg.norm(X, axis=1, keepdims=True), 1e-12)
    y = np.array(cls); g = np.array(grp)
    labels = sorted(set(y))
    print(f"[{COHORT}] {len(X):,} · 클래스 {dict(Counter(y))} · 그룹 {len(set(g))}")

    # 폴드 하나만 쓰면 테스트에 클래스가 하나뿐인 축퇴가 난다(sourcei 15카메라 중 7개가 단일클래스
    # → macro-F1 이 0.250 으로 고정됐다). 전 폴드를 돌려 평균한다.
    folds = [(tr, te) for tr, te in GroupKFold(n_splits=5).split(X, y, groups=g)
             if len(set(y[te])) >= 2 and len(set(y[tr])) >= 2]
    print(f"  사용 폴드 {len(folds)}/5 (테스트에 2클래스 이상)")

    budgets = [SEED_N * len(labels) + STEP * r for r in range(ROUNDS + 1)]
    runs = {s: [] for s in ("random", "margin", "coverage")}
    for fi, (tr_all, te) in enumerate(folds):
        for rep in range(REPS):
            rng = np.random.default_rng(100 + rep)
            seed = []
            for c in labels:
                idx = tr_all[y[tr_all] == c]
                if len(idx):
                    seed.extend(rng.choice(idx, size=min(SEED_N, len(idx)), replace=False))
            seed = np.array(seed)
            if len(set(y[seed])) < 2:
                continue
            for strat in runs:
                lab = seed.copy(); curve = []
                for r in range(ROUNDS + 1):
                    m = fit(X[lab], y[lab])
                    curve.append(macro_f1(y[te], m.predict(X[te]), labels))
                    if r == ROUNDS: break
                    pool = np.setdiff1d(tr_all, lab)
                    if len(pool) == 0:
                        curve.extend([curve[-1]] * (ROUNDS - r)); break
                    lab = np.concatenate([lab, pick(strat, m, X[pool], pool, lab, X, STEP, rng)])
                runs[strat].append(curve)
    res = {s: np.asarray(v) for s, v in runs.items()}
    print(f"\n  실행 {len(res['random'])}회 (폴드 {len(folds)} × 반복 {REPS}) · 씨앗 {SEED_N}/클래스 · 라운드당 +{STEP}")
    hdr = f"{'라벨수':>7}" + "".join(f"{s:>18}" for s in res)
    print(hdr); print("-" * len(hdr))
    for r, b in enumerate(budgets):
        line = f"{b:>7}"
        for s in res:
            line += f"{res[s][:, r].mean():>12.3f} ±{res[s][:, r].std():.3f}"
        print(line)
    print()
    # ⚠️ 대응(paired) 비교여야 한다 — 같은 폴드·같은 시드에서 전략만 바꿨으므로
    # 풀링 SD 로 평균을 비교하면 폴드 난이도 편차(±0.09~0.14)에 묻혀 검정력이 사라진다.
    print("  === 대응 비교 (같은 폴드·시드 내 차이) ===")
    n = len(res["random"])
    for s in ("margin", "coverage"):
        d = res[s] - res["random"]                       # (run, round)
        dl = d[:, -1]
        se = dl.std(ddof=1) / np.sqrt(n)
        t = dl.mean() / se if se > 0 else 0.0
        lo, hi = dl.mean() - 1.96 * se, dl.mean() + 1.96 * se
        win = (dl > 0).sum()
        print(f"  {s:<9} 최종 Δ {dl.mean():+.3f} · 95%CI [{lo:+.3f}, {hi:+.3f}] · t={t:+.2f} · "
              f"이긴 실행 {win}/{n} → {'유의' if lo > 0 else ('유의(음수)' if hi < 0 else '유의차 없음')}")
        best = d.mean(axis=0).argmax()
        print(f"            라운드별 평균 Δ: " + " ".join(f"{x:+.3f}" for x in d.mean(axis=0)))
    # 라벨 효율: random 이 최종에 도달한 성능을 AL 은 몇 장에서 달성하나
    tgt = res["random"][:, -1].mean()
    for s in ("margin", "coverage"):
        curve = res[s].mean(axis=0)
        hit = next((i for i, v in enumerate(curve) if v >= tgt), None)
        if hit is not None:
            print(f"  {s:<9} random 최종({tgt:.3f}) 도달 라벨수 {budgets[hit]} vs random {budgets[-1]} "
                  f"→ 절감 {100*(1-budgets[hit]/budgets[-1]):.0f}%")
        else:
            print(f"  {s:<9} random 최종({tgt:.3f}) 미도달")


if __name__ == "__main__":
    main()
