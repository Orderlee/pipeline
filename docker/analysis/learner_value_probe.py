#!/usr/bin/env python3
"""learner 가 의미 있나 — `al_frames` 로 처음 가능해진 네 가지를 한 번에 잰다.

기존 프로브(`camera_confound_probe.py`)는 **한 현장 안에서만** 채점했다. 그건
"이 현장에서 배우나"는 답하지만 "학습기를 재사용할 수 있나"는 답하지 못한다.
지금은 fire·smoke·falldown 이 4개 현장 전부에 있어 전이 행렬을 그릴 수 있다.

  E1 현장 내 (그룹 홀드아웃, 사람확정만)  — 정직한 현장별 기준선
  E2 현장 간 전이 (A 학습 → B 평가)      — ★ 학습기가 재사용되나, 아니면 현장별인가
  E3 현장 식별 대조군                     — 합쳐서 학습하면 '클래스' 대신 '현장'을 읽는가
  E4 AL 시뮬레이션 (margin vs random)     — 능동학습이 라벨을 실제로 아끼나

⚠️ E2 의 대각선은 전이가 아니다 — 같은 현장이므로 **그룹 홀드아웃**으로 채점한다.
   비대각선은 현장이 다르니 홀드아웃이 자동으로 성립한다.
⚠️ 얇은 칸(sourcep falldown 26 · sourcea fire 26 · sitej fire 72)은 CI 가 넓다. 절대값이
   아니라 **대각선 대비 얼마나 떨어지나**로만 읽는다.
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
# 2026-09-14 확장: 코호트가 늘면서 '전 현장 공통 클래스'가 줄었다(fire_smoke_gt 에 falldown 없음,
# cohorta_falldown_gt 에 fire/smoke 없음). 억지로 교집합을 잡으면 표본이 얇아지므로
# **클래스군별로 참여 코호트를 따로 지정**한다. env 로 전환한다.
CLASSES = [c for c in os.environ.get("CLASSES", "fire,smoke,falldown").split(",") if c]
COHORTS = [c for c in os.environ.get("COHORTS",
           "archive_sourcep,sitej_certbody,sourcei,sourcea_thumb").split(",") if c]
SHORT = {"archive_sourcep": "sourcep", "sitej_certbody": "sitej", "sourcei": "sourcei",
         "sourcea_thumb": "sourcea", "fire_smoke_gt": "firesmoke",
         "cohorta_falldown_gt": "cohorta", "cohorta_outdoor_fall_pool": "vn_pool"}
SHORT = {c: SHORT.get(c, c[:9]) for c in COHORTS}
# 자기학습 금지 게이트 — E1~E4 전부 label_source 가 이미 SELECT 목록에 있었지만 WHERE 에는
# 없었다(sourcei model 파생 캡션 normal 4,162 행이 조용히 섞여 있었다). GT/학습/전이 채점
# 전용 쿼리라 무조건 건다. 이 스크립트에는 풀 쿼리가 없다.
LABEL_SOURCES = [s for s in os.environ.get("LABEL_SOURCES", "human,derived").split(",") if s]

SQL = """
SELECT f.cohort, f.cls, f.group_key, f.label_source, e.embedding::text
FROM al_frames f
JOIN image_embeddings e ON e.entity_type='al_frame' AND e.entity_id = f.cohort||'/'||f.frame_key
WHERE f.cls = ANY(%s) AND f.cohort = ANY(%s) AND NOT f.ambiguous AND f.cls IS NOT NULL
  AND f.label_source = ANY(%s)
"""


def _report_label_source_gate():
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT count(*) FROM al_frames WHERE cls = ANY(%s) AND cohort = ANY(%s) AND NOT ambiguous "
                "AND cls IS NOT NULL", (CLASSES, COHORTS))
    total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM al_frames WHERE cls = ANY(%s) AND cohort = ANY(%s) AND NOT ambiguous "
                "AND cls IS NOT NULL AND label_source = ANY(%s)", (CLASSES, COHORTS, LABEL_SOURCES))
    kept = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"[LABEL_SOURCES gate] {','.join(LABEL_SOURCES)} · {total:,}행 중 {total - kept:,}행 제외 → {kept:,}행 사용")


def per_class_f1(t, p, labels):
    """클래스별 F1. macro 만 보면 얇은 클래스 하나가 전체를 끌어내려 원인을 못 본다 —
    sourcep falldown 26장(영상 3개) 때문에 대각선이 0.34 로 보이는 게 그 예다."""
    out = {}
    for c in labels:
        tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        out[c] = (2 * pr * rc / max(pr + rc, 1e-12), int((t == c).sum()))
    return out


def macro_f1(t, p, labels):
    f = []
    for c in labels:
        tp = ((p == c) & (t == c)).sum(); fp = ((p == c) & (t != c)).sum(); fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1); rc = tp / max(tp + fn, 1)
        f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


def fit(X, y):
    return LogisticRegression(max_iter=3000, C=1.0, class_weight="balanced").fit(X, y)


def load():
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute(SQL, (CLASSES, COHORTS, LABEL_SOURCES))
    coh, cls, grp, src, vecs = [], [], [], [], []
    for c, k, g, s, emb in cur:
        coh.append(c); cls.append(k); grp.append(g or "?"); src.append(s)
        vecs.append(np.fromstring(emb.strip("[]"), sep=","))
    cur.close(); conn.close()
    X = np.asarray(vecs, dtype=np.float32)
    X /= np.maximum(np.linalg.norm(X, axis=1, keepdims=True), 1e-12)
    return X, np.array(coh), np.array(cls), np.array(grp), np.array(src)


def main():
    _report_label_source_gate()
    X, coh, cls, grp, src = load()
    print(f"표본 {len(X):,} · 클래스 {dict(Counter(cls))}")
    for c in COHORTS:
        m = coh == c
        print(f"  {SHORT[c]:<9} {m.sum():>5} · {dict(Counter(cls[m]))}")

    print("\n=== E1/E2 전이 행렬 — macro-F1 (fire/smoke/falldown) ===")
    print("   행=학습 현장, 열=평가 현장. 대각선은 그룹 홀드아웃(전이 아님).")
    lbl = "학습→평가"
    hdr = f"{lbl:<12}" + "".join(f"{SHORT[c]:>11}" for c in COHORTS)
    print(hdr); print("-" * len(hdr))
    diag = {}
    for a in COHORTS:
        line = f"{SHORT[a]:<12}"
        ma = coh == a
        for b in COHORTS:
            mb = coh == b
            if a == b:
                ys, ps = [], []
                ng = len(set(grp[ma]))
                if ng < 2 or len(set(cls[ma])) < 2:
                    line += f"{'—':>11}"; continue
                for tr, te in GroupKFold(n_splits=min(5, ng)).split(X[ma], cls[ma], groups=grp[ma]):
                    if len(set(cls[ma][tr])) < 2:
                        continue
                    ps.append(fit(X[ma][tr], cls[ma][tr]).predict(X[ma][te])); ys.append(cls[ma][te])
                if not ys:
                    line += f"{'—':>11}"; continue
                v = macro_f1(np.concatenate(ys), np.concatenate(ps), CLASSES)
                diag[a] = v
                line += f"{v:>10.3f}*"
            else:
                if len(set(cls[ma])) < 2 or mb.sum() == 0:
                    line += f"{'—':>11}"; continue
                v = macro_f1(cls[mb], fit(X[ma], cls[ma]).predict(X[mb]), CLASSES)
                line += f"{v:>11.3f}"
        print(line)
    print("  * = 같은 현장 그룹 홀드아웃")

    print("\n=== 클래스별 F1 분해 (평가 현장 기준, n = 평가 표본 수) ===")
    for b in COHORTS:
        mb = coh == b
        print(f"  [{SHORT[b]} 평가]")
        for a in COHORTS:
            ma = coh == a
            if len(set(cls[ma])) < 2 or mb.sum() == 0: continue
            if a == b:
                ys, ps = [], []
                ng = len(set(grp[ma]))
                for tr, te in GroupKFold(n_splits=min(5, ng)).split(X[ma], cls[ma], groups=grp[ma]):
                    if len(set(cls[ma][tr])) < 2: continue
                    ps.append(fit(X[ma][tr], cls[ma][tr]).predict(X[ma][te])); ys.append(cls[ma][te])
                if not ys: continue
                d = per_class_f1(np.concatenate(ys), np.concatenate(ps), CLASSES)
                tag = "(자기,홀드아웃)"
            else:
                d = per_class_f1(cls[mb], fit(X[ma], cls[ma]).predict(X[mb]), CLASSES)
                tag = ""
            cells = "  ".join(f"{c}={d[c][0]:.2f}(n={d[c][1]})" for c in CLASSES)
            print(f"    ←{SHORT[a]:<9} {cells} {tag}")

    off = []
    for a in COHORTS:
        for b in COHORTS:
            if a == b: continue
            ma, mb = coh == a, coh == b
            if len(set(cls[ma])) < 2 or mb.sum() == 0: continue
            off.append(macro_f1(cls[mb], fit(X[ma], cls[ma]).predict(X[mb]), CLASSES))
    print(f"\n  대각선 평균 {np.mean(list(diag.values())):.3f} · 비대각선 평균 {np.mean(off):.3f} "
          f"· 전이 손실 {np.mean(list(diag.values())) - np.mean(off):+.3f}")

    print("\n=== E3 현장 식별 대조군 ===")
    ys, ps = [], []
    for tr, te in GroupKFold(n_splits=5).split(X, coh, groups=grp):
        ps.append(fit(X[tr], coh[tr]).predict(X[te])); ys.append(coh[te])
    acc = (np.concatenate(ps) == np.concatenate(ys)).mean()
    base = max(Counter(coh).values()) / len(coh)
    print(f"  현장 분류 정확도 {acc:.3f} (기저율 {base:.3f}) — 1.0 에 가까우면 임베딩이 현장을 그대로 읽는다")
    print(f"  → 합쳐서 학습하면 '클래스' 대신 '현장 사전확률'로 맞힐 수 있다는 뜻")


if __name__ == "__main__":
    main()
