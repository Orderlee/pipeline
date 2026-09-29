#!/usr/bin/env python3
"""GT 클래스별 이미지 임베딩 기하 프로파일 — 평균·퍼짐·분리도, 그리고 카메라 교락 검정.

PE-Core-L14-336 벡터는 **L2 정규화**돼 있으므로(단위구 위) 유클리드 거리 대신 코사인만
쓴다. 정규화 벡터에서는 `‖centroid‖` 자체가 집중도 지표다 — 1.0 이면 전원이 같은 방향,
0 이면 방향이 고르게 흩어진 것. 그래서 "범위"를 표준편차 대신 이 값과 자기 중심까지의
코사인 분위수로 표현한다.

⚠️ **클래스 차이를 곧 의미 차이로 읽지 말 것.** 이 도메인은 카메라(장소)가 클래스와
심하게 교락돼 있다 — 카메라 다수가 단일 클래스만 담고 있어서, 클래스 중심 간 거리가
사실은 장소 간 거리일 수 있다. 그래서 클래스와 카메라의 설명력(η²)을 **나란히** 내고,
카메라를 통제한 within-camera 분리도까지 같이 낸다. 두 숫자가 갈리면 클래스 결론은 못
쓴다.

`--csv <경로>` 로 표를 저장한다. 기본은 표준출력.
"""
from __future__ import annotations

import argparse
import collections
import csv
import sys

import numpy as np


def unit(X: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(X, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return X / n


def eta_squared(X: np.ndarray, labels: np.ndarray) -> float:
    """다변량 η² = 군간 제곱합 / 전체 제곱합. 0=설명력 없음, 1=완전 분리."""
    gm = X.mean(axis=0)
    sst = float(((X - gm) ** 2).sum())
    if sst == 0:
        return float("nan")
    ssb = 0.0
    for lab in np.unique(labels):
        m = labels == lab
        ssb += int(m.sum()) * float(((X[m].mean(axis=0) - gm) ** 2).sum())
    return ssb / sst


def participation_ratio(X: np.ndarray) -> float:
    """유효 차원 수 — 고유값 분포의 (Σλ)²/Σλ². 1024차원 중 실제로 몇 축을 쓰는지."""
    if len(X) < 2:
        return float("nan")
    Xc = X - X.mean(axis=0)
    # 표본 < 차원 이면 그램 행렬 고유값이 공분산과 같은 스펙트럼을 준다 (n×n 로 계산)
    G = Xc @ Xc.T
    ev = np.linalg.eigvalsh(G)
    ev = ev[ev > 1e-12]
    if ev.size == 0:
        return float("nan")
    return float(ev.sum() ** 2 / (ev ** 2).sum())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", nargs="?", default="sourcei")
    ap.add_argument("--embed-field", default=None, help="기본: embedding 또는 image_embedding 자동")
    ap.add_argument("--gt-field", default="ground_truth")
    ap.add_argument("--group-field", default="camera", help="교락 검정용 축 (기본 camera)")
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()

    import fiftyone as fo
    ds = fo.load_dataset(args.dataset)
    schema = ds.get_field_schema()
    ef = args.embed_field or next((f for f in ("embedding", "image_embedding") if f in schema), None)
    if ef is None:
        print(f"임베딩 필드가 없다 (후보: embedding, image_embedding)", file=sys.stderr)
        return 2

    paths = [ef, f"{args.gt_field}.label"]
    grp = args.group_field if args.group_field in schema else None
    if grp:
        fld = schema[grp]
        paths.append(grp + (".label" if type(fld).__name__ == "EmbeddedDocumentField" else ""))
    cols = ds.values(paths)

    keep = [i for i, (e, g) in enumerate(zip(cols[0], cols[1])) if e is not None and g]
    X = unit(np.asarray([cols[0][i] for i in keep], dtype=np.float32))
    y = np.asarray([str(cols[1][i]) for i in keep])
    cam = np.asarray([str(cols[2][i]) if cols[2][i] else "unknown" for i in keep]) if grp else None
    print(f"데이터셋 {args.dataset} · 임베딩 필드 `{ef}` · 표본 {len(y):,} × {X.shape[1]}차원 "
          f"(정규화 확인: ‖v‖ 중앙 {np.median(np.linalg.norm(X, axis=1)):.4f})")

    classes = sorted(set(y.tolist()))
    cent = {c: unit(X[y == c].mean(axis=0, keepdims=True))[0] for c in classes}
    raw_cent = {c: X[y == c].mean(axis=0) for c in classes}
    gmean = unit(X.mean(axis=0, keepdims=True))[0]

    # ── 클래스별 프로파일 ────────────────────────────────────────────────
    rows = []
    for c in classes:
        m = y == c
        Xc = X[m]
        own = Xc @ cent[c]                      # 자기 중심까지의 코사인
        others = {o: float(cent[c] @ cent[o]) for o in classes if o != c}
        near = max(others, key=others.get)
        # 각 표본이 어느 중심에 가장 가까운가 (무학습 프로토타입 분류)
        sims = np.stack([Xc @ cent[o] for o in classes], axis=1)
        proto_hit = float((np.asarray(classes)[sims.argmax(axis=1)] == c).mean())
        rows.append({
            "class": c, "n": int(m.sum()),
            "concentration": float(np.linalg.norm(raw_cent[c])),
            "cos_own_mean": float(own.mean()),
            "cos_own_p5": float(np.percentile(own, 5)),
            "cos_own_p95": float(np.percentile(own, 95)),
            "cos_to_global": float(cent[c] @ gmean),
            "nearest_class": near, "cos_to_nearest": others[near],
            "margin": float(own.mean()) - others[near],
            "proto_recall": proto_hit,
            "eff_dim": participation_ratio(Xc),
            "n_cams": int(len(set(cam[m].tolist()))) if cam is not None else -1,
        })

    w = f"{'클래스':10s} {'n':>6s} {'집중도':>7s} {'자기cos 평균':>12s} {'p5~p95':>15s} {'최근접':>10s} {'그중심cos':>9s} {'마진':>7s} {'프로토재현':>9s} {'유효차원':>8s} {'카메라':>6s}"
    print("\n" + w)
    for r in rows:
        print(f"{r['class']:10s} {r['n']:>6,} {r['concentration']:>7.3f} {r['cos_own_mean']:>12.3f} "
              f"{r['cos_own_p5']:>7.3f}~{r['cos_own_p95']:<7.3f} {r['nearest_class']:>10s} "
              f"{r['cos_to_nearest']:>9.3f} {r['margin']:>7.3f} {r['proto_recall']*100:>8.1f}% "
              f"{r['eff_dim']:>8.1f} {r['n_cams']:>6d}")

    # ── 클래스 중심 간 코사인 행렬 ───────────────────────────────────────
    print("\n=== 클래스 중심 간 코사인 (1.0 = 구분 불가) ===")
    print("           " + " ".join(f"{c[:8]:>9s}" for c in classes))
    for a in classes:
        print(f"  {a:9s} " + " ".join(f"{float(cent[a] @ cent[b]):>9.3f}" for b in classes))

    # ── 교락 검정 ────────────────────────────────────────────────────────
    print("\n=== 설명력 (다변량 η², 군간제곱합/전체제곱합) ===")
    e_cls = eta_squared(X, y)
    print(f"  GT 클래스   η² = {e_cls:.4f}")
    if cam is not None:
        e_cam = eta_squared(X, cam)
        print(f"  카메라      η² = {e_cam:.4f}   ← 이게 더 크면 '클래스 특징'은 장소 특징이다")
        pair = np.asarray([f"{a}|{b}" for a, b in zip(y, cam)])
        print(f"  클래스×카메라 η² = {eta_squared(X, pair):.4f} (상한)")

        print("\n=== 카메라를 통제한 분리도 (within-camera) ===")
        tot, hit, usable = 0, 0, 0
        for cm in sorted(set(cam.tolist())):
            mk = cam == cm
            ys = y[mk]
            if len(set(ys.tolist())) < 2:
                continue
            usable += 1
            Xs = X[mk]
            cs = sorted(set(ys.tolist()))
            ct = {c: unit(Xs[ys == c].mean(axis=0, keepdims=True))[0] for c in cs}
            sims = np.stack([Xs @ ct[c] for c in cs], axis=1)
            pred = np.asarray(cs)[sims.argmax(axis=1)]
            hit += int((pred == ys).sum()); tot += len(ys)
        if tot:
            print(f"  다중클래스 카메라 {usable}대 · 표본 {tot:,} · 프로토타입 정확도 {hit/tot*100:.1f}%")
            print(f"  (전체 풀에서의 프로토타입 정확도 "
                  f"{float((np.asarray(classes)[np.stack([X @ cent[c] for c in classes], axis=1).argmax(axis=1)] == y).mean())*100:.1f}%)")
        else:
            print("  ⚠️ 두 클래스 이상을 담은 카메라가 없다 — 클래스와 장소가 완전 교락, 분리 불가")

        print("\n=== 카메라별 클래스 구성 (교락 정도) ===")
        for cm, cnt in sorted(collections.Counter(cam.tolist()).items(), key=lambda kv: -kv[1]):
            comp = collections.Counter(y[cam == cm].tolist())
            top = comp.most_common(1)[0]
            print(f"  {cm:28s} n {cnt:>6,} · 클래스 {len(comp)}종 · 최빈 {top[0]} {top[1]/cnt*100:.0f}%")

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=list(rows[0]))
            wr.writeheader(); wr.writerows(rows)
        print(f"\nCSV → {args.csv}")
    return 0


def demo() -> None:
    """ponytail: η² 와 유효차원만 검증 — 나머지는 FiftyOne IO 다."""
    rng = np.random.default_rng(0)
    # 완전 분리된 두 덩이 → η² 가 1 에 가깝다
    A = np.concatenate([rng.normal(5, .01, (50, 3)), rng.normal(-5, .01, (50, 3))])
    lab = np.array(["a"] * 50 + ["b"] * 50)
    assert eta_squared(A, lab) > 0.99, eta_squared(A, lab)
    # 라벨과 무관한 잡음 → η² 가 작다 (군 2개면 1/n 수준의 우연 설명력만)
    assert eta_squared(rng.normal(0, 1, (200, 3)), np.array(["a", "b"] * 100)) < 0.05
    # 1축만 쓰는 데이터의 유효차원 ≈ 1
    Z = np.zeros((60, 4)); Z[:, 0] = rng.normal(0, 1, 60)
    assert 0.9 < participation_ratio(Z) < 1.1, participation_ratio(Z)
    print("demo OK")


if __name__ == "__main__":
    sys.exit(demo() if "--demo" in sys.argv else main())
