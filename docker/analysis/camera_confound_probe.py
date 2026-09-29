#!/usr/bin/env python3
"""카메라 교락이 선형 프로브를 죽이는가 — 표현 4종을 같은 폴드에서 채점한다.

왜 필요한가: `sourcei_ceiling_probe.py` 실측에서 프로브 학습곡선이 평평했다
(4장/클래스 0.299 → 전량 0.364, 폴드 SD ±0.20). 라벨을 32배 늘려도 SD 절반을 못 넘는다.
그리고 dcom-feasibility §1.5 가 이유를 이미 갖고 있다 — δ0=0.06 에서 π_camera 0.995 >
π_class 0.952, 즉 **이 임베딩 공간은 이벤트가 아니라 카메라를 잰다**.

그래서 "표현에서 카메라를 빼면 배우기 시작하나"를 묻는다. 표현 4종:

    R0 원본            현행 전체프레임 벡터 (기준선, 재현 확인용)
    R1 카메라 중심제거  X - mean(X | camera)   ← 카메라가 **가법적 오프셋**이면 이게 정확히 지운다
    R2 카메라 백색화    R1 + 카메라별 분산 정규화
    R3 SAM3 객체크롭    배경을 물리적으로 잘라냄 (crop_embed.py 산출물, 있으면 채점)

R1/R2 가 대조군인 이유: R3 가 좋아져도 그게 '배경 제거' 덕인지 'SAM3 가 정답 물체를 찾아준'
덕인지 구분이 안 된다. R1 은 탐지기를 안 쓰고 배경만 지우므로 그 둘을 분리한다.

**R1/R2 의 테스트 폴드 중심값은 테스트 입력만으로 계산한다(라벨 미사용).**
배포 시에도 새 카메라의 무라벨 프레임 평균은 구할 수 있으므로 이건 정당한 transductive
도메인적응이지 누수가 아니다.
"""

import collections
import json
import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "6")

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold

DATASET = os.environ.get("DATASET", "sourcei")
# SOURCE=npz:<경로> 로 FiftyOne 밖의 코호트도 같은 코드로 채점한다(sourcea 썸네일 등)
SOURCE = os.environ.get("SOURCE", "")
# GROUP=event 는 npz 의 `group` 배열로 홀드아웃한다. certbody sitej 처럼 **같은 이벤트를 여러
# 카메라가 동시 녹화**한 코호트는 카메라로 나눠도 이벤트가 학습에 남아 누수다(실측 28%).
GROUP = os.environ.get("GROUP", "camera")
OUT = "/data/fiftyone/frames_bank/report/sourcei_gt"
CROPS = f"{OUT}/crop_embeddings.npz"
CLASSES: list[str] = []  # 데이터셋에서 파생 (sitej_subway 는 5클래스, sourcei 는 4)
NORMAL = "normal"        # macro-F1 에서 빼는 다수 클래스
SHOTS = (4, 8, 16, 32, 64, 128)
RNG_SEED = 0


def macro_f1(t, p, classes=None):
    """normal 은 빼고 이벤트 클래스만 — 기존 ceiling_probe 와 동일 정의."""
    if classes is None:
        classes = [i for i, c in enumerate(CLASSES) if c != NORMAL]
    f = []
    for c in classes:
        tp = ((p == c) & (t == c)).sum()
        fp = ((p == c) & (t != c)).sum()
        fn = ((p != c) & (t == c)).sum()
        pr = tp / max(tp + fp, 1)
        rc = tp / max(tp + fn, 1)
        f.append(2 * pr * rc / max(pr + rc, 1e-12))
    return float(np.mean(f))


def l2(X):
    X = np.asarray(X, dtype=np.float32)
    n = np.linalg.norm(X, axis=1, keepdims=True)
    return X / np.maximum(n, 1e-12)


def center_by_camera(X, cam, whiten=False):
    """카메라별 중심 제거 (+옵션 분산 정규화). 라벨을 안 쓰므로 전체에 적용해도 된다."""
    Z = X.copy()
    for c in np.unique(cam):
        m = cam == c
        Z[m] -= Z[m].mean(axis=0, keepdims=True)
        if whiten:
            Z[m] /= np.maximum(Z[m].std(axis=0, keepdims=True), 1e-6)
    return l2(Z)


def delta0_purity(X, y, cam, deltas=(0.04, 0.06, 0.08, 0.12, 0.20)):
    """반경 δ 안의 이웃이 **클래스로 순수한가, 카메라로 순수한가**.

    dcom-feasibility §1.5 가 sourcei 에서 π_camera(0.995) > π_class(0.952) 를 재고
    "이 공간의 coverage 는 semantic 이 아니라 camera coverage 다"라고 판정한 그 측정.
    커버리지 기반 AL 이 무엇을 덮게 되는지가 여기서 갈린다.
    """
    S = X @ X.T
    np.fill_diagonal(S, -2.0)
    out = []
    for d in deltas:
        nb = S >= 1.0 - d
        pc, pk, deg = [], [], []
        for i in range(len(X)):
            j = np.where(nb[i])[0]
            if len(j) == 0:
                continue
            pc.append((y[j] == y[i]).mean())
            pk.append((cam[j] == cam[i]).mean())
            deg.append(len(j))
        if pc:
            out.append((d, float(np.mean(pc)), float(np.mean(pk)), float(np.mean(deg))))
    return out


def margin_auc(X, y, cam, folds, n_boot=4000):
    """P3 — 프로브의 margin 이 오답을 가리키나. DCoM 의 uncertainty 항이 서는 유일한 근거.

    margin = top1 확률 − top2 확률 (작을수록 헷갈림). 신호는 −margin, 표적은 '틀렸나'.
    AUC 0.5 = 무작위. 선행 실측(전체프레임 margin_cos/margin_iou)은 0.49~0.58 이었고
    클래스별 부호가 뒤집혔다 — 그건 learner 가 없던 시절의 zero-shot margin 이었다.

    CI 는 **카메라 군집 부트스트랩**이다. 프레임 단위 부트스트랩은 같은 카메라 프레임을
    독립으로 세어 CI 를 28~80배 좁게 만든다(거짓 정밀).
    """
    sig, err, grp = [], [], []
    for tr, te in folds:
        if len(set(y[te])) < 2:
            continue
        m = LogisticRegression(max_iter=2000, C=1.0, class_weight="balanced").fit(X[tr], y[tr])
        P = m.predict_proba(X[te])
        srt = np.sort(P, axis=1)
        sig.append(-(srt[:, -1] - srt[:, -2]))          # 작은 margin = 큰 신호
        err.append((m.classes_[P.argmax(1)] != y[te]).astype(int))
        grp.append(cam[te])
    sig = np.concatenate(sig); err = np.concatenate(err); grp = np.concatenate(grp)

    def auc(s, e):
        if e.sum() == 0 or e.sum() == len(e):
            return float("nan")
        r = np.argsort(np.argsort(s)) + 1.0
        n1 = e.sum(); n0 = len(e) - n1
        return float((r[e == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))

    point = auc(sig, err)
    cams = np.unique(grp)
    rng = np.random.default_rng(0)
    boots = []
    for _ in range(n_boot):
        pick = rng.choice(cams, size=len(cams), replace=True)
        idx = np.concatenate([np.where(grp == c)[0] for c in pick])
        a = auc(sig[idx], err[idx])
        if not np.isnan(a):
            boots.append(a)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return dict(auc=point, ci_lo=float(lo), ci_hi=float(hi),
                n=len(err), err_rate=float(err.mean()), n_cameras=len(cams))


def curve(X, y, cam, folds):
    """폴드별 학습량 곡선. 반환 {label: (mean, sd, n_folds)}."""
    rng = np.random.default_rng(RNG_SEED)
    res = collections.defaultdict(list)
    for tr, te in folds:
        if len(set(y[te])) < 2:
            continue
        full = LogisticRegression(max_iter=2000, C=1.0, class_weight="balanced").fit(X[tr], y[tr])
        res["전량"].append(macro_f1(y[te], full.predict(X[te])))
        for n in SHOTS:
            idx = []
            for c in range(len(CLASSES)):
                pool = tr[y[tr] == c]
                if len(pool):
                    idx.extend(rng.choice(pool, size=min(n, len(pool)), replace=False))
            idx = np.array(idx)
            if len(set(y[idx])) < 2:
                continue
            m = LogisticRegression(max_iter=2000, C=1.0, class_weight="balanced").fit(X[idx], y[idx])
            res[f"{n}장/클래스"].append(macro_f1(y[te], m.predict(X[te])))
    return {k: (float(np.mean(v)), float(np.std(v)), len(v)) for k, v in res.items()}


def selfcheck(X, Xc, cam, grp, folds):
    """이 스크립트가 틀리면 결론이 통째로 뒤집히는 두 지점만 못 박는다."""
    for c in np.unique(cam)[:5]:
        r = np.abs(Xc[cam == c].mean(axis=0)).max()
        assert r < 0.2, f"카메라 {c} 중심제거 후 잔차 {r:.3f} — 중심제거가 안 먹었다"
    for tr, te in folds:
        assert not (set(grp[tr]) & set(grp[te])), "그룹이 train/test 양쪽에 있다 — 홀드아웃 무효"
    leak = sum(1 for tr, te in folds for c in set(cam[te]) if c in set(cam[tr]))
    print(f"  selfcheck 통과: 중심제거 유효 + 그룹 누수 0 (참고: 카메라가 양쪽에 걸친 폴드-카메라 {leak}건)")


def main():
    if not SOURCE.startswith("npz:"):
        import fiftyone as fo

    if SOURCE.startswith("npz:"):
        d = np.load(SOURCE[4:], allow_pickle=True)
        ids = [str(i) for i in d["ids"]]
        emb, gt = d["vectors"], [str(c) for c in d["cls"]]
        cam = [str(c) for c in d["camera"]]          # 중심제거는 언제나 카메라 기준
        if GROUP in ("event", "session", "group"):
            if "group" not in d:
                raise SystemExit(f"GROUP={GROUP} 인데 npz 에 group 배열이 없다")
            grp = [str(c) for c in d["group"]]
        elif GROUP == "video" and "video" in d:
            grp = [str(c) for c in d["video"]]
        else:
            grp = cam
        print(f"  홀드아웃 단위: {GROUP} ({len(set(grp))}그룹) · 중심제거 단위: camera ({len(set(cam))})")
    else:
        ds = fo.load_dataset(DATASET)
        ids, emb, gt, cam = ds.values(["id", "embedding", "ground_truth.label", "camera"])
        grp = cam
    CLASSES[:] = sorted(set(gt))
    X0 = l2(emb)
    y = np.array([CLASSES.index(g) for g in gt])
    cam = np.array(cam)
    grp = np.array(grp)
    n_shots = [n for n in SHOTS if n <= min(collections.Counter(y).values()) * 2]
    print(f"[{DATASET}] {len(y):,} 프레임 / 카메라 {len(set(cam))} / GT {dict(collections.Counter(gt))}")
    print(f"  클래스 {CLASSES} · macro-F1 대상 = {NORMAL} 제외 {len(CLASSES) - 1}개\n")

    print("=== δ0 순도 — 반경 안의 이웃이 클래스로 순수한가 카메라로 순수한가 ===")
    print(f"{'δ':>6}{'π_class':>10}{'π_camera':>11}{'평균이웃':>10}")
    for d, pc, pk, deg in delta0_purity(X0, y, cam):
        mark = " ← 카메라 우세" if pk > pc else (" ← 클래스 우세" if pc > pk else "")
        print(f"{d:>6.2f}{pc:>10.3f}{pk:>11.3f}{deg:>10.0f}{mark}")
    print()

    folds = list(GroupKFold(n_splits=5).split(X0, y, groups=grp))
    X1 = center_by_camera(X0, cam)
    X2 = center_by_camera(X0, cam, whiten=True)
    selfcheck(X0, X1, cam, grp, folds)

    reps = {"R0 원본": X0, "R1 카메라 중심제거": X1, "R2 카메라 백색화": X2}

    # 부분 체크포인트를 전체프레임으로 메워 채점하면 R3 가 R0 으로 수렴해 "차이 없음"이라는
    # 가짜 결론이 나온다. 커버리지 미달이면 **점수를 내지 않는다** — 이 저장소가 반복해서
    # 당한 '부재에 기댄 안전'(크래시 대신 조용한 오답) 패턴을 여기서만은 막는다.
    MIN_COVERAGE = float(os.environ.get("R3_MIN_COVERAGE", "0.98"))
    if DATASET == "sourcei" and os.path.exists(CROPS):
        d = np.load(CROPS, allow_pickle=True)
        pos = {i: k for k, i in enumerate(list(d["ids"]))}
        take = np.array([pos.get(i, -1) for i in ids])
        nb = d["n_boxes"]
        ok = np.array([t >= 0 and int(nb[t]) > 0 for t in take])   # 박스 실제로 뜬 것만
        have = np.array([t >= 0 and int(nb[t]) >= 0 for t in take])  # 처리 완료(박스 0 포함)
        cov = have.mean()
        print(f"  R3 체크포인트: 처리 {int(have.sum()):,}/{len(ids):,} ({cov:.1%}) · "
              f"박스≥1 {int(ok.sum()):,} · 박스0 {int((have & ~ok).sum()):,}")
        if cov < MIN_COVERAGE:
            print(f"  ⛔ R3 채점 보류 — 커버리지 {cov:.1%} < {MIN_COVERAGE:.0%}. "
                  f"미완 체크포인트를 전체프레임으로 메우면 R0 으로 수렴해 결론이 가짜가 된다.")
        else:
            V = X0.copy()
            V[ok] = l2(d["vectors"])[take[ok]]  # 박스 0개는 전체프레임 폴백(정상 경로)
            reps["R3 SAM3 객체크롭"] = l2(V)
    else:
        print(f"  R3 크롭 없음 — {CROPS} 미생성 (crop_embed.py 먼저)")
    print()

    out = {}
    hdr = f"{'학습량':<14}" + "".join(f"{k:>22}" for k in reps)
    print(hdr)
    print("-" * len(hdr))
    rows = {k: curve(X, y, grp, folds) for k, X in reps.items()}
    for shot in [f"{n}장/클래스" for n in SHOTS] + ["전량"]:
        line = f"{shot:<14}"
        for k in reps:
            m, sd, _ = rows[k].get(shot, (float("nan"),) * 3)
            line += f"{m:>15.3f} ±{sd:.3f}"
        print(line)
    print()
    for k in reps:
        lo = rows[k][f"{SHOTS[0]}장/클래스"][0]
        hi = rows[k]["전량"][0]
        sd = rows[k]["전량"][1]
        verdict = "유의" if (hi - lo) > sd else "SD 안 (학습 안 됨)"
        print(f"{k:<20} 기울기 {hi - lo:+.3f}  vs 폴드SD ±{sd:.3f}  → {verdict}")
        out[k] = dict(curve=rows[k], slope=hi - lo, fold_sd=sd, learns=(hi - lo) > sd)

    print(f"\n=== P3 margin → 오답 AUC (카메라 군집 부트스트랩 95% CI) ===")
    print(f"{'표현':<20}{'AUC':>8}{'95% CI':>20}{'오답률':>9}   판정 (통과=CI하한>0.55)")
    for k, X in reps.items():
        a = margin_auc(X, y, grp, folds)
        ok = a["ci_lo"] > 0.55
        print(f"{k:<20}{a['auc']:>8.3f}   [{a['ci_lo']:.3f}, {a['ci_hi']:.3f}]{a['err_rate']:>9.3f}   "
              f"{'통과' if ok else '실패'}")
        out[k]["margin_auc"] = a

    tag = DATASET.lower().replace("-", "_")
    json.dump(out, open(f"{OUT}/camera_confound_probe_{tag}.json", "w"), ensure_ascii=False, indent=1)
    print(f"\n저장: {OUT}/camera_confound_probe_{tag}.json")


if __name__ == "__main__":
    main()
