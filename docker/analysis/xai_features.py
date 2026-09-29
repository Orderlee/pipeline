#!/usr/bin/env python3
"""XAI 용 피처 엔지니어링 — 프롬프트 규칙이 **언제 틀리는지**를 설명하는 표를 만든다.

목표는 정확도를 올리는 모델이 아니라 **해석 가능한 설명**이다. 그래서
  · 피처는 전부 사람이 읽을 수 있는 스칼라로 만든다 (1024차원 원벡터는 입력에서 뺀다).
  · 각 피처가 무엇인지·어떻게 계산했는지·라벨을 쓰는지를 `FEATURES` 에 문자열로 박는다.
    이 사전이 산출물의 본체다. 이름만 있고 정의가 없는 피처는 만들지 않는다.
  · 검증은 **이벤트/카메라 그룹 분할**로만 한다. 무작위 분할은 이 데이터에서 성적을
    부풀린다 (같은 이벤트 프레임이 사실상 같은 그림 — 10-최근접 이웃의 61%가 같은 이벤트).

라벨 누출 등급(`leak`)을 피처마다 붙인다.
  free   : 라벨을 전혀 쓰지 않는다. 예측 모델에 넣어도 된다.
  diag   : 계산에 GT 가 들어간다. **설명용으로만** 보고, 예측 모델 입력에서 제외한다.
  target : 정답 그 자체.

`diag` 를 예측에 섞으면 "라벨을 넣고 라벨을 맞히는" 표가 나온다. 그 사고를 막으려고
`design_matrix()` 가 `free` 만 골라 쓰고, 섞으려 하면 예외를 던진다.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys

import numpy as np

# ── 피처 사전 (산출물의 본체) ──────────────────────────────────────────────
# group: 묶음 / desc: 무엇인가 / formula: 어떻게 계산했나 / why: 왜 넣었나 / leak: 누출 등급
FEATURES: list[dict] = [
    # A. 프롬프트 점수 기하 — 판정 규칙이 실제로 보는 네 숫자에서 파생
    dict(name="cos_max", group="A_프롬프트점수", leak="free",
         desc="네 클래스 중 최고 코사인",
         formula="max(cos_best_normal, cos_best_falldown, cos_best_smoke, cos_best_fire)",
         why="이 프레임에 '어떤 문장이든' 얼마나 잘 맞는지. 전반적 정합도"),
    dict(name="cos_min", group="A_프롬프트점수", leak="free",
         desc="네 클래스 중 최저 코사인", formula="min(네 cos_best)",
         why="cos_max 와 짝지어 점수 분포의 폭을 만든다"),
    dict(name="cos_mean", group="A_프롬프트점수", leak="free",
         desc="네 클래스 코사인의 평균", formula="mean(네 cos_best)",
         why="프레임 자체의 '문장 친화도' 기준선. 개별 클래스 점수의 공통 성분"),
    dict(name="cos_range", group="A_프롬프트점수", leak="free",
         desc="최고와 최저의 차", formula="cos_max - cos_min",
         why="네 클래스를 얼마나 벌려 놓는지. 작으면 어느 클래스도 특별하지 않다"),
    dict(name="margin_top12", group="A_프롬프트점수", leak="free",
         desc="1위와 2위 클래스 점수의 차", formula="정렬 후 1번째 - 2번째 (= pred_margin_v1080)",
         why="판정의 확신도. 이 값이 0 근처면 argmax 가 동전던지기다"),
    dict(name="margin_top13", group="A_프롬프트점수", leak="free",
         desc="1위와 3위의 차", formula="정렬 후 1번째 - 3번째",
         why="2위만 보면 놓치는 '3자 경합' 상황을 잡는다"),
    dict(name="cos_entropy", group="A_프롬프트점수", leak="free",
         desc="네 점수를 softmax 한 분포의 엔트로피", formula="-Σ p log p, p = softmax(cos*20)",
         why="불확실성의 표준 척도. margin 은 상위 2개만, 엔트로피는 네 개 전부를 본다",
         note="온도 20 은 코사인 폭(0.19~0.28)을 확률로 펴기 위한 고정 상수"),
    dict(name="normal_lead", group="A_프롬프트점수", leak="free",
         desc="normal 점수가 이벤트 3클래스 최고를 얼마나 앞서는가",
         formula="cos_best_normal - max(falldown, smoke, fire)",
         why="이 데이터의 핵심 실패 기제. 양수면 normal 이 이벤트를 가져간다"),
    dict(name="normal_over_mean", group="A_프롬프트점수", leak="free",
         desc="normal 점수의 상대 높이", formula="cos_best_normal - cos_mean",
         why="normal 문장군이 상수항처럼 작동하는 정도"),
    dict(name="fire_minus_smoke", group="A_프롬프트점수", leak="free",
         desc="fire 와 smoke 점수의 차", formula="cos_best_fire - cos_best_smoke",
         why="두 클래스가 중심 코사인 0.931 로 가장 심하게 겹친다. 그 축의 위치"),
    dict(name="event_score_max", group="A_프롬프트점수", leak="free",
         desc="이벤트 3클래스 중 최고", formula="max(falldown, smoke, fire)",
         why="normal 을 제외했을 때 무엇이 얼마나 반응하는지"),

    # B. 임베딩 기하 — 라벨 없이, 카메라와 전역 통계만 사용
    dict(name="cos_to_global", group="B_임베딩기하", leak="free",
         desc="전체 평균 벡터와의 코사인", formula="cos(x, mean(X) 정규화)",
         why="전형성. 낮으면 데이터셋 전체에서 특이한 그림"),
    dict(name="cos_to_camera", group="B_임베딩기하", leak="free",
         desc="자기 카메라 평균 벡터와의 코사인", formula="cos(x, mean(X[같은 camera]) 정규화)",
         why="'이 장소의 평상 화면'에서 얼마나 벗어났는지. 이벤트는 벗어날 것으로 기대",
         note="카메라 식별자만 쓰고 GT 는 쓰지 않으므로 라벨 자유"),
    dict(name="camera_dev", group="B_임베딩기하", leak="free",
         desc="카메라 평균을 뺀 잔차의 길이", formula="||x - mean(X[같은 camera])||",
         why="cos_to_camera 의 거리 버전. 장소를 통제한 이상도"),
    dict(name="pc1", group="B_임베딩기하", leak="free",
         desc="제1주성분 좌표", formula="(x - 전역평균) · V1  (V=전체 SVD)",
         why="분산 27.4% 를 차지하고 카메라 설명력 0.951 — 사실상 '시점 좌표'"),
    dict(name="pc2", group="B_임베딩기하", leak="free", desc="제2주성분 좌표",
         formula="(x - 전역평균) · V2", why="분산 16.4%, 카메라 0.919 / 클래스 0.408 — 혼재 축"),
    dict(name="pc3", group="B_임베딩기하", leak="free", desc="제3주성분 좌표",
         formula="(x - 전역평균) · V3", why="분산 8.5%, 카메라 0.858"),
    dict(name="pc4", group="B_임베딩기하", leak="free", desc="제4주성분 좌표",
         formula="(x - 전역평균) · V4",
         why="클래스(0.470)가 카메라(0.644)를 처음 따라잡는 축. 이벤트 신호 후보"),
    dict(name="resid3_norm", group="B_임베딩기하", leak="free",
         desc="상위 3개 주성분을 뺀 잔차의 길이", formula="||(x-평균) - Σ_{k≤3} (x·Vk)Vk||",
         why="카메라 지배 축을 뺀 나머지 에너지. '장소로 설명 안 되는 부분'의 크기"),
    dict(name="knn_cos_mean", group="B_임베딩기하", leak="free",
         desc="10-최근접 이웃과의 평균 코사인(같은 이벤트 제외)",
         formula="mean(top10 cos, 같은 이벤트 프레임 제외)",
         why="국소 밀도. 낮으면 고립된 그림이라 어떤 규칙도 불안정",
         note="같은 이벤트를 빼야 근접중복이 만드는 가짜 밀도를 피한다"),
    dict(name="knn_same_camera", group="B_임베딩기하", leak="free",
         desc="10-최근접 이웃 중 같은 카메라 비율(같은 이벤트 제외)",
         formula="mean(camera[nn] == camera[i])",
         why="임베딩 근방이 장소로 닫혀 있는 정도. 1.0 이면 시점이 이웃을 결정"),
    dict(name="knn_pred_agree", group="B_임베딩기하", leak="free",
         desc="10-최근접 이웃 중 프롬프트 예측이 나와 같은 비율(같은 이벤트 제외)",
         formula="mean(pred[nn] == pred[i])",
         why="판정의 국소 일관성. GT 를 쓰지 않으므로 배포 상황에서도 계산 가능"),

    # C. 시간·구조 — 파일명에서 파생, 라벨 무관
    dict(name="event_size", group="C_구조", leak="free",
         desc="이 프레임이 속한 이벤트의 프레임 수", formula="count(같은 이벤트)",
         why="긴 이벤트는 다양한 국면을 담아 판정이 흔들릴 수 있다"),
    dict(name="frame_pos", group="C_구조", leak="free",
         desc="이벤트 안 상대 위치(0~1)", formula="순번 / (event_size - 1)",
         why="이벤트 시작·끝 프레임이 중간보다 애매할 수 있다"),
    dict(name="event_dispersion", group="C_구조", leak="free",
         desc="이벤트 내부 임베딩 퍼짐", formula="1 - mean(cos(x_i, 이벤트 평균))",
         why="정적인 이벤트인지 변화가 큰 이벤트인지. 0 이면 사실상 같은 한 장"),
    dict(name="clip_size", group="C_구조", leak="free",
         desc="같은 클립(원본 영상)의 프레임 수", formula="count(같은 클립)",
         why="클립이 크면 그 장소·상황이 데이터셋을 지배한다"),
    dict(name="person_count", group="C_구조", leak="free",
         desc="검출된 사람 수(기존 필드)", formula="person_count 그대로",
         why="normal 문장군이 사람·자세를 묘사하므로 사람 수가 오탈취와 얽힌다"),
    dict(name="fallen_person_count", group="C_구조", leak="free",
         desc="쓰러진 사람 수(기존 필드)", formula="fallen_person_count 그대로",
         why="falldown 판정의 직접 근거 후보"),

    # D. 씬 속성 — 기존 분류 결과(모델 파생이지만 GT 와 독립)
    dict(name="is_night", group="D_씬속성", leak="free",
         desc="야간 여부", formula="daynight.label == 'night'",
         why="임베딩 PC1 에 daynight 설명력 0.510 이 실려 있다"),
    dict(name="has_person", group="D_씬속성", leak="free",
         desc="사람 있음 여부", formula="person.label 이 사람 있음 계열",
         why="PC1 에 person 설명력 0.413"),
    dict(name="daynight_margin", group="D_씬속성", leak="free",
         desc="야간 분류의 확신도(기존 필드)", formula="daynight_margin 그대로",
         why="속성 자체가 애매한 프레임을 구분"),
    dict(name="person_margin", group="D_씬속성", leak="free",
         desc="사람 분류의 확신도(기존 필드)", formula="person_margin 그대로", why="위와 같음"),

    # E. 설명 전용 — GT 를 쓰므로 예측 입력 금지
    dict(name="gt_score_rank", group="E_설명전용", leak="diag",
         desc="정답 클래스가 네 점수 중 몇 위인가(1~4)",
         formula="rank(cos_best[GT]) 내림차순",
         why="틀린 프레임이 '아깝게' 틀렸는지 '전혀' 틀렸는지 구분"),
    dict(name="gt_deficit", group="E_설명전용", leak="diag",
         desc="정답 클래스가 1위에 얼마나 못 미치는가",
         formula="cos_max - cos_best[GT]  (맞으면 0)",
         why="복구 난이도. 0.001 이면 오프셋 하나로 뒤집히고, 0.05 면 문장 문제"),
    dict(name="knn_gt_purity", group="E_설명전용", leak="diag",
         desc="10-최근접 이웃 중 GT 가 같은 비율(같은 이벤트 제외)",
         formula="mean(gt[nn] == gt[i])",
         why="임베딩이 이 프레임 근방에서 클래스를 분리하는지의 국소 증거"),
]

TARGETS = [
    dict(name="correct", leak="target", desc="프롬프트 규칙 v1.0.8.0 이 맞혔는가",
         formula="pred_v1_0_8_0.label == ground_truth.label"),
    dict(name="gt_class", leak="target", desc="정답 클래스", formula="ground_truth.label"),
]

FREE = [f["name"] for f in FEATURES if f["leak"] == "free"]
DIAG = [f["name"] for f in FEATURES if f["leak"] == "diag"]
EVENT_CLASSES = ("falldown", "smoke", "fire")
CLASSES = ("normal",) + EVENT_CLASSES


def unit(A):
    n = np.linalg.norm(A, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return A / n


def build(dataset="sourcei", knn_k=10):
    import fiftyone as fo
    ds = fo.load_dataset(dataset)
    cols = ds.values([
        "id", "filepath", "embedding", "ground_truth.label", "camera",
        "pred_v1_0_8_0.label", "daynight.label", "person.label",
        "daynight_margin", "person_margin", "person_count", "fallen_person_count",
    ] + [f"cos_best_{c}" for c in CLASSES])
    keep = [i for i in range(len(cols[0]))
            if cols[2][i] is not None and cols[3][i] and cols[5][i]]
    g = lambda j: [cols[j][i] for i in keep]                     # noqa: E731

    X = unit(np.asarray(g(2), dtype=np.float32))
    gt = np.asarray([str(v) for v in g(3)])
    cam = np.asarray([str(v or "?") for v in g(4)])
    pred = np.asarray([str(v) for v in g(5)])
    stem = [os.path.splitext(os.path.basename(str(p)))[0] for p in g(1)]
    ev = np.asarray([s.rsplit("_", 1)[0] for s in stem])
    clip = np.asarray([s.split("__")[0] for s in stem])
    S = np.stack([np.asarray([np.nan if v is None else float(v) for v in g(12 + k)])
                  for k in range(4)], axis=1)                    # 열 순서 = CLASSES
    ci = {c: k for k, c in enumerate(CLASSES)}
    F: dict[str, np.ndarray] = {}

    # ── A ──
    srt = np.sort(S, axis=1)[:, ::-1]
    F["cos_max"], F["cos_min"] = S.max(1), S.min(1)
    F["cos_mean"] = S.mean(1)
    F["cos_range"] = F["cos_max"] - F["cos_min"]
    F["margin_top12"] = srt[:, 0] - srt[:, 1]
    F["margin_top13"] = srt[:, 0] - srt[:, 2]
    P = np.exp((S - S.max(1, keepdims=True)) * 20.0)
    P /= P.sum(1, keepdims=True)
    F["cos_entropy"] = -(P * np.log(P + 1e-12)).sum(1)
    ev_max = S[:, [ci[c] for c in EVENT_CLASSES]].max(1)
    F["event_score_max"] = ev_max
    F["normal_lead"] = S[:, ci["normal"]] - ev_max
    F["normal_over_mean"] = S[:, ci["normal"]] - F["cos_mean"]
    F["fire_minus_smoke"] = S[:, ci["fire"]] - S[:, ci["smoke"]]

    # ── B ──
    gmean = unit(X.mean(0, keepdims=True))[0]
    F["cos_to_global"] = X @ gmean
    cammean = {c: X[cam == c].mean(0) for c in set(cam.tolist())}
    cm = np.stack([cammean[c] for c in cam])
    F["cos_to_camera"] = np.einsum("ij,ij->i", X, unit(cm))
    F["camera_dev"] = np.linalg.norm(X - cm, axis=1)
    Xc = X - X.mean(0)
    U, Sv, Vt = np.linalg.svd(Xc, full_matrices=False)
    for k in range(4):
        F[f"pc{k+1}"] = (U[:, k] * Sv[k]).astype(np.float64)
    F["resid3_norm"] = np.linalg.norm(Xc - (U[:, :3] * Sv[:3]) @ Vt[:3], axis=1)

    # 최근접 이웃 — 같은 이벤트를 제외하고 상위 k
    n = len(gt)
    kc = np.empty(n); ksc = np.empty(n); kpa = np.empty(n); kgp = np.empty(n)
    step = 512
    for s in range(0, n, step):
        e = min(s + step, n)
        sim = X[s:e] @ X.T
        for r in range(e - s):
            i = s + r
            row = sim[r].copy()
            row[ev == ev[i]] = -2.0                # 자기 이벤트 전부 제외
            idx = np.argpartition(-row, knn_k)[:knn_k]
            kc[i] = row[idx].mean()
            ksc[i] = float((cam[idx] == cam[i]).mean())
            kpa[i] = float((pred[idx] == pred[i]).mean())
            kgp[i] = float((gt[idx] == gt[i]).mean())
    F["knn_cos_mean"], F["knn_same_camera"] = kc, ksc
    F["knn_pred_agree"], F["knn_gt_purity"] = kpa, kgp

    # ── C ──
    esz = collections.Counter(ev.tolist()); csz = collections.Counter(clip.tolist())
    F["event_size"] = np.asarray([esz[e] for e in ev], dtype=float)
    F["clip_size"] = np.asarray([csz[c] for c in clip], dtype=float)
    order = collections.defaultdict(list)
    for i, s in enumerate(stem):
        order[ev[i]].append((s, i))
    pos = np.zeros(n); disp = np.zeros(n)
    for e, lst in order.items():
        lst.sort()
        idx = [i for _s, i in lst]
        m = max(len(idx) - 1, 1)
        for r, i in enumerate(idx):
            pos[i] = r / m
        c0 = unit(X[idx].mean(0, keepdims=True))[0]
        disp[idx] = 1.0 - (X[idx] @ c0)
    F["frame_pos"], F["event_dispersion"] = pos, disp
    F["person_count"] = np.asarray([float(v or 0) for v in g(10)])
    F["fallen_person_count"] = np.asarray([float(v or 0) for v in g(11)])

    # ── D ──
    dnl = [str(v or "") for v in g(6)]; pl = [str(v or "") for v in g(7)]
    F["is_night"] = np.asarray([1.0 if "night" in v.lower() else 0.0 for v in dnl])
    F["has_person"] = np.asarray([0.0 if ("no" in v.lower() or v == "") else 1.0 for v in pl])
    F["daynight_margin"] = np.asarray([float(v) if v is not None else 0.0 for v in g(8)])
    F["person_margin"] = np.asarray([float(v) if v is not None else 0.0 for v in g(9)])

    # ── E ──
    gi = np.asarray([ci[c] for c in gt])
    F["gt_score_rank"] = (S > S[np.arange(n), gi][:, None]).sum(1) + 1.0
    F["gt_deficit"] = F["cos_max"] - S[np.arange(n), gi]
    meta = dict(id=np.asarray(g(0), dtype=object), gt=gt, pred=pred, cam=cam, ev=ev, clip=clip,
                correct=(pred == gt).astype(int))
    missing = [f["name"] for f in FEATURES if f["name"] not in F]
    if missing:
        raise SystemExit(f"사전에 있는데 계산이 없는 피처: {missing}")
    extra = [k for k in F if k not in {f["name"] for f in FEATURES}]
    if extra:
        raise SystemExit(f"계산했는데 사전에 없는 피처: {extra}")
    return F, meta


def design_matrix(F, names=None):
    """`free` 피처만 뽑아 (n, d) 행렬로. `diag` 를 섞으려 하면 막는다."""
    names = list(names or FREE)
    bad = [x for x in names if x in DIAG]
    if bad:
        raise ValueError(f"설명 전용(diag) 피처를 예측 입력에 넣을 수 없다: {bad}")
    M = np.stack([np.asarray(F[x], dtype=np.float64) for x in names], axis=1)
    M = np.nan_to_num(M, nan=0.0, posinf=0.0, neginf=0.0)
    return M, names


def dump_dictionary(path):
    import csv
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["name", "group", "leak", "desc", "formula", "why", "note"])
        w.writeheader()
        for f in FEATURES + TARGETS:
            r = {k: f.get(k, "") for k in w.fieldnames}
            w.writerow(r)
    return len(FEATURES)


def selftest():
    """계산 정의가 사전과 어긋나지 않는지 최소 검증."""
    rng = np.random.default_rng(0)
    S = rng.random((5, 4))
    srt = np.sort(S, axis=1)[:, ::-1]
    assert np.allclose(srt[:, 0] - srt[:, 1], S.max(1) - np.sort(S, 1)[:, -2])
    P = np.exp((S - S.max(1, keepdims=True)) * 20.0); P /= P.sum(1, keepdims=True)
    H = -(P * np.log(P + 1e-12)).sum(1)
    assert (H >= 0).all() and (H <= np.log(4) + 1e-9).all(), H
    # 균등 분포면 엔트로피가 최대
    Pu = np.full((1, 4), .25); assert abs(float(-(Pu*np.log(Pu)).sum()) - np.log(4)) < 1e-9
    # diag 차단
    try:
        design_matrix({n: np.zeros(3) for n in FREE + DIAG}, FREE + DIAG[:1]); raise AssertionError("차단 실패")
    except ValueError:
        pass
    assert len({f["name"] for f in FEATURES}) == len(FEATURES), "피처 이름 중복"
    for f in FEATURES:
        assert f["desc"] and f["formula"] and f["why"], f
    print(f"selftest OK — 피처 {len(FEATURES)}개 (free {len(FREE)} / diag {len(DIAG)})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", nargs="?", default="sourcei")
    ap.add_argument("--out", default="/workspace/_xai")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest(); sys.exit(0)
    os.makedirs(a.out, exist_ok=True)
    F, meta = build(a.dataset)
    np.savez(f"{a.out}/features.npz", **{k: np.asarray(v) for k, v in F.items()},
             **{f"meta_{k}": v for k, v in meta.items()})
    dump_dictionary(f"{a.out}/feature_dictionary.csv")
    print(f"표본 {len(meta['gt']):,} · 피처 {len(FEATURES)} (free {len(FREE)} / diag {len(DIAG)})")
    print(f"정답률(프롬프트 규칙) {meta['correct'].mean()*100:.1f}%")
    print(f"→ {a.out}/features.npz, {a.out}/feature_dictionary.csv")
