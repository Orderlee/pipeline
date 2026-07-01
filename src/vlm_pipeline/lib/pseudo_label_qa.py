"""GT-anchored pseudo-label 품질평가: 클래스별 precision / recall / F1.

저장된 pseudo-label(SAM3 박스 / Gemini 이벤트)을 사람 GT(image_label_annotations /
finalized labels)와 비교한다. **모델 추론 없음** — 양쪽 다 이미 materialize 된 상태를
비교하므로 GPU 불필요, CI 에서 검증 가능.

'품질평가' = 데이터 엔지니어가 읽는 신뢰도 리포트: "fire 박스 precision 0.82 /
recall 0.64 → recall 이 약점". mAP(모델 랭킹용, box_map.py)과 달리 **고정 IoU + conf**
에서의 TP/FP/FN 이라 "이 pseudo-label 을 믿어도 되나"를 바로 답한다.

bbox 는 공간 IoU(box_map._iou 재사용), timestamp 는 시간 tIoU — 매칭 로직은 공용.

⚠️ **bbox pseudo 소스 (C-1 수정)**: `sam3_segmentations/<stem>.json` 은 LS 리뷰가
finalize 前에 사람수정본으로 in-place 덮어쓴다 — 그 라이브 키를 "SAM3 원본"으로 읽으면
pseudo==GT 로 오염돼 품질수치가 무의미해진다. bbox pseudo 는 SAM3 추론 직후 1회만 쓰는
write-once 스냅샷(`build_pseudo_bbox_key` → `.pseudo.json`, timestamp 의
`build_pseudo_events_key`와 동일 패턴)에서만 읽어야 한다 — 호출자(`defs/train/label_qa.py`)
책임. 이 파일의 스코어러 자체는 입력 소스를 모르므로(순수 수학) 영향 없음.

Layer: L1-2 pure lib. dagster / defs / resources import 금지.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from vlm_pipeline.lib.box_map import _iou as _box_iou

Geom = Sequence[float]


def _temporal_iou(a: Geom, b: Geom) -> float:
    """1D 구간 [start, end] 의 IoU (temporal IoU)."""
    a0, a1 = float(a[0]), float(a[1])
    b0, b1 = float(b[0]), float(b[1])
    inter = max(0.0, min(a1, b1) - max(a0, b0))
    if inter <= 0.0:
        return 0.0
    union = (a1 - a0) + (b1 - b0) - inter
    return inter / union if union > 0.0 else 0.0


def _greedy_match(
    gts: list[Geom],
    preds: list[tuple[Geom, float]],
    overlap: Callable[[Geom, Geom], float],
    threshold: float,
) -> tuple[int, int, int]:
    """conf 내림차순 pred 를 1:1 로 gt 에 매칭(overlap>=threshold, 최대겹침 우선).

    box_map._ap_single_class 와 동일한 greedy 규칙. 반환 (tp, fp, fn).
    """
    ordered = sorted(preds, key=lambda p: float(p[1]), reverse=True)
    matched = [False] * len(gts)
    tp = 0
    for geom, _score in ordered:
        best_ov, best_j = 0.0, -1
        for j, g in enumerate(gts):
            if matched[j]:
                continue
            ov = overlap(geom, g)
            if ov > best_ov:
                best_ov, best_j = ov, j
        if best_j >= 0 and best_ov >= threshold:
            matched[best_j] = True
            tp += 1
    fp = len(ordered) - tp
    fn = matched.count(False)
    return tp, fp, fn


def _prf(tp: int, fp: int, fn: int) -> dict[str, Any]:
    p = tp / (tp + fp) if (tp + fp) else 0.0
    r = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(p, 6),
        "recall": round(r, 6),
        "f1": round(f1, 6),
    }


def _score(
    per_gt: list[dict[str, list[Geom]]],
    per_pred: list[dict[str, list[tuple[Geom, float]]]],
    overlap: Callable[[Geom, Geom], float],
    threshold: float,
    score_threshold: float,
) -> dict[str, Any]:
    """per_gt[i] / per_pred[i] 는 같은 asset(이미지/비디오) i 에 대응. 클래스별 pool 후 집계."""
    if len(per_gt) != len(per_pred):
        raise ValueError("per_gt and per_pred length mismatch")
    agg: dict[str, list[int]] = {}  # cls -> [tp, fp, fn]
    for gt_item, pred_item in zip(per_gt, per_pred):
        for cls in set(gt_item) | set(pred_item):
            gts = list(gt_item.get(cls, []))
            preds = [(geom, float(s)) for (geom, s) in pred_item.get(cls, []) if float(s) >= score_threshold]
            tp, fp, fn = _greedy_match(gts, preds, overlap, threshold)
            a = agg.setdefault(cls, [0, 0, 0])
            a[0] += tp
            a[1] += fp
            a[2] += fn
    per_class = {cls: _prf(*v) for cls, v in sorted(agg.items())}
    micro = _prf(
        sum(v[0] for v in agg.values()),
        sum(v[1] for v in agg.values()),
        sum(v[2] for v in agg.values()),
    )
    macro_f1 = round(sum(c["f1"] for c in per_class.values()) / len(per_class), 6) if per_class else 0.0
    return {
        "per_class": per_class,
        "micro": micro,
        "macro_f1": macro_f1,
        "item_count": len(per_gt),
        "threshold": threshold,
        "score_threshold": score_threshold,
    }


def score_bbox_quality(
    per_image_gt: list[dict[str, list[Geom]]],
    per_image_pred: list[dict[str, list[tuple[Geom, float]]]],
    *,
    iou_threshold: float = 0.5,
    score_threshold: float = 0.0,
) -> dict[str, Any]:
    """SAM3 pseudo 박스 vs 사람 GT 박스 품질.

    per_image_gt[i]:   ``{class: [[x1,y1,x2,y2], ...]}``  (GT 박스, xyxy 픽셀)
    per_image_pred[i]: ``{class: [([x1,y1,x2,y2], score), ...]}``  (pseudo 박스 + conf)
    반환: ``{per_class:{cls:{tp,fp,fn,precision,recall,f1}}, micro, macro_f1, ...}``.
    """
    return _score(per_image_gt, per_image_pred, _box_iou, iou_threshold, score_threshold)


def score_timestamp_quality(
    per_video_gt: list[dict[str, list[Geom]]],
    per_video_pred: list[dict[str, list[tuple[Geom, float]]]],
    *,
    tiou_threshold: float = 0.5,
    score_threshold: float = 0.0,
) -> dict[str, Any]:
    """Gemini pseudo 이벤트 vs 사람 GT 이벤트 품질 (temporal IoU 매칭).

    per_video_gt[i]:   ``{class: [[start_sec, end_sec], ...]}``
    per_video_pred[i]: ``{class: [([start_sec, end_sec], score), ...]}``
    class 는 이벤트 카테고리(없으면 단일 "event" 로 넣어 event-level P/R/F1).
    """
    return _score(per_video_gt, per_video_pred, _temporal_iou, tiou_threshold, score_threshold)
