"""Pseudo-label 품질평가 asset — 저장된 모델 라벨 vs 사람 GT (GPU 없음).

품질평가('가능여부' 문서 §5): 모델 추론을 다시 돌리지 않고, 이미 저장된 pseudo-label 을
사람이 확정한 GT 와 비교해 precision/recall/F1 을 낸다.

- **bbox**: pseudo = 생성 시점 스냅샷(`sam3_segmentations/<stem>.pseudo.json`, LS 리뷰가
  `sam3_segmentations/<stem>.json`을 사람수정본으로 in-place 덮어쓰기 前 write-once 보존) /
  GT = image_label_annotations(사람 확정 박스). → 스냅샷이 쌓인 이후의 이미지부터 채점 가능
  (그 전 생성분은 원본이 이미 없음 — `missing_pseudo`로 집계, all-FN).
- **timestamp**: pseudo = 생성 시점 스냅샷(`*.pseudo.json`, LS 덮어쓰기 전 보존) /
  GT = finalized `labels` 이벤트. → 스냅샷이 쌓인 이후의 영상부터 채점 가능(그 전 finalize 는
  원본이 이미 소실 — 설계상 event-level 만).

Layer L4: thin @asset → _run_* → 순수 스코어러(lib.pseudo_label_qa). GT 0 이면 빈 리포트.
"""

from __future__ import annotations

from typing import Any

from dagster import Field, MetadataValue, asset

from vlm_pipeline.lib.detection_coco import parse_coco_annotation_boxes
from vlm_pipeline.lib.key_builders import build_pseudo_bbox_key, build_pseudo_events_key
from vlm_pipeline.lib.pseudo_label_qa import score_bbox_quality, score_timestamp_quality
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

_LABELS_BUCKET = "vlm-labels"


def annotate_scorability(report: dict[str, Any]) -> dict[str, Any]:
    """채점 가능 표본 수를 계산해 붙이고, 0 이면 abstain 으로 표시한다.

    GT 는 있는데 pseudo 스냅샷이 하나도 없으면 모든 GT 가 FN 으로 잡혀 macro_f1=0.0 이 나온다.
    그건 "모델 성능이 0" 이 아니라 "측정 불가" 인데 숫자만 보면 구분이 안 된다.

    실측(2026-07-29): GT 248장 **전부** 스냅샷이 없다 — write-once 스냅샷 writer 는
    2026-07-07 배포분이고 현재 GT 는 그 이전 검출분이라 원본이 이미 소실됐다.
    즉 지금 이 asset 을 materialize 하면 macro_f1=0.0 만 나온다. 정기 스케줄을 걸면
    "SAM3 성능 0" 을 매일 리포트하는 오독 생산기가 되므로, 스케줄은 `scorable_items` 가
    0 을 벗어난 뒤에 붙인다.
    """
    scorable = int(report.get("gt_items") or 0) - int(report.get("missing_pseudo") or 0)
    report["scorable_items"] = scorable
    report["abstained"] = scorable <= 0
    return report


def format_qa_report_md(report: dict[str, Any]) -> str:
    """리포트 → Dagster UI 가 렌더하는 markdown 표 (per-class P/R/F1 + micro). 별도 대시보드 불필요."""
    if report.get("abstained"):
        return (
            f"**{report.get('modality', '?')} QA — ABSTAIN (채점 가능 표본 0)**\n\n"
            f"items={report.get('gt_items', '?')}, missing_pseudo={report.get('missing_pseudo', '?')}, "
            f"scorable_items={report.get('scorable_items', '?')}\n\n"
            f"GT 전체에 pseudo 스냅샷이 없어 모든 GT 가 FN 으로 집계된다. "
            f"`macro_f1={report.get('macro_f1')}` 은 **측정 불가를 뜻하며 성능 0 이 아니다.**"
        )
    head = (
        f"**{report.get('modality', '?')} QA** — items={report.get('gt_items', '?')}, "
        f"scorable={report.get('scorable_items', '?')}, "
        f"missing_pseudo={report.get('missing_pseudo', '?')}, "
        f"macro_f1={report.get('macro_f1')} (thr={report.get('threshold')})"
    )
    rows = ["", "| class | precision | recall | f1 | tp | fp | fn |", "|---|---|---|---|---|---|---|"]
    for cls, m in report.get("per_class", {}).items():
        rows.append(f"| {cls} | {m['precision']} | {m['recall']} | {m['f1']} | {m['tp']} | {m['fp']} | {m['fn']} |")
    mi = report.get("micro") or {}
    if mi:
        rows.append(
            f"| **micro** | {mi['precision']} | {mi['recall']} | {mi['f1']} | {mi['tp']} | {mi['fp']} | {mi['fn']} |"
        )
    return head + "\n" + "\n".join(rows)


def _xywh_to_xyxy(x: float, y: float, w: float, h: float) -> list[float]:
    return [float(x), float(y), float(x) + float(w), float(y) + float(h)]


def _fetch_pseudo_bbox(minio: MinIOResource, bucket: str, image_key: str) -> dict | None:
    """보존된 원본 SAM3 COCO 스냅샷을 클래스별 (box_xyxy, score) 로. 없으면 None (→ GT 는 전부 fn).

    `sam3_segmentations/<stem>.json` 은 LS 리뷰가 사람수정본으로 in-place 덮어쓰므로 GT와
    동일 객체가 될 수 있다(C-1) — 반드시 write-once `.pseudo.json` 스냅샷만 읽는다.
    구 라이브 키(`build_sam3_detection_key`)로 폴백하지 않는다(그 경우 pseudo==GT 로 오염).
    """
    key = build_pseudo_bbox_key(image_key)
    if not minio.exists(bucket, key):
        return None
    try:
        payload = minio.download_json(bucket, key)
    except Exception:  # noqa: BLE001 — per-image fail-forward; 못 읽으면 미검출 취급
        return None
    out: dict[str, list] = {}
    for b in parse_coco_annotation_boxes(payload):
        score = b["score"] if b["score"] is not None else 1.0
        out.setdefault(b["category"], []).append(
            (_xywh_to_xyxy(b["bbox_x"], b["bbox_y"], b["bbox_w"], b["bbox_h"]), score)
        )
    return out


def _run_bbox_label_qa(
    db: PostgresResource,
    minio: MinIOResource,
    *,
    folder: str | None = None,
    iou_threshold: float = 0.5,
    score_threshold: float = 0.0,
    labels_bucket: str = _LABELS_BUCKET,
) -> dict[str, Any]:
    gt_rows = db.find_sam3_finalized_bbox_candidates(folder)
    by_image: dict[str, dict] = {}
    for r in gt_rows:
        info = by_image.setdefault(str(r["image_id"]), {"image_key": r["image_key"], "gt": {}})
        info["gt"].setdefault(r["category"], []).append(
            _xywh_to_xyxy(r["bbox_x"], r["bbox_y"], r["bbox_w"], r["bbox_h"])
        )
    per_gt, per_pred, missing = [], [], 0
    for iid in sorted(by_image):
        per_gt.append(by_image[iid]["gt"])
        pred = _fetch_pseudo_bbox(minio, labels_bucket, by_image[iid]["image_key"])
        if pred is None:
            missing += 1
            per_pred.append({})
        else:
            per_pred.append(pred)
    report = score_bbox_quality(per_gt, per_pred, iou_threshold=iou_threshold, score_threshold=score_threshold)
    report.update(modality="bbox", folder=folder, gt_items=len(per_gt), missing_pseudo=missing)
    return annotate_scorability(report)


def _fetch_pseudo_events(minio: MinIOResource, bucket: str, labels_key: str) -> list | None:
    """보존된 원본 이벤트 스냅샷 → [([start,end], 1.0), ...]. 없으면 None."""
    key = build_pseudo_events_key(labels_key)
    if not minio.exists(bucket, key):
        return None
    try:
        payload = minio.download_json(bucket, key)
    except Exception:  # noqa: BLE001 — per-video fail-forward
        return None
    events = []
    for ev in payload or []:
        ts = ev.get("timestamp") if isinstance(ev, dict) else None
        if isinstance(ts, (list, tuple)) and len(ts) >= 2:
            events.append(([float(ts[0]), float(ts[1])], 1.0))
    return events


def _run_timestamp_label_qa(
    db: PostgresResource,
    minio: MinIOResource,
    *,
    folder: str | None = None,
    tiou_threshold: float = 0.5,
    labels_bucket: str = _LABELS_BUCKET,
) -> dict[str, Any]:
    gt_rows = db.find_finalized_timestamp_events(folder)
    by_video: dict[str, list] = {}
    for r in gt_rows:
        by_video.setdefault(str(r["labels_key"]), []).append(
            [float(r["timestamp_start_sec"]), float(r["timestamp_end_sec"])]
        )
    per_gt, per_pred, missing = [], [], 0
    for key in sorted(by_video):
        per_gt.append({"event": by_video[key]})
        pseudo = _fetch_pseudo_events(minio, labels_bucket, key)
        if pseudo is None:
            missing += 1
            per_pred.append({})
        else:
            per_pred.append({"event": pseudo})
    report = score_timestamp_quality(per_gt, per_pred, tiou_threshold=tiou_threshold)
    report.update(modality="timestamp", folder=folder, gt_items=len(per_gt), missing_pseudo=missing)
    return annotate_scorability(report)


_QA_CONFIG = {
    "folder": Field(str, is_required=False, description="특정 source_unit_name 만 채점 (없으면 전체)"),
    "score_threshold": Field(float, default_value=0.0, is_required=False),
}


@asset(
    name="pseudo_label_bbox_qa",
    description="저장된 SAM3 pseudo 박스 vs 사람 GT(image_label_annotations) 품질(P/R/F1). GPU 불필요.",
    group_name="train",
    config_schema={**_QA_CONFIG, "iou_threshold": Field(float, default_value=0.5, is_required=False)},
)
def pseudo_label_bbox_qa(context, db: PostgresResource, minio: MinIOResource) -> dict[str, Any]:
    cfg = context.op_config
    report = _run_bbox_label_qa(
        db,
        minio,
        folder=cfg.get("folder"),
        iou_threshold=float(cfg.get("iou_threshold", 0.5)),
        score_threshold=float(cfg.get("score_threshold", 0.0)),
    )
    if report["abstained"]:
        context.log.warning(
            "pseudo_label_bbox_qa: ABSTAIN — 채점 가능 표본 0 (gt_items=%s, missing_pseudo=%s). "
            "macro_f1=%s 는 측정 불가이며 성능 0 이 아니다.",
            report["gt_items"],
            report["missing_pseudo"],
            report["macro_f1"],
        )
    else:
        context.log.info(
            "pseudo_label_bbox_qa: gt_items=%s scorable=%s macro_f1=%s micro=%s",
            report["gt_items"],
            report["scorable_items"],
            report["macro_f1"],
            report["micro"],
        )
    context.add_output_metadata(
        {
            "macro_f1": report["macro_f1"],
            "gt_items": report["gt_items"],
            "scorable_items": report["scorable_items"],
            "abstained": report["abstained"],
            "missing_pseudo": report["missing_pseudo"],
            "per_class": MetadataValue.md(format_qa_report_md(report)),
        }
    )
    return report


@asset(
    name="pseudo_label_timestamp_qa",
    description="보존된 pseudo 이벤트 vs finalized labels 이벤트 품질(event-level P/R/F1). GPU 불필요.",
    group_name="train",
    config_schema={**_QA_CONFIG, "tiou_threshold": Field(float, default_value=0.5, is_required=False)},
)
def pseudo_label_timestamp_qa(context, db: PostgresResource, minio: MinIOResource) -> dict[str, Any]:
    cfg = context.op_config
    report = _run_timestamp_label_qa(
        db, minio, folder=cfg.get("folder"), tiou_threshold=float(cfg.get("tiou_threshold", 0.5))
    )
    if report["abstained"]:
        context.log.warning(
            "pseudo_label_timestamp_qa: ABSTAIN — 채점 가능 표본 0 (gt_items=%s, missing_pseudo=%s). "
            "macro_f1=%s 는 측정 불가이며 성능 0 이 아니다.",
            report["gt_items"],
            report["missing_pseudo"],
            report["macro_f1"],
        )
    else:
        context.log.info(
            "pseudo_label_timestamp_qa: gt_items=%s scorable=%s macro_f1=%s missing_pseudo=%s",
            report["gt_items"],
            report["scorable_items"],
            report["macro_f1"],
            report["missing_pseudo"],
        )
    context.add_output_metadata(
        {
            "macro_f1": report["macro_f1"],
            "gt_items": report["gt_items"],
            "scorable_items": report["scorable_items"],
            "abstained": report["abstained"],
            "missing_pseudo": report["missing_pseudo"],
            "per_class": MetadataValue.md(format_qa_report_md(report)),
        }
    )
    return report
