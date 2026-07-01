"""Pseudo-label FP/FN → FiftyOne (네이티브 evaluate_detections). analysis 컨테이너용 standalone.

`fiftyone_pgvector.py` 를 **수정하지 않는다**(별 파일 — eng-a WIP 보존). 매칭도 재발명하지 않고
FiftyOne 의 `evaluate_detections`(COCO IoU 매칭)를 그대로 쓴다 → 각 박스에 tp/fp/fn 태그 +
per-class P/R/F1 리포트가 공짜.

**MinIO 직접 빌드**: 사람 확정 GT(image_label_annotations)를 가진 모든 이미지를 MinIO 에서 직접
내려받아 격리 데이터셋 `pseudo_qa` 를 새로 만든다(공유 'frames' 무관 → frames 커버리지 밖 GT 도
전수 채점). 각 샘플에 GT(`ground_truth`) + SAM3 pseudo(`detections`, `sam3_segmentations/<stem>.pseudo.json`
write-once 스냅샷 COCO) 두 필드를 얹고 evaluate.
  - `sam3_segmentations/<stem>.json`(확장자 없는 쪽)은 LS 리뷰가 사람수정본으로 in-place
    덮어쓰므로 pseudo 로 쓰지 않는다(C-1) — 반드시 `.pseudo.json` 스냅샷만 읽는다.
  - 이미지는 `MEDIA_DIR` 에 캐시 — frames 가 이미 받은 건 재사용(재다운로드 skip).
  - :5153 앱에서 `pseudo_qa` 선택 → 박스 tp/fp/fn 필터.

⚠️ analysis 컨테이너엔 vlm_pipeline 이 없다 → SAM3 키빌더/COCO 파서를 여기 inline
   (`vlm_pipeline.lib.key_builders.build_sam3_detection_key`/`build_pseudo_bbox_key` /
   `lib.detection_coco.parse_coco_annotation_boxes` 미러 — 원본 변경 시 동기화 필요).
   fiftyone/boto3/PIL/psycopg2 는 lazy import.

실행 (analysis 컨테이너):
    # 이 스크립트는 이미지에 baked 되지 않음(Dockerfile COPY 목록 밖) — refresh_frames_labels.py 등과
    # 동일하게 배포 시 수동 복사: docker cp docker/analysis/label_qa_fiftyone.py docker-analysis-1:/workspace/
    python /workspace/label_qa_fiftyone.py --folder <source_unit_name> [--iou 0.5]
    python /workspace/label_qa_fiftyone.py --selftest   # 순수 로직 검증(무거운 dep 불필요)
"""

from __future__ import annotations

import argparse
import os
from pathlib import PurePosixPath
from typing import Any

_QA_DATASET = "pseudo_qa"       # QA 전용 격리 데이터셋 (매 실행 재생성)
_DEFAULT_EVAL_KEY = "pseudo_qa"
_PSEUDO_FIELD = "detections"    # SAM3 pseudo 박스 필드 (fiftyone_pgvector.py 관례와 동일 이름)
_LABELS_BUCKET = "vlm-labels"
MEDIA_DIR = "/data/fiftyone/media"  # 영속 볼륨 — frames 와 공유(fiftyone_pgvector.py 와 동일 경로)


# ── 순수 헬퍼 (FiftyOne/IO 불필요 — 단위 테스트 대상) ───────────────────────────

def to_relative_bbox(x: float, y: float, w: float, h: float, width: float, height: float) -> list[float] | None:
    """절대 COCO xywh(픽셀) → FiftyOne 상대 [x, y, w, h] (0~1). width/height<=0 이면 None."""
    if not width or not height or width <= 0 or height <= 0:
        return None
    return [float(x) / width, float(y) / height, float(w) / width, float(h) / height]


def gt_detections_by_image(gt_rows: list[dict]) -> dict[str, list[dict]]:
    """image_label_annotations 행들 → {image_id: [{label, bbox_abs:[x,y,w,h]}]}. GT 는 confidence 없음."""
    out: dict[str, list[dict]] = {}
    for r in gt_rows:
        out.setdefault(str(r["image_id"]), []).append(
            {"label": r["category"], "bbox_abs": [r["bbox_x"], r["bbox_y"], r["bbox_w"], r["bbox_h"]]}
        )
    return out


def sam3_detection_key(image_key: str) -> str:
    """라이브 SAM3 COCO 키 (LS 리뷰가 사람수정본으로 in-place 덮어씀 — QA pseudo 로 쓰지 말 것).

    vlm_pipeline.lib.key_builders.build_sam3_detection_key 미러(analysis 엔 vlm_pipeline 없음).
    """
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    raw_parent = parent.parent if parent.name == "image" else parent
    if str(raw_parent) and str(raw_parent) != ".":
        return str(raw_parent / "sam3_segmentations" / f"{stem}.json")
    return str(PurePosixPath("sam3_segmentations") / f"{stem}.json")


def pseudo_bbox_key(image_key: str) -> str:
    """원본(모델) SAM3 COCO write-once 스냅샷 키(`.pseudo.json`).

    vlm_pipeline.lib.key_builders.build_pseudo_bbox_key 미러. `sam3_detection_key()`의
    `.json`→`.pseudo.json` — QA 는 반드시 이 키를 pseudo 로 읽어야 한다(C-1).
    """
    detection_key = sam3_detection_key(image_key)
    detection_path = PurePosixPath(detection_key)
    suffix = detection_path.suffix or ".json"
    return str(detection_path.with_suffix("")) + ".pseudo" + suffix


def coco_boxes(payload: Any) -> list[dict]:
    """COCO payload → [{category, bbox_abs:[x,y,w,h], score}]. detection_coco.parse_coco_annotation_boxes 미러.

    malformed(bbox 누락·음수·w/h<=0) skip. score 는 [0,1] 밖이면 None(박스는 보존).
    """
    if not isinstance(payload, dict):
        return []
    cats: dict[Any, str] = {}
    for c in payload.get("categories") or []:
        if isinstance(c, dict) and c.get("id") is not None:
            cats[c["id"]] = str(c.get("name") or c["id"]).strip() or str(c["id"])
    boxes: list[dict] = []
    for ann in payload.get("annotations") or []:
        if not isinstance(ann, dict):
            continue
        bbox = ann.get("bbox")
        if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
            continue
        try:
            x, y, w, h = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
        except (TypeError, ValueError):
            continue
        if x < 0 or y < 0 or w <= 0 or h <= 0:
            continue
        cid = ann.get("category_id")
        cat = (cats.get(cid) or str(cid if cid is not None else "unknown")).strip() or "unknown"
        score = ann.get("score")
        try:
            score = float(score) if score is not None else None
        except (TypeError, ValueError):
            score = None
        if score is not None and not (0.0 <= score <= 1.0):
            score = None
        boxes.append({"category": cat, "bbox_abs": [x, y, w, h], "score": score})
    return boxes


# ── IO / FiftyOne 쉘 (lazy import; CI 미검증 — fiftyone_pgvector.py 와 동일 관례) ─────

def _minio_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", ""),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", ""),
    )


def _download_json(mc, bucket: str, key: str) -> Any:
    import json

    obj = mc.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def fetch_gt_rows(dsn: str, folder: str | None = None) -> list[dict]:
    """사람 확정 bbox(image_label_annotations ∩ finalized image_labels) + 이미지 위치(bucket/key)."""
    import psycopg2

    where_folder, params = "", []
    if folder:
        where_folder, params = "AND r.source_unit_name = %s", [folder]
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT im.image_id, im.image_bucket, im.image_key,
                       ila.category, ila.bbox_x, ila.bbox_y, ila.bbox_w, ila.bbox_h
                FROM image_label_annotations ila
                JOIN image_labels il ON il.image_label_id = ila.image_label_id
                JOIN image_metadata im ON im.image_id = ila.image_id
                JOIN raw_files r ON r.asset_id = im.source_asset_id
                WHERE il.review_status = 'finalized'
                  AND im.image_key IS NOT NULL
                  AND im.image_bucket IS NOT NULL
                  {where_folder}
                """,
                tuple(params),
            )
            cols = ["image_id", "image_bucket", "image_key",
                    "category", "bbox_x", "bbox_y", "bbox_w", "bbox_h"]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def push_qa_to_fiftyone(
    *,
    dsn: str,
    qa_dataset: str = _QA_DATASET,
    eval_key: str = _DEFAULT_EVAL_KEY,
    iou: float = 0.5,
    folder: str | None = None,
    media_dir: str = MEDIA_DIR,
    labels_bucket: str = _LABELS_BUCKET,
) -> Any:
    """MinIO 에서 GT 이미지 직접 빌드 → GT + pseudo 필드 → 네이티브 evaluate_detections. frames 무관(전수)."""
    import fiftyone as fo
    from PIL import Image

    rows = fetch_gt_rows(dsn, folder)
    gt_by_image = gt_detections_by_image(rows)
    if not gt_by_image:
        print("[label_qa_fiftyone] GT 0건 (image_label_annotations 비어있음) — 채점할 게 없음. 먼저 사람 라벨링 필요.")
        return None
    img_meta = {str(r["image_id"]): (r["image_bucket"], r["image_key"]) for r in rows}

    os.makedirs(media_dir, exist_ok=True)
    mc = _minio_client()
    if fo.dataset_exists(qa_dataset):
        fo.delete_dataset(qa_dataset)
    ds = fo.Dataset(qa_dataset, persistent=True)

    samples, skipped, missing_pseudo = [], 0, 0
    for image_id, gt_boxes in gt_by_image.items():
        bucket, key = img_meta.get(image_id, (None, None))
        if not bucket or not key:
            skipped += 1
            continue
        local_path = os.path.join(media_dir, f"{image_id}{os.path.splitext(key)[1] or '.jpg'}")
        if not os.path.exists(local_path):  # frames 가 이미 받은 건 재사용
            try:
                mc.download_file(bucket, key, local_path)
            except Exception:  # noqa: BLE001 — 객체 누락 등 skip(per-image fail-forward)
                skipped += 1
                continue
        try:
            with Image.open(local_path) as im:
                width, height = im.size
        except Exception:  # noqa: BLE001
            skipped += 1
            continue

        gt_dets = []
        for g in gt_boxes:
            rel = to_relative_bbox(*g["bbox_abs"], width, height)
            if rel is not None:
                gt_dets.append(fo.Detection(label=g["label"], bounding_box=rel))

        try:
            payload = _download_json(mc, labels_bucket, pseudo_bbox_key(key))
        except Exception:  # noqa: BLE001 — pseudo COCO 없음 → 전부 미검출(fn)
            payload = None
            missing_pseudo += 1
        pseudo_dets = []
        for b in coco_boxes(payload):
            rel = to_relative_bbox(*b["bbox_abs"], width, height)
            if rel is None:
                continue
            kwargs = {"label": b["category"], "bounding_box": rel}
            if b["score"] is not None:
                kwargs["confidence"] = b["score"]
            pseudo_dets.append(fo.Detection(**kwargs))

        sample = fo.Sample(filepath=local_path)
        sample["image_id"] = image_id
        sample["ground_truth"] = fo.Detections(detections=gt_dets)
        sample[_PSEUDO_FIELD] = fo.Detections(detections=pseudo_dets)
        samples.append(sample)

    ds.add_samples(samples)
    results = ds.evaluate_detections(
        _PSEUDO_FIELD, gt_field="ground_truth", eval_key=eval_key, iou=iou, method="coco"
    )
    print(f"[label_qa_fiftyone] dataset='{qa_dataset}', images={len(samples)}, "
          f"skipped={skipped}, missing_pseudo={missing_pseudo}, eval_key='{eval_key}', iou={iou}")
    results.print_report()
    print(
        f"\n:5153 앱 → '{qa_dataset}' 데이터셋 선택 → pseudo 오검출(FP): detections 의 "
        f"F(\"{eval_key}\") == \"fp\" · 미검출(FN): ground_truth 의 F(\"{eval_key}\") == \"fn\""
    )
    return results


def _selftest() -> None:
    assert to_relative_bbox(50, 100, 20, 40, 200, 400) == [0.25, 0.25, 0.1, 0.1]
    assert to_relative_bbox(0, 0, 10, 10, 0, 10) is None
    # sam3 키: <raw_parent>/sam3_segmentations/<stem>.json (parent 가 'image' 면 한 단계 위)
    assert sam3_detection_key("proj/a/frames/f1.jpg") == "proj/a/frames/sam3_segmentations/f1.json"
    assert sam3_detection_key("proj/a/image/f1.jpg") == "proj/a/sam3_segmentations/f1.json"
    # pseudo 스냅샷 키: 동일 경로 + .pseudo.json (QA 는 이 키만 읽어야 함, C-1)
    assert pseudo_bbox_key("proj/a/frames/f1.jpg") == "proj/a/frames/sam3_segmentations/f1.pseudo.json"
    assert pseudo_bbox_key("proj/a/image/f1.jpg") == "proj/a/sam3_segmentations/f1.pseudo.json"
    # coco 파서: 정상 박스 + score, malformed skip
    boxes = coco_boxes({
        "categories": [{"id": 1, "name": "fire"}],
        "annotations": [
            {"category_id": 1, "bbox": [0, 0, 10, 10], "score": 0.9},
            {"category_id": 1, "bbox": [0, 0, -1, 5]},   # w<=0 → skip
        ],
    })
    assert len(boxes) == 1 and boxes[0] == {"category": "fire", "bbox_abs": [0.0, 0.0, 10.0, 10.0], "score": 0.9}
    g = gt_detections_by_image(
        [{"image_id": "i1", "category": "fire", "bbox_x": 0, "bbox_y": 0, "bbox_w": 5, "bbox_h": 5},
         {"image_id": "i1", "category": "smoke", "bbox_x": 1, "bbox_y": 1, "bbox_w": 2, "bbox_h": 2}]
    )
    assert {d["label"] for d in g["i1"]} == {"fire", "smoke"} and len(g["i1"]) == 2
    print("selftest OK")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Push pseudo-label FP/FN into FiftyOne (native evaluate_detections)")
    ap.add_argument("--selftest", action="store_true", help="순수 로직만 검증 (무거운 dep 불필요)")
    ap.add_argument("--folder", default=None, help="특정 source_unit_name 만 (없으면 전체)")
    ap.add_argument("--qa-dataset", default=_QA_DATASET, help="QA 격리 데이터셋(매 실행 재생성)")
    ap.add_argument("--eval-key", default=_DEFAULT_EVAL_KEY)
    ap.add_argument("--iou", type=float, default=0.5)
    ap.add_argument("--media-dir", default=MEDIA_DIR)
    ap.add_argument("--dsn", default=os.environ.get("DATAOPS_POSTGRES_DSN"))
    a = ap.parse_args()
    if a.selftest:
        _selftest()
    else:
        if not a.dsn:
            raise SystemExit("DATAOPS_POSTGRES_DSN 필요 (--dsn 또는 env)")
        push_qa_to_fiftyone(
            dsn=a.dsn, qa_dataset=a.qa_dataset, eval_key=a.eval_key,
            iou=a.iou, folder=a.folder, media_dir=a.media_dir,
        )
