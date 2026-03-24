"""YOLO @asset — YOLO-World-L object detection (별도 Docker 서버 호출).

YOLO 서버: docker/yolo/ (GPU 1 전용, 모델 상주)
이 asset: HTTP로 detection 요청 → vlm-labels JSON 업로드 + image_labels INSERT

파이프라인 흐름:
  clip_to_frame → [yolo_detection_sensor] → yolo_image_detection
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4

from dagster import Field, asset

from vlm_pipeline.lib.env_utils import (
    YOLO_OUTPUTS,
    dispatch_raw_key_prefix_folder,
    int_env,
    should_run_any_output,
)
from vlm_pipeline.lib.spec_config import load_persisted_spec_config, parse_requested_outputs
from vlm_pipeline.lib.yolo_world import get_yolo_client
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="yolo_image_detection",
    deps=["clip_to_frame"],
    description="YOLO-World-L object detection → vlm-labels JSON + image_labels INSERT",
    group_name="yolo",
    config_schema={
        "limit": Field(int, default_value=500),
        "confidence_threshold": Field(float, default_value=0.25),
        "iou_threshold": Field(float, default_value=0.45),
        "batch_size": Field(int, default_value=4),
    },
)
def yolo_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """processed_clip_frame 이미지에 YOLO-World-L detection 실행."""
    return _run_yolo_image_detection(context, db, minio)


def _run_yolo_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """processed_clip_frame 이미지에 YOLO-World-L detection 실행."""
    if not should_run_any_output(context, YOLO_OUTPUTS):
        context.log.info("yolo_image_detection 스킵: outputs에 YOLO 계열 요청이 없습니다.")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True}

    limit = int(context.op_config.get("limit", 500))
    conf = float(context.op_config.get("confidence_threshold", 0.25))
    iou = float(context.op_config.get("iou_threshold", 0.45))
    batch_size = int(context.op_config.get("batch_size", 4))
    folder_name = dispatch_raw_key_prefix_folder(context.run.tags if context.run else None)

    candidates = db.find_yolo_pending_images(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("YOLO 대상 이미지 없음")
        return {"processed": 0, "failed": 0, "total_detections": 0}

    client = get_yolo_client()

    if not client.wait_until_ready(max_wait_sec=120):
        context.log.error("YOLO 서버가 준비되지 않았습니다")
        return {"processed": 0, "failed": len(candidates), "total_detections": 0, "error": "yolo_server_not_ready"}

    health = client.health()
    context.log.info(
        f"YOLO 서버 연결: url={client.api_url} "
        f"device={health.get('device')} classes={health.get('classes_count')} "
        f"gpu_mem={health.get('gpu_memory', {}).get('free_gb', '?')}GB free"
    )

    total_candidates = len(candidates)
    total_batches = (total_candidates + batch_size - 1) // batch_size
    context.log.info(
        f"yolo_image_detection 시작: 총 {total_candidates}건 ({total_batches} 배치, batch_size={batch_size})"
    )

    processed = 0
    failed = 0
    total_detections = 0
    label_rows_buffer: list[dict] = []
    flush_threshold = int_env("YOLO_DB_FLUSH_THRESHOLD", 100, 10)

    for batch_start in range(0, len(candidates), batch_size):
        batch = candidates[batch_start : batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        image_bytes_list: list[bytes] = []
        valid_candidates: list[dict] = []

        for cand in batch:
            try:
                img_bytes = minio.download(
                    str(cand.get("image_bucket") or "vlm-processed"),
                    str(cand["image_key"]),
                )
                image_bytes_list.append(img_bytes)
                valid_candidates.append(cand)
            except Exception as exc:
                failed += 1
                context.log.error(
                    f"YOLO 이미지 다운로드 실패: image_id={cand['image_id']} "
                    f"key={cand.get('image_key')}: {exc}"
                )

        if not image_bytes_list:
            continue

        try:
            batch_results = client.detect_batch(
                image_bytes_list, conf=conf, iou=iou,
            )
        except Exception as exc:
            failed += len(valid_candidates)
            context.log.error(f"YOLO 배치 추론 실패: {exc}")
            continue

        for idx, cand in enumerate(valid_candidates):
            image_id = cand["image_id"]
            source_clip_id = cand.get("source_clip_id")
            image_key = str(cand["image_key"])

            result = batch_results[idx] if idx < len(batch_results) else {}
            detections = result.get("detections", [])

            try:
                label_json = {
                    "image_id": image_id,
                    "source_clip_id": source_clip_id,
                    "image_key": image_key,
                    "model": "yolov8l-worldv2",
                    "confidence_threshold": conf,
                    "iou_threshold": iou,
                    "detections": detections,
                    "detection_count": len(detections),
                    "detected_at": datetime.now().isoformat(),
                }

                labels_key = _build_yolo_label_key(image_key)
                label_bytes = json.dumps(label_json, ensure_ascii=False).encode("utf-8")
                minio.upload("vlm-labels", labels_key, label_bytes, "application/json")

                label_rows_buffer.append({
                    "image_label_id": str(uuid4()),
                    "image_id": image_id,
                    "source_clip_id": source_clip_id,
                    "labels_bucket": "vlm-labels",
                    "labels_key": labels_key,
                    "label_format": "yolo_detection_json",
                    "label_tool": "yolo-world",
                    "label_source": "auto",
                    "review_status": "auto_generated",
                    "label_status": "completed",
                    "object_count": len(detections),
                    "created_at": datetime.now(),
                })

                processed += 1
                total_detections += len(detections)

            except Exception as exc:
                failed += 1
                context.log.error(f"YOLO 결과 저장 실패: image_id={image_id}: {exc}")

        done_so_far = min(batch_start + batch_size, total_candidates)
        context.log.info(
            f"yolo_image_detection 진행: 배치 [{batch_num}/{total_batches}] "
            f"({done_so_far}/{total_candidates}, {done_so_far * 100 // total_candidates}%) "
            f"ok={processed} fail={failed} detections={total_detections}"
        )

        if len(label_rows_buffer) >= flush_threshold:
            _flush_labels(db, label_rows_buffer, context)
            label_rows_buffer.clear()

    if label_rows_buffer:
        _flush_labels(db, label_rows_buffer, context)
        label_rows_buffer.clear()

    summary = {"processed": processed, "failed": failed, "total_detections": total_detections}
    context.add_output_metadata(summary)
    context.log.info(f"YOLO 완료: {summary}")
    return summary


@asset(
    name="bbox_labeling",
    deps=["clip_to_frame"],
    description="Staging spec flow: requested_outputs에 bbox 포함 시 frame_status=completed 대상 YOLO detection",
    group_name="yolo",
    config_schema={"limit": Field(int, default_value=500)},
)
def bbox_labeling(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """run tag: spec_id, requested_outputs, resolved_config_id. bbox 요청 시 spec.classes × config.bbox.target_classes."""
    tags = context.run.tags if context.run else {}
    requested = parse_requested_outputs(tags)
    if "bbox" not in requested:
        context.log.info("bbox_labeling 스킵: requested_outputs에 bbox 없음")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True}

    spec_id = str(tags.get("spec_id") or "").strip()
    resolved_config_id = None
    target_classes: list[str] = []
    if spec_id:
        config_bundle = load_persisted_spec_config(db, spec_id)
        resolved_config_id = config_bundle["resolved_config_id"]
        spec_classes = config_bundle["spec"].get("classes") or []
        if not isinstance(spec_classes, list):
            spec_classes = []
        bbox_config = config_bundle["config_json"].get("bbox", {})
        config_target_classes = bbox_config.get("target_classes") or []
        if isinstance(config_target_classes, list) and config_target_classes:
            target_classes = [c for c in spec_classes if c in set(config_target_classes)]
        else:
            target_classes = list(spec_classes)
        context.log.info(
            "bbox_labeling: spec_id=%s resolved_config_id=%s target_classes=%s",
            spec_id,
            resolved_config_id,
            ",".join(target_classes) if target_classes else "(empty)",
        )

    # TODO: spec_id·frame_status=completed 기준 백로그 조회, target_classes = intersection(spec.classes, config.bbox.target_classes)
    context.log.info("bbox_labeling: 스펙/설정 연동 TODO")
    return {
        "processed": 0,
        "failed": 0,
        "total_detections": 0,
        "resolved_config_id": resolved_config_id,
        "target_classes": target_classes,
    }


def _build_yolo_label_key(image_key: str) -> str:
    key_path = PurePosixPath(str(image_key or "").strip())
    stem = key_path.stem or "image"
    parent = key_path.parent
    if parent.name == "image":
        raw_parent = parent.parent
    else:
        raw_parent = parent
    if str(raw_parent) and str(raw_parent) != ".":
        return str(raw_parent / "detections" / f"{stem}.json")
    return str(PurePosixPath("detections") / f"{stem}.json")


def _flush_labels(db: DuckDBResource, rows: list[dict], context) -> None:
    try:
        count = db.batch_insert_image_labels(rows)
        context.log.debug(f"YOLO image_labels flush: {count}건")
    except Exception as exc:
        context.log.error(f"YOLO image_labels flush 실패: {exc}")
