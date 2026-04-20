"""YOLO @asset — YOLO-World-L object detection (별도 Docker 서버 호출).

YOLO 서버: docker/yolo/ (GPU 1 전용, 모델 상주).
이 asset: HTTP로 detection 요청 → COCO detection JSON 업로드 + image_labels INSERT.

파이프라인 흐름:
  clip_to_frame → [yolo_detection_sensor] → yolo_image_detection
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime

from dagster import Field, asset

from vlm_pipeline.lib.detection_common import (
    flush_image_labels,
    normalize_classes,
    resolve_target_classes,
    stable_image_label_id,
)
from vlm_pipeline.lib.env_utils import (
    YOLO_OUTPUTS,
    bool_env,
    dispatch_raw_key_prefix_folder,
    int_env,
    should_run_any_output,
)
from vlm_pipeline.lib.spec_config import parse_requested_outputs
from vlm_pipeline.lib.yolo_thresholds import (
    filter_detections_by_class_confidence,
    resolve_active_class_confidence_thresholds,
    resolve_effective_request_confidence_threshold,
)
from vlm_pipeline.lib.detection_coco import build_coco_detection_payload
from vlm_pipeline.lib.yolo_world import get_yolo_client
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


_YOLO_CONFIG_SCHEMA = {
    "limit": Field(int, default_value=500),
    "confidence_threshold": Field(float, default_value=0.25),
    "iou_threshold": Field(float, default_value=0.45),
    "batch_size": Field(int, default_value=8),
}


def _make_yolo_detection_asset(*, name: str, deps: list[str], description: str):
    @asset(
        name=name,
        deps=deps,
        description=description,
        group_name="yolo",
        config_schema=_YOLO_CONFIG_SCHEMA,
    )
    def _yolo_detection_asset(
        context,
        db: DuckDBResource,
        minio: MinIOResource,
    ) -> dict:
        return _run_yolo_image_detection(context, db, minio)

    return _yolo_detection_asset


yolo_image_detection = _make_yolo_detection_asset(
    name="yolo_image_detection",
    deps=["clip_to_frame"],
    description="YOLO-World-L object detection → vlm-labels COCO JSON + image_labels INSERT",
)
dispatch_yolo_image_detection = _make_yolo_detection_asset(
    name="dispatch_yolo_image_detection",
    deps=["clip_to_frame", "raw_video_to_frame"],
    description="Dispatch 라인: frame 추출 완료 후 YOLO-World-L object detection",
)


def _run_yolo_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    folder_name_override: str | None = None,
    spec_id_override: str | None = None,
    target_classes_override: list[str] | None = None,
    class_source_override: str | None = None,
    resolved_config_id_override: str | None = None,
) -> dict:
    """processed_clip_frame 이미지에 YOLO-World-L detection 실행."""
    if not bool_env("ENABLE_YOLO_DETECTION", True):
        context.log.info(
            "yolo_image_detection 스킵: ENABLE_YOLO_DETECTION=false (SAM3가 primary bbox 엔진)"
        )
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True}

    if not should_run_any_output(context, YOLO_OUTPUTS):
        context.log.info("yolo_image_detection 스킵: outputs에 YOLO 계열 요청이 없습니다.")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True}

    limit = int(context.op_config.get("limit", 500))
    conf = float(context.op_config.get("confidence_threshold", 0.25))
    iou = float(context.op_config.get("iou_threshold", 0.45))
    batch_size = int(context.op_config.get("batch_size", 8))
    tags = context.run.tags if context.run else {}
    requested_outputs = parse_requested_outputs(tags)
    resolution = resolve_target_classes(tags, db)
    folder_name = folder_name_override
    if folder_name is None and not spec_id_override:
        folder_name = dispatch_raw_key_prefix_folder(tags)

    spec_id = spec_id_override if spec_id_override is not None else resolution.spec_id
    resolved_config_id = (
        resolved_config_id_override if resolved_config_id_override is not None else resolution.resolved_config_id
    )
    store_image_classification = not bool(spec_id) and "classification_image" in requested_outputs

    if target_classes_override is not None:
        target_classes = normalize_classes(target_classes_override)
        class_source = class_source_override or ("explicit_target_classes" if target_classes else "server_default")
    else:
        target_classes = list(resolution.classes)
        class_source = class_source_override or resolution.class_source

    active_class_confidence_thresholds = resolve_active_class_confidence_thresholds(target_classes, conf)
    effective_request_confidence_threshold = resolve_effective_request_confidence_threshold(
        conf,
        active_class_confidence_thresholds,
    )

    candidates = db.find_yolo_pending_images(
        limit=limit,
        folder_name=folder_name,
        spec_id=spec_id,
        include_image_classification=store_image_classification,
    )
    if not candidates:
        context.log.info("YOLO 대상 이미지 없음")
        return {
            "processed": 0,
            "failed": 0,
            "total_detections": 0,
            "requested_classes": target_classes,
            "class_source": class_source,
            "resolved_config_id": resolved_config_id,
            "effective_request_confidence_threshold": effective_request_confidence_threshold,
            "class_confidence_thresholds": active_class_confidence_thresholds,
        }

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
    context.log.info(
        "YOLO target classes resolved: "
        f"source={class_source} spec_id={spec_id or '-'} "
        f"classes={', '.join(target_classes) if target_classes else '(server default)'}"
    )
    context.log.info(
        "YOLO confidence thresholds: "
        f"base={conf:.3f} request={effective_request_confidence_threshold:.3f} "
        f"class_thresholds={json.dumps(active_class_confidence_thresholds, ensure_ascii=False, sort_keys=True)}"
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
                image_bytes_list,
                conf=effective_request_confidence_threshold,
                iou=iou,
                classes=target_classes or None,
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
            detections = filter_detections_by_class_confidence(
                result.get("detections", []),
                global_confidence_threshold=conf,
                class_confidence_thresholds=active_class_confidence_thresholds,
            )
            image_size = result.get("image_size")

            try:
                detected_at = datetime.now()
                label_json = _build_coco_detection_payload(
                    image_id=image_id,
                    source_clip_id=source_clip_id,
                    image_key=image_key,
                    image_width=result.get("image_width") or (image_size[0] if isinstance(image_size, list) and len(image_size) >= 1 else cand.get("width")),
                    image_height=result.get("image_height") or (image_size[1] if isinstance(image_size, list) and len(image_size) >= 2 else cand.get("height")),
                    detections=detections,
                    requested_classes=target_classes,
                    class_source=class_source,
                    resolved_config_id=resolved_config_id,
                    confidence_threshold=conf,
                    iou_threshold=iou,
                    detected_at=detected_at,
                    effective_request_confidence_threshold=effective_request_confidence_threshold,
                    class_confidence_thresholds=active_class_confidence_thresholds,
                    elapsed_ms=result.get("elapsed_ms"),
                )
                annotation_count = len(label_json.get("annotations") or [])

                labels_key = _build_yolo_label_key(image_key)
                label_bytes = json.dumps(label_json, ensure_ascii=False).encode("utf-8")
                minio.upload("vlm-labels", labels_key, label_bytes, "application/json")

                label_rows_buffer.append({
                    "image_label_id": stable_image_label_id(str(image_id), labels_key),
                    "image_id": image_id,
                    "source_clip_id": source_clip_id,
                    "labels_bucket": "vlm-labels",
                    "labels_key": labels_key,
                    "label_format": "coco",
                    "label_tool": "yolo-world",
                    "label_source": "auto",
                    "review_status": "auto_generated",
                    "label_status": "completed",
                    "object_count": annotation_count,
                    "created_at": detected_at,
                })

                if store_image_classification:
                    classification_key = _build_image_classification_key(image_key)
                    classification_payload = _build_image_classification_payload(
                        image_id=image_id,
                        source_clip_id=source_clip_id,
                        image_key=image_key,
                        detections=detections,
                        bbox_labels_key=labels_key,
                        requested_classes=target_classes,
                        class_source=class_source,
                        resolved_config_id=resolved_config_id,
                        detected_at=detected_at,
                    )
                    minio.upload(
                        "vlm-labels",
                        classification_key,
                        json.dumps(classification_payload, ensure_ascii=False).encode("utf-8"),
                        "application/json",
                    )
                    label_rows_buffer.append({
                        "image_label_id": stable_image_label_id(str(image_id), classification_key),
                        "image_id": image_id,
                        "source_clip_id": source_clip_id,
                        "labels_bucket": "vlm-labels",
                        "labels_key": classification_key,
                        "label_format": "image_classification_json",
                        "label_tool": "yolo-world-classification",
                        "label_source": "auto",
                        "review_status": "auto_generated",
                        "label_status": "completed",
                        "object_count": len(classification_payload.get("predicted_classes") or []),
                        "created_at": detected_at,
                    })

                processed += 1
                total_detections += annotation_count

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
            flush_image_labels(db, label_rows_buffer, context, tool_name="YOLO")
            label_rows_buffer.clear()

    if label_rows_buffer:
        flush_image_labels(db, label_rows_buffer, context, tool_name="YOLO")
        label_rows_buffer.clear()

    summary = {
        "processed": processed,
        "failed": failed,
        "total_detections": total_detections,
        "requested_classes": target_classes,
        "class_source": class_source,
        "resolved_config_id": resolved_config_id,
        "effective_request_confidence_threshold": effective_request_confidence_threshold,
        "class_confidence_thresholds": active_class_confidence_thresholds,
    }
    context.add_output_metadata(
        {
            "processed": processed,
            "failed": failed,
            "total_detections": total_detections,
            "requested_classes": ", ".join(target_classes) if target_classes else "(server default)",
            "class_source": class_source,
            "resolved_config_id": resolved_config_id or "",
            "effective_request_confidence_threshold": effective_request_confidence_threshold,
            "class_confidence_thresholds": json.dumps(
                active_class_confidence_thresholds,
                ensure_ascii=False,
                sort_keys=True,
            ),
        }
    )
    context.log.info(f"YOLO 완료: {summary}")
    return summary


def _build_yolo_label_key(image_key: str) -> str:
    from vlm_pipeline.lib.key_builders import build_yolo_label_key
    return build_yolo_label_key(image_key)


def _build_image_classification_key(image_key: str) -> str:
    from vlm_pipeline.lib.key_builders import build_image_classification_key
    return build_image_classification_key(image_key)


def _build_coco_detection_payload(
    *,
    image_id: str,
    source_clip_id: str | None,
    image_key: str,
    image_width: object,
    image_height: object,
    detections: list[dict[str, object]],
    requested_classes: list[str],
    class_source: str,
    resolved_config_id: str | None,
    confidence_threshold: float,
    iou_threshold: float,
    detected_at: datetime,
    effective_request_confidence_threshold: float,
    class_confidence_thresholds: Mapping[str, object],
    elapsed_ms: float | None = None,
) -> dict[str, object]:
    # Backward-compatible wrapper for existing tests/imports.
    return build_coco_detection_payload(
        image_id=image_id,
        source_clip_id=source_clip_id,
        image_key=image_key,
        image_width=image_width,
        image_height=image_height,
        detections=detections,
        requested_classes=requested_classes,
        class_source=class_source,
        resolved_config_id=resolved_config_id,
        confidence_threshold=confidence_threshold,
        iou_threshold=iou_threshold,
        detected_at=detected_at,
        effective_request_confidence_threshold=effective_request_confidence_threshold,
        class_confidence_thresholds=class_confidence_thresholds,
        elapsed_ms=elapsed_ms,
    )


def _build_image_classification_payload(
    *,
    image_id: str,
    source_clip_id: str | None,
    image_key: str,
    detections: list[dict[str, object]],
    bbox_labels_key: str,
    requested_classes: list[str],
    class_source: str,
    resolved_config_id: str | None,
    detected_at: datetime,
) -> dict[str, object]:
    class_counts: dict[str, int] = {}
    for detection in detections:
        class_name = str(detection.get("class") or detection.get("class_name") or "").strip().lower()
        if not class_name:
            continue
        class_counts[class_name] = int(class_counts.get(class_name, 0)) + 1

    predicted_classes = [
        class_name
        for class_name, _count in sorted(class_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return {
        "image_id": image_id,
        "source_clip_id": source_clip_id,
        "image_key": image_key,
        "predicted_classes": predicted_classes,
        "class_counts": class_counts,
        "bbox_labels_key": bbox_labels_key,
        "requested_classes": list(requested_classes),
        "class_source": class_source,
        "resolved_config_id": resolved_config_id,
        "generated_at": detected_at.isoformat(),
    }



# Backward-compatible aliases for external imports.
_normalize_yolo_classes = normalize_classes
