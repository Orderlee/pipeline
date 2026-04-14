"""SAM3.1 @asset — SAM3 text-prompted bbox detection (별도 Docker 서버 호출).

SAM3 서버: docker/sam3/ (GPU 0 전용, 모델 상주).
이 asset: HTTP로 segment 요청 → mask에서 bbox 추출 → JSON 업로드 + image_labels INSERT.

SAM3는 텍스트 프롬프트로 segmentation 후 mask_bbox/model_box를 산출한다.
YOLO의 COCO bbox 결과와 1:1 비교가 목적이므로, 동일 이미지에 병렬 실행한다.

파이프라인 흐름:
  clip_to_frame → sam3_image_detection (YOLO와 병렬)
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import PurePosixPath

from dagster import Field, asset

from vlm_pipeline.lib.detection_common import (
    flush_image_labels,
    normalize_classes,
    resolve_spec_bbox_classes,
    resolve_target_classes,
    stable_image_label_id,
)
from vlm_pipeline.lib.env_utils import (
    SAM3_OUTPUTS,
    bool_env,
    dispatch_raw_key_prefix_folder,
    int_env,
    should_run_any_output,
)
from vlm_pipeline.lib.sam3 import get_sam3_client
from vlm_pipeline.lib.spec_config import parse_requested_outputs
from vlm_pipeline.lib.detection_coco import build_coco_detection_payload, convert_sam3_detections_for_coco
from vlm_pipeline.lib.key_builders import build_sam3_detection_key
from vlm_pipeline.lib.yolo_thresholds import resolve_active_class_confidence_thresholds
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@asset(
    name="sam3_image_detection",
    deps=["clip_to_frame"],
    description="SAM3.1 text-prompted bbox detection → vlm-labels JSON + image_labels INSERT (YOLO 비교용)",
    group_name="sam3",
    config_schema={
        "limit": Field(int, default_value=200),
        "score_threshold": Field(float, default_value=0.0),
        "max_masks_per_prompt": Field(int, default_value=50),
    },
)
def sam3_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """processed_clip_frame 이미지에 SAM3.1 bbox detection 실행."""
    return _run_sam3_image_detection(context, db, minio)


@asset(
    name="dispatch_sam3_image_detection",
    deps=["clip_to_frame", "raw_video_to_frame"],
    description="Dispatch 라인: frame 추출 완료 후 SAM3.1 text-prompted bbox detection (YOLO 비교용)",
    group_name="sam3",
    config_schema={
        "limit": Field(int, default_value=200),
        "score_threshold": Field(float, default_value=0.0),
        "max_masks_per_prompt": Field(int, default_value=50),
    },
)
def dispatch_sam3_image_detection(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """dispatch_stage_job용 SAM3.1 bbox detection 실행."""
    return _run_sam3_image_detection(context, db, minio)


def _run_sam3_image_detection(
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
    """processed_clip_frame/raw_video_frame 이미지에 SAM3.1 bbox detection 실행.

    SAM3 서버는 텍스트 프롬프트 기반 segmentation을 수행하고, mask에서 bbox를 추출한다.
    결과 JSON에는 mask_bbox, model_box, mask_rle 등이 포함되며,
    YOLO COCO bbox와 직접 비교 가능하다.
    """
    if not bool_env("ENABLE_SAM3_DETECTION", False):
        if not should_run_any_output(context, SAM3_OUTPUTS):
            context.log.info("sam3_image_detection 스킵: outputs에 SAM3 계열 요청이 없습니다.")
            return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True, "label_tool": "sam3"}

    limit = int(context.op_config.get("limit", 200))
    score_threshold = float(context.op_config.get("score_threshold", 0.0))
    max_masks_per_prompt = int(context.op_config.get("max_masks_per_prompt", 50))
    tags = context.run.tags if context.run else {}
    resolution = resolve_target_classes(tags, db)
    folder_name = folder_name_override
    if folder_name is None and not spec_id_override:
        folder_name = dispatch_raw_key_prefix_folder(tags)

    spec_id = spec_id_override if spec_id_override is not None else resolution.spec_id
    resolved_config_id = (
        resolved_config_id_override if resolved_config_id_override is not None else resolution.resolved_config_id
    )

    if target_classes_override is not None:
        target_classes = normalize_classes(target_classes_override)
        class_source = class_source_override or ("explicit_target_classes" if target_classes else "server_default")
    else:
        target_classes = list(resolution.classes)
        class_source = class_source_override or resolution.class_source

    candidates = db.find_sam3_pending_images(
        limit=limit,
        folder_name=folder_name,
        spec_id=spec_id,
    )
    if not candidates:
        context.log.info("SAM3 대상 이미지 없음")
        return {
            "processed": 0,
            "failed": 0,
            "total_detections": 0,
            "requested_classes": target_classes,
            "class_source": class_source,
            "resolved_config_id": resolved_config_id,
            "label_tool": "sam3",
        }

    client = get_sam3_client()

    if not client.wait_until_ready(max_wait_sec=120):
        context.log.error("SAM3 서버가 준비되지 않았습니다")
        return {
            "processed": 0, "failed": len(candidates), "total_detections": 0,
            "error": "sam3_server_not_ready", "label_tool": "sam3",
        }

    health = client.health()
    context.log.info(
        f"SAM3 서버 연결: url={client.api_url} "
        f"device={health.get('device')} "
        f"gpu_mem={health.get('gpu_memory_peak_gb', '?')}GB"
    )
    context.log.info(
        "SAM3 target classes resolved: "
        f"source={class_source} spec_id={spec_id or '-'} "
        f"classes={', '.join(target_classes) if target_classes else '(none — will skip)'}"
    )

    if not target_classes:
        context.log.warning("SAM3 스킵: 프롬프트 클래스가 비어 있습니다. SAM3는 텍스트 프롬프트가 필수입니다.")
        return {
            "processed": 0, "failed": 0, "total_detections": 0,
            "requested_classes": [], "class_source": class_source,
            "resolved_config_id": resolved_config_id,
            "skipped_reason": "no_prompt_classes", "label_tool": "sam3",
        }

    prompt_score_thresholds = resolve_active_class_confidence_thresholds(target_classes, score_threshold)

    total_candidates = len(candidates)
    context.log.info(f"sam3_image_detection 시작: 총 {total_candidates}건")

    processed = 0
    failed = 0
    total_detections = 0
    label_rows_buffer: list[dict] = []
    flush_threshold = int_env("SAM3_DB_FLUSH_THRESHOLD", 50, 10)

    minio.ensure_bucket("vlm-labels")

    for idx, cand in enumerate(candidates, start=1):
        image_id = str(cand["image_id"])
        image_key = str(cand["image_key"])
        source_clip_id = cand.get("source_clip_id")

        try:
            img_bytes = minio.download(
                str(cand.get("image_bucket") or "vlm-processed"),
                image_key,
            )
        except Exception as exc:
            failed += 1
            context.log.error(f"SAM3 이미지 다운로드 실패: image_id={image_id} key={image_key}: {exc}")
            continue

        try:
            sam_result = client.segment(
                img_bytes,
                prompts=target_classes,
                filename=PurePosixPath(image_key).name or "image.jpg",
                score_threshold=score_threshold,
                max_masks_per_prompt=max(1, max_masks_per_prompt),
                per_prompt_score_thresholds=prompt_score_thresholds,
            )
            sam_detections = list(sam_result.get("detections") or [])
            coco_detections = convert_sam3_detections_for_coco(sam_detections)

            detected_at = datetime.now()
            sam3_labels_key = build_sam3_detection_key(image_key)
            coco_payload = build_coco_detection_payload(
                image_id=image_id,
                source_clip_id=source_clip_id,
                image_key=image_key,
                image_width=cand.get("width"),
                image_height=cand.get("height"),
                detections=coco_detections,
                requested_classes=target_classes,
                class_source=class_source,
                resolved_config_id=resolved_config_id,
                confidence_threshold=score_threshold,
                iou_threshold=0.0,
                detected_at=detected_at,
                effective_request_confidence_threshold=score_threshold,
                class_confidence_thresholds=prompt_score_thresholds,
                elapsed_ms=sam_result.get("elapsed_ms"),
                model_name="sam3.1",
            )
            coco_payload["meta"]["sam3_prompt_score_thresholds"] = prompt_score_thresholds
            coco_payload["meta"]["sam3_total_latency_ms"] = float(sam_result.get("elapsed_ms") or 0.0)
            coco_payload["meta"]["sam3_per_prompt_latency_ms"] = sam_result.get("per_prompt_latency_ms") or {}
            coco_payload["meta"]["sam3_device"] = sam_result.get("device")
            coco_payload["meta"]["gpu_memory_peak_gb"] = sam_result.get("gpu_memory_peak_gb")
            annotation_count = len(coco_payload.get("annotations") or [])

            label_bytes = json.dumps(coco_payload, ensure_ascii=False).encode("utf-8")
            minio.upload("vlm-labels", sam3_labels_key, label_bytes, "application/json")

            label_rows_buffer.append({
                "image_label_id": stable_image_label_id(image_id, sam3_labels_key),
                "image_id": image_id,
                "source_clip_id": source_clip_id,
                "labels_bucket": "vlm-labels",
                "labels_key": sam3_labels_key,
                "label_format": "coco",
                "label_tool": "sam3",
                "label_source": "auto",
                "review_status": "auto_generated",
                "label_status": "completed",
                "object_count": annotation_count,
                "created_at": detected_at,
            })

            processed += 1
            total_detections += annotation_count

        except Exception as exc:
            failed += 1
            context.log.error(f"SAM3 추론/저장 실패: image_id={image_id}: {exc}")

        if idx % 20 == 0 or idx == total_candidates:
            context.log.info(
                f"sam3_image_detection 진행: [{idx}/{total_candidates}] "
                f"ok={processed} fail={failed} detections={total_detections}"
            )

        if len(label_rows_buffer) >= flush_threshold:
            flush_image_labels(db, label_rows_buffer, context, tool_name="SAM3")
            label_rows_buffer.clear()

    if label_rows_buffer:
        flush_image_labels(db, label_rows_buffer, context, tool_name="SAM3")
        label_rows_buffer.clear()

    summary = {
        "processed": processed,
        "failed": failed,
        "total_detections": total_detections,
        "requested_classes": target_classes,
        "class_source": class_source,
        "resolved_config_id": resolved_config_id,
        "label_tool": "sam3",
    }
    context.add_output_metadata(
        {
            "processed": processed,
            "failed": failed,
            "total_detections": total_detections,
            "requested_classes": ", ".join(target_classes) if target_classes else "(none)",
            "class_source": class_source,
            "resolved_config_id": resolved_config_id or "",
            "label_tool": "sam3",
        }
    )
    context.log.info(f"SAM3 완료: {summary}")
    return summary


@asset(
    name="sam3_bbox_labeling",
    deps=["clip_to_frame"],
    description="Legacy spec flow: requested_outputs에 bbox 포함 시 SAM3 bbox detection (YOLO bbox_labeling 병렬)",
    group_name="sam3",
    config_schema={"limit": Field(int, default_value=200)},
)
def sam3_bbox_labeling(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """run tag: spec_id, requested_outputs, resolved_config_id. bbox 요청 시 spec.classes × config.bbox.target_classes."""
    tags = context.run.tags if context.run else {}
    requested = parse_requested_outputs(tags)
    if "bbox" not in requested:
        context.log.info("sam3_bbox_labeling 스킵: requested_outputs에 bbox 없음")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True, "label_tool": "sam3"}

    spec_id = str(tags.get("spec_id") or "").strip()
    if not spec_id:
        context.log.info("sam3_bbox_labeling 스킵: spec_id 없음")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True, "label_tool": "sam3"}

    bbox_resolution = resolve_spec_bbox_classes(db, spec_id)
    target_classes = bbox_resolution.target_classes
    class_source = bbox_resolution.class_source
    resolved_config_id = bbox_resolution.resolved_config_id

    context.log.info(
        "sam3_bbox_labeling: "
        f"spec_id={spec_id} resolved_config_id={resolved_config_id} "
        f"target_classes={','.join(target_classes) if target_classes else '(none)'} "
        f"source={class_source}"
    )
    return _run_sam3_image_detection(
        context,
        db,
        minio,
        spec_id_override=spec_id,
        target_classes_override=target_classes,
        class_source_override=class_source,
        resolved_config_id_override=resolved_config_id,
    )

