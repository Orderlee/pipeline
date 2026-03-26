"""YOLO @asset — YOLO-World-L object detection (별도 Docker 서버 호출).

YOLO 서버: docker/yolo/ (GPU 1 전용, 모델 상주)
이 asset: HTTP로 detection 요청 → vlm-labels JSON 업로드 + image_labels INSERT

파이프라인 흐름:
  clip_to_frame → [yolo_detection_sensor] → yolo_image_detection
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4

from dagster import Field, asset

from vlm_pipeline.lib.env_utils import (
    YOLO_OUTPUTS,
    derive_classes_from_categories,
    dispatch_raw_key_prefix_folder,
    int_env,
    should_run_any_output,
)
from vlm_pipeline.lib.spec_config import load_persisted_spec_config, parse_requested_outputs
from vlm_pipeline.lib.yolo_world import get_yolo_client
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


@dataclass(frozen=True)
class YoloTargetClassResolution:
    classes: list[str]
    class_source: str
    spec_id: str | None = None
    resolved_config_id: str | None = None


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
    *,
    folder_name_override: str | None = None,
    spec_id_override: str | None = None,
    target_classes_override: list[str] | None = None,
    class_source_override: str | None = None,
    resolved_config_id_override: str | None = None,
) -> dict:
    """processed_clip_frame 이미지에 YOLO-World-L detection 실행."""
    if not should_run_any_output(context, YOLO_OUTPUTS):
        context.log.info("yolo_image_detection 스킵: outputs에 YOLO 계열 요청이 없습니다.")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True}

    limit = int(context.op_config.get("limit", 500))
    conf = float(context.op_config.get("confidence_threshold", 0.25))
    iou = float(context.op_config.get("iou_threshold", 0.45))
    batch_size = int(context.op_config.get("batch_size", 4))
    tags = context.run.tags if context.run else {}
    resolution = _resolve_yolo_target_classes(tags, db)
    folder_name = folder_name_override
    if folder_name is None and not spec_id_override:
        folder_name = dispatch_raw_key_prefix_folder(tags)

    spec_id = spec_id_override if spec_id_override is not None else resolution.spec_id
    resolved_config_id = (
        resolved_config_id_override if resolved_config_id_override is not None else resolution.resolved_config_id
    )

    if target_classes_override is not None:
        target_classes = _normalize_yolo_classes(target_classes_override)
        class_source = class_source_override or ("explicit_target_classes" if target_classes else "server_default")
    else:
        target_classes = list(resolution.classes)
        class_source = class_source_override or resolution.class_source

    candidates = db.find_yolo_pending_images(
        limit=limit,
        folder_name=folder_name,
        spec_id=spec_id,
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
                conf=conf,
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
                    "requested_classes": target_classes,
                    "requested_classes_count": len(target_classes),
                    "class_source": class_source,
                    "resolved_config_id": resolved_config_id,
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

    summary = {
        "processed": processed,
        "failed": failed,
        "total_detections": total_detections,
        "requested_classes": target_classes,
        "class_source": class_source,
        "resolved_config_id": resolved_config_id,
    }
    context.add_output_metadata(
        {
            "processed": processed,
            "failed": failed,
            "total_detections": total_detections,
            "requested_classes": ", ".join(target_classes) if target_classes else "(server default)",
            "class_source": class_source,
            "resolved_config_id": resolved_config_id or "",
        }
    )
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
    if not spec_id:
        context.log.info("bbox_labeling 스킵: spec_id 없음")
        return {"processed": 0, "failed": 0, "total_detections": 0, "skipped": True}

    config_bundle = load_persisted_spec_config(db, spec_id)
    resolved_config_id = str(config_bundle["resolved_config_id"] or "").strip() or None
    spec_classes = _normalize_yolo_classes(config_bundle["spec"].get("classes") or [])
    bbox_config = config_bundle["config_json"].get("bbox", {})
    config_target_classes = _normalize_yolo_classes(bbox_config.get("target_classes") or [])

    class_source = "server_default"
    target_classes: list[str] = []
    if config_target_classes:
        allowed = set(config_target_classes)
        target_classes = [value for value in spec_classes if value in allowed]
        class_source = "spec_config_intersection" if target_classes else "server_default"
    elif spec_classes:
        target_classes = list(spec_classes)
        class_source = "spec_classes"

    context.log.info(
        "bbox_labeling: "
        f"spec_id={spec_id} resolved_config_id={resolved_config_id} "
        f"target_classes={','.join(target_classes) if target_classes else '(server default)'} "
        f"source={class_source}"
    )
    return _run_yolo_image_detection(
        context,
        db,
        minio,
        spec_id_override=spec_id,
        target_classes_override=target_classes,
        class_source_override=class_source,
        resolved_config_id_override=resolved_config_id,
    )


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


def _parse_tag_list(raw_value: object) -> list[str]:
    if isinstance(raw_value, list):
        return _normalize_yolo_classes(raw_value)

    rendered = str(raw_value or "").strip()
    if not rendered:
        return []

    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            if isinstance(parsed, list):
                return _normalize_yolo_classes(parsed)
    except Exception:
        pass

    return _normalize_yolo_classes(rendered.split(","))


def _normalize_yolo_classes(values: Iterable[object] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or []:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _resolve_yolo_target_classes(
    tags: Mapping[str, str] | None,
    db: DuckDBResource,
) -> YoloTargetClassResolution:
    if not tags:
        return YoloTargetClassResolution(classes=[], class_source="server_default")

    spec_id = str(tags.get("spec_id") or "").strip()
    if spec_id:
        config_bundle = load_persisted_spec_config(db, spec_id)
        resolved_config_id = str(config_bundle["resolved_config_id"] or "").strip() or None
        spec_classes = _normalize_yolo_classes(config_bundle["spec"].get("classes") or [])
        bbox_config = config_bundle["config_json"].get("bbox", {})
        config_target_classes = _normalize_yolo_classes(bbox_config.get("target_classes") or [])

        if config_target_classes:
            allowed = set(config_target_classes)
            target_classes = [value for value in spec_classes if value in allowed]
            if target_classes:
                return YoloTargetClassResolution(
                    classes=target_classes,
                    class_source="spec_config_intersection",
                    spec_id=spec_id,
                    resolved_config_id=resolved_config_id,
                )
        if spec_classes:
            return YoloTargetClassResolution(
                classes=spec_classes,
                class_source="spec_classes",
                spec_id=spec_id,
                resolved_config_id=resolved_config_id,
            )
        return YoloTargetClassResolution(
            classes=[],
            class_source="server_default",
            spec_id=spec_id,
            resolved_config_id=resolved_config_id,
        )

    tag_classes = _parse_tag_list(tags.get("classes"))
    if tag_classes:
        return YoloTargetClassResolution(classes=tag_classes, class_source="dispatch_tags")

    categories = _parse_tag_list(tags.get("categories"))
    derived_classes = _normalize_yolo_classes(derive_classes_from_categories(categories))
    if derived_classes:
        return YoloTargetClassResolution(
            classes=derived_classes,
            class_source="dispatch_categories_derived",
        )

    return YoloTargetClassResolution(classes=[], class_source="server_default")
