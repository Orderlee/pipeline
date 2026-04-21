"""SAM3.1 @asset — SAM3 text-prompted bbox detection (별도 Docker 서버 호출).

SAM3 서버: docker/sam3/ (GPU 0 전용, 모델 상주).
이 asset: HTTP로 segment 요청 → mask에서 bbox 추출 → JSON 업로드 + image_labels INSERT.

SAM3는 텍스트 프롬프트로 segmentation 후 mask_bbox/model_box를 산출한다.
YOLO의 COCO bbox 결과와 1:1 비교가 목적이므로, 동일 이미지에 병렬 실행한다.

파이프라인 흐름:
  clip_to_frame → sam3_image_detection (YOLO와 병렬)
"""

from __future__ import annotations

from datetime import datetime

from dagster import Failure, Field, MetadataValue, asset

from vlm_pipeline.lib.detection_common import (
    flush_image_labels,
    normalize_classes,
    resolve_target_classes,
)
from vlm_pipeline.lib.env_utils import (
    SAM3_OUTPUTS,
    bool_env,
    dispatch_raw_key_prefix_folder,
    int_env,
    should_run_any_output,
)
from vlm_pipeline.lib.sam3 import get_sam3_client
from vlm_pipeline.lib.sam3_labeling import run_sam3_and_build_label_row
from vlm_pipeline.lib.spec_config import parse_requested_outputs
from vlm_pipeline.lib.yolo_thresholds import resolve_active_class_confidence_thresholds
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


_SAM3_CONFIG_SCHEMA = {
    "limit": Field(int, default_value=200),
    "score_threshold": Field(float, default_value=0.0),
    "max_masks_per_prompt": Field(int, default_value=50),
}


def _make_sam3_detection_asset(*, name: str, deps: list[str], description: str):
    @asset(
        name=name,
        deps=deps,
        description=description,
        group_name="sam3",
        config_schema=_SAM3_CONFIG_SCHEMA,
    )
    def _sam3_detection_asset(
        context,
        db: DuckDBResource,
        minio: MinIOResource,
    ) -> dict:
        return _run_sam3_image_detection(context, db, minio)

    return _sam3_detection_asset


sam3_image_detection = _make_sam3_detection_asset(
    name="sam3_image_detection",
    deps=["clip_to_frame"],
    description="SAM3.1 text-prompted bbox detection → vlm-labels JSON + image_labels INSERT (YOLO 비교용)",
)
dispatch_sam3_image_detection = _make_sam3_detection_asset(
    name="dispatch_sam3_image_detection",
    deps=["raw_video_to_frame"],
    description="Dispatch 라인: frame 추출 완료 후 SAM3.1 text-prompted bbox detection (YOLO 비교용)",
)


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

    SAM3가 primary bbox 엔진. 텍스트 프롬프트 기반 segmentation을 수행하고 mask에서 bbox를 추출한다.
    결과 JSON에는 mask_bbox, model_box, mask_rle 등이 포함되며, YOLO COCO bbox 포맷과 호환된다.
    env ENABLE_SAM3_DETECTION=false 여도 outputs에 bbox/segmentation 요청이 있으면 실행된다.
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
        context.log.error(
            f"SAM3 서버가 준비되지 않았습니다: url={client.api_url} pending={len(candidates)}건"
        )
        raise Failure(
            description="SAM3 서버 미준비 — primary bbox 엔진 연결 실패",
            metadata={
                "sam3_api_url": MetadataValue.text(client.api_url),
                "pending_candidates": MetadataValue.int(len(candidates)),
                "label_tool": MetadataValue.text("sam3"),
                "error": MetadataValue.text("sam3_server_not_ready"),
            },
        )

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
    completed_assets: set[str] = set()
    flush_threshold = int_env("SAM3_DB_FLUSH_THRESHOLD", 50, 10)

    minio.ensure_bucket("vlm-labels")

    for idx, cand in enumerate(candidates, start=1):
        image_id = str(cand["image_id"])
        image_key = str(cand["image_key"])
        source_clip_id = cand.get("source_clip_id")
        source_asset_id = cand.get("source_asset_id")

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
            label_row, annotation_count = run_sam3_and_build_label_row(
                client=client,
                minio=minio,
                image_id=image_id,
                image_key=image_key,
                image_bytes=img_bytes,
                image_width=cand.get("width"),
                image_height=cand.get("height"),
                prompts=target_classes,
                class_source=class_source,
                resolved_config_id=resolved_config_id,
                source_clip_id=source_clip_id,
                score_threshold=score_threshold,
                max_masks_per_prompt=max_masks_per_prompt,
                per_prompt_score_thresholds=prompt_score_thresholds,
                include_detailed_meta=True,
            )
            label_rows_buffer.append(label_row)
            processed += 1
            total_detections += annotation_count
            if source_asset_id:
                completed_assets.add(str(source_asset_id))

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

    if completed_assets:
        bbox_completed_at = datetime.now()
        for asset_id in completed_assets:
            try:
                db.update_bbox_status(asset_id, "completed", completed_at=bbox_completed_at)
            except Exception as exc:
                context.log.warning(f"bbox_status 전이 실패: asset_id={asset_id}: {exc}")
        context.log.info(f"video_metadata.bbox_status=completed 전이: {len(completed_assets)}건")

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


