"""SAM3 segment → COCO JSON 업로드 → image_labels row 생성 공용 헬퍼.

detection_assets.sam3_image_detection 과 build/classification.build_classification 의
fallback 경로가 동일한 패턴으로 SAM3 호출 결과를 저장한다. 이 모듈이 그 공통 블록을 보유한다.

Layer 2: 순수 유틸 (Dagster 의존 없음). DB 쓰기는 호출자가 담당.

CLAUDE.md 의 L1-2 → L4 import 금지 규칙 준수를 위해 ``MinIOResource`` 는 ``TYPE_CHECKING``
가드 하에서만 import. 런타임엔 lib 가 resources 를 참조하지 않으며, 시그니처의 타입 힌트는
``from __future__ import annotations`` 덕에 문자열로만 평가된다.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from vlm_pipeline.lib.detection_coco import (
    build_coco_detection_payload,
    convert_sam3_detections_for_coco,
)
from vlm_pipeline.lib.detection_common import stable_image_label_id
from vlm_pipeline.lib.key_builders import build_pseudo_bbox_key, build_sam3_detection_key

if TYPE_CHECKING:
    from vlm_pipeline.resources.minio import MinIOResource

LABELS_BUCKET_DEFAULT = "vlm-labels"


def run_sam3_and_build_label_row(
    *,
    client,
    minio: MinIOResource,
    image_id: str,
    image_key: str,
    image_bytes: bytes,
    image_width: Any,
    image_height: Any,
    prompts: list[str],
    class_source: str,
    resolved_config_id: str | None = None,
    source_clip_id: str | None = None,
    score_threshold: float = 0.0,
    max_masks_per_prompt: int = 50,
    per_prompt_score_thresholds: dict[str, float] | None = None,
    detected_at: datetime | None = None,
    include_detailed_meta: bool = False,
    labels_bucket: str = LABELS_BUCKET_DEFAULT,
) -> tuple[dict, int]:
    """SAM3 segment → COCO payload 생성 → vlm-labels 업로드 → label_row dict 반환.

    Returns:
        (label_row, annotation_count) — label_row 는 image_labels 테이블 INSERT 용 dict.

    Raises:
        모든 예외는 호출자로 전파한다 (network, timeout, JSON 파싱 등).
        호출자가 per-image failure 카운팅을 담당.
    """
    detected_at = detected_at or datetime.now()

    sam_result = client.segment(
        image_bytes,
        prompts=prompts,
        filename=PurePosixPath(image_key).name or "image.jpg",
        score_threshold=score_threshold,
        max_masks_per_prompt=max(1, int(max_masks_per_prompt)),
        per_prompt_score_thresholds=per_prompt_score_thresholds,
    )
    sam_detections = list(sam_result.get("detections") or [])
    coco_detections = convert_sam3_detections_for_coco(sam_detections)

    coco_payload = build_coco_detection_payload(
        image_id=image_id,
        source_clip_id=source_clip_id,
        image_key=image_key,
        image_width=image_width,
        image_height=image_height,
        detections=coco_detections,
        requested_classes=prompts,
        class_source=class_source,
        resolved_config_id=resolved_config_id,
        confidence_threshold=score_threshold,
        iou_threshold=0.0,
        detected_at=detected_at,
        effective_request_confidence_threshold=score_threshold,
        class_confidence_thresholds=per_prompt_score_thresholds or {},
        elapsed_ms=sam_result.get("elapsed_ms"),
        model_name="sam3.1",
    )

    if include_detailed_meta:
        meta = coco_payload.setdefault("meta", {})
        meta["sam3_prompt_score_thresholds"] = per_prompt_score_thresholds or {}
        meta["sam3_total_latency_ms"] = float(sam_result.get("elapsed_ms") or 0.0)
        meta["sam3_per_prompt_latency_ms"] = sam_result.get("per_prompt_latency_ms") or {}
        meta["sam3_device"] = sam_result.get("device")
        meta["gpu_memory_peak_gb"] = sam_result.get("gpu_memory_peak_gb")

    annotation_count = len(coco_payload.get("annotations") or [])

    sam3_labels_key = build_sam3_detection_key(image_key)
    minio.upload_json(labels_bucket, sam3_labels_key, coco_payload)
    # pseudo-label QA 보존: 원본(모델) SAM3 COCO 를 별도 키에 1회 스냅샷 → LS 리뷰가
    # sam3_segmentations/ 를 사람수정본으로 in-place 덮어써도 원본이 남아 bbox 품질평가
    # (pseudo vs GT) 가 가능. best-effort: 실패해도 detection 은 성공. 최초 1회만.
    try:
        pseudo_key = build_pseudo_bbox_key(image_key)
        if not minio.exists(labels_bucket, pseudo_key):
            minio.upload_json(labels_bucket, pseudo_key, coco_payload)
    except Exception:  # noqa: BLE001
        pass

    label_row = {
        "image_label_id": stable_image_label_id(image_id, sam3_labels_key),
        "image_id": image_id,
        "source_clip_id": source_clip_id,
        "labels_bucket": labels_bucket,
        "labels_key": sam3_labels_key,
        "label_format": "coco",
        "label_tool": "sam3",
        "label_source": "auto",
        "review_status": "auto_generated",
        "label_status": "completed",
        "object_count": annotation_count,
        "created_at": detected_at,
    }
    return label_row, annotation_count
