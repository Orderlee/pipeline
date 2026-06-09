"""SAM3 in-line fallback — image_metadata 없는 video 대상 프레임 추출 + SAM3 detection.

Layer 3: ops/helpers (classification.py 에서 import).
"""

from __future__ import annotations

from datetime import datetime

from vlm_pipeline.defs.process.helpers import _materialize_video_path
from vlm_pipeline.defs.process.raw_frames import extract_raw_video_frames
from vlm_pipeline.lib.detection_common import parse_tag_list
from vlm_pipeline.lib.file_loader import cleanup_temp_path
from vlm_pipeline.lib.sam3 import get_sam3_client
from vlm_pipeline.lib.sam3_labeling import run_sam3_and_build_label_row
from vlm_pipeline.resources.minio import MinIOResource
from vlm_pipeline.resources.postgres import PostgresResource

LABELS_BUCKET = "vlm-labels"
PROCESSED_BUCKET = "vlm-processed"


def _fallback_classify_video(
    context,
    db: PostgresResource,
    minio: MinIOResource,
    video: dict,
    dispatch_row: dict | None,
    folder_prefix: str,
) -> list[dict]:
    """image_metadata 가 없는 video 에 대해 프레임 추출 + SAM3 호출 → image 후보 반환."""
    log = context.log
    asset_id = video["asset_id"]
    raw_bucket = video.get("raw_bucket") or "vlm-raw"
    raw_key = video["raw_key"]

    prompt_classes = parse_tag_list((dispatch_row or {}).get("categories"))
    if not prompt_classes:
        log.warning(f"[fallback] dispatch.categories 비어있음 — asset={asset_id} skip")
        return []

    duration_sec = float(video.get("duration_sec") or 0.0)
    if duration_sec <= 0:
        log.warning(f"[fallback] invalid duration — asset={asset_id} skip")
        return []

    fps_raw = video.get("fps")
    fps = float(fps_raw) if fps_raw else None
    frame_count = int(video.get("frame_count") or 0)

    client = get_sam3_client()
    if not client.is_ready():
        if not client.wait_until_ready(max_wait_sec=60):
            log.error(f"[fallback] SAM3 서버 미준비 — asset={asset_id} skip")
            return []

    temp_video_path = None
    uploaded_frame_keys: list[str] = []
    try:
        video_path, temp_video_path = _materialize_video_path(
            minio,
            {"raw_bucket": raw_bucket, "raw_key": raw_key},
        )

        frame_rows, uploaded_frame_keys = extract_raw_video_frames(
            minio,
            asset_id=asset_id,
            video_path=video_path,
            raw_key=raw_key,
            duration_sec=duration_sec,
            fps=fps,
            frame_count=frame_count,
            max_frames=16,
            jpeg_quality=90,
            image_profile="current",
            frame_interval_sec=None,
        )
        db.replace_video_frame_metadata(
            asset_id,
            frame_rows,
            image_role="raw_video_frame",
        )

        candidates: list[dict] = []
        label_rows: list[dict] = []
        detected_at = datetime.now()

        for row in frame_rows:
            image_id = row["image_id"]
            image_key = row["image_key"]
            try:
                img_bytes = minio.download(PROCESSED_BUCKET, image_key)
            except Exception as exc:
                log.error(f"[fallback] image download 실패: {image_key}: {exc}")
                continue

            try:
                label_row, _ann_count = run_sam3_and_build_label_row(
                    client=client,
                    minio=minio,
                    image_id=image_id,
                    image_key=image_key,
                    image_bytes=img_bytes,
                    image_width=row.get("width"),
                    image_height=row.get("height"),
                    prompts=prompt_classes,
                    class_source="classification_fallback",
                    detected_at=detected_at,
                    labels_bucket=LABELS_BUCKET,
                )
            except Exception as exc:
                log.error(f"[fallback] SAM3 segment 실패: {image_key}: {exc}")
                continue

            label_rows.append(label_row)
            candidates.append(
                {
                    "image_id": image_id,
                    "image_bucket": PROCESSED_BUCKET,
                    "image_key": image_key,
                    "source_asset_id": asset_id,
                    "labels_key_list": [label_row["labels_key"]],
                }
            )

        if label_rows:
            db.batch_insert_image_labels(label_rows)

        log.info(f"[fallback] asset={asset_id} frames={len(frame_rows)} image_candidates={len(candidates)}")
        return candidates

    except Exception as exc:
        log.error(f"[fallback] 전체 실패: asset={asset_id}: {exc}")
        return []
    finally:
        cleanup_temp_path(temp_video_path)
