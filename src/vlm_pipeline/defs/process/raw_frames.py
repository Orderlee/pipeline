"""raw_video_to_frame — dispatch YOLO 전용 raw video 프레임 추출 구현."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.env_utils import (
    dispatch_raw_key_prefix_folder,
    requested_outputs_require_raw_video_frames,
)
from vlm_pipeline.lib.spec_config import parse_requested_outputs
from vlm_pipeline.lib.video_frames import (
    describe_frame_bytes,
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
    resolve_frame_sampling_policy,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .helpers import (
    _cleanup_temp_path,
    _coerce_float,
    _delete_minio_keys,
    _materialize_video_path,
)


def raw_video_to_frame_impl(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """[Dispatch YOLO 전용] raw video에서 직접 이미지 추출 — @asset 래퍼에서 호출."""
    tags = context.run.tags if context.run else {}
    requested_outputs = parse_requested_outputs(tags)
    if not requested_outputs_require_raw_video_frames(requested_outputs):
        context.log.info("raw_video_to_frame 스킵: dispatch YOLO 전용 요청이 아닙니다.")
        return {"processed": 0, "failed": 0, "frames_extracted": 0, "skipped": True}

    folder_name = dispatch_raw_key_prefix_folder(tags)
    image_profile = tags.get("image_profile", "current")
    requested_outputs = requested_outputs or ["bbox"]
    limit = int(context.op_config.get("limit", 1000))

    jpeg_quality_tag = context.run.tags.get("jpeg_quality")
    jpeg_quality = int(jpeg_quality_tag) if jpeg_quality_tag else int(context.op_config.get("jpeg_quality", 90))

    candidates = db.find_raw_video_extract_pending(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("RAW VIDEO EXTRACT 대상 없음")
        return {"processed": 0, "failed": 0, "frames_extracted": 0}

    total_candidates = len(candidates)
    context.log.info(f"raw_video_to_frame 시작: 총 {total_candidates}건 처리 예정")

    processed = 0
    failed = 0
    total_frames = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        raw_bucket = cand.get("raw_bucket", "vlm-raw")
        raw_key = cand["raw_key"]
        archive_path = cand.get("archive_path")
        duration_sec = _coerce_float(cand.get("duration_sec"))
        fps = _coerce_float(cand.get("fps"))
        frame_count = int(cand.get("frame_count") or 0)
        temp_video_path: Path | None = None
        uploaded_frame_keys: list[str] = []

        if duration_sec is None or duration_sec <= 0:
            db.update_video_frame_extract_status(asset_id, "failed", error_message="invalid_duration")
            failed += 1
            continue

        try:
            db.update_video_frame_extract_status(asset_id, "processing")
            video_path, temp_video_path = _materialize_video_path(
                minio,
                {"archive_path": archive_path, "raw_bucket": raw_bucket, "raw_key": raw_key},
            )

            sampling = resolve_frame_sampling_policy(
                sampling_mode="raw_video",
                requested_outputs=requested_outputs,
                image_profile=image_profile,
                duration_sec=duration_sec,
                fps=fps,
                frame_count=frame_count,
            )
            context.log.info(
                "raw_video_to_frame sampling: asset=%s mode=%s outputs=%s image_profile=%s "
                "effective_max_frames=%d frame_interval_sec=%.3f source=%s",
                asset_id,
                sampling.sampling_mode,
                ",".join(requested_outputs) if requested_outputs else "-",
                image_profile,
                sampling.effective_max_frames,
                float(sampling.frame_interval_sec),
                sampling.policy_source,
            )

            frame_rows, uploaded_frame_keys = _extract_raw_video_frames(
                minio,
                asset_id=asset_id,
                video_path=video_path,
                raw_key=raw_key,
                duration_sec=duration_sec,
                fps=fps,
                frame_count=frame_count,
                max_frames=sampling.effective_max_frames,
                jpeg_quality=jpeg_quality,
                image_profile=image_profile,
                frame_interval_sec=sampling.frame_interval_sec,
            )

            db.replace_video_frame_metadata(asset_id, frame_rows, image_role="raw_video_frame")
            frames_count = len(frame_rows)
            db.update_video_frame_extract_status(
                asset_id,
                "completed",
                frame_count=frames_count,
                error_message=None,
                extracted_at=datetime.now(),
            )
            total_frames += frames_count
            processed += 1
            context.log.info(
                f"raw_video_to_frame 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} frames={frames_count} ✅"
            )

        except Exception as e:
            context.log.error(
                f"raw_video_to_frame 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ {e}"
            )
            db.update_video_frame_extract_status(
                asset_id,
                "failed",
                frame_count=0,
                error_message=str(e)[:500],
                extracted_at=datetime.now(),
            )
            db.replace_video_frame_metadata(asset_id, [], image_role="raw_video_frame")
            if uploaded_frame_keys:
                _delete_minio_keys(minio, "vlm-processed", uploaded_frame_keys)
            failed += 1
        finally:
            _cleanup_temp_path(temp_video_path)

    summary = {"processed": processed, "failed": failed, "frames_extracted": total_frames}
    context.add_output_metadata(summary)
    context.log.info(f"RAW VIDEO EXTRACT 완료: {summary}")
    return summary


def _extract_raw_video_frames(
    minio: MinIOResource,
    *,
    asset_id: str,
    video_path: Path,
    raw_key: str,
    duration_sec: float,
    fps: float | None,
    frame_count: int | None,
    max_frames: int,
    jpeg_quality: int,
    image_profile: str = "current",
    frame_interval_sec: float | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    now = datetime.now()
    timestamps = plan_frame_timestamps(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=max_frames,
        image_profile=image_profile,
        frame_interval_sec=frame_interval_sec,
    )

    frame_rows: list[dict[str, Any]] = []
    uploaded_keys: list[str] = []

    try:
        for frame_index, frame_sec in enumerate(timestamps, start=1):
            frame_bytes = extract_frame_jpeg_bytes(
                video_path, frame_sec, jpeg_quality=jpeg_quality,
            )
            image_key = _build_raw_video_image_key(raw_key, frame_index)
            minio.upload("vlm-processed", image_key, frame_bytes, "image/jpeg")
            uploaded_keys.append(image_key)

            frame_meta = describe_frame_bytes(frame_bytes)
            frame_rows.append({
                "image_id": str(uuid4()),
                "source_clip_id": None,
                "image_bucket": "vlm-processed",
                "image_key": image_key,
                "image_role": "raw_video_frame",
                "frame_index": frame_index,
                "frame_sec": float(frame_sec),
                "checksum": sha256_bytes(frame_bytes),
                "file_size": len(frame_bytes),
                "width": frame_meta["width"],
                "height": frame_meta["height"],
                "color_mode": frame_meta["color_mode"],
                "bit_depth": frame_meta["bit_depth"],
                "has_alpha": frame_meta["has_alpha"],
                "orientation": frame_meta["orientation"],
                "extracted_at": now,
            })
    except Exception:
        if uploaded_keys:
            _delete_minio_keys(minio, "vlm-processed", uploaded_keys)
        raise

    return frame_rows, uploaded_keys


def _build_raw_video_image_key(raw_key: str, frame_index: int) -> str:
    from vlm_pipeline.lib.key_builders import build_raw_video_image_key
    return build_raw_video_image_key(raw_key, frame_index)
