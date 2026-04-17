"""clip_to_frame MVP 흐름 — outputs=[captioning]만 처리, frame_status 미사용."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path

from PIL import Image

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.env_utils import (
    dispatch_raw_key_prefix_folder,
    should_run_output,
)
from vlm_pipeline.lib.file_loader import cleanup_temp_path
from vlm_pipeline.lib.video_frames import resolve_frame_sampling_policy
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .frame_extract_common import (
    _build_initial_clip_row,
    _cleanup_failed_clip,
    _parse_candidate,
)
from .helpers_clip_media import _extract_video_clip_media
from .helpers_frame_caption import _extract_clip_frames
from .helpers_key_utils import (
    _build_processed_clip_key,
    _materialize_video_path,
    _stable_clip_id,
)
from .clip_windows import (
    candidate_clip_window_plan_key,
    plan_asset_event_clip_extraction_windows,
    sort_process_candidates,
)


def clip_to_frame_mvp(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST+LABEL 완료 → clip + 프레임 추출 (MVP, frame_status 미사용)."""
    if not should_run_output(context, "captioning"):
        context.log.info("clip_to_frame 스킵: outputs에 captioning이 없습니다.")
        return {"processed": 0, "failed": 0, "frames_extracted": 0, "skipped": True}

    db.ensure_runtime_schema()
    folder_name = dispatch_raw_key_prefix_folder(context.run.tags if context.run else None)
    image_profile = context.run.tags.get("image_profile", "current")
    requested_outputs = ["captioning"]

    candidates = db.find_processable(folder_name=folder_name)
    if not candidates:
        context.log.info("PROCESS 대상 없음")
        return {"processed": 0, "failed": 0, "frames_extracted": 0}

    limit = int(context.op_config.get("limit", 1000))
    jpeg_quality = int(context.op_config.get("jpeg_quality", 90))
    candidates = sort_process_candidates(candidates[:limit])
    clip_window_plans = plan_asset_event_clip_extraction_windows(candidates)
    total_candidates = len(candidates)
    context.log.info(f"clip_to_frame 시작: 총 {total_candidates}건 처리 예정")

    processed = 0
    failed = 0
    total_frames = 0

    for idx, cand in enumerate(candidates, start=1):
        cf = _parse_candidate(cand)
        temp_clip_path: Path | None = None
        temp_video_path: Path | None = None
        video_path: Path | None = None
        clip_id: str | None = None
        clip_key: str | None = None
        uploaded_clip_key: str | None = None
        uploaded_frame_keys: list[str] = []
        uploaded_caption_keys: list[str] = []

        try:
            clip_key = _build_processed_clip_key(
                cf.raw_key,
                event_index=cf.event_index,
                clip_start_sec=cf.event_start_sec,
                clip_end_sec=cf.event_end_sec,
                media_type=cf.media_type,
            )
            clip_id = _stable_clip_id(cf.label_id, cf.event_index, cf.event_start_sec, cf.event_end_sec, clip_key)
            width, height, codec = None, None, None
            file_bytes = None
            clip_duration = None
            clip_fps = None
            clip_frame_count = None

            db.insert_processed_clip(_build_initial_clip_row(cf, clip_id, clip_key))

            if cf.media_type == "image":
                file_bytes = minio.download(cf.raw_bucket, cf.raw_key)
                with Image.open(BytesIO(file_bytes)) as img:
                    width, height = img.size
                    codec = (img.format or "jpeg").lower()
                minio.upload("vlm-processed", clip_key, file_bytes, f"image/{codec}")
                uploaded_clip_key = clip_key

            elif cf.event_start_sec is not None and cf.event_end_sec is not None and cf.event_end_sec > cf.event_start_sec:
                # 이벤트 시작이 영상 길이를 넘어서면 추출 불가 → 해당 clip만 failed로 마크.
                if (
                    cf.source_video_duration_sec is not None
                    and cf.source_video_duration_sec > 0
                    and float(cf.event_start_sec) >= float(cf.source_video_duration_sec)
                ):
                    raise RuntimeError(
                        "event_start_beyond_video_duration:"
                        f"event_start={float(cf.event_start_sec):.3f}:"
                        f"duration={float(cf.source_video_duration_sec):.3f}"
                    )
                window_plan = clip_window_plans.get(candidate_clip_window_plan_key(cand), {})
                if not window_plan or "extract_start_sec" not in window_plan or "extract_end_sec" not in window_plan:
                    # 플랜이 skip된 candidate (event 전체가 duration 초과 등) → fail-forward
                    raise RuntimeError(
                        "clip_window_plan_missing:event_beyond_video_duration:"
                        f"event_start={float(cf.event_start_sec):.3f}:"
                        f"event_end={float(cf.event_end_sec):.3f}:"
                        f"duration={cf.source_video_duration_sec}"
                    )
                video_path, temp_video_path = _materialize_video_path(
                    minio,
                    {"archive_path": cf.archive_path, "raw_bucket": cf.raw_bucket, "raw_key": cf.raw_key},
                )
                extracted_clip = _extract_video_clip_media(
                    context,
                    db,
                    minio,
                    asset_id=cf.asset_id,
                    clip_id=clip_id,
                    clip_key=clip_key,
                    video_path=video_path,
                    event_start_sec=float(cf.event_start_sec),
                    event_end_sec=float(cf.event_end_sec),
                    source_duration_sec=cf.source_video_duration_sec,
                    extract_start_sec=float(window_plan["extract_start_sec"]),
                    extract_end_sec=float(window_plan["extract_end_sec"]),
                    window_strategy=str(window_plan.get("window_strategy") or "buffered"),
                    video_width=cand.get("video_width"),
                    video_height=cand.get("video_height"),
                    video_codec=cand.get("video_codec"),
                )
                temp_clip_path = extracted_clip["temp_clip_path"]
                file_bytes = extracted_clip["file_bytes"]
                uploaded_clip_key = extracted_clip["uploaded_clip_key"]
                clip_duration = extracted_clip["clip_duration"]
                clip_fps = extracted_clip["clip_fps"]
                clip_frame_count = extracted_clip["clip_frame_count"]
                width = extracted_clip["width"]
                height = extracted_clip["height"]
                codec = extracted_clip["codec"]
            else:
                raise RuntimeError("video_clip_range_missing")

            checksum = extracted_clip["checksum"] if cf.media_type == "video" else (sha256_bytes(file_bytes) if file_bytes else None)
            file_size = extracted_clip["file_size"] if cf.media_type == "video" else (len(file_bytes) if file_bytes else None)

            updated_row = _build_initial_clip_row(cf, clip_id, clip_key)
            updated_row.update(checksum=checksum, file_size=file_size, width=width, height=height,
                               codec=codec, duration_sec=clip_duration, fps=clip_fps, frame_count=clip_frame_count)
            db.insert_processed_clip(updated_row)

            frames_count = 0
            if cf.media_type == "video":
                if temp_clip_path is None or clip_duration is None or clip_duration <= 0:
                    raise RuntimeError("clip_meta_missing_or_invalid")
                sampling = resolve_frame_sampling_policy(
                    sampling_mode="clip_event",
                    requested_outputs=requested_outputs,
                    image_profile=image_profile,
                    duration_sec=clip_duration,
                    fps=clip_fps,
                    frame_count=clip_frame_count,
                )
                context.log.info(
                    "clip_to_frame sampling: asset=%s mode=%s outputs=%s image_profile=%s "
                    "effective_max_frames=%d frame_interval_sec=%.3f source=%s",
                    cf.asset_id,
                    sampling.sampling_mode,
                    ",".join(requested_outputs),
                    image_profile,
                    sampling.effective_max_frames,
                    float(sampling.frame_interval_sec),
                    sampling.policy_source,
                )
                db.update_clip_image_extract_status(clip_id, "processing")
                frame_rows, uploaded_frame_keys, uploaded_caption_keys = _extract_clip_frames(
                    minio,
                    clip_id=clip_id,
                    source_asset_id=cf.asset_id,
                    clip_path=temp_clip_path,
                    clip_key=clip_key,
                    duration_sec=clip_duration,
                    fps=clip_fps,
                    frame_count=clip_frame_count,
                    max_frames=sampling.effective_max_frames,
                    jpeg_quality=jpeg_quality,
                    image_profile=image_profile,
                    frame_interval_sec=sampling.frame_interval_sec,
                )
                db.replace_processed_clip_frame_metadata(cf.asset_id, clip_id, frame_rows)
                frames_count = len(frame_rows)
                db.update_clip_image_extract_status(
                    clip_id, "completed", count=frames_count, error=None, extracted_at=datetime.now(),
                )
                total_frames += frames_count
            else:
                db.update_clip_image_extract_status(
                    clip_id, "completed", count=0, error=None, extracted_at=datetime.now(),
                )

            db.update_processed_clip_status(clip_id, "completed")
            processed += 1
            context.log.info(
                f"clip_to_frame 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={cf.asset_id} frames={frames_count} ✅"
            )

        except Exception as e:
            context.log.error(
                f"clip_to_frame 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={cf.asset_id} ❌ {e}"
            )
            _cleanup_failed_clip(
                db, minio, clip_id=clip_id, asset_id=cf.asset_id, error_message=str(e)[:500],
                uploaded_clip_key=uploaded_clip_key, uploaded_frame_keys=uploaded_frame_keys,
                uploaded_caption_keys=uploaded_caption_keys,
            )
            failed += 1
        finally:
            cleanup_temp_path(temp_clip_path)
            cleanup_temp_path(temp_video_path)

    summary = {"processed": processed, "failed": failed, "frames_extracted": total_frames}
    context.add_output_metadata(summary)
    context.log.info(f"PROCESS 완료: {summary}")
    return summary
