"""clip_to_frame spec/dispatch 흐름 — frame_status·선택적 프레임 이미지 캡션."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image

from vlm_pipeline.defs.spec.config_resolver import load_persisted_spec_config
from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.env_utils import (
    dispatch_raw_key_prefix_folder,
    requested_outputs_require_caption_labels,
    requested_outputs_require_frame_image_caption,
    requested_outputs_require_raw_video_frames,
)
from vlm_pipeline.lib.file_loader import cleanup_temp_path
from vlm_pipeline.lib.gemini import GeminiAnalyzer
from vlm_pipeline.lib.spec_config import (
    is_standard_spec_run,
    parse_requested_outputs,
)
from vlm_pipeline.lib.video_frames import resolve_frame_sampling_policy
from vlm_pipeline.resources.postgres import PostgresResource
from vlm_pipeline.resources.minio import MinIOResource

from .clip_windows import (
    candidate_clip_window_plan_key,
    plan_asset_event_clip_extraction_windows,
    sort_process_candidates,
)
from .frame_extract_common import (
    _build_initial_clip_row,
    _cleanup_failed_clip,
    _parse_candidate,
)
from .helpers_clip_media import _extract_video_clip_media
from .helpers_frame_caption import (
    _MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET,
    _extract_clip_frames,
    _reset_empty_output_failure,
    _track_empty_output_failure,
)
from .helpers_key_utils import (
    _build_processed_clip_key,
    _coerce_int,
    _materialize_video_path,
    _stable_clip_id,
)
from .helpers_metadata import _load_gemini_label_event


@dataclass
class _RoutingDecision:
    skip: bool = False
    skip_reason: str = ""
    spec_id: str = ""
    resolved_config_id: str | None = None
    spec_max_frames_per_video: int | None = None
    folder_name: str | None = None
    image_profile: str = "current"
    limit: int = 1000
    jpeg_quality: int = 90
    candidates: list[dict] = field(default_factory=list)
    enable_image_captioning: bool = False
    image_caption_analyzer: Any = None
    clip_window_plans: dict = field(default_factory=dict)
    requested: list[str] = field(default_factory=list)


def _decide_frame_extract_routing(
    context,
    db: PostgresResource,
    minio: MinIOResource,
    requested: list[str],
    standard_spec_run: bool,
) -> _RoutingDecision:
    """tags 파싱 + skip 조건 + spec config 로드 + 후보 조회."""
    if not standard_spec_run and requested_outputs_require_raw_video_frames(requested):
        context.log.info("clip_to_frame 스킵: dispatch labeling_method가 YOLO 전용입니다.")
        return _RoutingDecision(skip=True, skip_reason="yolo_only")

    should_run = (
        bool({"bbox"} & set(requested)) or requested_outputs_require_caption_labels(requested) or standard_spec_run
    )
    if not should_run:
        context.log.info("clip_to_frame 스킵: outputs에 captioning/bbox 없음")
        return _RoutingDecision(skip=True, skip_reason="no_captioning_or_bbox")

    tags = context.run.tags if context.run else {}
    db.ensure_runtime_schema()
    spec_id = str(tags.get("spec_id") or "").strip()
    resolved_config_id = None
    spec_max_frames_per_video: int | None = None
    if spec_id:
        config_bundle = load_persisted_spec_config(db, spec_id)
        resolved_config_id = config_bundle["resolved_config_id"]
        frame_config = config_bundle["config_json"].get("frame_extraction", {})
        spec_max_frames_per_video = _coerce_int(frame_config.get("max_frames_per_video"))
        context.log.info(
            "clip_to_frame: spec_id=%s resolved_config_id=%s frame_keys=%s spec_max_frames_per_video=%s",
            spec_id,
            resolved_config_id,
            sorted(frame_config.keys()),
            spec_max_frames_per_video,
        )

    folder_name = dispatch_raw_key_prefix_folder(tags)
    image_profile = tags.get("image_profile", "current")
    limit = int(context.op_config.get("limit", 1000))
    jpeg_quality = int(tags.get("jpeg_quality") or 90)

    candidates = db.find_processable(folder_name=folder_name, spec_id=spec_id or None)
    if not candidates:
        context.log.info("clip_to_frame: 대상 없음")
        return _RoutingDecision(
            skip=False,
            spec_id=spec_id,
            resolved_config_id=resolved_config_id,
            candidates=[],
        )

    candidates = sort_process_candidates(candidates[:limit])
    clip_window_plans = plan_asset_event_clip_extraction_windows(candidates)
    enable_image_captioning = requested_outputs_require_frame_image_caption(requested) and any(
        str(candidate.get("media_type") or "").strip().lower() == "video" for candidate in candidates
    )
    image_caption_analyzer = GeminiAnalyzer() if enable_image_captioning else None

    return _RoutingDecision(
        skip=False,
        spec_id=spec_id,
        resolved_config_id=resolved_config_id,
        spec_max_frames_per_video=spec_max_frames_per_video,
        folder_name=folder_name,
        image_profile=image_profile,
        limit=limit,
        jpeg_quality=jpeg_quality,
        candidates=candidates,
        enable_image_captioning=enable_image_captioning,
        image_caption_analyzer=image_caption_analyzer,
        clip_window_plans=clip_window_plans,
        requested=requested,
    )


@dataclass
class _ClipExtractionResult:
    frames_count: int = 0
    image_captions_count: int = 0
    error: str | None = None
    temp_clip_path: Path | None = None
    temp_video_path: Path | None = None


def _extract_clip_frames_with_db(
    context,
    db: PostgresResource,
    minio: MinIOResource,
    cand: dict,
    routing: _RoutingDecision,
    idx: int,
    total_candidates: int,
    label_events_cache: dict[str, list[dict[str, Any]]],
) -> _ClipExtractionResult:
    """DB+MinIO 기반 단일 clip 미디어 추출 + 프레임 추출 + DB write."""
    cf = _parse_candidate(cand)
    result = _ClipExtractionResult()
    clip_id: str | None = None
    clip_key: str | None = None
    uploaded_clip_key: str | None = None
    uploaded_frame_keys: list[str] = []
    uploaded_caption_keys: list[str] = []

    try:
        context.log.info(
            "clip_to_frame start: [%d/%d] asset=%s media_type=%s label_id=%s event_index=%s",
            idx,
            total_candidates,
            cf.asset_id,
            cf.media_type,
            cf.label_id,
            cf.event_index,
        )
        event_context: dict[str, Any] = {}
        if routing.enable_image_captioning:
            try:
                event_context = _load_gemini_label_event(
                    minio,
                    cf.labels_key,
                    cf.event_index,
                    cache=label_events_cache,
                )
            except Exception as exc:
                context.log.warning(
                    "clip_to_frame: event context load 실패 asset=%s label_key=%s err=%s",
                    cf.asset_id,
                    cf.labels_key,
                    exc,
                )
        event_category = str(event_context.get("category") or "").strip() or None
        event_caption = (
            cf.caption_text
            or str(event_context.get("ko_caption") or "").strip()
            or str(event_context.get("en_caption") or "").strip()
            or None
        )

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
        elif (
            cf.event_start_sec is not None
            and cf.event_end_sec is not None
            and cf.event_end_sec > cf.event_start_sec
        ):
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
            window_plan = routing.clip_window_plans.get(candidate_clip_window_plan_key(cand), {})
            if not window_plan or "extract_start_sec" not in window_plan or "extract_end_sec" not in window_plan:
                raise RuntimeError(
                    "clip_window_plan_missing:event_beyond_video_duration:"
                    f"event_start={float(cf.event_start_sec):.3f}:"
                    f"event_end={float(cf.event_end_sec):.3f}:"
                    f"duration={cf.source_video_duration_sec}"
                )
            video_path, result.temp_video_path = _materialize_video_path(
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
            result.temp_clip_path = extracted_clip["temp_clip_path"]
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

        checksum = (
            extracted_clip["checksum"]
            if cf.media_type == "video"
            else (sha256_bytes(file_bytes) if file_bytes else None)
        )
        file_size = (
            extracted_clip["file_size"] if cf.media_type == "video" else (len(file_bytes) if file_bytes else None)
        )
        updated_row = _build_initial_clip_row(cf, clip_id, clip_key)
        updated_row.update(
            checksum=checksum,
            file_size=file_size,
            width=width,
            height=height,
            codec=codec,
            duration_sec=clip_duration,
            fps=clip_fps,
            frame_count=clip_frame_count,
        )
        db.insert_processed_clip(updated_row)

        frames_count, image_captions_count = _trigger_caption_for_frames(
            context,
            db,
            minio,
            cf=cf,
            clip_id=clip_id,
            clip_key=clip_key,
            temp_clip_path=result.temp_clip_path,
            clip_duration=clip_duration,
            clip_fps=clip_fps,
            clip_frame_count=clip_frame_count,
            routing=routing,
            event_category=event_category,
            event_caption=event_caption,
            tags=context.run.tags if context.run else {},
            uploaded_frame_keys_out=uploaded_frame_keys,
            uploaded_caption_keys_out=uploaded_caption_keys,
        )

        db.update_processed_clip_status(clip_id, "completed")
        result.frames_count = frames_count
        result.image_captions_count = image_captions_count
        context.log.info(
            "clip_to_frame 진행: [%d/%d] asset=%s frames=%d captions=%d ✅",
            idx,
            total_candidates,
            cf.asset_id,
            frames_count,
            image_captions_count,
        )

    except Exception as exc:
        result.error = str(exc)[:500]
        context.log.error(
            "clip_to_frame 진행: [%d/%d] asset=%s ❌ %s",
            idx,
            total_candidates,
            cf.asset_id,
            exc,
        )
        _cleanup_failed_clip(
            db,
            minio,
            clip_id=clip_id,
            asset_id=cf.asset_id,
            error_message=result.error,
            uploaded_clip_key=uploaded_clip_key,
            uploaded_frame_keys=uploaded_frame_keys,
            uploaded_caption_keys=uploaded_caption_keys,
        )

    return result


def _trigger_caption_for_frames(
    context,
    db: PostgresResource,
    minio: MinIOResource,
    *,
    cf,
    clip_id: str,
    clip_key: str,
    temp_clip_path: Path | None,
    clip_duration: float | None,
    clip_fps: float | None,
    clip_frame_count: int | None,
    routing: _RoutingDecision,
    event_category: str | None,
    event_caption: str | None,
    tags: dict,
    uploaded_frame_keys_out: list[str],
    uploaded_caption_keys_out: list[str],
) -> tuple[int, int]:
    """caption 호출 + image_metadata 저장. (frames_count, image_captions_count) 반환."""
    if cf.media_type == "video":
        if temp_clip_path is None or clip_duration is None or clip_duration <= 0:
            raise RuntimeError("clip_meta_missing_or_invalid")
        sampling = resolve_frame_sampling_policy(
            sampling_mode="clip_event",
            requested_outputs=routing.requested,
            image_profile=routing.image_profile,
            duration_sec=clip_duration,
            fps=clip_fps,
            frame_count=clip_frame_count,
            spec_max_frames_per_video=routing.spec_max_frames_per_video,
        )
        context.log.info(
            "clip_to_frame sampling: asset=%s mode=%s outputs=%s image_profile=%s "
            "effective_max_frames=%d frame_interval_sec=%.3f source=%s",
            cf.asset_id,
            sampling.sampling_mode,
            ",".join(routing.requested) if routing.requested else "-",
            routing.image_profile,
            sampling.effective_max_frames,
            float(sampling.frame_interval_sec),
            sampling.policy_source,
        )
        db.update_clip_image_extract_status(clip_id, "processing")
        frame_rows, frame_keys, caption_keys = _extract_clip_frames(
            minio,
            clip_id=clip_id,
            source_asset_id=cf.asset_id,
            clip_path=temp_clip_path,
            clip_key=clip_key,
            duration_sec=clip_duration,
            fps=clip_fps,
            frame_count=clip_frame_count,
            max_frames=sampling.effective_max_frames,
            jpeg_quality=routing.jpeg_quality,
            image_profile=routing.image_profile,
            frame_interval_sec=sampling.frame_interval_sec,
            image_caption_analyzer=routing.image_caption_analyzer,
            image_caption_event_category=event_category,
            image_caption_event_caption_text=event_caption,
            image_caption_parent_label_key=cf.labels_key,
            store_image_caption_json=bool(tags.get("request_id")) and routing.enable_image_captioning,
            image_caption_log=context.log if routing.enable_image_captioning else None,
            progress_log=context.log,
        )
        uploaded_frame_keys_out.extend(frame_keys)
        uploaded_caption_keys_out.extend(caption_keys)
        db.replace_processed_clip_frame_metadata(cf.asset_id, clip_id, frame_rows)
        frames_count = len(frame_rows)
        image_captions_count = sum(1 for row in frame_rows if row.get("image_caption_text"))
        db.update_clip_image_extract_status(
            clip_id,
            "completed",
            count=frames_count,
            error=None,
            extracted_at=datetime.now(),
        )
        return frames_count, image_captions_count
    else:
        db.update_clip_image_extract_status(
            clip_id,
            "completed",
            count=0,
            error=None,
            extracted_at=datetime.now(),
        )
        return 0, 0


def clip_to_frame_routed_impl(
    context,
    db: PostgresResource,
    minio: MinIOResource,
) -> dict:
    """spec/dispatch: frame_status·선택적 프레임 이미지 캡션."""
    tags = context.run.tags if context.run else {}
    requested = parse_requested_outputs(tags)
    standard_spec_run = is_standard_spec_run(tags)

    routing = _decide_frame_extract_routing(context, db, minio, requested, standard_spec_run)

    if routing.skip:
        return {"processed": 0, "failed": 0, "frames_extracted": 0, "skipped": True}

    if not routing.candidates:
        return {
            "processed": 0,
            "failed": 0,
            "frames_extracted": 0,
            "image_captions": 0,
            "resolved_config_id": routing.resolved_config_id,
        }

    total_candidates = len(routing.candidates)
    label_events_cache: dict[str, list[dict[str, Any]]] = {}
    processed = 0
    failed = 0
    total_frames = 0
    image_captions = 0
    asset_errors: dict[str, str] = {}
    consecutive_empty_output_failures: dict[str, int] = {}
    suppressed_asset_ids: set[str] = set()

    asset_ids_in_order: list[str] = []
    seen_asset_ids: set[str] = set()
    for candidate in routing.candidates:
        aid = str(candidate.get("asset_id") or "").strip()
        if aid and aid not in seen_asset_ids:
            seen_asset_ids.add(aid)
            asset_ids_in_order.append(aid)

    for aid in asset_ids_in_order:
        db.update_frame_status(aid, "processing")

    context.log.info("clip_to_frame 시작: 총 %d건 처리 예정", total_candidates)
    for idx, cand in enumerate(routing.candidates, start=1):
        cf = _parse_candidate(cand)
        if cf.asset_id in suppressed_asset_ids:
            failed += 1
            asset_errors.setdefault(
                cf.asset_id,
                "ffmpeg_frame_extract_failed:empty_output (repeated>=3)",
            )
            context.log.warning(
                "clip_to_frame skip: [%d/%d] asset=%s repeated_empty_output>=%d",
                idx,
                total_candidates,
                cf.asset_id,
                _MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET,
            )
            continue

        result = _extract_clip_frames_with_db(
            context,
            db,
            minio,
            cand=cand,
            routing=routing,
            idx=idx,
            total_candidates=total_candidates,
            label_events_cache=label_events_cache,
        )

        try:
            if result.error:
                failed += 1
                asset_errors[cf.asset_id] = result.error
                suppress_after_failure = _track_empty_output_failure(
                    cf.asset_id,
                    result.error,
                    consecutive_failures=consecutive_empty_output_failures,
                    suppressed_asset_ids=suppressed_asset_ids,
                )
                if suppress_after_failure:
                    context.log.warning(
                        "clip_to_frame repeated empty_output 억제: asset=%s threshold=%d",
                        cf.asset_id,
                        _MAX_CONSECUTIVE_EMPTY_OUTPUT_FAILURES_PER_ASSET,
                    )
            else:
                processed += 1
                total_frames += result.frames_count
                image_captions += result.image_captions_count
                _reset_empty_output_failure(cf.asset_id, consecutive_failures=consecutive_empty_output_failures)
        finally:
            cleanup_temp_path(result.temp_clip_path)
            cleanup_temp_path(result.temp_video_path)

    completed_at = datetime.now()
    for aid in asset_ids_in_order:
        error_message = asset_errors.get(aid)
        if error_message:
            db.update_frame_status(
                aid,
                "failed",
                error=error_message,
                completed_at=completed_at,
            )
        else:
            db.update_frame_status(aid, "completed", completed_at=completed_at)

    return {
        "processed": processed,
        "failed": failed,
        "frames_extracted": total_frames,
        "image_captions": image_captions,
        "resolved_config_id": routing.resolved_config_id,
    }
