"""PROCESS @asset — clip_captioning (Gemini JSON → labels) + clip_to_frame (clip 생성 + 이미지 추출).

clip_captioning: vlm-labels의 Gemini JSON을 정규화하여 labels 테이블에 upsert.
clip_to_frame: labels 기반 clip 생성 → ffprobe 메타 → 적응형 프레임 추출 → image_metadata.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime
from hashlib import sha1
from io import BytesIO
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile, gettempdir
from typing import Any
from uuid import uuid4

from dagster import Field, asset
from PIL import Image

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.env_utils import should_run_output
from vlm_pipeline.lib.gemini import extract_clean_json_text
from vlm_pipeline.lib.video_frames import (
    describe_frame_bytes,
    extract_frame_jpeg_bytes,
    plan_frame_timestamps,
)
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource


# ═══════════════════════════════════════════════════════════════
# clip_captioning — Gemini JSON 정규화 → labels upsert
# ═══════════════════════════════════════════════════════════════

@asset(
    name="clip_captioning",
    deps=["clip_timestamp"],
    description="Gemini JSON 정규화 → labels 테이블 upsert (이벤트별 row 생성)",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=200)},
)
def clip_captioning(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """auto_label_status='generated'인 video의 Gemini JSON을 labels에 upsert."""
    if not should_run_output(context, "captioning"):
        context.log.info("clip_captioning 스킵: outputs에 captioning이 없습니다.")
        return {"processed": 0, "failed": 0, "labels_inserted": 0, "skipped": True}

    folder_name = context.run.tags.get("folder_name")
    limit = int(context.op_config.get("limit", 200))
    candidates = db.find_captioning_pending_videos(limit=limit, folder_name=folder_name)
    if not candidates:
        context.log.info("CAPTIONING 대상 없음")
        return {"processed": 0, "failed": 0, "labels_inserted": 0}

    total_candidates = len(candidates)
    context.log.info(f"clip_captioning 시작: 총 {total_candidates}건 처리 예정")
    processed = 0
    failed = 0
    labels_inserted = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        auto_label_key = str(cand.get("auto_label_key") or "")

        if not auto_label_key:
            context.log.warning(f"auto_label_key 없음: asset_id={asset_id}")
            db.update_auto_label_status(
                asset_id,
                "failed",
                error="captioning_missing_auto_label_key",
            )
            failed += 1
            continue

        try:
            json_bytes = minio.download("vlm-labels", auto_label_key)
            raw_text = json_bytes.decode("utf-8", errors="replace")
            cleaned = extract_clean_json_text(raw_text)
            events = json.loads(cleaned)

            if not isinstance(events, list):
                events = [events] if isinstance(events, dict) else []

            valid_events = _filter_valid_events(events)
            event_count = len(valid_events)
            label_rows: list[dict[str, Any]] = []

            for event_index, event in enumerate(valid_events):
                ts = event.get("timestamp") or []
                start_sec = float(ts[0]) if len(ts) > 0 else None
                end_sec = float(ts[1]) if len(ts) > 1 else None
                if start_sec is not None and end_sec is not None and end_sec <= start_sec:
                    continue

                ko_caption = str(event.get("ko_caption") or "").strip()
                en_caption = str(event.get("en_caption") or "").strip()
                caption_text = ko_caption or en_caption or None

                label_id = _stable_gemini_label_id(asset_id, event_index, start_sec, end_sec)
                label_rows.append(
                    {
                        "label_id": label_id,
                        "asset_id": asset_id,
                        "labels_bucket": "vlm-labels",
                        "labels_key": auto_label_key,
                        "label_format": "gemini_event_json",
                        "label_tool": "gemini",
                        "label_source": "auto",
                        "review_status": "auto_generated",
                        "event_index": event_index,
                        "event_count": event_count,
                        "timestamp_start_sec": start_sec,
                        "timestamp_end_sec": end_sec,
                        "caption_text": caption_text,
                        "object_count": 0,
                        "label_status": "completed",
                    }
                )

            inserted = db.replace_gemini_labels(asset_id, auto_label_key, label_rows)
            labels_inserted += inserted
            db.update_auto_label_status(
                asset_id,
                "completed",
                label_key=auto_label_key,
                labeled_at=datetime.now(),
            )
            processed += 1
            context.log.info(
                f"clip_captioning 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} events={event_count} labels={inserted} ✅"
            )

        except json.JSONDecodeError as exc:
            failed += 1
            db.update_auto_label_status(asset_id, "failed", error=f"json_parse_error: {exc}")
            context.log.error(
                f"clip_captioning 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ JSON 파싱 실패: {exc}"
            )
        except Exception as exc:
            failed += 1
            context.log.error(
                f"clip_captioning 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ {exc}"
            )

    summary = {"processed": processed, "failed": failed, "labels_inserted": labels_inserted}
    context.add_output_metadata(summary)
    context.log.info(f"CAPTIONING 완료: {summary}")
    return summary


@asset(
    name="clip_captioning_routed",
    deps=["clip_timestamp_routed"],
    description="Staging spec flow: requested_outputs에 captioning 포함 시 caption 생성",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=200)},
)
def clip_captioning_routed(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """run tag: spec_id, requested_outputs. requested_outputs에 captioning 있을 때만 실행."""
    tags = context.run.tags if context.run else {}
    requested = (tags.get("requested_outputs") or "").strip().split("_")
    if "captioning" not in requested:
        context.log.info("clip_captioning_routed 스킵: requested_outputs에 captioning 없음")
        return {"processed": 0, "failed": 0, "labels_inserted": 0, "skipped": True}
    # TODO: spec_id 기준 timestamp_status=completed 백로그 조회 후 기존 clip_captioning 로직 재사용
    context.log.info("clip_captioning_routed: 스펙 백로그 연동 TODO")
    return {"processed": 0, "failed": 0, "labels_inserted": 0}


# ═══════════════════════════════════════════════════════════════
# clip_to_frame — clip 생성 + ffprobe + 적응형 프레임 추출
# ═══════════════════════════════════════════════════════════════

@asset(
    deps=["clip_captioning"],
    name="clip_to_frame",
    description="labels 기반 clip 생성 → ffprobe 메타 → 적응형 프레임 추출 → image_metadata",
    group_name="auto_labeling",
    config_schema={
        "limit": Field(int, default_value=1000),
        "jpeg_quality": Field(int, default_value=90),
        "max_frames_per_video": Field(int, default_value=12),
    },
)
def clip_to_frame(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """INGEST+LABEL 완료 → clip 생성 + processed_clips INSERT + 적응형 프레임 추출."""
    if not should_run_output(context, "captioning"):
        context.log.info("clip_to_frame 스킵: outputs에 captioning이 없습니다.")
        return {"processed": 0, "failed": 0, "frames_extracted": 0, "skipped": True}

    folder_name = context.run.tags.get("folder_name")
    image_profile = context.run.tags.get("image_profile", "current")

    candidates = db.find_processable(folder_name=folder_name)
    if not candidates:
        context.log.info("PROCESS 대상 없음")
        return {"processed": 0, "failed": 0, "frames_extracted": 0}

    limit = int(context.op_config.get("limit", 1000))
    jpeg_quality = int(context.op_config.get("jpeg_quality", 90))
    max_frames = int(context.op_config.get("max_frames_per_video", 12))
    candidates = candidates[:limit]
    total_candidates = len(candidates)
    context.log.info(f"clip_to_frame 시작: 총 {total_candidates}건 처리 예정")

    processed = 0
    failed = 0
    total_frames = 0

    for idx, cand in enumerate(candidates, start=1):
        asset_id = cand["asset_id"]
        raw_bucket = cand["raw_bucket"]
        raw_key = cand["raw_key"]
        media_type = cand["media_type"]
        archive_path = cand.get("archive_path")
        label_id = cand["label_id"]
        labels_key = cand["labels_key"]
        label_source = str(cand.get("label_source") or "manual")
        event_index = int(cand.get("event_index") or 0)
        clip_start_sec = _coerce_float(cand.get("timestamp_start_sec"))
        clip_end_sec = _coerce_float(cand.get("timestamp_end_sec"))
        caption_text = str(cand.get("caption_text") or "").strip() or None
        temp_clip_path: Path | None = None
        temp_video_path: Path | None = None
        clip_id: str | None = None
        clip_key: str | None = None
        uploaded_clip_key: str | None = None
        uploaded_frame_keys: list[str] = []
        clip_created_at = datetime.now()

        try:
            clip_key = _build_processed_clip_key(
                raw_key,
                event_index=event_index,
                clip_start_sec=clip_start_sec,
                clip_end_sec=clip_end_sec,
                media_type=media_type,
            )
            clip_id = _stable_clip_id(label_id, event_index, clip_start_sec, clip_end_sec, clip_key)
            width, height, codec = None, None, None
            file_bytes = None
            clip_duration = None
            clip_fps = None
            clip_frame_count = None

            db.insert_processed_clip(
                {
                    "clip_id": clip_id,
                    "source_asset_id": asset_id,
                    "source_label_id": label_id,
                    "event_index": event_index,
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "processed_bucket": "vlm-processed",
                    "clip_key": clip_key,
                    "label_key": labels_key,
                    "data_source": label_source,
                    "caption_text": caption_text,
                    "image_extract_status": "pending" if media_type == "video" else "completed",
                    "image_extract_count": 0,
                    "process_status": "processing",
                    "created_at": clip_created_at,
                }
            )

            if media_type == "image":
                file_bytes = minio.download(raw_bucket, raw_key)
                with Image.open(BytesIO(file_bytes)) as img:
                    width, height = img.size
                    codec = (img.format or "jpeg").lower()
                minio.upload("vlm-processed", clip_key, file_bytes, f"image/{codec}")
                uploaded_clip_key = clip_key

            elif clip_start_sec is not None and clip_end_sec is not None and clip_end_sec > clip_start_sec:
                video_path, temp_video_path = _materialize_video_path(
                    minio,
                    {"archive_path": archive_path, "raw_bucket": raw_bucket, "raw_key": raw_key},
                )
                temp_clip_path = _extract_video_clip_path(
                    video_path,
                    clip_start_sec=clip_start_sec,
                    clip_end_sec=clip_end_sec,
                )
                file_bytes = temp_clip_path.read_bytes()
                minio.upload("vlm-processed", clip_key, file_bytes, "video/mp4")
                uploaded_clip_key = clip_key

                clip_meta = _ffprobe_clip_meta(temp_clip_path)
                clip_duration = clip_meta.get("duration_sec")
                clip_fps = clip_meta.get("fps")
                clip_frame_count = clip_meta.get("frame_count")
                width = clip_meta.get("width") or cand.get("video_width")
                height = clip_meta.get("height") or cand.get("video_height")
                codec = clip_meta.get("codec") or cand.get("video_codec") or "mp4"
            else:
                raise RuntimeError("video_clip_range_missing")

            checksum = sha256_bytes(file_bytes) if file_bytes else None
            file_size = len(file_bytes) if file_bytes else None

            db.insert_processed_clip(
                {
                    "clip_id": clip_id,
                    "source_asset_id": asset_id,
                    "source_label_id": label_id,
                    "event_index": event_index,
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "checksum": checksum,
                    "file_size": file_size,
                    "processed_bucket": "vlm-processed",
                    "clip_key": clip_key,
                    "label_key": labels_key,
                    "data_source": label_source,
                    "caption_text": caption_text,
                    "width": width,
                    "height": height,
                    "codec": codec,
                    "duration_sec": clip_duration,
                    "fps": clip_fps,
                    "frame_count": clip_frame_count,
                    "image_extract_status": "pending" if media_type == "video" else "completed",
                    "image_extract_count": 0,
                    "process_status": "processing",
                    "created_at": clip_created_at,
                }
            )

            # 적응형 프레임 추출 (video clip만)
            frames_count = 0
            if media_type == "video":
                if temp_clip_path is None or clip_duration is None or clip_duration <= 0:
                    raise RuntimeError("clip_meta_missing_or_invalid")
                db.update_clip_image_extract_status(clip_id, "processing")
                frame_rows, uploaded_frame_keys = _extract_clip_frames(
                    minio,
                    clip_id=clip_id,
                    source_asset_id=asset_id,
                    clip_path=temp_clip_path,
                    clip_key=clip_key,
                    duration_sec=clip_duration,
                    fps=clip_fps,
                    frame_count=clip_frame_count,
                    max_frames=max_frames,
                    jpeg_quality=jpeg_quality,
                    image_profile=image_profile,
                )
                db.replace_processed_clip_frame_metadata(asset_id, clip_id, frame_rows)
                frames_count = len(frame_rows)
                db.update_clip_image_extract_status(
                    clip_id,
                    "completed",
                    count=frames_count,
                    error=None,
                    extracted_at=datetime.now(),
                )
                total_frames += frames_count
            else:
                db.update_clip_image_extract_status(
                    clip_id,
                    "completed",
                    count=0,
                    error=None,
                    extracted_at=datetime.now(),
                )

            db.update_processed_clip_status(clip_id, "completed")
            processed += 1
            context.log.info(
                f"clip_to_frame 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} frames={frames_count} ✅"
            )

        except Exception as e:
            context.log.error(
                f"clip_to_frame 진행: [{idx}/{total_candidates}] "
                f"({idx * 100 // total_candidates}%) "
                f"asset={asset_id} ❌ {e}"
            )
            if clip_id:
                db.update_processed_clip_status(clip_id, "failed")
                db.update_clip_image_extract_status(
                    clip_id,
                    "failed",
                    count=0,
                    error=str(e)[:500],
                    extracted_at=datetime.now(),
                )
                db.replace_processed_clip_frame_metadata(asset_id, clip_id, [])
            cleanup_keys = list(uploaded_frame_keys)
            if uploaded_clip_key:
                cleanup_keys.append(uploaded_clip_key)
            if cleanup_keys:
                _delete_minio_keys(minio, "vlm-processed", cleanup_keys)
            failed += 1
        finally:
            _cleanup_temp_path(temp_clip_path)
            _cleanup_temp_path(temp_video_path)

    summary = {"processed": processed, "failed": failed, "frames_extracted": total_frames}
    context.add_output_metadata(summary)
    context.log.info(f"PROCESS 완료: {summary}")
    return summary


processed_clips = clip_to_frame


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _filter_valid_events(events: list) -> list[dict]:
    """Gemini 응답에서 유효한 이벤트만 필터링."""
    valid = []
    for event in events:
        if not isinstance(event, dict):
            continue
        ts = event.get("timestamp")
        if not isinstance(ts, list) or len(ts) < 2:
            continue
        try:
            start = float(ts[0])
            end = float(ts[1])
        except (TypeError, ValueError):
            continue
        if start < 0 or end <= start:
            continue
        valid.append(event)
    valid.sort(key=lambda e: float(e["timestamp"][0]))
    return valid


def _stable_gemini_label_id(
    asset_id: str, event_index: int, start_sec: float | None, end_sec: float | None,
) -> str:
    token = f"{asset_id}|gemini|auto|{event_index}|{start_sec}|{end_sec}"
    return sha1(token.encode("utf-8")).hexdigest()


def _ffprobe_clip_meta(clip_path: Path) -> dict[str, Any]:
    """ffprobe로 clip의 duration, fps, frame_count, width, height, codec 추출."""
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", str(clip_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30, check=False)
        if result.returncode != 0:
            return {}
        data = json.loads(result.stdout)
    except Exception:
        return {}

    video_stream = None
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    if not video_stream:
        return {}

    duration = None
    fmt_duration = data.get("format", {}).get("duration")
    stream_duration = video_stream.get("duration")
    for d in (stream_duration, fmt_duration):
        if d:
            try:
                duration = float(d)
                break
            except (TypeError, ValueError):
                pass

    fps = None
    r_frame_rate = video_stream.get("r_frame_rate", "")
    if "/" in str(r_frame_rate):
        parts = r_frame_rate.split("/")
        try:
            fps = float(parts[0]) / float(parts[1])
        except (ValueError, ZeroDivisionError):
            pass

    frame_count = None
    nb_frames = video_stream.get("nb_frames")
    if nb_frames:
        try:
            frame_count = int(nb_frames)
        except (TypeError, ValueError):
            pass

    return {
        "duration_sec": duration,
        "fps": fps,
        "frame_count": frame_count,
        "width": video_stream.get("width"),
        "height": video_stream.get("height"),
        "codec": video_stream.get("codec_name"),
    }


def _extract_clip_frames(
    minio: MinIOResource,
    *,
    clip_id: str,
    source_asset_id: str,
    clip_path: Path,
    clip_key: str,
    duration_sec: float,
    fps: float | None,
    frame_count: int | None,
    max_frames: int,
    jpeg_quality: int,
    image_profile: str = "current",
) -> tuple[list[dict[str, Any]], list[str]]:
    """clip 비디오에서 적응형 프레임 추출 → vlm-processed 업로드용 row/keys 반환."""
    now = datetime.now()
    timestamps = plan_frame_timestamps(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=max_frames,
        image_profile=image_profile,
    )

    frame_rows: list[dict[str, Any]] = []
    uploaded_keys: list[str] = []

    try:
        for frame_index, frame_sec in enumerate(timestamps, start=1):
            frame_bytes = extract_frame_jpeg_bytes(
                clip_path, frame_sec, jpeg_quality=jpeg_quality,
            )
            image_key = _build_processed_clip_image_key(clip_key, frame_index)
            minio.upload("vlm-processed", image_key, frame_bytes, "image/jpeg")
            uploaded_keys.append(image_key)

            frame_meta = describe_frame_bytes(frame_bytes)
            frame_rows.append({
                "image_id": str(uuid4()),
                "source_clip_id": clip_id,
                "image_bucket": "vlm-processed",
                "image_key": image_key,
                "image_role": "processed_clip_frame",
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


def _materialize_object_path(
    minio: MinIOResource, bucket: str, key: str, *, fallback_name: str,
) -> tuple[Path, Path]:
    suffix = Path(str(key or fallback_name)).suffix or Path(fallback_name).suffix or ".mp4"
    tmp_file = NamedTemporaryFile(delete=False, suffix=suffix)
    try:
        minio.download_fileobj(str(bucket or "").strip(), str(key or "").strip(), tmp_file)
    finally:
        tmp_file.close()
    return Path(tmp_file.name), Path(tmp_file.name)


def _materialize_video_path(minio: MinIOResource, candidate: dict) -> tuple[Path, Path | None]:
    archive_path = Path(str(candidate.get("archive_path") or "").strip())
    if archive_path.exists():
        return archive_path, None
    return _materialize_object_path(
        minio,
        str(candidate.get("raw_bucket") or "vlm-raw"),
        str(candidate.get("raw_key") or ""),
        fallback_name="video.mp4",
    )


def _cleanup_temp_path(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _build_nonexistent_temp_path(suffix: str) -> Path:
    return Path(gettempdir()) / f"vlm_{uuid4().hex}{suffix}"


def _delete_minio_keys(minio: MinIOResource, bucket: str, keys: list[str]) -> None:
    for key in keys:
        if not key:
            continue
        try:
            minio.delete(bucket, key)
        except Exception:
            continue


def _build_processed_clip_key(
    raw_key: str, *, event_index: int,
    clip_start_sec: float | None, clip_end_sec: float | None, media_type: str,
) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "asset"
    suffix = ".mp4" if str(media_type or "").strip().lower() == "video" else (key_path.suffix or ".jpg")
    parent = key_path.parent
    if clip_start_sec is not None and clip_end_sec is not None:
        start_ms = int(round(float(clip_start_sec) * 1000))
        end_ms = int(round(float(clip_end_sec) * 1000))
        filename = f"{stem}_{start_ms:08d}_{end_ms:08d}{suffix}"
    else:
        filename = f"{stem}_e{int(event_index):03d}{suffix}"
    if str(parent) and str(parent) != ".":
        return str(parent / "clips" / filename)
    return str(PurePosixPath("clips") / filename)


def _build_processed_clip_image_key(clip_key: str, frame_index: int) -> str:
    clip_path = PurePosixPath(str(clip_key or "").strip())
    clip_stem = clip_path.stem or "clip"
    parent = clip_path.parent
    if parent.name == "clips":
        image_parent = parent.parent / "image"
    elif str(parent) and str(parent) != ".":
        image_parent = parent / "image"
    else:
        image_parent = PurePosixPath("image")
    return str(image_parent / f"{clip_stem}_{int(frame_index):08d}.jpg")


def _extract_video_clip_path(
    video_path: Path, *, clip_start_sec: float, clip_end_sec: float,
) -> Path:
    duration_sec = max(0.05, float(clip_end_sec) - float(clip_start_sec))
    output_path = _build_nonexistent_temp_path(".mp4")

    copy_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", f"{float(clip_start_sec):.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(video_path), "-map", "0:v:0", "-map", "0:a?",
        "-c", "copy", "-movflags", "+faststart", str(output_path),
    ]
    copy_proc = subprocess.run(copy_cmd, capture_output=True, check=False)
    if copy_proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return output_path
    _cleanup_temp_path(output_path)
    output_path = _build_nonexistent_temp_path(".mp4")

    reencode_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", f"{float(clip_start_sec):.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(video_path), "-map", "0:v:0", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac",
        "-movflags", "+faststart", str(output_path),
    ]
    reencode_proc = subprocess.run(reencode_cmd, capture_output=True, check=False)
    if reencode_proc.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        stderr = (reencode_proc.stderr or copy_proc.stderr or b"").decode("utf-8", errors="ignore").strip()
        _cleanup_temp_path(output_path)
        raise RuntimeError(f"ffmpeg_clip_extract_failed:{stderr or 'empty_output'}")
    return output_path


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stable_clip_id(
    label_id: str, event_index: int,
    clip_start_sec: float | None, clip_end_sec: float | None, clip_key: str,
) -> str:
    token = "|".join([
        str(label_id), str(event_index),
        str(clip_start_sec), str(clip_end_sec), str(clip_key),
    ])
    return sha1(token.encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════════
# Staging spec flow: clip_to_frame_routed
# ═══════════════════════════════════════════════════════════════

@asset(
    name="clip_to_frame_routed",
    deps=["clip_timestamp_routed"],
    description="Staging spec flow: requested_outputs에 bbox 포함 시 clip+frame 추출",
    group_name="auto_labeling",
    config_schema={"limit": Field(int, default_value=1000)},
)
def clip_to_frame_routed(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    """run tag: spec_id, requested_outputs. bbox 요청 시 내부 stage로 frame 추출."""
    tags = context.run.tags if context.run else {}
    requested = (tags.get("requested_outputs") or "").strip().split("_")
    if "bbox" not in requested:
        context.log.info("clip_to_frame_routed 스킵: requested_outputs에 bbox 없음")
        return {"processed": 0, "failed": 0, "frames_extracted": 0, "skipped": True}
    # TODO: spec_id 기준 timestamp_status=completed 백로그 조회 후 clip 생성 + frame 추출, frame_status 갱신
    context.log.info("clip_to_frame_routed: 스펙 백로그 연동 TODO")
    return {"processed": 0, "failed": 0, "frames_extracted": 0}


# ═══════════════════════════════════════════════════════════════
# [STAGING 전용] raw_video_to_frame
# ═══════════════════════════════════════════════════════════════

@asset(
    name="raw_video_to_frame",
    deps=["raw_ingest"],
    description="[STAGING YOLO 전용] raw video에서 직접 이미지 추출",
    group_name="yolo",
    config_schema={
        "limit": Field(int, default_value=1000),
        "jpeg_quality": Field(int, default_value=90),
        "max_frames_per_video": Field(int, default_value=24),
    },
)
def raw_video_to_frame(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
) -> dict:
    if not should_run_output(context, "bbox"):
        context.log.info("raw_video_to_frame 스킵: outputs에 bbox가 없습니다.")
        return {"processed": 0, "failed": 0, "frames_extracted": 0, "skipped": True}

    folder_name = context.run.tags.get("folder_name")
    image_profile = context.run.tags.get("image_profile", "current")
    limit = int(context.op_config.get("limit", 1000))

    # dispatch JSON에서 파라미터가 있으면 우선 사용, 없으면 config 기본값
    jpeg_quality_tag = context.run.tags.get("jpeg_quality")
    jpeg_quality = int(jpeg_quality_tag) if jpeg_quality_tag else int(context.op_config.get("jpeg_quality", 90))

    max_frames_tag = context.run.tags.get("max_frames_per_video")
    max_frames = int(max_frames_tag) if max_frames_tag else int(context.op_config.get("max_frames_per_video", 24))

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

            frame_rows, uploaded_frame_keys = _extract_raw_video_frames(
                minio,
                asset_id=asset_id,
                video_path=video_path,
                raw_key=raw_key,
                duration_sec=duration_sec,
                fps=fps,
                frame_count=frame_count,
                max_frames=max_frames,
                jpeg_quality=jpeg_quality,
                image_profile=image_profile,
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
) -> tuple[list[dict[str, Any]], list[str]]:
    now = datetime.now()
    timestamps = plan_frame_timestamps(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=max_frames,
        image_profile=image_profile,
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
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    if parent.name == "image":
        image_parent = parent
    elif str(parent) and str(parent) != ".":
        image_parent = parent / "image"
    else:
        image_parent = PurePosixPath("image")
    return str(image_parent / f"{stem}_{int(frame_index):08d}.jpg")
