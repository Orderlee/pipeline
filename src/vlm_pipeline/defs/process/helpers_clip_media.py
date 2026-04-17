"""PROCESS 공유 헬퍼 — event 기반 video clip 추출 + MinIO 업로드 + dedup 재시도."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from vlm_pipeline.lib.checksum import sha256_bytes
from vlm_pipeline.lib.file_loader import build_nonexistent_temp_path, cleanup_temp_path
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from .clip_windows import (
    _DEFAULT_EVENT_CLIP_POST_BUFFER_SEC,
    _DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC,
    resolve_event_only_clip_extraction_window,
    window_values_match,
)
from .helpers_key_utils import _delete_minio_keys
from .helpers_metadata import _ffprobe_clip_meta


def _log_clip_extraction_window(
    context,
    *,
    asset_id: str,
    event_start_sec: float,
    event_end_sec: float,
    extract_start_sec: float,
    extract_end_sec: float,
    window_strategy: str,
) -> None:
    buffered_start_sec = float(event_start_sec) - _DEFAULT_EVENT_CLIP_PRE_BUFFER_SEC
    buffered_end_sec = float(event_end_sec) + _DEFAULT_EVENT_CLIP_POST_BUFFER_SEC
    context.log.info(
        "clip_to_frame extraction window: asset=%s event=[%.3f, %.3f] extract=[%.3f, %.3f] "
        "start_clamped=%s end_clamped=%s strategy=%s",
        asset_id,
        float(event_start_sec),
        float(event_end_sec),
        extract_start_sec,
        extract_end_sec,
        extract_start_sec != buffered_start_sec,
        extract_end_sec != buffered_end_sec,
        window_strategy,
    )


def _extract_video_clip_path(
    video_path: Path, *, clip_start_sec: float, clip_end_sec: float,
) -> Path:
    duration_sec = max(0.05, float(clip_end_sec) - float(clip_start_sec))
    output_path = build_nonexistent_temp_path(".mp4")

    copy_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", f"{float(clip_start_sec):.3f}", "-t", f"{duration_sec:.3f}",
        "-i", str(video_path), "-map", "0:v:0", "-map", "0:a?",
        "-c", "copy", "-movflags", "+faststart", str(output_path),
    ]
    copy_proc = subprocess.run(copy_cmd, capture_output=True, check=False)
    if copy_proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return output_path
    cleanup_temp_path(output_path)
    output_path = build_nonexistent_temp_path(".mp4")

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
        cleanup_temp_path(output_path)
        raise RuntimeError(f"ffmpeg_clip_extract_failed:{stderr or 'empty_output'}")
    return output_path


def _extract_video_clip_media(
    context,
    db: DuckDBResource,
    minio: MinIOResource,
    *,
    asset_id: str,
    clip_id: str,
    clip_key: str,
    video_path: Path,
    event_start_sec: float,
    event_end_sec: float,
    source_duration_sec: float | None,
    extract_start_sec: float,
    extract_end_sec: float,
    window_strategy: str,
    video_width: object,
    video_height: object,
    video_codec: object,
) -> dict[str, Any]:
    def _extract_once(
        *,
        start_sec: float,
        end_sec: float,
        strategy: str,
    ) -> dict[str, Any]:
        _log_clip_extraction_window(
            context,
            asset_id=asset_id,
            event_start_sec=event_start_sec,
            event_end_sec=event_end_sec,
            extract_start_sec=start_sec,
            extract_end_sec=end_sec,
            window_strategy=strategy,
        )
        temp_clip_path = _extract_video_clip_path(
            video_path,
            clip_start_sec=start_sec,
            clip_end_sec=end_sec,
        )
        file_bytes = temp_clip_path.read_bytes()
        minio.upload("vlm-processed", clip_key, file_bytes, "video/mp4")
        clip_meta = _ffprobe_clip_meta(temp_clip_path)
        return {
            "temp_clip_path": temp_clip_path,
            "uploaded_clip_key": clip_key,
            "file_bytes": file_bytes,
            "clip_duration": clip_meta.get("duration_sec"),
            "clip_fps": clip_meta.get("fps"),
            "clip_frame_count": clip_meta.get("frame_count"),
            "width": clip_meta.get("width") or video_width,
            "height": clip_meta.get("height") or video_height,
            "codec": clip_meta.get("codec") or video_codec or "mp4",
            "checksum": sha256_bytes(file_bytes),
            "file_size": len(file_bytes),
            "extract_start_sec": start_sec,
            "extract_end_sec": end_sec,
            "window_strategy": strategy,
        }

    extracted = _extract_once(
        start_sec=extract_start_sec,
        end_sec=extract_end_sec,
        strategy=window_strategy,
    )
    existing_clip = db.find_processed_clip_by_checksum(
        str(extracted["checksum"]),
        source_asset_id=asset_id,
        exclude_clip_id=clip_id,
    )
    if existing_clip is None:
        return extracted

    retry_start_sec, retry_end_sec = resolve_event_only_clip_extraction_window(
        event_start_sec=event_start_sec,
        event_end_sec=event_end_sec,
        source_duration_sec=source_duration_sec,
    )
    if window_values_match(
        float(extracted["extract_start_sec"]),
        float(extracted["extract_end_sec"]),
        retry_start_sec,
        retry_end_sec,
    ):
        raise RuntimeError(
            "duplicate_clip_media_after_window_split:"
            f"existing_clip_id={existing_clip['clip_id']}:checksum={extracted['checksum']}"
        )

    context.log.warning(
        "clip_to_frame checksum collision: asset=%s clip_id=%s existing_clip_id=%s checksum=%s "
        "strategy=%s -> retry event_only",
                asset_id,
        clip_id,
        existing_clip["clip_id"],
        extracted["checksum"],
        extracted["window_strategy"],
    )
    _delete_minio_keys(minio, "vlm-processed", [clip_key])
    cleanup_temp_path(extracted["temp_clip_path"])
    extracted = _extract_once(
        start_sec=retry_start_sec,
        end_sec=retry_end_sec,
        strategy="event_only_retry",
    )
    existing_clip = db.find_processed_clip_by_checksum(
        str(extracted["checksum"]),
        source_asset_id=asset_id,
        exclude_clip_id=clip_id,
    )
    if existing_clip is not None:
        raise RuntimeError(
            "duplicate_clip_media_after_window_split:"
            f"existing_clip_id={existing_clip['clip_id']}:checksum={extracted['checksum']}"
        )
    return extracted
