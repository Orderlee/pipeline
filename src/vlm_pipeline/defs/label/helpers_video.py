"""Video materialisation + ffmpeg-based segment extraction (no Gemini coupling)."""

from __future__ import annotations

import subprocess
from pathlib import Path

from vlm_pipeline.lib.file_loader import cleanup_temp_path
from vlm_pipeline.resources.minio import MinIOResource

from .helpers_common import _build_nonexistent_temp_path


def materialize_video(
    minio: MinIOResource,
    candidate: dict,
) -> tuple[Path, Path | None]:
    """비디오 파일을 로컬에 확보. archive_path 우선, MinIO fallback."""
    from vlm_pipeline.lib.media_utils import materialize_video_path
    return materialize_video_path(minio, candidate)


def extract_video_segment_path(
    video_path: Path,
    *,
    start_sec: float,
    end_sec: float,
) -> Path:
    duration_value = max(0.05, float(end_sec) - float(start_sec))
    output_path = _build_nonexistent_temp_path(".mp4")

    copy_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{float(start_sec):.3f}",
        "-t",
        f"{duration_value:.3f}",
        "-i",
        str(video_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    copy_proc = subprocess.run(copy_cmd, capture_output=True, check=False)
    if copy_proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return output_path

    cleanup_temp_path(output_path)
    output_path = _build_nonexistent_temp_path(".mp4")
    reencode_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{float(start_sec):.3f}",
        "-t",
        f"{duration_value:.3f}",
        "-i",
        str(video_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    reencode_proc = subprocess.run(reencode_cmd, capture_output=True, check=False)
    if reencode_proc.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        stderr = (reencode_proc.stderr or copy_proc.stderr or b"").decode("utf-8", errors="ignore").strip()
        cleanup_temp_path(output_path)
        raise RuntimeError(f"ffmpeg_chunk_extract_failed:{stderr or 'empty_output'}")
    return output_path
