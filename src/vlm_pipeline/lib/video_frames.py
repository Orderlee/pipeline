"""비디오 프레임 추출/계획 helper."""

from __future__ import annotations

import io
import logging
import os
import subprocess
import time
from pathlib import Path, PurePosixPath

from PIL import Image

logger = logging.getLogger(__name__)


def resolve_duration_sec(duration_sec: float | int | None, fps: float | int | None, frame_count: int | None) -> float:
    try:
        duration = float(duration_sec or 0.0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration > 0:
        return duration

    try:
        fps_value = float(fps or 0.0)
    except (TypeError, ValueError):
        fps_value = 0.0
    try:
        frame_count_value = int(frame_count or 0)
    except (TypeError, ValueError):
        frame_count_value = 0

    if fps_value > 0 and frame_count_value > 0:
        return frame_count_value / fps_value
    return 0.0


def target_frame_count(
    *,
    duration_sec: float | int | None,
    fps: float | int | None,
    frame_count: int | None,
    max_frames_per_video: int = 12,
    image_profile: str = "current",
) -> int:
    duration = resolve_duration_sec(duration_sec, fps, frame_count)
    try:
        fps_value = float(fps or 0.0)
    except (TypeError, ValueError):
        fps_value = 0.0

    if image_profile == "dense":
        if duration < 10:
            target = 6
        elif duration < 30:
            target = 10
        elif duration < 120:
            target = 16
        else:
            target = 24

        if fps_value > 0:
            if fps_value < 3:
                target = min(target, 4)
            elif fps_value < 10:
                target = min(target, 12)
        upper_bound = 24
    else:
        if duration < 10:
            target = 3
        elif duration < 30:
            target = 5
        elif duration < 120:
            target = 8
        else:
            target = 12

        if fps_value > 0:
            if fps_value < 3:
                target = min(target, 3)
            elif fps_value < 10:
                target = min(target, 6)
        upper_bound = max_frames_per_video or 12

    try:
        frame_count_value = int(frame_count or 0)
    except (TypeError, ValueError):
        frame_count_value = 0

    target = min(target, max(1, int(upper_bound)))
    if frame_count_value > 0:
        target = min(target, frame_count_value)
    return max(1, target)


def plan_frame_timestamps(
    *,
    duration_sec: float | int | None,
    fps: float | int | None,
    frame_count: int | None,
    max_frames_per_video: int = 12,
    image_profile: str = "current",
) -> list[float]:
    duration = resolve_duration_sec(duration_sec, fps, frame_count)
    target = target_frame_count(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=max_frames_per_video,
        image_profile=image_profile,
    )

    if duration <= 0:
        return [0.0]

    start_ratio, end_ratio = (0.2, 0.8) if duration < 2 else (0.1, 0.9)
    start_sec = duration * start_ratio
    end_sec = duration * end_ratio
    if end_sec <= start_sec:
        midpoint = max(0.0, duration * 0.5)
        return [round(midpoint, 3)]

    timestamps: list[float] = []
    dedupe_ms: set[int] = set()
    for idx in range(target):
        sec = start_sec + ((idx + 1) / (target + 1)) * (end_sec - start_sec)
        sec = min(max(0.0, sec), duration)
        millis = max(0, int(round(sec * 1000)))
        if millis in dedupe_ms:
            continue
        dedupe_ms.add(millis)
        timestamps.append(round(millis / 1000.0, 3))

    if timestamps:
        return timestamps

    midpoint = max(0.0, duration * 0.5)
    return [round(midpoint, 3)]


def build_frame_key(raw_key: str, frame_index: int, frame_sec: float) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    _ = frame_sec
    frame_name = f"{stem}_{int(frame_index):08d}.jpg"
    return str(parent / stem / frame_name) if str(parent) != "." else str(
        PurePosixPath(stem) / frame_name
    )


def extract_frame_jpeg_bytes(
    video_path: str | Path,
    sec: float,
    *,
    jpeg_quality: int = 90,
    timeout_sec: int | None = None,
    max_retries: int | None = None,
) -> bytes:
    """ffmpeg로 비디오에서 단일 프레임을 JPEG 바이트로 추출.

    NAS/CIFS 마운트 환경에서 I/O 지연이 발생할 수 있으므로
    타임아웃을 충분히 설정하고 재시도 로직을 포함한다.

    Args:
        video_path: 비디오 파일 경로
        sec: 추출할 프레임의 타임스탬프(초)
        jpeg_quality: JPEG 품질 (1-100)
        timeout_sec: ffmpeg 프로세스 타임아웃(초). 기본 120초.
                     환경변수 VIDEO_FRAME_EXTRACT_TIMEOUT_SEC로 오버라이드 가능.
        max_retries: 타임아웃/실패 시 재시도 횟수. 기본 2회.
                     환경변수 VIDEO_FRAME_EXTRACT_RETRIES로 오버라이드 가능.
    """
    timeout = timeout_sec or int(os.getenv("VIDEO_FRAME_EXTRACT_TIMEOUT_SEC", "120"))
    retries = max_retries if max_retries is not None else int(
        os.getenv("VIDEO_FRAME_EXTRACT_RETRIES", "2")
    )
    retries = max(0, retries)
    # ffmpeg mjpeg -q:v 2~31 (lower = better). PIL quality 90 → -q:v 4 근사.
    q_v = max(2, min(31, round(2 + (100 - max(1, min(100, int(jpeg_quality)))) * 29 / 100)))

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{float(sec):.3f}",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-f",
        "image2pipe",
        "-vcodec",
        "mjpeg",
        "-q:v",
        str(q_v),
        "pipe:1",
    ]

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        current_timeout = timeout * (attempt + 1)  # 재시도마다 타임아웃 증가
        try:
            proc = subprocess.run(
                cmd, capture_output=True, timeout=current_timeout, check=False,
            )
        except subprocess.TimeoutExpired as exc:
            last_error = exc
            if attempt < retries:
                wait_sec = 2 ** attempt  # 1s, 2s, 4s ...
                logger.warning(
                    "ffmpeg 프레임 추출 타임아웃 (attempt %d/%d, timeout=%ds): %s @ %.3fs — %ds 후 재시도",
                    attempt + 1, retries + 1, current_timeout,
                    video_path, sec, wait_sec,
                )
                time.sleep(wait_sec)
                continue
            raise RuntimeError(
                f"ffmpeg_frame_extract_timeout:{current_timeout}s "
                f"path={video_path} sec={sec:.3f} "
                f"(retries={retries} exhausted)"
            ) from exc

        if proc.returncode != 0 or not proc.stdout:
            stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
            last_error = RuntimeError(
                f"ffmpeg_frame_extract_failed:{stderr or 'empty_output'}"
            )
            if attempt < retries:
                wait_sec = 2 ** attempt
                logger.warning(
                    "ffmpeg 프레임 추출 실패 (attempt %d/%d, rc=%d): %s @ %.3fs — %ds 후 재시도",
                    attempt + 1, retries + 1, proc.returncode,
                    video_path, sec, wait_sec,
                )
                time.sleep(wait_sec)
                continue
            raise last_error

        # 성공 — mjpeg로 직접 JPEG 출력하므로 PIL 변환 없이 반환 (I/O·CPU 절감)
        return proc.stdout

    # 방어 코드 (실제 도달하지 않음)
    raise last_error or RuntimeError("ffmpeg_frame_extract_unexpected_state")


def frame_bit_depth(color_mode: str) -> int:
    if color_mode in {"1", "L", "P"}:
        return 8
    if color_mode in {"I;16", "I;16B", "I;16L"}:
        return 16
    return 8


def describe_frame_bytes(frame_bytes: bytes) -> dict[str, object]:
    with Image.open(io.BytesIO(frame_bytes)) as image:
        width, height = image.size
        color_mode = image.mode or "RGB"
    return {
        "width": int(width),
        "height": int(height),
        "color_mode": str(color_mode),
        "bit_depth": frame_bit_depth(str(color_mode)),
        "has_alpha": False,
        "orientation": 1,
        "file_size": len(frame_bytes),
    }
