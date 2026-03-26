"""비디오 프레임 추출/계획 helper.

운영(`clip_to_frame`)·스테이징(`raw_video_to_frame`) 모두 이 모듈을 공유한다.
`frame_interval_sec` 균등 추출 시 타임스탬프는 `sec < duration` 만 포함하여
클립 끝 시점에서 ffmpeg가 빈 출력을 내는 문제(empty_output)를 피한다.
"""

from __future__ import annotations

import io
import logging
import os
import subprocess
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from PIL import Image

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FrameSamplingDecision:
    sampling_mode: str
    effective_max_frames: int
    frame_interval_sec: float
    policy_source: str


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
    max_frames_per_video: int | None = None,
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
        upper_bound = max_frames_per_video or 24
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
    max_frames_per_video: int | None = None,
    image_profile: str = "current",
    frame_interval_sec: float | None = None,
) -> list[float]:
    """구간 내 추출 시점(초) 목록. frame_interval_sec이 주어지면 초당 N장(1/interval)으로 균등 추출."""
    duration = resolve_duration_sec(duration_sec, fps, frame_count)

    if duration <= 0:
        return [0.0]

    if frame_interval_sec is not None and frame_interval_sec > 0:
        timestamps: list[float] = []
        sec = 0.0
        # sec == duration 은 디코딩 가능한 마지막 프레임 이후이므로 제외 (empty_output 방지)
        while sec < duration:
            timestamps.append(round(sec, 3))
            sec += frame_interval_sec
        if not timestamps:
            return [0.0]
        if max_frames_per_video is not None and max_frames_per_video > 0 and len(timestamps) > max_frames_per_video:
            return _downsample_timestamps_evenly(timestamps, max_frames_per_video)
        return timestamps

    target = target_frame_count(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=max_frames_per_video,
        image_profile=image_profile,
    )

    start_ratio, end_ratio = (0.2, 0.8) if duration < 2 else (0.1, 0.9)
    start_sec = duration * start_ratio
    end_sec = duration * end_ratio
    if end_sec <= start_sec:
        midpoint = max(0.0, duration * 0.5)
        return [round(midpoint, 3)]

    result: list[float] = []
    dedupe_ms: set[int] = set()
    for idx in range(target):
        sec = start_sec + ((idx + 1) / (target + 1)) * (end_sec - start_sec)
        sec = min(max(0.0, sec), duration)
        # interval 경로와 동일: t == duration 에는 보통 디코딩 가능 프레임이 없음
        if duration > 0 and sec >= duration:
            sec = max(0.0, duration - 1e-3)
        millis = max(0, int(round(sec * 1000)))
        if millis in dedupe_ms:
            continue
        dedupe_ms.add(millis)
        result.append(round(millis / 1000.0, 3))

    if result:
        return result

    midpoint = max(0.0, duration * 0.5)
    return [round(midpoint, 3)]


def resolve_frame_sampling_policy(
    *,
    sampling_mode: str,
    requested_outputs: Iterable[str] | None,
    image_profile: str,
    duration_sec: float | int | None,
    fps: float | int | None,
    frame_count: int | None,
    spec_max_frames_per_video: int | None = None,
) -> FrameSamplingDecision:
    normalized_mode = str(sampling_mode or "").strip().lower() or "clip_event"
    normalized_profile = "dense" if str(image_profile or "").strip().lower() == "dense" else "current"
    normalized_outputs = {
        str(value or "").strip().lower()
        for value in (requested_outputs or [])
        if str(value or "").strip()
    }
    bbox_only = "bbox" in normalized_outputs and "captioning" not in normalized_outputs

    base_target = target_frame_count(
        duration_sec=duration_sec,
        fps=fps,
        frame_count=frame_count,
        max_frames_per_video=24 if normalized_profile == "dense" else 12,
        image_profile=normalized_profile,
    )

    if normalized_mode == "raw_video":
        dynamic_upper_bound = 36 if normalized_profile == "dense" else 24
        dynamic_floor = 10 if normalized_profile == "dense" else 6
        multiplier = 2 if bbox_only else 1
        effective_max_frames = min(dynamic_upper_bound, max(dynamic_floor, base_target * multiplier))
    else:
        effective_max_frames = base_target
        if bbox_only:
            clip_bbox_cap = 24 if normalized_profile == "dense" else 16
            effective_max_frames = min(clip_bbox_cap, max(effective_max_frames, base_target + 2))

    policy_source = "dynamic_default"
    if spec_max_frames_per_video is not None:
        try:
            cap_value = max(1, int(spec_max_frames_per_video))
        except (TypeError, ValueError):
            cap_value = None
        if cap_value is not None:
            effective_max_frames = min(effective_max_frames, cap_value)
            policy_source = "spec_override"

    duration = resolve_duration_sec(duration_sec, fps, frame_count)
    frame_interval_sec = 10.0 if duration >= 3600 else 1.0

    return FrameSamplingDecision(
        sampling_mode=normalized_mode,
        effective_max_frames=max(1, int(effective_max_frames)),
        frame_interval_sec=frame_interval_sec,
        policy_source=policy_source,
    )


def _downsample_timestamps_evenly(timestamps: list[float], target_count: int) -> list[float]:
    if target_count <= 0 or len(timestamps) <= target_count:
        return timestamps

    if target_count == 1:
        return [timestamps[0]]

    last_index = len(timestamps) - 1
    selected_indices: list[int] = []
    previous_index = -1
    for offset in range(target_count):
        remaining_slots = target_count - offset - 1
        ideal_index = int(round((offset * last_index) / (target_count - 1)))
        next_index = max(previous_index + 1, ideal_index)
        max_index = last_index - remaining_slots
        next_index = min(next_index, max_index)
        selected_indices.append(next_index)
        previous_index = next_index

    return [timestamps[index] for index in selected_indices]


def build_frame_key(raw_key: str, frame_index: int, frame_sec: float) -> str:
    key_path = PurePosixPath(str(raw_key or "").strip())
    stem = key_path.stem or "video"
    parent = key_path.parent
    _ = frame_sec
    frame_name = f"{stem}_{int(frame_index):08d}.jpg"
    return str(parent / stem / frame_name) if str(parent) != "." else str(
        PurePosixPath(stem) / frame_name
    )


def _build_extract_frame_cmd(video_path: str | Path, sec: float, q_v: int) -> list[str]:
    return [
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


def _fallback_seek_seconds(sec: float) -> list[float]:
    base_sec = max(0.0, float(sec))
    candidates: list[float] = [round(base_sec, 3)]
    seen = {candidates[0]}
    for delta in (0.1, 0.25, 0.5):
        fallback_sec = round(max(0.0, base_sec - delta), 3)
        if fallback_sec in seen:
            continue
        seen.add(fallback_sec)
        candidates.append(fallback_sec)
    return candidates


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
    requested_sec = round(float(sec), 3)
    seek_candidates = _fallback_seek_seconds(requested_sec)

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        current_timeout = timeout * (attempt + 1)  # 재시도마다 타임아웃 증가
        timeout_error: subprocess.TimeoutExpired | None = None
        frame_error: RuntimeError | None = None

        for seek_index, seek_sec in enumerate(seek_candidates, start=1):
            cmd = _build_extract_frame_cmd(video_path, seek_sec, q_v)
            try:
                proc = subprocess.run(
                    cmd, capture_output=True, timeout=current_timeout, check=False,
                )
            except subprocess.TimeoutExpired as exc:
                timeout_error = exc
                last_error = exc
                break

            stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
            if proc.returncode == 0 and proc.stdout:
                if seek_sec != requested_sec:
                    logger.info(
                        "ffmpeg 프레임 추출 fallback 성공: %s requested=%.3fs actual=%.3fs",
                        video_path,
                        requested_sec,
                        seek_sec,
                    )
                return proc.stdout

            is_empty_output = not proc.stdout and not stderr
            if is_empty_output and seek_index < len(seek_candidates):
                logger.warning(
                    "ffmpeg empty_output fallback: %s requested=%.3fs retry=%.3fs (%d/%d)",
                    video_path,
                    requested_sec,
                    seek_sec,
                    seek_index,
                    len(seek_candidates),
                )
                continue

            frame_error = RuntimeError(
                f"ffmpeg_frame_extract_failed:{stderr or 'empty_output'}"
            )
            last_error = frame_error
            break

        if timeout_error is not None:
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
            ) from timeout_error

        if frame_error is not None:
            if attempt < retries:
                wait_sec = 2 ** attempt
                logger.warning(
                    "ffmpeg 프레임 추출 실패 (attempt %d/%d): %s @ %.3fs — %ds 후 재시도",
                    attempt + 1, retries + 1,
                    video_path, requested_sec, wait_sec,
                )
                time.sleep(wait_sec)
                continue
            raise frame_error

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
