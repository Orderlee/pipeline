"""비디오 인코딩 품질 검사 + 표준 스펙 재인코딩.

Layer 1: 순수 Python, Dagster 의존 없음.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

from vlm_pipeline.lib.env_utils import int_env


STANDARD_PRESET_NAME = "standard"

STANDARD_PRESET_FFMPEG_ARGS: list[str] = [
    "-c:v",
    "libx264",
    "-profile:v",
    "baseline",
    "-level",
    "4.2",
    "-pix_fmt",
    "yuv420p",
    "-g",
    "30",
    "-keyint_min",
    "30",
    "-sc_threshold",
    "0",
    "-x264-params",
    "bframes=0:repeat-headers=1",
    "-preset",
    "veryfast",
    "-movflags",
    "+faststart",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
]

KEYFRAME_RATIO_THRESHOLD = 0.02


def _force_reencode_all() -> bool:
    return os.getenv("FORCE_REENCODE_ALL", "false").strip().lower() == "true"


def probe_keyframe_info(file_path: Path, sample_packets: int = 300) -> float | None:
    timeout_sec = int_env("VIDEO_FFPROBE_TIMEOUT_SEC", 120, 10)
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_packets",
        "-show_entries",
        "packet=flags",
        "-read_intervals",
        f"%+#{sample_packets}",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except (subprocess.TimeoutExpired, OSError):
        return None

    if proc.returncode != 0:
        return None

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    total = len(lines)
    if total == 0:
        return None

    keyframes = sum(1 for line in lines if line.startswith("K"))
    return keyframes / total


def needs_reencode(video_meta: dict, file_path: Path) -> tuple[bool, str | None]:
    if _force_reencode_all():
        return True, "force_reencode_all"

    codec = str(video_meta.get("original_codec") or "").strip().lower()
    profile = str(video_meta.get("original_profile") or "").strip().lower()
    has_b_frames = bool(video_meta.get("original_has_b_frames") or False)
    try:
        level = int(video_meta.get("original_level_int") or 0)
    except (TypeError, ValueError):
        level = 0

    if codec != "h264":
        return True, f"codec={codec}"
    if "baseline" not in profile:
        return True, f"profile={profile}"
    if has_b_frames:
        return True, "has_b_frames"
    if level > 42:
        return True, f"level={level}"

    ratio = probe_keyframe_info(file_path)
    if ratio is not None and ratio < KEYFRAME_RATIO_THRESHOLD:
        return True, f"keyframe_ratio={ratio:.4f}"

    return False, None


def reencode_to_tmp(src_path: Path, threads: int = 4) -> Path:
    timeout_sec = int_env("VIDEO_REENCODE_TIMEOUT_SEC", 600, 60)
    suffix = src_path.suffix or ".mp4"

    with NamedTemporaryFile(suffix=suffix, prefix="reencode_", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src_path),
        *STANDARD_PRESET_FFMPEG_ARGS,
        "-threads",
        str(max(1, int(threads))),
        str(tmp_path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"reencode_timeout:{timeout_sec}s path={src_path}") from exc
    except OSError as exc:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"reencode_exec_failed:{exc}") from exc

    if proc.returncode != 0:
        tmp_path.unlink(missing_ok=True)
        stderr_tail = (proc.stderr or "")[-300:].strip()
        raise RuntimeError(f"reencode_failed:returncode={proc.returncode} stderr={stderr_tail}")

    return tmp_path
