"""비디오 인코딩 품질 검사 + 표준 스펙 재인코딩.

Layer 1: 순수 Python, Dagster 의존 없음.

표준 스펙 (MACS RTSP WebRTC 호환 + cv2 seek 안정성 통합):
  - H.264 Baseline profile, level 4.2
  - GOP 30 (I-frame 30프레임마다 고정, sc_threshold=0)
  - B-frame 없음 (bframes=0)
  - yuv420p, faststart, AAC 128kbps

참고:
  - KlingAI 생성영상: I-frame 1개 문제 → GOP 30으로 해결
  - MACS RTSP 송출: Baseline + no B-frame 조건 충족
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

from vlm_pipeline.lib.env_utils import int_env


# ── 표준 인코딩 프리셋 ─────────────────────────────────────────────────────────
STANDARD_PRESET_NAME = "standard"

STANDARD_PRESET_FFMPEG_ARGS: list[str] = [
    "-c:v", "libx264",
    "-profile:v", "baseline",
    "-level", "4.2",
    "-pix_fmt", "yuv420p",
    "-g", "30",
    "-keyint_min", "30",
    "-sc_threshold", "0",
    "-x264-params", "bframes=0:repeat-headers=1",
    "-preset", "veryfast",
    "-movflags", "+faststart",
    "-c:a", "aac",
    "-b:a", "128k",
]

# keyframe 비율이 이 값 미만이면 재인코딩 필요 (KlingAI 패턴 감지)
KEYFRAME_RATIO_THRESHOLD = 0.02


def _force_reencode_all() -> bool:
    """FORCE_REENCODE_ALL=true 이면 조건 무관하게 전체 재인코딩."""
    return os.getenv("FORCE_REENCODE_ALL", "false").strip().lower() == "true"


def probe_keyframe_info(file_path: Path, sample_packets: int = 300) -> float | None:
    """처음 N 패킷만 샘플링해 keyframe 비율을 추정.

    ffprobe -read_intervals 를 사용해 전체 파일을 스캔하지 않고 빠르게 처리.

    Args:
        file_path: 검사할 비디오 파일 경로
        sample_packets: 샘플링할 패킷 수 (기본 300)

    Returns:
        keyframe_ratio: keyframe 수 / 전체 샘플 패킷 수 (0.0 ~ 1.0)
        None: ffprobe 실패 시
    """
    timeout_sec = int_env("VIDEO_FFPROBE_TIMEOUT_SEC", 120, 10)
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_packets",
        "-show_entries", "packet=flags",
        "-read_intervals", f"%+#{sample_packets}",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout_sec
        )
    except (subprocess.TimeoutExpired, OSError):
        return None

    if proc.returncode != 0:
        return None

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    total = len(lines)
    if total == 0:
        return None

    # 'K_' 또는 'K__' 등 K로 시작하는 줄이 keyframe
    keyframes = sum(1 for line in lines if line.startswith("K"))
    return keyframes / total


def needs_reencode(video_meta: dict, file_path: Path) -> tuple[bool, str | None]:
    """인코딩 품질 판정.

    빠른 체크(기존 ffprobe 결과 재사용) → 조건 통과 시에만 keyframe probe 호출.

    Args:
        video_meta: load_video_once()의 video_metadata dict
                    (original_codec, original_profile, original_has_b_frames,
                     original_level_int 필드 포함)
        file_path: 원본 파일 경로 (keyframe probe 필요 시 사용)

    Returns:
        (reencode_required, reason)
        reason 예: "codec=hevc", "profile=high", "has_b_frames",
                   "level=51", "keyframe_ratio=0.0067", "force_reencode_all"
    """
    if _force_reencode_all():
        return True, "force_reencode_all"

    codec = str(video_meta.get("original_codec") or "").strip().lower()
    profile = str(video_meta.get("original_profile") or "").strip().lower()
    has_b_frames = bool(video_meta.get("original_has_b_frames") or False)
    try:
        level = int(video_meta.get("original_level_int") or 0)
    except (TypeError, ValueError):
        level = 0

    # ── 빠른 체크 (추가 ffprobe 없음) ───────────────────────────────────────
    if codec != "h264":
        return True, f"codec={codec}"
    if "baseline" not in profile:
        return True, f"profile={profile}"
    if has_b_frames:
        return True, "has_b_frames"
    if level > 42:
        return True, f"level={level}"

    # ── 위 조건 모두 통과 시에만 keyframe probe (경량 추가 ffprobe 1회) ──────
    ratio = probe_keyframe_info(file_path)
    if ratio is not None and ratio < KEYFRAME_RATIO_THRESHOLD:
        return True, f"keyframe_ratio={ratio:.4f}"

    return False, None


def reencode_to_tmp(src_path: Path, threads: int = 4) -> Path:
    """표준 스펙으로 재인코딩 → 로컬 임시 파일 경로 반환.

    호출자가 반환된 Path 사용 후 직접 삭제해야 합니다 (finally 블록 권장).

    Args:
        src_path: 원본 비디오 파일 경로
        threads: ffmpeg 인코딩 스레드 수 (기본 4)

    Returns:
        재인코딩된 임시 파일 경로

    Raises:
        RuntimeError: ffmpeg 타임아웃 / 실행 실패 / 비정상 종료 시
    """
    timeout_sec = int_env("VIDEO_REENCODE_TIMEOUT_SEC", 600, 60)
    suffix = src_path.suffix or ".mp4"

    with NamedTemporaryFile(suffix=suffix, prefix="reencode_", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    cmd = [
        "ffmpeg", "-y",
        "-i", str(src_path),
        *STANDARD_PRESET_FFMPEG_ARGS,
        "-threads", str(max(1, int(threads))),
        str(tmp_path),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout_sec
        )
    except subprocess.TimeoutExpired as exc:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"reencode_timeout:{timeout_sec}s path={src_path}"
        ) from exc
    except OSError as exc:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"reencode_exec_failed:{exc}") from exc

    if proc.returncode != 0:
        tmp_path.unlink(missing_ok=True)
        stderr_tail = (proc.stderr or "")[-300:].strip()
        raise RuntimeError(
            f"reencode_failed:returncode={proc.returncode} stderr={stderr_tail}"
        )

    return tmp_path
