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

import functools
import itertools
import logging
import os
import subprocess
import threading
from pathlib import Path
from tempfile import NamedTemporaryFile

from vlm_pipeline.lib.env_utils import int_env
from vlm_pipeline.lib.video_loader import probe_duration_sec

logger = logging.getLogger(__name__)

# 2026-05-22: NVENC dual-GPU round-robin counter (thread-safe).
# `REENCODE_NVENC_GPU_INDICES` env 로 사용할 GPU index 리스트 지정 (default "0,1").
# 각 reencode 호출마다 다음 GPU 선택 → GPU 0/1 의 NVENC unit 둘 다 활용 → throughput 2× 기대.
_NVENC_GPU_COUNTER = itertools.count(0)
_NVENC_GPU_LOCK = threading.Lock()


def _nvenc_gpu_indices() -> list[int]:
    """env `REENCODE_NVENC_GPU_INDICES` ('0,1' 형태) 파싱. 빈 값이면 [0,1] (dual-GPU default)."""
    raw = (os.getenv("REENCODE_NVENC_GPU_INDICES") or "0,1").strip()
    out: list[int] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError:
            continue
    return out or [0]


def _next_nvenc_gpu_index() -> int:
    """thread-safe round-robin 으로 다음 NVENC GPU index 반환."""
    indices = _nvenc_gpu_indices()
    if len(indices) == 1:
        return indices[0]
    with _NVENC_GPU_LOCK:
        n = next(_NVENC_GPU_COUNTER)
    return indices[n % len(indices)]


# ── 표준 인코딩 프리셋 ─────────────────────────────────────────────────────────
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

STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE: list[str] = [
    "-c:v",
    "h264_nvenc",
    # 2026-05-22: `-gpu N` 은 _resolve_ffmpeg_preset_args() 에서 round-robin 동적 삽입.
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
    "-bf",
    "0",  # no B-frames (libx264 의 bframes=0 동치)
    "-no-scenecut",
    "1",  # 고정 GOP 30 유지 (libx264 의 sc_threshold=0 동치).
    # 없으면 scene cut 시 GOP 끊겨 KlingAI 패턴 재현 위험.
    "-preset",
    "p4",  # NVENC 프리셋 (p1=fastest, p7=best quality)
    "-movflags",
    "+faststart",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
]

# 하위 호환: 기존 import 가 STANDARD_PRESET_FFMPEG_ARGS_NVENC 를 직접 참조하는 경우.
# 정적 리스트로는 round-robin 못 하니 GPU 0 fallback (테스트/import-time 용).
# `-gpu N` 은 `-c:v h264_nvenc` 한 쌍 다음에 삽입해야 ffmpeg 가 codec 선택 후 옵션 적용.
STANDARD_PRESET_FFMPEG_ARGS_NVENC: list[str] = (
    STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE[:2] + ["-gpu", "0"] + STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE[2:]
)
# 주의: libx264 의 `-x264-params repeat-headers=1` 는 RTSP 스트리밍 시 SPS/PPS 를 매 IDR
# 앞에 inline 하는 옵션이지만, 재인코딩 산출물은 archive→MinIO→ML 처리 경로라
# 중간 join 이 없어 NVENC 에서는 일부러 생략. 필요해지면 `-bsf:v dump_extra` 추가.

# keyframe 비율이 이 값 미만이면 재인코딩 필요 (KlingAI 패턴 감지)
KEYFRAME_RATIO_THRESHOLD = 0.02


@functools.lru_cache(maxsize=1)
def _nvenc_available() -> bool:
    """ffmpeg 가 h264_nvenc 인코더를 지원하는지 1회 확인 (caching)."""
    try:
        proc = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return proc.returncode == 0 and "h264_nvenc" in proc.stdout
    except (subprocess.TimeoutExpired, OSError):
        return False


def _resolve_ffmpeg_preset_args() -> list[str]:
    """env REENCODE_USE_NVENC=true 이고 NVENC 가용시 GPU 인코딩, 아니면 libx264.

    NVENC: REENCODE_NVENC_GPU_INDICES (default '0,1') 에서 round-robin 으로 GPU 선택
    → GPU 당 NVENC unit 1개씩 (RTX A4000) 라 2개 GPU 활용 시 throughput 2× 기대.

    Returns:
        ffmpeg arg list (preset 전체)
    """
    use_nvenc = os.getenv("REENCODE_USE_NVENC", "false").strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    if use_nvenc and _nvenc_available():
        gpu_idx = _next_nvenc_gpu_index()
        logger.info(f"reencode_encoder=h264_nvenc gpu={gpu_idx} (REENCODE_USE_NVENC=true)")
        # `-c:v h264_nvenc` 한 쌍을 함께 유지하고 그 다음에 `-gpu N` 삽입.
        # 잘못 끊으면 ffmpeg 가 `-c:v -gpu` 로 해석 → codec 미선택 → reencode_failed.
        return (
            STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE[:2]
            + ["-gpu", str(gpu_idx)]
            + STANDARD_PRESET_FFMPEG_ARGS_NVENC_BASE[2:]
        )
    if use_nvenc:
        logger.info("reencode_encoder=libx264 (REENCODE_USE_NVENC=true but NVENC not available, fallback)")
    else:
        logger.info("reencode_encoder=libx264 (REENCODE_USE_NVENC not set)")
    return list(STANDARD_PRESET_FFMPEG_ARGS)


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


_probe_duration_sec = probe_duration_sec

# 타임아웃 = max(기본값, 영상길이 × 배수) — 긴 영상도 안전하게 처리
_REENCODE_TIMEOUT_MULTIPLIER = 3.0
_REENCODE_TIMEOUT_MIN_SEC = 600
_REENCODE_TIMEOUT_MAX_SEC = 7200


def _compute_reencode_timeout(src_path: Path) -> int:
    """영상 길이에 비례하는 타임아웃을 계산.

    기본값(VIDEO_REENCODE_TIMEOUT_SEC)과 영상길이×배수 중 큰 값을 사용.
    10분 영상 → 최소 1800초, 1시간 영상 → 최소 10800초(cap 7200초).
    """
    base_timeout = int_env("VIDEO_REENCODE_TIMEOUT_SEC", _REENCODE_TIMEOUT_MIN_SEC, 60)
    duration = _probe_duration_sec(src_path)
    if duration is None or duration <= 0:
        return base_timeout

    dynamic_timeout = int(duration * _REENCODE_TIMEOUT_MULTIPLIER)
    timeout = max(base_timeout, dynamic_timeout)
    return min(timeout, int_env("VIDEO_REENCODE_TIMEOUT_MAX_SEC", _REENCODE_TIMEOUT_MAX_SEC, 600))


def reencode_with_fallback(
    src_path: Path,
    *,
    threads: int = 4,
    max_retries: int = 3,
    backoff_base_sec: float = 0.5,
) -> tuple[Path | None, str | None]:
    """reencode_to_tmp 를 retry 와 fallback 으로 감싼다.

    Returns:
        (tmp_path, None) on success
        (None, error_reason) on fallback (모든 retry 실패)
    """
    import time

    # 방어: 0 이하 또는 비정상 값 cap (Codex 권장)
    max_retries = max(1, min(10, int(max_retries)))

    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return reencode_to_tmp(src_path, threads=threads), None
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                time.sleep(backoff_base_sec * (2**attempt))
    reason = f"fallback:{type(last_exc).__name__}:{str(last_exc)[:180]}"
    return None, reason


def reencode_to_tmp(src_path: Path, threads: int = 4) -> Path:
    """표준 스펙으로 재인코딩 → 로컬 임시 파일 경로 반환.

    호출자가 반환된 Path 사용 후 직접 삭제해야 합니다 (finally 블록 권장).
    타임아웃은 영상 길이에 비례하여 자동 계산된다.

    Args:
        src_path: 원본 비디오 파일 경로
        threads: ffmpeg 인코딩 스레드 수 (기본 4)

    Returns:
        재인코딩된 임시 파일 경로

    Raises:
        RuntimeError: ffmpeg 타임아웃 / 실행 실패 / 비정상 종료 시
    """
    timeout_sec = _compute_reencode_timeout(src_path)
    suffix = src_path.suffix or ".mp4"

    with NamedTemporaryFile(suffix=suffix, prefix="reencode_", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src_path),
        *_resolve_ffmpeg_preset_args(),
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
