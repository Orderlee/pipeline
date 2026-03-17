"""비디오 메타데이터 추출 — ffprobe 1회 호출.

Layer 1: 순수 Python, Dagster 의존 없음.
"""

import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from tempfile import SpooledTemporaryFile

from vlm_pipeline.lib.env_utils import int_env

from .checksum import sha256sum
from .video_env import classify_video_environment


def _float_env(name: str, default: float, minimum: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def _run_ffprobe_with_retry(cmd: list[str], timeout_sec: int) -> subprocess.CompletedProcess:
    """ffprobe 실행 (timeout/OSError는 재시도)."""
    retry_count = int_env("VIDEO_FFPROBE_RETRY_COUNT", 1, 0)
    retry_delay_ms = int_env("VIDEO_FFPROBE_RETRY_DELAY_MS", 250, 0)
    timeout_backoff = _float_env("VIDEO_FFPROBE_TIMEOUT_BACKOFF", 2.0, 1.0)
    current_timeout = max(1, int(timeout_sec))

    for attempt in range(retry_count + 1):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=current_timeout)
        except subprocess.TimeoutExpired as exc:
            if attempt >= retry_count:
                raise RuntimeError(
                    f"ffprobe_timeout:{current_timeout}s path={cmd[-1]}"
                ) from exc
            next_timeout = int(current_timeout * timeout_backoff)
            current_timeout = max(current_timeout + 1, next_timeout)
        except OSError as exc:
            if attempt >= retry_count:
                raise RuntimeError(f"ffprobe_exec_failed:{exc}") from exc

        if retry_delay_ms > 0:
            time.sleep(retry_delay_ms / 1000.0)

    # 방어 코드 (실제 도달하지 않음)
    raise RuntimeError("ffprobe_unexpected_retry_state")


def _stream_video_with_checksum(
    file_path: Path,
    chunk_size: int = 1024 * 1024,
    spool_max_size_mb: int = 64,
) -> tuple[SpooledTemporaryFile, int, str]:
    """비디오를 1회 읽어 업로드용 stream + checksum + file_size를 생성."""
    from hashlib import sha256

    max_size = max(1, int(spool_max_size_mb)) * 1024 * 1024
    stream = SpooledTemporaryFile(max_size=max_size, mode="w+b")
    hasher = sha256()
    total_size = 0

    with file_path.open("rb") as src:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            total_size += len(chunk)
            hasher.update(chunk)
            stream.write(chunk)

    stream.seek(0)
    return stream, total_size, hasher.hexdigest()


def _parse_non_negative_int(raw_value: object) -> int:
    """숫자 문자열/정수를 안전하게 int로 변환."""
    if raw_value is None:
        return 0
    try:
        text = str(raw_value).strip()
    except Exception:
        return 0
    if not text.isdigit():
        return 0
    return int(text)


def _probe_frame_count_fallback(file_path: Path) -> int:
    """nb_frames 미제공 영상에서 ffprobe -count_frames로 프레임 수 재조회."""
    timeout_sec = int_env("VIDEO_FFPROBE_COUNT_TIMEOUT_SEC", 120, 10)
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-count_frames",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=nb_read_frames",
        "-of",
        "default=nk=1:nw=1",
        str(file_path),
    ]
    try:
        proc = _run_ffprobe_with_retry(cmd, timeout_sec=timeout_sec)
    except RuntimeError:
        return 0
    if proc.returncode != 0:
        return 0

    for line in (proc.stdout or "").splitlines():
        frame_count = _parse_non_negative_int(line)
        if frame_count > 0:
            return frame_count
    return 0


def load_video_once(path: str | Path, include_file_stream: bool = False) -> dict:
    """ffprobe 1회 호출로 비디오 메타데이터 + checksum 추출.

    Returns:
        dict with keys:
          - file_size: int
          - checksum: str (SHA-256)
          - file_stream: SpooledTemporaryFile (include_file_stream=True일 때만)
          - video_metadata: dict (width, height, duration_sec, fps, codec, bitrate, frame_count, has_audio)
    """
    file_path = Path(path)

    file_stream = None
    if include_file_stream:
        file_stream, file_size, checksum = _stream_video_with_checksum(file_path)
    else:
        checksum = sha256sum(file_path)
        file_size = file_path.stat().st_size

    # ffprobe 1회 호출
    timeout_sec = int_env("VIDEO_FFPROBE_TIMEOUT_SEC", 120, 10)
    cmd = [
        "ffprobe",
        "-v", "error",
        "-print_format", "json",
        "-show_format", "-show_streams",
        str(file_path),
    ]
    proc = _run_ffprobe_with_retry(cmd, timeout_sec=timeout_sec)
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe_failed:{proc.stderr.strip()}")

    parsed = json.loads(proc.stdout or "{}")
    streams = parsed.get("streams", [])
    format_info = parsed.get("format", {})

    video_stream = next(
        (s for s in streams if s.get("codec_type") == "video"), {}
    )
    has_audio = any(s.get("codec_type") == "audio" for s in streams)

    width = int(video_stream.get("width") or 0)
    height = int(video_stream.get("height") or 0)
    codec = str(video_stream.get("codec_name") or "")

    # fps 파싱 (예: "30000/1001" → 29.97)
    fps_raw = str(video_stream.get("avg_frame_rate") or "0/1")
    fps = 0.0
    if "/" in fps_raw:
        num_s, den_s = fps_raw.split("/", 1)
        try:
            den_f = float(den_s)
            fps = float(num_s) / den_f if den_f else 0.0
        except ValueError:
            fps = 0.0

    duration_sec = float(format_info.get("duration") or 0.0)
    bitrate = int(format_info.get("bit_rate") or 0)
    frame_count = _parse_non_negative_int(video_stream.get("nb_frames"))
    if frame_count <= 0:
        frame_count = _probe_frame_count_fallback(file_path)

    # data_in_out_check.py + data_in_out_check_2.py 로직 결합:
    # Places365(CUDA 우선) 시도 후 실패 시 heuristic voting fallback
    try:
        env_meta = classify_video_environment(file_path, duration_sec)
    except Exception:
        env_meta = {
            "environment_type": None,
            "daynight_type": None,
            "outdoor_score": None,
            "avg_brightness": None,
            "env_method": None,
        }

    result = {
        "file_size": file_size,
        "checksum": checksum,
        "video_metadata": {
            "width": width,
            "height": height,
            "duration_sec": duration_sec,
            "fps": fps,
            "codec": codec,
            "bitrate": bitrate,
            "frame_count": frame_count,
            "has_audio": has_audio,
            "environment_type": env_meta.get("environment_type"),
            "daynight_type": env_meta.get("daynight_type"),
            "outdoor_score": env_meta.get("outdoor_score"),
            "avg_brightness": env_meta.get("avg_brightness"),
            "env_method": env_meta.get("env_method"),
            "extracted_at": datetime.now(),
        },
    }
    if file_stream is not None:
        result["file_stream"] = file_stream
    return result
