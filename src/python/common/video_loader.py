import json
import subprocess
from datetime import datetime
from pathlib import Path

from .checksum import sha256sum


def _parse_non_negative_int(raw_value: object) -> int:
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
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except (subprocess.TimeoutExpired, OSError):
        return 0
    if proc.returncode != 0:
        return 0

    for line in (proc.stdout or "").splitlines():
        frame_count = _parse_non_negative_int(line)
        if frame_count > 0:
            return frame_count
    return 0


def load_video_once(path: str | Path) -> dict:
    """Run ffprobe once and return checksum + video metadata."""
    file_path = Path(path)
    checksum = sha256sum(file_path)
    stat = file_path.stat()

    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(file_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe_failed:{proc.stderr.strip()}")

    parsed = json.loads(proc.stdout or "{}")
    streams = parsed.get("streams", [])
    format_info = parsed.get("format", {})

    video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
    has_audio = any(s.get("codec_type") == "audio" for s in streams)

    width = int(video_stream.get("width") or 0)
    height = int(video_stream.get("height") or 0)
    codec = str(video_stream.get("codec_name") or "")
    fps_raw = str(video_stream.get("avg_frame_rate") or "0/1")
    fps = 0.0
    if "/" in fps_raw:
        num, den = fps_raw.split("/", 1)
        try:
            den_f = float(den)
            fps = float(num) / den_f if den_f else 0.0
        except ValueError:
            fps = 0.0

    duration_sec = float(format_info.get("duration") or 0.0)
    bitrate = int(format_info.get("bit_rate") or 0)
    frame_count = _parse_non_negative_int(video_stream.get("nb_frames"))
    if frame_count <= 0:
        frame_count = _probe_frame_count_fallback(file_path)

    return {
        "file_size": stat.st_size,
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
            "extracted_at": datetime.now(),
        },
    }
