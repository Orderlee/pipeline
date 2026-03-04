"""비디오 환경 메타데이터 추출.

data_in_out_check.py + data_in_out_check_2.py 아이디어를 합쳐서:
- N개 프레임 샘플링 후 voting
- day/night(밝기) + indoor/outdoor(Places365 우선, 실패 시 heuristic fallback)

주의:
- CUDA 우선 정책: torch.cuda.is_available()면 CUDA로 추론.
- torch/torchvision 또는 모델 로딩 실패 시 ingest를 막지 않고 heuristic으로 fallback.
"""

from __future__ import annotations

import importlib.util
import io
import os
import shutil
import subprocess
import urllib.request
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

try:
    import torch
    from torch.nn import functional as F
    from torchvision import transforms as T
except Exception:  # pragma: no cover - optional dependency
    torch = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _sample_timestamps(duration_sec: float, sample_count: int) -> list[float]:
    if sample_count <= 1:
        return [max(0.0, duration_sec * 0.5)] if duration_sec > 0 else [0.0]
    if duration_sec <= 0:
        return [float(i) for i in range(sample_count)]
    return [duration_sec * (i + 1) / (sample_count + 1) for i in range(sample_count)]


@lru_cache(maxsize=1)
def _ffmpeg_available() -> bool:
    try:
        proc = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return proc.returncode == 0
    except Exception:
        return False


def _extract_frame(video_path: Path, sec: float, timeout_sec: int = 20) -> Image.Image | None:
    if not _ffmpeg_available():
        return None

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{sec:.3f}",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-f",
        "image2pipe",
        "-vcodec",
        "png",
        "pipe:1",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout_sec, check=False)
    except Exception:
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    try:
        img = Image.open(io.BytesIO(proc.stdout))
        img.load()
        return img.convert("RGB")
    except Exception:
        return None


def _analyze_heuristic(frame_rgb: Image.Image, day_night_threshold: float) -> tuple[str, str, float]:
    """data_in_out_check_2.py 기반 heuristic."""
    hsv = np.asarray(frame_rgb.convert("HSV"), dtype=np.uint8)
    h = hsv[:, :, 0].astype(np.float32)
    s = hsv[:, :, 1].astype(np.float32)
    v = hsv[:, :, 2].astype(np.float32)

    brightness = float(v.mean())
    daynight = "day" if brightness >= day_night_threshold else "night"

    # PIL HSV 기준 blue mask (OpenCV 범위 근사)
    blue_mask = (h >= 127) & (h <= 184) & (s >= 50) & (v >= 70)
    blue_ratio = float(blue_mask.mean())

    hgt = v.shape[0]
    top = v[: max(1, hgt // 3), :]
    bottom = v[max(0, (2 * hgt) // 3) :, :]
    top_mean = float(top.mean()) if top.size else brightness
    bottom_mean = float(bottom.mean()) if bottom.size else brightness

    is_outdoor = blue_ratio > 0.03 or (top_mean > (bottom_mean * 1.5) and brightness > 100.0)
    return ("outdoor" if is_outdoor else "indoor"), daynight, brightness


def _download_if_missing(url: str, dst: Path) -> None:
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=30) as resp, dst.open("wb") as fh:
        shutil.copyfileobj(resp, fh)


def _recursion_change_bn(module):
    if torch is None:
        return module
    if isinstance(module, torch.nn.BatchNorm2d):
        module.track_running_stats = 1
    else:
        for _, child in module._modules.items():
            _recursion_change_bn(child)
    return module


def _resolve_torch_device():
    """CUDA 우선: 가능하면 CUDA, 아니면 CPU."""
    if torch is None:
        return None
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


@lru_cache(maxsize=1)
def _load_places365_runtime() -> dict[str, Any] | None:
    """Places365 runtime 준비 (optional)."""
    use_places = _env_bool("VIDEO_ENV_USE_PLACES365", True)
    if not use_places:
        return None
    if torch is None or F is None or T is None:
        return None

    device = _resolve_torch_device()
    if device is None:
        return None

    model_dir = Path(os.getenv("VIDEO_ENV_MODEL_DIR", "/tmp/places365"))
    model_file = model_dir / "wideresnet18_places365.pth.tar"
    wideresnet_file = model_dir / "wideresnet.py"
    io_file = model_dir / "IO_places365.txt"
    auto_download = _env_bool("VIDEO_ENV_AUTO_DOWNLOAD", True)

    try:
        if auto_download:
            _download_if_missing(
                "http://places2.csail.mit.edu/models_places365/wideresnet18_places365.pth.tar",
                model_file,
            )
            _download_if_missing(
                "https://raw.githubusercontent.com/csailvision/places365/master/wideresnet.py",
                wideresnet_file,
            )
            _download_if_missing(
                "https://raw.githubusercontent.com/csailvision/places365/master/IO_places365.txt",
                io_file,
            )
        else:
            if not (model_file.exists() and wideresnet_file.exists() and io_file.exists()):
                return None

        spec = importlib.util.spec_from_file_location("places365_wideresnet", wideresnet_file)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        model = module.resnet18(num_classes=365)
        checkpoint = torch.load(model_file, map_location="cpu")
        state_dict = {k.replace("module.", ""): v for k, v in checkpoint["state_dict"].items()}
        model.load_state_dict(state_dict)
        model = _recursion_change_bn(model)
        model.avgpool = torch.nn.AvgPool2d(kernel_size=14, stride=1, padding=0)
        model.eval()
        model.to(device)

        labels_io = []
        with io_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                items = line.rstrip().split()
                labels_io.append(int(items[-1]) - 1)  # 0 indoor / 1 outdoor

        transform = T.Compose(
            [
                T.Resize((224, 224)),
                T.ToTensor(),
                T.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
            ]
        )

        return {
            "model": model,
            "labels_io": np.asarray(labels_io, dtype=np.float32),
            "transform": transform,
            "device": device,
        }
    except Exception:
        return None


def _places_outdoor_score(frame_rgb: Image.Image, top_k: int) -> float | None:
    runtime = _load_places365_runtime()
    if runtime is None or torch is None or F is None:
        return None

    model = runtime["model"]
    labels_io = runtime["labels_io"]
    transform = runtime["transform"]
    device = runtime["device"]

    with torch.no_grad():
        x = transform(frame_rgb).unsqueeze(0).to(device)
        probs = F.softmax(model(x), dim=1)[0].cpu().numpy()

    idx = np.argsort(probs)[::-1][:top_k]
    return float(labels_io[idx].mean())


def classify_video_environment(video_path: str | Path, duration_sec: float) -> dict[str, Any]:
    """비디오 환경 메타데이터 추출 (ingest 실패 방지용 안전 fallback 포함)."""
    path = Path(video_path)
    sample_count = max(1, int(os.getenv("VIDEO_ENV_SAMPLE_COUNT", "3")))
    day_night_threshold = float(os.getenv("VIDEO_DAY_NIGHT_THRESHOLD", "90"))
    places_top_k = max(1, int(os.getenv("VIDEO_ENV_TOP_K", "10")))

    timestamps = _sample_timestamps(duration_sec, sample_count)
    frames = [f for ts in timestamps if (f := _extract_frame(path, ts)) is not None]

    if not frames:
        return {
            "environment_type": None,
            "daynight_type": None,
            "outdoor_score": None,
            "avg_brightness": None,
            "env_method": None,
        }

    io_votes: list[str] = []
    daynight_votes: list[str] = []
    brightness_values: list[float] = []
    places_scores: list[float] = []

    for frame in frames:
        io_label, daynight, brightness = _analyze_heuristic(frame, day_night_threshold)
        io_votes.append(io_label)
        daynight_votes.append(daynight)
        brightness_values.append(brightness)

        pscore = _places_outdoor_score(frame, places_top_k)
        if pscore is not None:
            places_scores.append(pscore)

    if places_scores:
        avg_outdoor_score = float(np.mean(places_scores))
        env_label = "outdoor" if avg_outdoor_score >= 0.5 else "indoor"
        runtime = _load_places365_runtime()
        device = runtime["device"].type if runtime else "cpu"
        env_method = f"places365_{device}"
    else:
        env_label = Counter(io_votes).most_common(1)[0][0]
        avg_outdoor_score = float(sum(1 for v in io_votes if v == "outdoor") / len(io_votes))
        env_method = "heuristic"

    daynight_label = Counter(daynight_votes).most_common(1)[0][0]
    avg_brightness = float(np.mean(brightness_values))

    return {
        "environment_type": env_label,
        "daynight_type": daynight_label,
        "outdoor_score": avg_outdoor_score,
        "avg_brightness": avg_brightness,
        "env_method": env_method,
    }

