"""SAM3 FastAPI server for shadow segmentation/bbox benchmarking."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager, nullcontext
from io import BytesIO
from typing import Any

import numpy as np
import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from PIL import Image

logger = logging.getLogger("sam3-server")

PORT = int(os.environ.get("SAM3_PORT", "8002"))
PREFERRED_DEVICE = str(os.environ.get("SAM3_DEVICE", "cuda:0") or "cuda:0").strip()
DEFAULT_SCORE_THRESHOLD = float(os.environ.get("SAM3_SCORE_THRESHOLD", "0.0"))
DEFAULT_MAX_MASKS_PER_PROMPT = int(os.environ.get("SAM3_MAX_MASKS_PER_PROMPT", "50"))

_model = None
_processor = None
_device = PREFERRED_DEVICE
_model_loaded_at: float | None = None
_load_error: str | None = None
_predict_lock = threading.Lock()


def _load_torch():
    import torch

    return torch


def _resolve_device() -> str:
    torch = _load_torch()
    preferred = PREFERRED_DEVICE.lower()
    if preferred.startswith("cuda") and torch.cuda.is_available():
        return PREFERRED_DEVICE
    if preferred.startswith("cuda"):
        logger.warning("SAM3 CUDA device requested but unavailable, falling back to CPU")
    return "cpu"


def _autocast_context():
    torch = _load_torch()
    if str(_device or "").lower().startswith("cuda"):
        return torch.autocast(device_type="cuda", dtype=torch.bfloat16)
    return nullcontext()


def _load_model() -> None:
    global _model, _processor, _device, _model_loaded_at, _load_error
    try:
        from sam3.model.sam3_image_processor import Sam3Processor
        from sam3.model_builder import build_sam3_image_model

        _device = _resolve_device()
        checkpoint_path = os.environ.get("SAM3_CHECKPOINT_PATH") or None
        load_from_hf = checkpoint_path is None
        logger.info("SAM3 loading on device=%s checkpoint=%s hf=%s", _device, checkpoint_path, load_from_hf)
        _model = build_sam3_image_model(
            device=_device,
            checkpoint_path=checkpoint_path,
            load_from_HF=load_from_hf,
        )
        if hasattr(_model, "to"):
            _model = _model.to(_device)
        _processor = Sam3Processor(_model, device=_device)
        _model_loaded_at = time.time()
        _load_error = None
        logger.info("SAM3 model loaded")
    except Exception as exc:  # noqa: BLE001
        _model = None
        _processor = None
        _model_loaded_at = None
        _load_error = str(exc)
        logger.exception("SAM3 model load failed: %s", exc)


def _to_numpy(value: Any) -> np.ndarray:
    if value is None:
        return np.asarray([])
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if getattr(value, "dtype", None) is not None:
        torch = _load_torch()
        if value.dtype == torch.bfloat16:
            value = value.float()
    if hasattr(value, "numpy"):
        return np.asarray(value.numpy())
    return np.asarray(value)


def _normalize_box(box: Any) -> list[float] | None:
    values = _to_numpy(box).reshape(-1).tolist()
    if len(values) < 4:
        return None
    x1, y1, x2, y2 = [float(values[idx]) for idx in range(4)]
    left = min(x1, x2)
    top = min(y1, y2)
    right = max(x1, x2)
    bottom = max(y1, y2)
    if right <= left or bottom <= top:
        return None
    return [round(left, 2), round(top, 2), round(right, 2), round(bottom, 2)]


def _coerce_mask_2d(mask: Any) -> np.ndarray | None:
    array = np.asarray(mask)
    array = np.squeeze(array)
    while array.ndim > 2:
        array = array[0]
    if array.ndim != 2:
        return None
    return np.asarray(array, dtype=np.uint8)


def _mask_to_bbox(mask: np.ndarray) -> list[float] | None:
    normalized_mask = _coerce_mask_2d(mask)
    if normalized_mask is None:
        return None
    ys, xs = np.where(normalized_mask > 0)
    if len(xs) == 0 or len(ys) == 0:
        return None
    left = float(xs.min())
    top = float(ys.min())
    right = float(xs.max() + 1)
    bottom = float(ys.max() + 1)
    return [round(left, 2), round(top, 2), round(right, 2), round(bottom, 2)]


def _encode_mask_rle(mask: np.ndarray) -> dict[str, Any]:
    flat = mask.astype(np.uint8).reshape(-1)
    counts: list[int] = []
    last_value = 0
    run_length = 0
    for raw_value in flat:
        value = int(raw_value)
        if value == last_value:
            run_length += 1
            continue
        counts.append(run_length)
        run_length = 1
        last_value = value
    counts.append(run_length)
    return {
        "order": "row-major",
        "size": [int(mask.shape[0]), int(mask.shape[1])],
        "counts": counts,
    }


def _parse_prompts(raw_value: str | None) -> list[str]:
    rendered = str(raw_value or "").strip()
    if not rendered:
        return []
    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            if isinstance(parsed, list):
                prompts = [str(item).strip().lower() for item in parsed if str(item).strip()]
                return list(dict.fromkeys(prompts))
    except Exception:
        logger.warning("SAM3 prompts_json parse failed, retrying as comma-separated")
    prompts = [part.strip().lower() for part in rendered.replace("\n", ",").split(",") if part.strip()]
    return list(dict.fromkeys(prompts))


def _parse_prompt_score_thresholds(raw_value: str | None) -> dict[str, float]:
    rendered = str(raw_value or "").strip()
    if not rendered:
        return {}
    try:
        parsed = json.loads(rendered)
    except Exception:
        logger.warning("SAM3 per_prompt_score_thresholds_json parse failed", exc_info=True)
        return {}
    if not isinstance(parsed, dict):
        return {}

    thresholds: dict[str, float] = {}
    for key, value in parsed.items():
        prompt = str(key or "").strip().lower()
        if not prompt:
            continue
        try:
            thresholds[prompt] = float(value)
        except (TypeError, ValueError):
            continue
    return thresholds


def _resolve_cuda_device_index() -> int | None:
    rendered = str(_device or "").strip().lower()
    if not rendered.startswith("cuda"):
        return None
    if ":" not in rendered:
        return 0
    try:
        return int(rendered.split(":", 1)[1])
    except ValueError:
        return 0


def _reset_gpu_peak_memory() -> None:
    torch = _load_torch()
    device_idx = _resolve_cuda_device_index()
    if device_idx is None or not torch.cuda.is_available():
        return
    try:
        torch.cuda.reset_peak_memory_stats(device_idx)
    except Exception:
        logger.debug("SAM3 peak memory reset skipped", exc_info=True)


def _gpu_peak_memory_gb() -> float | None:
    torch = _load_torch()
    device_idx = _resolve_cuda_device_index()
    if device_idx is None or not torch.cuda.is_available():
        return None
    try:
        return round(float(torch.cuda.max_memory_allocated(device_idx)) / 1e9, 3)
    except Exception:
        logger.debug("SAM3 peak memory read skipped", exc_info=True)
        return None


def _run_segmentation(
    image: Image.Image,
    prompts: list[str],
    *,
    score_threshold: float,
    max_masks_per_prompt: int,
    per_prompt_score_thresholds: dict[str, float] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    if _processor is None:
        raise RuntimeError(_load_error or "sam3_model_not_loaded")

    detections: list[dict[str, Any]] = []
    prompt_latencies_ms: dict[str, float] = {}

    with _predict_lock:
        with _autocast_context():
            inference_state = _processor.set_image(image)
            for prompt in prompts:
                prompt_start = time.time()
                output = _processor.set_text_prompt(state=inference_state, prompt=prompt)
                prompt_latencies_ms[prompt] = round((time.time() - prompt_start) * 1000, 1)
                prompt_threshold = float((per_prompt_score_thresholds or {}).get(prompt, score_threshold))

                masks = _to_numpy(output.get("masks"))
                boxes = _to_numpy(output.get("boxes"))
                scores = _to_numpy(output.get("scores")).reshape(-1)
                if masks.ndim == 2:
                    masks = np.expand_dims(masks, axis=0)
                if boxes.ndim == 1 and boxes.size >= 4:
                    boxes = np.expand_dims(boxes, axis=0)

                total_items = min(len(masks), len(scores) or len(masks))
                if boxes.size and len(boxes) < total_items:
                    total_items = len(boxes)

                kept = 0
                for idx in range(total_items):
                    score = float(scores[idx]) if len(scores) > idx else 0.0
                    if score < prompt_threshold:
                        continue
                    mask = _coerce_mask_2d(np.asarray(masks[idx] > 0, dtype=np.uint8))
                    if mask is None:
                        continue
                    mask_bbox = _mask_to_bbox(mask)
                    if mask_bbox is None:
                        continue
                    model_box = _normalize_box(boxes[idx]) if boxes.size else None
                    detections.append(
                        {
                            "prompt_class": prompt,
                            "score": round(score, 4),
                            "score_threshold": round(prompt_threshold, 4),
                            "mask_bbox": mask_bbox,
                            "model_box": model_box,
                            "mask_rle": _encode_mask_rle(mask),
                            "area": int(mask.sum()),
                        }
                    )
                    kept += 1
                    if kept >= max_masks_per_prompt:
                        break

    return detections, prompt_latencies_ms


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


app = FastAPI(title="SAM3 Segmentation Server", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "model_loaded": _processor is not None,
        "device": _device,
        "loaded_at": _model_loaded_at,
        "error": _load_error,
    }


@app.get("/info")
async def info() -> dict[str, Any]:
    return {
        "model_loaded": _processor is not None,
        "device": _device,
        "default_score_threshold": DEFAULT_SCORE_THRESHOLD,
        "default_max_masks_per_prompt": DEFAULT_MAX_MASKS_PER_PROMPT,
        "error": _load_error,
    }


@app.post("/segment")
async def segment(
    file: UploadFile = File(...),
    prompts_json: str | None = Form(None),
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    max_masks_per_prompt: int = DEFAULT_MAX_MASKS_PER_PROMPT,
    per_prompt_score_thresholds_json: str | None = Form(None),
):
    if _processor is None:
        raise HTTPException(status_code=503, detail=_load_error or "sam3_model_not_loaded")

    prompts = _parse_prompts(prompts_json)
    if not prompts:
        raise HTTPException(status_code=400, detail="empty_prompts")
    per_prompt_score_thresholds = _parse_prompt_score_thresholds(per_prompt_score_thresholds_json)

    try:
        image = Image.open(BytesIO(await file.read())).convert("RGB")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"image_read_failed:{exc}")

    _reset_gpu_peak_memory()
    total_start = time.time()
    detections, prompt_latencies_ms = _run_segmentation(
        image,
        prompts,
        score_threshold=float(score_threshold),
        max_masks_per_prompt=max(1, int(max_masks_per_prompt)),
        per_prompt_score_thresholds=per_prompt_score_thresholds,
    )
    elapsed_ms = round((time.time() - total_start) * 1000, 1)
    return {
        "detections": detections,
        "detection_count": len(detections),
        "elapsed_ms": elapsed_ms,
        "per_prompt_latency_ms": prompt_latencies_ms,
        "image_size": [image.size[0], image.size[1]],
        "device": _device,
        "gpu_memory_peak_gb": _gpu_peak_memory_gb(),
        "per_prompt_score_thresholds": per_prompt_score_thresholds,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
