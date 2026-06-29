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
IDLE_UNLOAD_SECONDS = int(os.environ.get("SAM3_IDLE_UNLOAD_SECONDS", "300"))
IDLE_CHECK_INTERVAL_SECONDS = int(os.environ.get("SAM3_IDLE_CHECK_INTERVAL_SECONDS", "60"))

_model = None
_processor = None
_device = PREFERRED_DEVICE
_model_loaded_at: float | None = None
_load_error: str | None = None
_predict_lock = threading.Lock()
_last_request_at: float = time.time()
_idle_thread: threading.Thread | None = None
_idle_stop_event = threading.Event()

# ─── GPU 정비 게이트 (server-side, fail-safe) ────────────────────────────────
# maintenance 활성 동안 /segment·/warmup 은 503, lazy-reload 거부.
# 프로세스-로컬 in-memory store 가 진실 (컨테이너는 vlm_pipeline-free).
_maintenance: dict[str, Any] = {
    "active": False,
    "owner_run_id": None,
    "entered_at": None,
    "heartbeat_at": None,
    "ttl_seconds": int(os.environ.get("MAINTENANCE_DEFAULT_TTL_SECONDS", "1800")),
    "note": None,
}
_maintenance_lock = threading.Lock()


def _maintenance_active() -> bool:
    return bool(_maintenance.get("active"))


def _set_maintenance(active: bool, **fields: Any) -> dict[str, Any]:
    with _maintenance_lock:
        _maintenance["active"] = bool(active)
        if active:
            now = time.time()
            _maintenance["owner_run_id"] = fields.get("owner_run_id")
            _maintenance["entered_at"] = now
            _maintenance["heartbeat_at"] = now
            _maintenance["ttl_seconds"] = int(fields.get("ttl_seconds") or _maintenance["ttl_seconds"])
            _maintenance["note"] = fields.get("note")
        else:
            _maintenance["owner_run_id"] = None
            _maintenance["entered_at"] = None
            _maintenance["heartbeat_at"] = None
            _maintenance["note"] = None
        return dict(_maintenance)


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


def _unload_model() -> None:
    """idle 시 VRAM 해제. 다음 inference 호출시 lazy reload."""
    global _model, _processor, _model_loaded_at
    with _predict_lock:
        if _model is None and _processor is None:
            return
        _processor = None
        if _model is not None:
            del _model
            _model = None
        _model_loaded_at = None
    try:
        torch = _load_torch()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        logger.exception("SAM3 empty_cache failed")
    logger.info("SAM3 idle unload 완료 (VRAM 해제)")


def _ensure_model_loaded() -> None:
    """idle unload 이후 첫 request 가 도착하면 lazy reload. 정비 중이면 거부."""
    if _maintenance_active():
        return
    if _processor is not None:
        return
    with _predict_lock:
        if _processor is None:
            logger.info("SAM3 lazy reload (idle 후 첫 request)")
            _load_model()


def _touch_request() -> None:
    global _last_request_at
    _last_request_at = time.time()


def _idle_watcher() -> None:
    while not _idle_stop_event.is_set():
        try:
            if _processor is not None and (time.time() - _last_request_at) >= IDLE_UNLOAD_SECONDS:
                _unload_model()
        except Exception:
            logger.exception("SAM3 idle watcher tick failed")
        _idle_stop_event.wait(IDLE_CHECK_INTERVAL_SECONDS)


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


def _gpu_memory_info() -> dict[str, float | None]:
    """현재 GPU 의 total/free/used GB 를 반환. CPU 모드면 모든 값 None."""
    torch = _load_torch()
    device_idx = _resolve_cuda_device_index()
    if device_idx is None or not torch.cuda.is_available():
        return {"total_gb": None, "free_gb": None, "used_gb": None}
    try:
        free_bytes, total_bytes = torch.cuda.mem_get_info(device_idx)
        total_gb = round(float(total_bytes) / 1e9, 2)
        free_gb = round(float(free_bytes) / 1e9, 2)
        used_gb = round(total_gb - free_gb, 2)
        return {"total_gb": total_gb, "free_gb": free_gb, "used_gb": used_gb}
    except Exception:
        logger.debug("SAM3 gpu memory info read skipped", exc_info=True)
        return {"total_gb": None, "free_gb": None, "used_gb": None}


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
    global _idle_thread
    _load_model()
    _idle_stop_event.clear()
    _idle_thread = threading.Thread(target=_idle_watcher, name="sam3-idle-watcher", daemon=True)
    _idle_thread.start()
    logger.info(
        "SAM3 idle watcher started: idle_unload=%ds check_every=%ds",
        IDLE_UNLOAD_SECONDS, IDLE_CHECK_INTERVAL_SECONDS,
    )
    yield
    _idle_stop_event.set()
    if _idle_thread is not None and _idle_thread.is_alive():
        _idle_thread.join(timeout=5)
    _unload_model()
    logger.info("SAM3 모델 해제 완료 (lifespan teardown)")


app = FastAPI(title="SAM3 Segmentation Server", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "model_loaded": _processor is not None,
        "device": _device,
        "loaded_at": _model_loaded_at,
        "error": _load_error,
        "gpu_memory": _gpu_memory_info(),
    }


@app.post("/warmup")
async def warmup() -> dict[str, Any]:
    """Force synchronous lazy reload if model was idle-unloaded.

    Returns when the model is ready (or the load fails). Clients should call
    this BEFORE polling ``/health`` to ensure they don't see a stale "not loaded"
    state caused by the idle-unload watcher.
    """
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
    return {
        "model_loaded": _processor is not None,
        "device": _device,
        "loaded_at": _model_loaded_at,
        "error": _load_error,
    }


@app.post("/unload")
async def unload() -> dict[str, Any]:
    """Manually free GPU memory NOW (operator-triggered).

    Useful when work is known to be done and waiting for the idle timer is too
    long. Next ``/warmup`` or ``/segment`` call lazy-reloads the model.
    """
    _unload_model()
    return {
        "model_loaded": _processor is not None,
        "device": _device,
        "loaded_at": _model_loaded_at,
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
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
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


@app.post("/maintenance/enter")
async def maintenance_enter(
    owner_run_id: str | None = Form(None),
    ttl_seconds: int | None = Form(None),
    note: str | None = Form(None),
) -> dict[str, Any]:
    """정비 진입: 게이트 활성 + 모델 unload. 이후 /segment·/warmup 은 503."""
    state = _set_maintenance(True, owner_run_id=owner_run_id, ttl_seconds=ttl_seconds, note=note)
    _unload_model()
    logger.warning("SAM3 maintenance ENTER owner_run_id=%s ttl=%ss", owner_run_id, state["ttl_seconds"])
    return state


@app.post("/maintenance/exit")
async def maintenance_exit() -> dict[str, Any]:
    state = _set_maintenance(False)
    logger.warning("SAM3 maintenance EXIT")
    return state


@app.post("/maintenance/heartbeat")
async def maintenance_heartbeat() -> dict[str, Any]:
    with _maintenance_lock:
        if _maintenance["active"]:
            _maintenance["heartbeat_at"] = time.time()
        return dict(_maintenance)


@app.get("/maintenance/status")
async def maintenance_status() -> dict[str, Any]:
    return dict(_maintenance)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
