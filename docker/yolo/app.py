"""YOLO-World-L FastAPI 서버 — GPU 1 전용, 모델 상주.

엔드포인트:
  POST /detect          — 단일 이미지 detection
  POST /detect/batch    — 배치 이미지 detection (최대 8장)
  GET  /health          — 헬스체크 (모델 로드 상태)
  GET  /info            — 모델 정보
  POST /classes         — detection 클래스 변경
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import torch
import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from PIL import Image
from pydantic import BaseModel
from ultralytics import YOLOWorld

logger = logging.getLogger("yolo-server")

MODEL_PATH = os.environ.get("YOLO_MODEL_PATH", "/data/models/yolo/yolov8l-worldv2.pt")
DEVICE = os.environ.get("YOLO_DEVICE", "cuda:1")
IMGSZ = int(os.environ.get("YOLO_IMGSZ", "640"))
MAX_BATCH = int(os.environ.get("YOLO_MAX_BATCH", "8"))

DEFAULT_SAFETY_CLASSES: list[str] = [
    "person", "car", "truck", "bus", "motorcycle", "bicycle",
    "fire", "smoke", "flame", "knife", "gun",
    "bag", "backpack", "suitcase",
    "helmet", "safety vest", "hard hat",
    "traffic cone", "barricade",
    "dog", "cat",
]


def _load_default_classes() -> list[str]:
    """YOLO-World 기본 텍스트 프롬프트(클래스 목록)를 env에서 읽는다."""
    raw_value = str(os.environ.get("YOLO_DEFAULT_CLASSES", "") or "").strip()
    if not raw_value:
        return list(DEFAULT_SAFETY_CLASSES)

    try:
        if raw_value.startswith("["):
            values = json.loads(raw_value)
            if isinstance(values, list):
                classes = [str(item).strip() for item in values if str(item).strip()]
                if classes:
                    return classes
    except Exception:
        logger.warning("YOLO_DEFAULT_CLASSES JSON 파싱 실패, comma-separated 형식으로 재시도합니다")

    classes = [part.strip() for part in raw_value.replace("\n", ",").split(",") if part.strip()]
    return classes or list(DEFAULT_SAFETY_CLASSES)

def _normalize_classes(values: list[str] | tuple[str, ...] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or []:
        rendered = str(value or "").strip().lower()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return normalized


def _parse_request_classes(raw_value: str | None) -> list[str] | None:
    rendered = str(raw_value or "").strip()
    if not rendered:
        return None
    try:
        if rendered.startswith("["):
            parsed = json.loads(rendered)
            if isinstance(parsed, list):
                classes = _normalize_classes([str(item) for item in parsed])
                return classes or None
    except Exception:
        logger.warning("YOLO request classes_json 파싱 실패, comma-separated 형식으로 재시도합니다")

    classes = _normalize_classes(rendered.replace("\n", ",").split(","))
    return classes or None


_model: YOLOWorld | None = None
_classes: list[str] = _normalize_classes(_load_default_classes())
_model_loaded_at: float | None = None
_model_lock = threading.Lock()
_applied_classes_signature: tuple[str, ...] | None = None


def _apply_model_classes(classes: list[str]) -> list[str]:
    global _applied_classes_signature

    normalized = _normalize_classes(classes) or list(DEFAULT_SAFETY_CLASSES)
    signature = tuple(normalized)
    if _model is not None and _applied_classes_signature != signature:
        _model.set_classes(normalized)
        _applied_classes_signature = signature
    return normalized


def _load_model() -> None:
    global _model, _model_loaded_at
    path = Path(MODEL_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"모델 파일 없음: {path}\n"
            f"다운로드: sudo bash scripts/download_yolo_model.sh"
        )
    logger.info(f"YOLO-World 모델 로딩: {path} → {DEVICE}")
    _model = YOLOWorld(str(path))
    _model.to(DEVICE)
    _apply_model_classes(_classes)
    _model_loaded_at = time.time()
    logger.info(f"YOLO-World 모델 로드 완료: classes={len(_classes)} device={DEVICE}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield
    global _model
    if _model is not None:
        del _model
        _model = None
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        logger.info("YOLO-World 모델 해제 완료")


app = FastAPI(
    title="YOLO-World Detection Server",
    version="1.0.0",
    lifespan=lifespan,
)


def _run_detection(
    img: Image.Image,
    conf: float,
    iou: float,
    max_det: int,
    *,
    requested_classes: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if _model is None:
        raise RuntimeError("모델이 로드되지 않았습니다")

    img_w, img_h = img.size
    effective_classes = _normalize_classes(requested_classes) or list(_classes)
    with _model_lock:
        applied_classes = _apply_model_classes(effective_classes)
        results = _model.predict(
            source=img, imgsz=IMGSZ, conf=conf, iou=iou,
            max_det=max_det, verbose=False,
        )
    detections: list[dict[str, Any]] = []
    if not results:
        return detections, applied_classes

    boxes = results[0].boxes
    if boxes is None or len(boxes) == 0:
        return detections, applied_classes

    for i in range(len(boxes)):
        cls_id = int(boxes.cls[i].item())
        confidence = float(boxes.conf[i].item())
        x1, y1, x2, y2 = boxes.xyxy[i].tolist()
        class_name = applied_classes[cls_id] if cls_id < len(applied_classes) else f"class_{cls_id}"
        detections.append({
            "class": class_name,
            "confidence": round(confidence, 4),
            "bbox": [round(x1, 1), round(y1, 1), round(x2, 1), round(y2, 1)],
            "bbox_norm": [
                round(x1 / img_w, 4), round(y1 / img_h, 4),
                round(x2 / img_w, 4), round(y2 / img_h, 4),
            ],
        })
    return detections, applied_classes


class DetectParams(BaseModel):
    conf: float = 0.25
    iou: float = 0.45
    max_det: int = 300


@app.post("/detect")
async def detect(
    file: UploadFile = File(...),
    classes_json: str | None = Form(None),
    conf: float = 0.25,
    iou: float = 0.45,
    max_det: int = 300,
):
    """단일 이미지 detection."""
    try:
        img_bytes = await file.read()
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"이미지 읽기 실패: {exc}")

    t0 = time.time()
    detections, applied_classes = _run_detection(
        img,
        conf=conf,
        iou=iou,
        max_det=max_det,
        requested_classes=_parse_request_classes(classes_json),
    )
    elapsed_ms = round((time.time() - t0) * 1000, 1)

    return {
        "detections": detections,
        "detection_count": len(detections),
        "elapsed_ms": elapsed_ms,
        "image_size": [img.size[0], img.size[1]],
    }


@app.post("/detect/batch")
async def detect_batch(
    files: list[UploadFile] = File(...),
    classes_json: str | None = Form(None),
    conf: float = 0.25,
    iou: float = 0.45,
    max_det: int = 300,
):
    """배치 이미지 detection (최대 MAX_BATCH장)."""
    if len(files) > MAX_BATCH:
        raise HTTPException(
            status_code=400,
            detail=f"최대 {MAX_BATCH}장까지 가능합니다 (요청: {len(files)}장)",
        )

    images: list[Image.Image] = []
    for f in files:
        try:
            img_bytes = await f.read()
            images.append(Image.open(BytesIO(img_bytes)).convert("RGB"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"이미지 읽기 실패: {f.filename}: {exc}")

    if _model is None:
        raise HTTPException(status_code=503, detail="모델이 로드되지 않았습니다")

    requested_classes = _parse_request_classes(classes_json)
    t0 = time.time()
    with _model_lock:
        applied_classes = _apply_model_classes(requested_classes or list(_classes))
        results = _model.predict(
            source=images, imgsz=IMGSZ, conf=conf, iou=iou,
            max_det=max_det, verbose=False,
        )
    elapsed_ms = round((time.time() - t0) * 1000, 1)
    per_image_elapsed_ms = round(elapsed_ms / max(1, len(results)), 1)

    batch_results = []
    for idx, result in enumerate(results):
        img_w, img_h = images[idx].size
        detections: list[dict[str, Any]] = []
        boxes = result.boxes
        if boxes is not None and len(boxes) > 0:
            for i in range(len(boxes)):
                cls_id = int(boxes.cls[i].item())
                confidence = float(boxes.conf[i].item())
                x1, y1, x2, y2 = boxes.xyxy[i].tolist()
                class_name = applied_classes[cls_id] if cls_id < len(applied_classes) else f"class_{cls_id}"
                detections.append({
                    "class": class_name,
                    "confidence": round(confidence, 4),
                    "bbox": [round(x1, 1), round(y1, 1), round(x2, 1), round(y2, 1)],
                    "bbox_norm": [
                        round(x1 / img_w, 4), round(y1 / img_h, 4),
                        round(x2 / img_w, 4), round(y2 / img_h, 4),
                    ],
                })
        batch_results.append({
            "detections": detections,
            "detection_count": len(detections),
            "image_size": [img_w, img_h],
            "elapsed_ms": per_image_elapsed_ms,
        })

    return {
        "results": batch_results,
        "total_images": len(batch_results),
        "elapsed_ms": elapsed_ms,
        "requested_classes": applied_classes,
        "requested_classes_count": len(applied_classes),
    }


class ClassesRequest(BaseModel):
    classes: list[str]


@app.post("/classes")
async def set_classes(req: ClassesRequest):
    """detection 클래스 변경."""
    global _classes
    requested = _normalize_classes(req.classes)
    if not requested:
        raise HTTPException(status_code=400, detail="classes 리스트가 비어있습니다")
    _classes = requested
    with _model_lock:
        _apply_model_classes(_classes)
    return {"classes": _classes, "count": len(_classes)}


@app.get("/health")
async def health():
    """헬스체크."""
    gpu_available = torch.cuda.is_available()
    gpu_mem = {}
    if gpu_available:
        try:
            device_idx = int(DEVICE.split(":")[-1]) if ":" in DEVICE else 0
            total = torch.cuda.get_device_properties(device_idx).total_mem
            allocated = torch.cuda.memory_allocated(device_idx)
            reserved = torch.cuda.memory_reserved(device_idx)
            gpu_mem = {
                "total_gb": round(total / 1e9, 2),
                "allocated_gb": round(allocated / 1e9, 2),
                "reserved_gb": round(reserved / 1e9, 2),
                "free_gb": round((total - reserved) / 1e9, 2),
            }
        except Exception:
            pass

    return {
        "status": "ok" if _model is not None else "loading",
        "model_loaded": _model is not None,
        "model_path": MODEL_PATH,
        "device": DEVICE,
        "gpu_available": gpu_available,
        "gpu_memory": gpu_mem,
        "classes_count": len(_classes),
        "imgsz": IMGSZ,
    }


@app.get("/info")
async def info():
    """모델 정보."""
    return {
        "model": "yolov8l-worldv2",
        "model_path": MODEL_PATH,
        "device": DEVICE,
        "imgsz": IMGSZ,
        "max_batch": MAX_BATCH,
        "classes": _classes,
        "classes_count": len(_classes),
        "loaded_at": _model_loaded_at,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    port = int(os.environ.get("YOLO_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
