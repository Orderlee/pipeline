"""camera_angle 추정 서비스 — Depth Anything V2-S + RANSAC 바닥평면 피팅.

벤치마크 근거: docs/design-docs/camera-angle-grouping-2026-07-29.md §7 및 Notion 보고서.
사람 GT 98편에서 plan-vs-rest AUC 0.947 (Qwen 7%, GeoCalib 0.207 역상관, PerspectiveFields 0.698).
`level`↔`oblique` 는 어떤 모델도 분리하지 못했으므로 **plan vs non_plan 2-bin 만** 산출한다.

엔드포인트
  POST /angle   (multipart: file=<jpeg>)  → {"camera_angle","tilt_deg","angle_method"}
  GET  /health                            → {"status","model_loaded","device"}
  GET  /info                              → 모델·임계값 정보

CPU 로 충분하다(실측 0.78s/프레임). GPU 를 점유하지 않으므로 embedding-service·SAM3 와 경합 없음.
"""

from __future__ import annotations

import io
import logging
import math
import os
import threading
import time

import numpy as np
import torch
import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from PIL import Image
from transformers import AutoImageProcessor, AutoModelForDepthEstimation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("angle-service")

MODEL_ID = os.environ.get("ANGLE_MODEL_ID", "depth-anything/Depth-Anything-V2-Small-hf")
DEVICE = os.environ.get("ANGLE_DEVICE", "cpu")
PORT = int(os.environ.get("ANGLE_PORT", "8000"))
# 임계값 실측 근거(사람 GT 98편): 30° → plan 재현 2/3, 오검출 3/95 (정밀도 40%).
# 검수 후보를 전량 수집하려면 20° (재현 3/3, 오검출 17/95).
PLAN_THRESHOLD_DEG = float(os.environ.get("ANGLE_PLAN_THRESHOLD_DEG", "30"))
# 바닥 후보 영역: 프레임 하단 60% (CCTV 에서 지면은 아래쪽에 있다)
GROUND_TOP_FRAC = float(os.environ.get("ANGLE_GROUND_TOP_FRAC", "0.40"))
# 내부 파라미터 미지 → 통상적인 CCTV 화각 가정. 실측 튜닝 노브로 남겨둔다.
FOCAL_FACTOR = float(os.environ.get("ANGLE_FOCAL_FACTOR", "0.7"))
RANSAC_ITERS = int(os.environ.get("ANGLE_RANSAC_ITERS", "300"))
MAX_POINTS = int(os.environ.get("ANGLE_MAX_POINTS", "4000"))

# 동시 요청 수 × torch 스레드 수가 코어를 초과하면 oversubscription 으로 오히려 느려진다.
# CPU 모드에서 동시성을 쓸 때만 의미 있는 노브 (GPU 모드에서는 무관).
_TORCH_THREADS = int(os.environ.get("ANGLE_TORCH_THREADS", "0"))
if _TORCH_THREADS > 0:
    torch.set_num_threads(_TORCH_THREADS)

_lock = threading.Lock()
_state: dict[str, object] = {"proc": None, "net": None}


def _load() -> tuple[object, object]:
    with _lock:
        if _state["net"] is None:
            t0 = time.time()
            _state["proc"] = AutoImageProcessor.from_pretrained(MODEL_ID)
            _state["net"] = AutoModelForDepthEstimation.from_pretrained(MODEL_ID).to(DEVICE).eval()
            logger.info(f"model loaded: {MODEL_ID} on {DEVICE} ({time.time() - t0:.1f}s)")
    return _state["proc"], _state["net"]


def _fit_plane_ransac(pts: np.ndarray, seed: int = 0) -> np.ndarray | None:
    """RANSAC 평면 피팅 → 단위 법선. pts: (N,3). 결정론적(seed 고정)."""
    rng = np.random.default_rng(seed)
    thresh = 0.02 * float(np.percentile(np.abs(pts[:, 2]), 90) or 1.0)
    best_n, best_cnt = None, -1
    for _ in range(RANSAC_ITERS):
        p0, p1, p2 = pts[rng.choice(len(pts), 3, replace=False)]
        n = np.cross(p1 - p0, p2 - p0)
        norm = np.linalg.norm(n)
        if norm < 1e-9:
            continue
        n = n / norm
        cnt = int((np.abs((pts - p0) @ n) < thresh).sum())
        if cnt > best_cnt:
            best_cnt, best_n = cnt, n
    return best_n


def estimate_tilt_deg(img: Image.Image) -> float:
    """바닥평면 법선과 광축 사이 각으로부터 카메라 하향각(도)을 추정."""
    proc, net = _load()
    inputs = proc(images=img, return_tensors="pt").to(DEVICE)
    with torch.no_grad():
        disp = net(**inputs).predicted_depth[0].detach().cpu().numpy()
    h, w = disp.shape
    # DAv2 출력은 상대 역깊이(가까울수록 큼) → Z ∝ 1/disp
    z = 1.0 / np.maximum(disp, 1e-3)
    y0 = int(h * GROUND_TOP_FRAC)
    step = max(1, int(math.sqrt(max(1, (h - y0) * w) / MAX_POINTS)))
    ys, xs = np.mgrid[y0:h, 0:w]
    ys, xs = ys[::step, ::step].ravel(), xs[::step, ::step].ravel()
    zz = z[y0:h, :][::step, ::step].ravel()
    f = FOCAL_FACTOR * w
    cx, cy = w / 2.0, h / 2.0
    pts = np.stack([(xs - cx) * zz / f, (ys - cy) * zz / f, zz], axis=1)
    pts = pts[np.isfinite(pts).all(axis=1)]
    if len(pts) < 50:
        raise ValueError("평면 피팅용 포인트 부족")
    n = _fit_plane_ransac(pts)
    if n is None:
        raise ValueError("RANSAC 실패")
    # 광축(0,0,1) 과 바닥 법선 사이 각 → tilt = 90 - 그 각
    ang = math.degrees(math.acos(min(1.0, abs(float(n[2])))))
    return 90.0 - ang


app = FastAPI(title="camera-angle (Depth Anything V2-S + plane fit)")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_loaded": _state["net"] is not None, "device": DEVICE}


@app.get("/info")
def info() -> dict:
    return {
        "model_id": MODEL_ID,
        "device": DEVICE,
        "plan_threshold_deg": PLAN_THRESHOLD_DEG,
        "labels": ["plan_view", "non_plan"],
        "note": "level/oblique 는 분리 불가로 산출하지 않는다 (벤치마크 근거: level AUC ≤ 0.68)",
    }


@app.post("/warmup")
def warmup() -> dict:
    _load()
    return {"status": "ok"}


@app.post("/angle")
def angle(file: UploadFile = File(...)) -> dict:
    # ⚠️ `async def` 로 두면 안 된다 — 추론이 동기 블로킹이라 이벤트 루프를 잡아
    #    요청이 완전히 직렬화된다(실측: 동시 8건 2.71s = 순차와 동일).
    #    `def` 로 두면 FastAPI 가 threadpool 에서 실행해 실제로 병렬 처리된다.
    raw = file.file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"decode_failed:{exc}") from exc
    try:
        tilt = estimate_tilt_deg(img)
    except ValueError as exc:
        # 판정 근거 부족 → indeterminate. 호출자는 이를 정상 응답으로 처리한다.
        return {"camera_angle": "indeterminate", "tilt_deg": None,
                "angle_method": f"dav2-s+plane:{exc}"}
    return {
        "camera_angle": "plan_view" if tilt >= PLAN_THRESHOLD_DEG else "non_plan",
        "tilt_deg": round(tilt, 2),
        "angle_method": "dav2-s+plane",
    }


if __name__ == "__main__":
    _load()  # 기동 시 미리 로드해 첫 요청 지연 제거
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
