"""임베딩 서비스 (FastAPI). PE-Core-L14-336 이미지/텍스트 → 1024-d 벡터.

백엔드는 EMBEDDING_BACKEND 로 선택 (open_clip 기본; perception_models 는 추후).
SAM3 컨테이너 패턴 미러: /health(model_loaded) → asset 의 wait_until_ready 가 폴링.

idle-unload (SAM3/YOLO 패턴 미러): EMBEDDING_IDLE_UNLOAD_SECONDS 동안 무요청이면
백그라운드 watcher 가 모델을 VRAM 에서 해제한다. 다음 요청(/warmup·/embed·/embed_text)이
도착하면 lazy reload. 클라이언트는 /warmup 으로 reload 를 동기 트리거할 수 있다.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

logger = logging.getLogger("embedding-service")

IDLE_UNLOAD_SECONDS = int(os.environ.get("EMBEDDING_IDLE_UNLOAD_SECONDS", "300"))
IDLE_CHECK_INTERVAL_SECONDS = int(os.environ.get("EMBEDDING_IDLE_CHECK_INTERVAL_SECONDS", "60"))


def _make_backend():
    name = os.environ.get("EMBEDDING_BACKEND", "open_clip")
    device = os.environ.get("EMBEDDING_DEVICE", "cuda:0")
    if name == "open_clip":
        from backends.open_clip_be import OpenClipBackend

        return OpenClipBackend(device=device)
    if name == "perception_models":
        raise NotImplementedError(
            "perception_models 백엔드는 아직 미구현 — open_clip 사용. "
            "추후 backends/perception_be.py 추가 + EMBEDDING_BACKEND=perception_models"
        )
    raise ValueError(f"unknown EMBEDDING_BACKEND={name!r}")


_backend = _make_backend()
_load_error: str | None = None
_predict_lock = threading.Lock()
_last_request_at: float = time.time()
_idle_thread: threading.Thread | None = None
_idle_stop_event = threading.Event()

# ─── GPU 정비 게이트 (server-side, fail-safe) ────────────────────────────────
# maintenance 활성 동안 /embed·/embed_text·/warmup 은 503, lazy-reload 거부.
# 프로세스-로컬 in-memory store 가 진실. (PG 영속화는 guard 센서가 별도 관리;
# 이 컨테이너는 vlm_pipeline-free 유지 — backends.* 만 import.)
_maintenance: dict = {
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


def _set_maintenance(active: bool, **fields) -> dict:
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


def _load_model() -> None:
    global _load_error
    try:
        _backend.load()
        _load_error = None
        logger.info("embedding model loaded: %s (dim=%s)", _backend.name, _backend.dim)
    except Exception as exc:  # noqa: BLE001
        _load_error = str(exc)
        logger.exception("embedding model load failed: %s", exc)


def _unload_model() -> None:
    """idle 시 VRAM 해제. 다음 inference 호출시 lazy reload."""
    with _predict_lock:
        if not _backend.is_loaded():
            return
        _backend.unload()
    logger.info("embedding idle unload 완료 (VRAM 해제)")


def _ensure_model_loaded() -> None:
    """idle unload 이후 첫 request 가 도착하면 lazy reload. 정비 중이면 거부."""
    if _maintenance_active():
        return
    if _backend.is_loaded():
        return
    with _predict_lock:
        if not _backend.is_loaded():
            logger.info("embedding lazy reload (idle 후 첫 request)")
            _load_model()


def _touch_request() -> None:
    global _last_request_at
    _last_request_at = time.time()


def _idle_watcher() -> None:
    while not _idle_stop_event.is_set():
        try:
            if _backend.is_loaded() and (time.time() - _last_request_at) >= IDLE_UNLOAD_SECONDS:
                _unload_model()
        except Exception:
            logger.exception("embedding idle watcher tick failed")
        _idle_stop_event.wait(IDLE_CHECK_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _idle_thread
    _load_model()
    _idle_stop_event.clear()
    _idle_thread = threading.Thread(target=_idle_watcher, name="embedding-idle-watcher", daemon=True)
    _idle_thread.start()
    logger.info(
        "embedding idle watcher started: idle_unload=%ds check_every=%ds",
        IDLE_UNLOAD_SECONDS,
        IDLE_CHECK_INTERVAL_SECONDS,
    )
    yield
    _idle_stop_event.set()
    if _idle_thread is not None and _idle_thread.is_alive():
        _idle_thread.join(timeout=5)
    _unload_model()
    logger.info("embedding 모델 해제 완료 (lifespan teardown)")


app = FastAPI(title="vlm-embedding-service", lifespan=lifespan)


def _status() -> dict:
    return {
        "model_loaded": _backend.is_loaded(),
        "model_name": _backend.name,
        "dim": _backend.dim,
        "error": _load_error,
    }


@app.get("/health")
def health() -> dict:
    return _status()


@app.post("/warmup")
def warmup() -> dict:
    """idle-unload 후 lazy reload 를 동기 트리거. 이미 로드면 no-op.

    클라이언트는 /health 폴링 전에 이걸 호출해 stale "not loaded" 상태를 피한다.
    """
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
    return _status()


@app.post("/unload")
def unload() -> dict:
    """수동 VRAM 해제 (operator-triggered). idle timer 를 기다리기 싫을 때.

    다음 /warmup·/embed·/embed_text 호출이 lazy reload 한다.
    """
    _unload_model()
    return {"model_loaded": _backend.is_loaded()}


@app.post("/embed")
async def embed(file: UploadFile = File(...)) -> dict:
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
    if not _backend.is_loaded():
        raise HTTPException(status_code=503, detail=_load_error or "embedding_model_not_loaded")
    data = await file.read()
    with _predict_lock:
        vector = _backend.embed_image(data)
    return {"vector": vector, "dim": _backend.dim, "model_name": _backend.name}


@app.post("/embed_text")
def embed_text(text: str = Form(...)) -> dict:
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
    if not _backend.is_loaded():
        raise HTTPException(status_code=503, detail=_load_error or "embedding_model_not_loaded")
    with _predict_lock:
        vector = _backend.embed_text(text)
    return {"vector": vector, "dim": _backend.dim, "model_name": _backend.name}


@app.post("/maintenance/enter")
def maintenance_enter(
    owner_run_id: str | None = Form(None),
    ttl_seconds: int | None = Form(None),
    note: str | None = Form(None),
) -> dict:
    """정비 진입: 게이트 활성 + (선택) 모델 unload. 이후 inference 는 503."""
    state = _set_maintenance(True, owner_run_id=owner_run_id, ttl_seconds=ttl_seconds, note=note)
    _unload_model()
    logger.warning("embedding maintenance ENTER owner_run_id=%s ttl=%ss", owner_run_id, state["ttl_seconds"])
    return state


@app.post("/maintenance/exit")
def maintenance_exit() -> dict:
    """정비 종료: 게이트 해제. 호출자는 이후 /warmup 으로 재로딩."""
    state = _set_maintenance(False)
    logger.warning("embedding maintenance EXIT")
    return state


@app.post("/maintenance/heartbeat")
def maintenance_heartbeat() -> dict:
    """정비 owner 가 살아있음을 알림 (TTL 갱신)."""
    with _maintenance_lock:
        if _maintenance["active"]:
            _maintenance["heartbeat_at"] = time.time()
        return dict(_maintenance)


@app.get("/maintenance/status")
def maintenance_status() -> dict:
    return dict(_maintenance)
