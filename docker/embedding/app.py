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

from gpu_guard import Slot, free_vram_gb

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


def _make_plm():
    """PLM(PE-Lang+Llama) 백엔드. PLM_ENABLED 미설정이면 None — 코드는 실려도 잠들어 있다.

    기본 비활성인 이유: 이 이미지는 prod 임베딩 서빙이라 배포가 곧 라벨링 중단이다.
    코드를 먼저 태우고 플래그로 나중에 켜면 켜는 순간엔 배포가 필요 없다(recreate 만).
    """
    if os.environ.get("PLM_ENABLED", "").strip().lower() not in ("1", "true", "yes"):
        return None
    from backends.plm_be import PlmBackend

    return PlmBackend(device=os.environ.get("PLM_DEVICE", "cuda:1"))


_backend = _make_backend()
_load_error: str | None = None
_idle_thread: threading.Thread | None = None
_idle_stop_event = threading.Event()

# 슬롯 = (백엔드, 락, 마지막 사용시각). GPU 마다 하나씩, 서로 독립적으로 뜨고 내린다.
#   pe_core : cuda:0 — 임베딩(채점). 기존 동작 그대로.
#   plm     : cuda:1 — 생성. **SAM3 와 GPU1 공유** → min_free_gb 가드로 항상 양보.
# 중복 로드는 Slot.ensure_loaded 의 double-checked 락이 막는다.
_pe = Slot("pe_core", _backend, os.environ.get("EMBEDDING_DEVICE", "cuda:0"))
_plm_backend = _make_plm()
_plm = (
    Slot("plm", _plm_backend, os.environ.get("PLM_DEVICE", "cuda:1"),
         min_free_gb=float(os.environ.get("PLM_MIN_FREE_GB", "9")))
    if _plm_backend is not None else None
)
PLM_IDLE_UNLOAD_SECONDS = int(os.environ.get("PLM_IDLE_UNLOAD_SECONDS", "120"))


def _slot(target: str):
    """이름 → 슬롯. 없는 이름은 400."""
    if target in ("pe_core", "embedding", "pe"):
        return _pe
    if target in ("plm", "pe_lang", "caption"):
        if _plm is None:
            raise HTTPException(status_code=503, detail="plm_disabled (PLM_ENABLED 미설정)")
        return _plm
    raise HTTPException(status_code=400, detail=f"unknown target: {target}")

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
    with _maintenance_lock:
        if not _maintenance.get("active"):
            return False
        heartbeat = float(_maintenance.get("heartbeat_at") or 0)
        ttl = int(_maintenance.get("ttl_seconds") or 0)
        if heartbeat and ttl > 0 and time.time() - heartbeat > ttl:
            owner = _maintenance.get("owner_run_id")
            _maintenance.update(
                {
                    "active": False,
                    "owner_run_id": None,
                    "entered_at": None,
                    "heartbeat_at": None,
                    "note": None,
                }
            )
            logger.warning("embedding maintenance TTL expired — auto release owner=%s", owner)
            return False
        return True


def _current_owner() -> str | None:
    """정비창의 현재 owner. 비활성이면 None (TTL 만료도 여기서 반영된다)."""
    if not _maintenance_active():
        return None
    with _maintenance_lock:
        return _maintenance.get("owner_run_id")


def _assert_owner(action: str, owner_run_id: str | None, force: bool) -> str:
    """정비창 소유권 검사. 어긋나면 409. 반환값 = 통과 사유(감사용 라벨).

    왜 필요한가: 정비창은 GPU 한 장을 독점하는 전역 상태인데 owner 가 검증되지
    않으면 **아무 job 이나 남의 창을 닫거나(exit) 가로챌 수(enter)** 있다.
    comfy_local job 하나가 trainer/SAM3 의 drain 을 해제해도 아무도 모른다.

    왜 '엄격하게' 안 하는가: 이 게이트가 닫힌 채 남으면 서비스가 통째로 503 이
    된다(2026-09-18 3일 장애). 그래서 **잠그는 쪽보다 푸는 쪽으로 기운다**:
      - force=true          → 무조건 통과. 운영자 escape hatch(clear_maintenance.sh)
                              와 fail-safe 센서(maintenance_guard_sensor)의 경로다.
      - 정비 비활성          → 통과. exit 는 idempotent no-op, enter 는 신규 진입.
      - 현재 owner 가 없음    → 통과. 누가 잡았는지 모르는 창은 누구든 풀 수 있어야 한다.
      - owner_run_id 미지정   → 통과하되 'unverified' 로 기록. 옛 호출자 하위호환이며,
                              force 와 **구분해서** 남기므로 조용한 통과가 아니다.
      - owner_run_id 일치     → 통과.
      - 그 외                → 409 maintenance_owner_mismatch.
    """
    if force:
        return "force"
    current = _current_owner()
    if current is None:
        return "unowned"
    if owner_run_id is None:
        logger.warning(
            "embedding maintenance %s without owner_run_id — current owner=%s (unverified)",
            action,
            current,
        )
        return "unverified"
    if owner_run_id == current:
        return "owner"
    logger.warning(
        "embedding maintenance %s REFUSED — owner mismatch (current=%s requested=%s)",
        action,
        current,
        owner_run_id,
    )
    raise HTTPException(
        status_code=409,
        detail={
            "error": "maintenance_owner_mismatch",
            "action": action,
            "current_owner_run_id": current,
            "requested_owner_run_id": owner_run_id,
            "hint": "남의 정비창을 해제/탈취하려면 force=true (scripts/clear_maintenance.sh)",
        },
    )


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
    """PE-Core 최초 로드 (startup). 실패해도 프로세스는 살린다 — /health 가 error 를 보고한다."""
    global _load_error
    try:
        _pe.ensure_loaded(logger)
        _load_error = None
        logger.info("embedding model loaded: %s (dim=%s)", _backend.name, _backend.dim)
    except Exception as exc:  # noqa: BLE001
        _load_error = str(exc)


def _unload_model() -> None:
    """PE-Core VRAM 해제. 다음 inference 호출시 lazy reload."""
    _pe.unload(logger)


def _ensure_model_loaded() -> None:
    """idle unload 이후 첫 request 가 도착하면 lazy reload. 정비 중이면 거부."""
    global _load_error
    if _maintenance_active():
        return
    try:
        _pe.ensure_loaded(logger)
        _load_error = None
    except Exception as exc:  # noqa: BLE001
        _load_error = str(exc)


def _touch(slot) -> None:
    slot.last_used = time.time()


def _touch_request() -> None:
    _touch(_pe)


def _idle_watcher() -> None:
    """슬롯마다 다른 타이머로 회수한다.

    PLM 이 훨씬 짧은 이유(기본 120s): GPU1 을 SAM3 가 쓰므로 배치가 끝나면 빨리 비켜야 한다.
    PE-Core 는 상시 서빙이라 기존 300s 를 유지한다.
    """
    watched = [(_pe, IDLE_UNLOAD_SECONDS)]
    if _plm is not None:
        watched.append((_plm, PLM_IDLE_UNLOAD_SECONDS))
    while not _idle_stop_event.is_set():
        for slot, timeout in watched:
            try:
                if slot.is_loaded() and (time.time() - slot.last_used) >= timeout:
                    slot.unload(logger)
            except Exception:
                logger.exception("[%s] idle watcher tick failed", slot.name)
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
    if _plm is not None:
        _plm.unload(logger)
    logger.info("embedding 모델 해제 완료 (lifespan teardown)")


app = FastAPI(title="vlm-embedding-service", lifespan=lifespan)


def _status() -> dict:
    # 기존 4개 키는 계약이라 그대로 둔다 (Dagster 자산이 model_loaded 를 폴링).
    out = {
        "model_loaded": _pe.is_loaded(),
        "model_name": _backend.name,
        "dim": _backend.dim,
        "error": _load_error,
        "slots": {"pe_core": _pe.status()},
        "vram_free_gb": {"cuda:0": free_vram_gb("cuda:0"), "cuda:1": free_vram_gb("cuda:1")},
    }
    if _plm is not None:
        out["slots"]["plm"] = _plm.status()
    return out


@app.get("/health")
def health() -> dict:
    return _status()


@app.post("/warmup")
def warmup(target: str = "pe_core") -> dict:
    """idle-unload 후 lazy reload 를 동기 트리거. 이미 로드면 no-op.

    클라이언트는 /health 폴링 전에 이걸 호출해 stale "not loaded" 상태를 피한다.
    """
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    slot = _slot(target)
    _touch(slot)
    try:
        slot.ensure_loaded(logger)
    except RuntimeError as exc:                       # VRAM 부족 = 지금은 못 함, 나중에 다시
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _status()


@app.post("/unload")
def unload(target: str = "pe_core") -> dict:
    """수동 VRAM 해제 (operator-triggered). idle timer 를 기다리기 싫을 때.

    다음 /warmup·/embed·/embed_text 호출이 lazy reload 한다.
    target="all" 이면 두 GPU 를 한 번에 비운다 (작업 끝나고 반납할 때).
    """
    if target == "all":
        _pe.unload(logger)
        if _plm is not None:
            _plm.unload(logger)
    else:
        _slot(target).unload(logger)
    return _status()


@app.post("/embed")
async def embed(file: UploadFile = File(...)) -> dict:
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    _touch_request()
    _ensure_model_loaded()
    if not _backend.is_loaded():
        raise HTTPException(status_code=503, detail=_load_error or "embedding_model_not_loaded")
    data = await file.read()
    with _pe.lock:
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
    with _pe.lock:
        vector = _backend.embed_text(text)
    return {"vector": vector, "dim": _backend.dim, "model_name": _backend.name}


@app.post("/caption")
async def caption(
    file: UploadFile = File(...),
    prompt: str = Form("Describe what is visible in this surveillance frame in one sentence."),
    max_new_tokens: int | None = Form(None),
) -> dict:
    """이미지 → 서술 문장 (PLM, cuda:1).

    ⚠️ GPU1 은 SAM3 소유다. 여유가 없으면 **503 으로 거절**하지 기다리지 않는다 —
    호출자(배치)가 재시도한다. 서빙을 밀어내지 않는 것이 이 서비스의 규칙이다.
    """
    if _maintenance_active():
        raise HTTPException(status_code=503, detail="gpu_under_maintenance")
    slot = _slot("plm")
    _touch(slot)
    try:
        slot.ensure_loaded(logger)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"plm_load_failed: {exc}") from exc
    data = await file.read()
    with slot.lock:
        text = slot.backend.caption(data, prompt, max_new_tokens)
    _touch(slot)
    return {"text": text, "model_name": slot.backend.name, "device": slot.device}


@app.post("/maintenance/enter")
def maintenance_enter(
    owner_run_id: str | None = Form(None),
    ttl_seconds: int | None = Form(None),
    note: str | None = Form(None),
    force: bool = Form(False),
) -> dict:
    """정비 진입: 게이트 활성 + (선택) 모델 unload. 이후 inference 는 503.

    이미 **다른** owner 가 정비 중이면 409 — exit 만 지키고 enter 를 열어두면
    '가로채서 진입 → 내 것처럼 해제' 로 owner 검증이 그대로 우회된다.
    같은 owner 의 재진입은 TTL/note 갱신(idempotent)이다.
    """
    granted = _assert_owner("enter", owner_run_id, force)
    state = _set_maintenance(True, owner_run_id=owner_run_id, ttl_seconds=ttl_seconds, note=note)
    _unload_model()
    logger.warning(
        "embedding maintenance ENTER owner_run_id=%s ttl=%ss granted_by=%s",
        owner_run_id,
        state["ttl_seconds"],
        granted,
    )
    return {**state, "granted_by": granted}


@app.post("/maintenance/exit")
def maintenance_exit(
    owner_run_id: str | None = Form(None),
    force: bool = Form(False),
) -> dict:
    """정비 종료: 게이트 해제. 호출자는 이후 /warmup 으로 재로딩.

    owner_run_id 가 현재 owner 와 다르면 409 — 남의 정비창은 닫지 못한다.
    운영자 강제 해제는 force=true (`scripts/clear_maintenance.sh`).
    """
    released = _assert_owner("exit", owner_run_id, force)
    state = _set_maintenance(False)
    logger.warning("embedding maintenance EXIT released_by=%s owner_run_id=%s", released, owner_run_id)
    return {**state, "released_by": released}


@app.post("/maintenance/heartbeat")
def maintenance_heartbeat(
    owner_run_id: str | None = Form(None),
    force: bool = Form(False),
) -> dict:
    """정비 owner 가 살아있음을 알림 (TTL 갱신).

    exit 와 같은 소유권 규칙을 쓴다. 남의 창을 무한히 연장하는 heartbeat 가
    바로 2026-09-18 의 3일 503 을 만든 기전이라, 여기도 막아야 의미가 있다.
    """
    _assert_owner("heartbeat", owner_run_id, force)
    active = _maintenance_active()
    with _maintenance_lock:
        if active and _maintenance["active"]:
            _maintenance["heartbeat_at"] = time.time()
        return dict(_maintenance)


@app.get("/maintenance/status")
def maintenance_status() -> dict:
    _maintenance_active()  # apply fail-safe TTL expiration before reporting
    with _maintenance_lock:
        return dict(_maintenance)
