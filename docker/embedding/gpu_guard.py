"""GPU 슬롯 가드 — 중복 로드 방지 + VRAM 여유 검사.

이 호스트는 GPU 2장을 여러 컨테이너가 나눠 쓴다:
    GPU0  embedding-service(PE-Core) · dagster torch(Places365) · ffmpeg NVENC
    GPU1  SAM3(prod 서빙, workers>1) · trainer · ffmpeg NVENC

컨테이너 경계를 넘는 락은 없다 — SAM3 는 이 코드를 모르고, sam3 는 모델 볼륨을
read-only 로만 본다(공유 락파일을 쓸 곳이 없다). 그래서 조정은 **물리량 한 가지**로 한다:
로드 직전에 그 장치의 free VRAM 을 재고, 모자라면 로드하지 않는다.

단방향 규칙: 배치(PLM)가 서빙(SAM3)에게 양보한다. 역방향은 없다.

# ponytail: 로드 도중 남이 잡아가는 레이스는 남는다(검사~할당 사이). PLM 작업은
#   재시도 가능한 배치라 여기까지가 값어치. 진짜 상호배제가 필요해지면 그때
#   gpu_maintenance_lock(PG) 을 쓰되, 이 컨테이너의 "vlm_pipeline-free" 규칙을 먼저 깰지 정할 것.
"""

from __future__ import annotations

import threading
import time

GB = 1024 ** 3


def device_index(device: str) -> int:
    """'cuda:1' → 1. 'cpu' 등 비-cuda 는 -1."""
    if not device.startswith("cuda"):
        return -1
    _, _, idx = device.partition(":")
    return int(idx) if idx else 0


def free_vram_gb(device: str) -> float | None:
    """해당 장치의 free VRAM(GB). torch 미로드/비-cuda 면 None.

    torch.cuda.mem_get_info 는 **드라이버가 보고하는 장치 전체 free** 라
    같은 GPU 를 쓰는 다른 컨테이너(SAM3 등)의 점유도 반영된다 — 그래서 이게
    컨테이너 간 조정 신호로 쓸 수 있는 유일한 값이다.
    """
    idx = device_index(device)
    if idx < 0:
        return None
    try:
        import torch

        if not torch.cuda.is_available():
            return None
        free, _total = torch.cuda.mem_get_info(idx)
        return free / GB
    except Exception:
        return None


def require_free(device: str, need_gb: float) -> None:
    """여유가 need_gb 미만이면 RuntimeError. 호출자가 503 으로 바꿔 내보낸다."""
    free = free_vram_gb(device)
    if free is None:
        return  # 계측 불가(예: CPU) — 가드 없이 진행
    if free < need_gb:
        raise RuntimeError(
            f"insufficient_vram device={device} free={free:.1f}GB need={need_gb:.1f}GB "
            "(GPU1 은 SAM3 우선 — 서빙이 물러날 때까지 배치는 대기한다)"
        )


class Slot:
    """모델 하나의 수명주기 = (백엔드, 락, 마지막 사용시각).

    **중복 로드 방지**가 이 클래스의 존재 이유다: 로드는 반드시 락 안에서
    is_loaded() 재확인 후에만 일어난다(double-checked). uvicorn workers=1 전제 —
    EMBEDDING_WORKERS 를 올리면 프로세스마다 사본이 생겨 이 보장이 깨진다.
    """

    def __init__(self, name: str, backend, device: str, min_free_gb: float = 0.0) -> None:
        self.name = name
        self.backend = backend
        self.device = device
        self.min_free_gb = min_free_gb
        self.lock = threading.Lock()
        # 0.0 으로 두면 idle watcher 의 첫 tick 이 "300초 넘게 무요청"으로 오판해
        # 기동 직후 모델을 내려버린다. 기존 동작(_last_request_at = time.time())을 유지한다.
        self.last_used = time.time()
        self.error: str | None = None

    def is_loaded(self) -> bool:
        try:
            return bool(self.backend.is_loaded())
        except Exception:
            return False

    def ensure_loaded(self, logger) -> None:
        if self.is_loaded():
            return
        with self.lock:
            if self.is_loaded():          # double-check — 대기 중 남이 이미 올렸을 수 있다
                return
            if self.min_free_gb:
                require_free(self.device, self.min_free_gb)
            logger.info("[%s] load on %s (free=%s GB)", self.name, self.device, free_vram_gb(self.device))
            try:
                self.backend.load()
                self.error = None
            except Exception as exc:      # noqa: BLE001
                self.error = str(exc)
                logger.exception("[%s] load failed: %s", self.name, exc)
                raise

    def unload(self, logger) -> None:
        with self.lock:
            if not self.is_loaded():
                return
            self.backend.unload()
        logger.info("[%s] unload 완료 — %s free=%s GB", self.name, self.device, free_vram_gb(self.device))

    def status(self) -> dict:
        return {
            "loaded": self.is_loaded(),
            "device": self.device,
            "model_name": getattr(self.backend, "name", self.name),
            "free_vram_gb": free_vram_gb(self.device),
            "error": self.error,
        }
