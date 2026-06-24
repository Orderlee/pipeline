"""PE-Core 임베딩 서비스 HTTP 클라이언트 (L1 lib — 순수 Python + requests).

embedding-service 컨테이너(FastAPI)와 통신. lib/sam3.py 패턴 미러:
  - env 로 base_url (EMBEDDING_API_URL)
  - lazy requests.Session (keep-alive)
  - /health 기반 wait_until_ready
  - 모듈 레벨 싱글턴 get_embedding_client()

⚠️ lib 계층 규칙: dagster / defs / resources / ops import 금지.
"""

from __future__ import annotations

import os
import time
from functools import lru_cache

import requests

DEFAULT_URL = "http://embedding-service:8003"


class EmbeddingClient:
    """임베딩 서비스 HTTP 클라이언트. 이미지/텍스트 → 1024-d 벡터."""

    def __init__(self, base_url: str | None = None, timeout: float = 60.0) -> None:
        self.base_url = (base_url or os.environ.get("EMBEDDING_API_URL", DEFAULT_URL)).rstrip("/")
        self.timeout = timeout
        self.__session: requests.Session | None = None

    @property
    def _session(self) -> requests.Session:
        if self.__session is None:
            self.__session = requests.Session()
        return self.__session

    def warmup(self, timeout_sec: float = 120.0) -> dict:
        """idle-unload 후 lazy reload 를 동기 트리거. 이미 로드면 no-op.

        sam3.py 패턴 미러 — idle-unload 상태에선 /health 가 model_loaded:false 만
        반환하므로, reload 를 깨우려면 /warmup POST 가 필요하다.
        """
        r = self._session.post(f"{self.base_url}/warmup", timeout=timeout_sec)
        r.raise_for_status()
        return r.json()

    def wait_until_ready(self, max_wait_sec: float = 120.0, poll_sec: float = 3.0) -> bool:
        """/warmup 으로 reload 를 동기 트리거한 뒤, model_loaded=true 까지 대기.

        idle-unload 상태면 /health 만으론 영원히 false 이므로 먼저 /warmup 을 친다.
        /warmup 미지원(구버전 서버)이면 예외 → /health polling fallback.
        """
        try:
            info = self.warmup(timeout_sec=max_wait_sec)
            if info.get("model_loaded"):
                return True
        except requests.RequestException:
            pass  # 구버전 서버(/warmup 없음) 호환 — polling 으로 진행
        deadline = time.monotonic() + max_wait_sec
        while time.monotonic() < deadline:
            try:
                r = self._session.get(f"{self.base_url}/health", timeout=5)
                if r.ok and r.json().get("model_loaded"):
                    return True
            except requests.RequestException:
                pass
            time.sleep(poll_sec)
        return False

    def embed(self, image_bytes: bytes) -> list[float]:
        """이미지 바이트 → 1024-d 벡터."""
        r = self._session.post(
            f"{self.base_url}/embed",
            files={"file": ("image.jpg", image_bytes, "application/octet-stream")},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()["vector"]

    def embed_text(self, text: str) -> list[float]:
        """텍스트 → 1024-d 벡터 (동일 임베딩 공간, 텍스트→이미지 검색용)."""
        r = self._session.post(
            f"{self.base_url}/embed_text",
            data={"text": text},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()["vector"]


@lru_cache(maxsize=1)
def get_embedding_client() -> EmbeddingClient:
    """프로세스 레벨 싱글턴."""
    return EmbeddingClient()
