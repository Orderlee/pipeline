"""GPU 모델 서비스용 HTTP 클라이언트 베이스 — YOLO, SAM3 등 공통 패턴."""

from __future__ import annotations

import os
import time
from typing import Any

import requests


class ModelServiceClient:
    """HTTP 기반 GPU 모델 서비스 클라이언트 공통 베이스.

    서브클래스는 ``_env_key``와 ``_default_url``을 오버라이드하여
    환경변수 기반 URL 해석을 자동으로 얻는다.
    """

    _env_key: str = ""
    _default_url: str = "http://localhost:8000"

    def __init__(self, api_url: str | None = None, timeout: int = 60):
        resolved = api_url or os.environ.get(self._env_key, "") or self._default_url
        self.api_url = resolved.rstrip("/")
        self.timeout = timeout
        self._session: requests.Session | None = None

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def health(self) -> dict[str, Any]:
        resp = self.session.get(f"{self.api_url}/health", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def is_ready(self) -> bool:
        try:
            return bool(self.health().get("model_loaded"))
        except Exception:
            return False

    def wait_until_ready(self, max_wait_sec: int = 120, poll_interval: float = 3.0) -> bool:
        deadline = time.time() + max_wait_sec
        while time.time() < deadline:
            if self.is_ready():
                return True
            time.sleep(poll_interval)
        return False

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
