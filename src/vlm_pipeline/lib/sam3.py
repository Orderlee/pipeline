"""SAM3 HTTP client for shadow segmentation benchmark."""

from __future__ import annotations

import io
import json
import os
import time
from typing import Any

import requests


def _default_api_url() -> str:
    return os.environ.get("SAM3_API_URL", "http://sam3:8002")


class SAM3Client:
    """SAM3 segmentation HTTP client."""

    def __init__(self, api_url: str | None = None, timeout: int = 300):
        self.api_url = (api_url or _default_api_url()).rstrip("/")
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

    def wait_until_ready(self, max_wait_sec: int = 300, poll_interval: float = 5.0) -> bool:
        deadline = time.time() + max_wait_sec
        while time.time() < deadline:
            if self.is_ready():
                return True
            time.sleep(poll_interval)
        return False

    def segment(
        self,
        image_bytes: bytes,
        *,
        prompts: list[str],
        filename: str = "image.jpg",
        score_threshold: float = 0.0,
        max_masks_per_prompt: int = 50,
    ) -> dict[str, Any]:
        files = {"file": (filename, io.BytesIO(image_bytes), "image/jpeg")}
        data = {
            "prompts_json": json.dumps(prompts, ensure_ascii=False),
            "score_threshold": str(float(score_threshold)),
            "max_masks_per_prompt": str(int(max_masks_per_prompt)),
        }
        resp = self.session.post(
            f"{self.api_url}/segment",
            files=files,
            data=data,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None


_client_instance: SAM3Client | None = None


def get_sam3_client() -> SAM3Client:
    global _client_instance
    if _client_instance is None:
        _client_instance = SAM3Client()
    return _client_instance
