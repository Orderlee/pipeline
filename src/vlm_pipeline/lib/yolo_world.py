"""YOLO-World HTTP 클라이언트 — 별도 Docker 컨테이너의 YOLO API 호출.

YOLO-World-L 모델은 별도 Docker 컨테이너(yolo 서비스)에서 GPU 1 전용으로 상주.
이 모듈은 HTTP를 통해 detection 요청을 보내는 클라이언트.

환경변수:
  YOLO_API_URL: YOLO 서버 주소 (기본: http://yolo:8001)
"""

from __future__ import annotations

import io
import json
import os
import time
from typing import Any

import requests


def _default_api_url() -> str:
    return os.environ.get("YOLO_API_URL", "http://yolo:8001")


class YOLOWorldClient:
    """YOLO-World HTTP 클라이언트."""

    def __init__(self, api_url: str | None = None, timeout: int = 60):
        self.api_url = (api_url or _default_api_url()).rstrip("/")
        self.timeout = timeout
        self._session: requests.Session | None = None

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def health(self) -> dict[str, Any]:
        resp = self.session.get(
            f"{self.api_url}/health",
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def is_ready(self) -> bool:
        try:
            info = self.health()
            return info.get("model_loaded", False)
        except Exception:
            return False

    def wait_until_ready(self, max_wait_sec: int = 120, poll_interval: float = 3.0) -> bool:
        """모델 로드 완료까지 대기."""
        deadline = time.time() + max_wait_sec
        while time.time() < deadline:
            if self.is_ready():
                return True
            time.sleep(poll_interval)
        return False

    def detect(
        self,
        image_bytes: bytes,
        *,
        conf: float = 0.25,
        iou: float = 0.45,
        max_det: int = 300,
        filename: str = "image.jpg",
        classes: list[str] | None = None,
    ) -> dict[str, Any]:
        """단일 이미지 detection."""
        files = {"file": (filename, io.BytesIO(image_bytes), "image/jpeg")}
        params = {"conf": conf, "iou": iou, "max_det": max_det}
        data = {"classes_json": json.dumps(classes, ensure_ascii=False)} if classes else None
        resp = self.session.post(
            f"{self.api_url}/detect",
            files=files,
            data=data,
            params=params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def detect_batch(
        self,
        image_bytes_list: list[bytes],
        *,
        conf: float = 0.25,
        iou: float = 0.45,
        max_det: int = 300,
        classes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """배치 이미지 detection."""
        if not image_bytes_list:
            return []

        files = []
        for idx, img_bytes in enumerate(image_bytes_list):
            files.append(("files", (f"image_{idx}.jpg", io.BytesIO(img_bytes), "image/jpeg")))

        params = {"conf": conf, "iou": iou, "max_det": max_det}
        data = {"classes_json": json.dumps(classes, ensure_ascii=False)} if classes else None
        resp = self.session.post(
            f"{self.api_url}/detect/batch",
            files=files,
            data=data,
            params=params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        results = list(data.get("results", []) or [])
        try:
            fallback_elapsed_ms = round(float(data.get("elapsed_ms") or 0.0) / max(1, len(results)), 1)
        except (TypeError, ValueError):
            fallback_elapsed_ms = 0.0
        for result in results:
            if isinstance(result, dict) and "elapsed_ms" not in result:
                result["elapsed_ms"] = fallback_elapsed_ms
        return results

    def set_classes(self, classes: list[str]) -> dict[str, Any]:
        """detection 클래스 변경."""
        resp = self.session.post(
            f"{self.api_url}/classes",
            json={"classes": classes},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None


_client_instance: YOLOWorldClient | None = None


def get_yolo_client() -> YOLOWorldClient:
    """싱글턴 클라이언트 반환."""
    global _client_instance
    if _client_instance is None:
        _client_instance = YOLOWorldClient()
    return _client_instance
