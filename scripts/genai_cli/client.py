"""HTTP client — REST API 호출 + Accept: application/json 강제."""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Any

import requests

from genai_cli.config import CliConfig


class APIError(RuntimeError):
    def __init__(self, status: int, message: str, body: Any = None):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message
        self.body = body


class HTTPClient:
    def __init__(self, cfg: CliConfig, *, timeout: int = 60):
        self.cfg = cfg
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if cfg.auth:
            self.session.auth = cfg.auth

    def _url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self.cfg.base}{path}"

    def _raise(self, r: requests.Response) -> None:
        if r.ok:
            return
        try:
            body = r.json()
            msg = body.get("detail") if isinstance(body, dict) else str(body)
        except Exception:
            body = r.text[:500]
            msg = body
        raise APIError(r.status_code, str(msg)[:500], body)

    def get(self, path: str, **params: Any) -> Any:
        r = self.session.get(self._url(path), params=params or None, timeout=self.timeout)
        self._raise(r)
        return r.json()

    def post(self, path: str, *, json: Any = None, data: Any = None,
             files: Any = None, timeout: int | None = None) -> Any:
        r = self.session.post(
            self._url(path),
            json=json, data=data, files=files,
            timeout=timeout or self.timeout,
        )
        self._raise(r)
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    # ----- 도메인 헬퍼 ------------------------------------------------

    def healthz(self) -> dict:
        return self.get("/healthz")

    def submit_batch(self, *, engine: str, prompt: str, files: list[Path],
                     options: dict | None = None,
                     bulk_group_id: str | None = None) -> dict:
        """POST /genai/batches (multipart). bulk_group_id 는 form field 로 별도 송신."""
        data = {"engine": engine, "prompt": prompt}
        for k, v in (options or {}).items():
            if v is not None:
                data[k] = str(v)
        if bulk_group_id:
            data["bulk_group_id"] = bulk_group_id
        files_payload = [
            ("files", (p.name, p.open("rb"), self._mime(p))) for p in files
        ]
        try:
            return self.post(
                "/genai/batches",
                data=data, files=files_payload,
                timeout=180,
            )
        finally:
            for _, (_n, fh, _m) in files_payload:
                try:
                    fh.close()
                except Exception:
                    pass

    def get_limits(self) -> dict:
        """GET /genai/limits — 한도 + 호출 사용자 24h 사용량 + per-batch 상한."""
        return self.get("/genai/limits")

    def list_batches(self, *, status: str | None = None,
                     engine: str | None = None, limit: int = 50) -> list[dict]:
        # API 서버가 query param 무시할 수도 — 일단 보내고 client 측 filter
        rows = self.get("/genai/batches", limit=limit)
        if not isinstance(rows, list):
            rows = rows.get("batches", []) if isinstance(rows, dict) else []
        if status:
            rows = [r for r in rows if (r.get("status") == status)]
        if engine:
            rows = [r for r in rows if (r.get("engine") == engine)]
        return rows

    def get_batch(self, batch_id: str) -> dict:
        return self.get(f"/genai/batches/{batch_id}")

    def retry_job(self, job_id: str) -> dict:
        return self.post(f"/genai/jobs/{job_id}/retry")

    def costs(self, *, range_: str = "week") -> dict:
        return self.get("/genai/costs", range=range_)

    @staticmethod
    def _mime(p: Path) -> str:
        ext = p.suffix.lower()
        return {
            ".png": "image/png",
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".webp": "image/webp",
        }.get(ext, "application/octet-stream")
