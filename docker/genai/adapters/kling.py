"""Kling AI image2video 어댑터.

JWT 인증: AccessKey (iss) + SecretKey (HS256 sign).
공식 엔드포인트: https://api.klingai.com/v1/videos/image2video (image2video 비디오 생성)
                /v1/videos/image2video/{task_id} (status poll)

API 키 (KLING_ACCESS_KEY / KLING_SECRET_KEY) 미설정 시 mocking 모드 동작:
  - submit() → fake provider_job_id 반환
  - poll() → 첫 호출 'running', N초 후 'done' + 작은 더미 mp4 placeholder

이렇게 두면 사용자가 키를 채우기 전에도 e2e 흐름(NAS atomic write → ingest sensor →
build asset) 을 검증 가능.
"""

from __future__ import annotations

import os
import time
from typing import Any

from .base import BaseGenAIAdapter, PollResult, SubmitResult


_FAKE_MP4_PLACEHOLDER = (
    # 최소 ftyp/mdat 박스를 가진 0-frame mp4. ffprobe 가 read 시도는 안 하니
    # archive/dataset 검증에는 충분한 placeholder.
    b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"
    b"\x00\x00\x00\x08free"
    b"\x00\x00\x00\x08mdat"
)


class KlingAdapter(BaseGenAIAdapter):
    engine = "kling"
    output_media = "video"
    is_synchronous = False
    output_ext = ".mp4"

    # Kling Pro 요금제 가용 모델 — 실 호출은 credit 자동 차감됨.
    # env KLING_AVAILABLE_MODELS 로 override (CSV), 운영자가 본인 plan 기준으로 조정.
    DEFAULT_MODELS: tuple[str, ...] = (
        "kling-video-o1",      # VIDEO O1 (3.0 omni 전신)
        "kling-video-3",       # VIDEO 3
        "kling-v2-1-master",   # V2.1 Master (pro only)
        "kling-v2-master",     # V2 Master
        "kling-v1-6",          # V1.6
        "kling-v1-5",          # V1.5
        "kling-v1",            # V1 legacy
    )

    def __init__(self) -> None:
        self.access_key = os.getenv("KLING_ACCESS_KEY", "").strip()
        self.secret_key = os.getenv("KLING_SECRET_KEY", "").strip()
        self.api_base = os.getenv("KLING_API_BASE", "https://api.klingai.com").rstrip("/")
        self.model_name = os.getenv("KLING_MODEL_NAME", "kling-video-o1").strip()
        self.mode = os.getenv("KLING_MODE", "pro").strip()           # std | pro (Pro 요금제 → pro 권장)
        self.duration = os.getenv("KLING_DURATION", "5").strip()      # 5 or 10
        models_csv = os.getenv("KLING_AVAILABLE_MODELS", "").strip()
        self.available_models: tuple[str, ...] = (
            tuple(m.strip() for m in models_csv.split(",") if m.strip())
            if models_csv else self.DEFAULT_MODELS
        )
        self.submit_timeout = int(os.getenv("KLING_SUBMIT_TIMEOUT", "60"))
        self.poll_timeout = int(os.getenv("KLING_POLL_TIMEOUT", "30"))
        self.download_timeout = int(os.getenv("KLING_DOWNLOAD_TIMEOUT", "300"))
        self.is_mock = not (self.access_key and self.secret_key)
        if self.is_mock:
            # 모킹 모드 추적용 in-memory store: provider_job_id -> {submitted_at, ...}
            self._mock_jobs: dict[str, dict[str, Any]] = {}

    # --------------------------------------------------------------
    # JWT 서명 (Kling 공식 가이드)
    # --------------------------------------------------------------
    def _build_jwt(self) -> str:
        import jwt  # PyJWT
        now = int(time.time())
        payload = {
            "iss": self.access_key,
            "exp": now + 1800,        # 30 minutes
            "nbf": now - 5,
        }
        return jwt.encode(payload, self.secret_key, algorithm="HS256",
                          headers={"alg": "HS256", "typ": "JWT"})

    # --------------------------------------------------------------
    # public API
    # --------------------------------------------------------------
    def submit(
        self,
        image_bytes: bytes,
        image_filename: str,
        prompt: str,
        options: dict | None = None,
    ) -> SubmitResult:
        if self.is_mock:
            import uuid
            provider_id = f"kling-mock-{uuid.uuid4().hex[:12]}"
            self._mock_jobs[provider_id] = {
                "submitted_at": time.time(),
                "prompt": prompt,
            }
            return SubmitResult(
                provider_job_id=provider_id,
                is_synchronous=False,
            )

        # 실제 호출: image bytes → base64 → JSON body. requests 사용.
        import base64
        import requests
        opts = options or {}
        body = {
            "model_name": opts.get("model_name", self.model_name),
            "mode": opts.get("mode", self.mode),
            "image": base64.b64encode(image_bytes).decode("ascii"),
            "prompt": prompt,
            "duration": str(opts.get("duration", self.duration)),
            "aspect_ratio": opts.get("aspect_ratio", "16:9"),
        }
        # negative prompt / cfg_scale / camera control 등 향후 옵션은 opts 통해 그대로 통과
        for extra in ("negative_prompt", "cfg_scale", "camera_control", "static_mask",
                      "dynamic_masks", "image_tail", "callback_url"):
            if extra in opts and opts[extra] is not None:
                body[extra] = opts[extra]
        headers = {
            "Authorization": f"Bearer {self._build_jwt()}",
            "Content-Type": "application/json",
        }
        r = requests.post(
            f"{self.api_base}/v1/videos/image2video",
            json=body,
            headers=headers,
            timeout=self.submit_timeout,
        )
        if not r.ok:
            # 4xx/5xx 의 body 까지 노출 — Kling 의 자세한 거부 사유 (model 미허용,
            # mode mismatch, parameter invalid 등) 가 보통 body 에 들어 있음.
            raise RuntimeError(
                f"kling submit HTTP {r.status_code} (model={body['model_name']!r} "
                f"mode={body.get('mode')!r} duration={body.get('duration')!r}): "
                f"{r.text[:500]}"
            )
        data = r.json().get("data", {}) or {}
        task_id = data.get("task_id")
        if not task_id:
            raise RuntimeError(f"kling submit 응답에 task_id 없음: {r.text[:500]}")
        return SubmitResult(provider_job_id=str(task_id), is_synchronous=False)

    def poll(self, provider_job_id: str) -> PollResult:
        if self.is_mock:
            job = self._mock_jobs.get(provider_job_id)
            if job is None:
                # 컨테이너 재시작으로 in-memory mock store 가 초기화된 상태.
                # 'failed' 가 아니라 'done' + placeholder 로 처리해 finalize 가 진행되도록
                # — mocking 모드는 e2e 흐름 검증용이라 stale id 도 정상 종료가 안전.
                return PollResult(
                    status="done",
                    result_url=f"mock://kling/{provider_job_id}.mp4",
                    cost_units=0.0,
                )
            elapsed = time.time() - job["submitted_at"]
            if elapsed < 2:
                return PollResult(status="running")
            return PollResult(
                status="done",
                result_url=f"mock://kling/{provider_job_id}.mp4",
                cost_units=0.0,
            )

        import requests
        headers = {"Authorization": f"Bearer {self._build_jwt()}"}
        r = requests.get(
            f"{self.api_base}/v1/videos/image2video/{provider_job_id}",
            headers=headers,
            timeout=self.poll_timeout,
        )
        if not r.ok:
            return PollResult(
                status="failed",
                error_message=f"kling poll HTTP {r.status_code}: {r.text[:300]}",
            )
        data = r.json().get("data", {}) or {}
        task_status = (data.get("task_status") or "").lower()
        if task_status in ("submitted", "processing", "running"):
            return PollResult(status="running")
        if task_status == "succeed":
            videos = (data.get("task_result") or {}).get("videos") or []
            if not videos:
                return PollResult(status="failed", error_message="empty videos[]")
            url = videos[0].get("url")
            # cost 추출 — Kling 응답 구조에서 credit 차감량 (필드명은 model/version 별로 다양:
            # 'cost' / 'price' / 'consume_credits' 등). 가장 흔한 후보들 시도.
            cost = (data.get("cost") or data.get("price")
                    or data.get("consume_credits") or data.get("credits_used"))
            try:
                cost_units = float(cost) if cost is not None else None
            except (TypeError, ValueError):
                cost_units = None
            return PollResult(status="done", result_url=url, cost_units=cost_units)
        if task_status == "failed":
            return PollResult(status="failed",
                              error_message=data.get("task_status_msg") or "kling failed")
        return PollResult(status="running")

    def download_result(self, result_url: str) -> bytes:
        if self.is_mock or result_url.startswith("mock://"):
            return _FAKE_MP4_PLACEHOLDER

        import requests
        r = requests.get(result_url, timeout=self.download_timeout, stream=True)
        r.raise_for_status()
        return r.content
