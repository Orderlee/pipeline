"""Higgsfield via fal.ai. 비동기 엔진 (image → video).

fal.ai 의 Higgsfield 모델 (fal-ai/higgsfield-* 등) 을 queue 모드로 호출.
fal-client SDK 사용. FAL_KEY 미설정 시 mocking 모드.

queue 모드 패턴:
  1) submit_async() → request_id 반환
  2) status(request_id) 폴링 → IN_PROGRESS / COMPLETED / FAILED
  3) result(request_id) → video.url
"""

from __future__ import annotations

import os
import time

from .base import (
    _FAKE_MP4_PLACEHOLDER,
    BaseGenAIAdapter,
    PollResult,
    SubmitResult,
    _image_mime,
    download_bytes_with_retry,
)


class HiggsfieldAdapter(BaseGenAIAdapter):
    engine = "higgsfield"
    output_media = "video"
    is_synchronous = False
    output_ext = ".mp4"

    def __init__(self) -> None:
        self.api_key = (os.getenv("FAL_KEY") or "").strip()
        # ⚠ 운영자: fal.ai 대시보드에서 정확한 Higgsfield 모델 ID 확인 후
        # HIGGSFIELD_FAL_MODEL env 로 주입 권장. 아래 default 는 후보값 — 실서비스 시
        # smoke-test 필수.
        self.model_id = os.getenv("HIGGSFIELD_FAL_MODEL", "fal-ai/higgsfield/i2v-soul")
        self.submit_timeout = int(os.getenv("HIGGSFIELD_SUBMIT_TIMEOUT", "60"))
        self.poll_timeout = int(os.getenv("HIGGSFIELD_POLL_TIMEOUT", "30"))
        self.download_timeout = int(os.getenv("HIGGSFIELD_DOWNLOAD_TIMEOUT", "300"))
        self.is_mock = not self.api_key
        if self.is_mock:
            self._mock_jobs: dict[str, float] = {}

    def submit(self, image_bytes, image_filename, prompt, options=None):
        if self.is_mock:
            import uuid
            rid = f"higgs-mock-{uuid.uuid4().hex[:12]}"
            self._mock_jobs[rid] = time.time()
            return SubmitResult(provider_job_id=rid, is_synchronous=False)

        try:
            import fal_client  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("fal-client 미설치. requirements.txt 확인.") from exc

        os.environ["FAL_KEY"] = self.api_key  # SDK 가 env 로 읽음
        # 이미지 업로드 (data URL 또는 fal storage)
        image_url = fal_client.upload(image_bytes, content_type=_image_mime(image_filename))
        opts = options or {}
        body = {
            "image_url": image_url,
            "prompt": prompt,
            **{k: v for k, v in opts.items() if k in ("duration", "aspect_ratio", "seed")},
        }
        handle = fal_client.submit(self.model_id, arguments=body)
        return SubmitResult(provider_job_id=str(handle.request_id), is_synchronous=False)

    def poll(self, provider_job_id):
        if self.is_mock:
            ts = self._mock_jobs.get(provider_job_id)
            if ts is None:
                return PollResult(
                    status="done",
                    result_url=f"mock://higgs/{provider_job_id}.mp4",
                    cost_units=0.0,
                )
            if time.time() - ts < 2:
                return PollResult(status="running")
            return PollResult(
                status="done",
                result_url=f"mock://higgs/{provider_job_id}.mp4",
                cost_units=0.0,
            )

        try:
            import fal_client  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("fal-client 미설치. requirements.txt 확인.") from exc

        os.environ["FAL_KEY"] = self.api_key
        try:
            status = fal_client.status(self.model_id, provider_job_id)
        except Exception as exc:
            return PollResult(status="failed", error_message=str(exc))

        # fal_client.status 는 IN_PROGRESS / IN_QUEUE / COMPLETED / FAILED 등
        st = getattr(status, "status", None) or str(status)
        st_norm = (st or "").upper()
        if st_norm in ("IN_PROGRESS", "IN_QUEUE", "RUNNING"):
            return PollResult(status="running")
        if st_norm == "COMPLETED":
            try:
                result = fal_client.result(self.model_id, provider_job_id)
            except Exception as exc:
                return PollResult(status="failed", error_message=str(exc))
            video = (result or {}).get("video") or {}
            url = video.get("url")
            if not url:
                return PollResult(status="failed", error_message="empty video.url")
            return PollResult(status="done", result_url=url)
        if st_norm == "FAILED":
            err = getattr(status, "error", None) or "fal failed"
            return PollResult(status="failed", error_message=str(err))
        return PollResult(status="running")

    def download_result(self, result_url):
        if self.is_mock or result_url.startswith("mock://"):
            return _FAKE_MP4_PLACEHOLDER
        return download_bytes_with_retry(result_url, self.download_timeout)
