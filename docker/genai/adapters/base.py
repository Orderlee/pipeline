"""BaseGenAIAdapter — 4 엔진 (Kling, Higgsfield, Nanobanana, GPT Image) 공통 인터페이스.

엔진별 동작 차이를 흡수하는 얇은 Protocol:
  - 동기 엔진(Nanobanana, GPT Image): submit() 안에서 결과까지 받아 download_result()
    가 즉시 bytes 반환. poll() 은 항상 'done' 반환.
  - 비동기 엔진(Kling, Higgsfield): submit() 은 provider_job_id 반환 후 종료.
    Dagster polling sensor 가 poll() 으로 상태 갱신, 'done' 시 download_result()
    호출.

Adapter 는 외부 API 호출만 책임지고, DB/NAS 쓰기는 호출자(orchestrator)가 처리.
"""

from __future__ import annotations

import os
import time
from typing import Protocol


# 어댑터 공용 placeholder bytes (mocking 모드 / validation 통과용).
# 최소 ftyp/mdat 박스를 가진 0-frame mp4. ffprobe 가 read 시도는 안 하니
# archive/dataset 검증에는 충분한 placeholder.
_FAKE_MP4_PLACEHOLDER = (
    b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"
    b"\x00\x00\x00\x08free"
    b"\x00\x00\x00\x08mdat"
)

# 1x1 흑색 PNG (validation 통과용 placeholder)
_FAKE_PNG_PLACEHOLDER = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\rIDATx\x9cc\xfc\xff\xff?\x03\x00\x06\xff\x02\xfe\xa3\xa6T\x9d\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _image_mime(filename: str) -> str:
    name = (filename or "").lower()
    if name.endswith(".png"):
        return "image/png"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    if name.endswith(".webp"):
        return "image/webp"
    return "image/png"


def _has_vertex_creds() -> bool:
    for v in ("GEMINI_GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS"):
        path = (os.getenv(v) or "").strip()
        if path and os.path.exists(path):
            return True
    return False


def download_bytes_with_retry(url: str, timeout: int, attempts: int = 3) -> bytes:
    """결과 CDN 다운로드 — transient 절단에 짧은 재시도 (2s/4s backoff).

    Kling CDN 이 간헐적으로 연결을 끊는다 (RemoteDisconnected 2026-07-03,
    SSL UNEXPECTED_EOF 2026-07-02 실측). 여기서 1회 실패로 raise 하면 호출자
    (_do_finalize)가 job 을 영구 failed 처리 → 이미 과금된 결과물 소실.
    4xx(만료 URL 등)는 재시도 무의미라 즉시 raise."""
    import requests

    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            r = requests.get(url, timeout=timeout, stream=True)
            r.raise_for_status()
            return r.content
        except requests.exceptions.RequestException as exc:
            resp = getattr(exc, "response", None)
            if resp is not None and resp.status_code < 500:
                raise
            last_exc = exc
            if attempt + 1 < attempts:
                time.sleep(2**attempt * 2)
    raise last_exc  # type: ignore[misc]


class SubmitResult:
    def __init__(
        self,
        provider_job_id: str | None,
        immediate_result: bytes | None = None,
        immediate_ext: str | None = None,
        cost_units: float | None = None,
        is_synchronous: bool = False,
    ):
        self.provider_job_id = provider_job_id
        self.immediate_result = immediate_result
        self.immediate_ext = immediate_ext
        self.cost_units = cost_units
        self.is_synchronous = is_synchronous


class PollResult:
    def __init__(
        self,
        status: str,             # 'running' | 'done' | 'failed'
        result_url: str | None = None,
        error_message: str | None = None,
        cost_units: float | None = None,
    ):
        self.status = status
        self.result_url = result_url
        self.error_message = error_message
        self.cost_units = cost_units


class BaseGenAIAdapter(Protocol):
    engine: str               # 'kling' | 'higgsfield' | 'nanobanana' | 'gpt_image'
    output_media: str         # 'video' | 'image'
    is_synchronous: bool      # True: submit 시 결과 즉시. False: polling 필요.
    output_ext: str           # default 출력 확장자 (.mp4, .png)

    def submit(
        self,
        image_bytes: bytes,
        image_filename: str,
        prompt: str,
        options: dict | None = None,
    ) -> SubmitResult:
        """이미지 + 프롬프트 → 외부 API submit. 동기 엔진은 결과까지 포함."""
        ...

    def poll(self, provider_job_id: str) -> PollResult:
        """비동기 엔진의 진행 상태 조회. 동기 엔진은 ('done', '<inline>') 반환."""
        ...

    def download_result(self, result_url: str) -> bytes:
        """완료된 결과 URL → bytes. 동기 엔진은 submit 시 받은 bytes 를 직접 반환 가능."""
        ...
