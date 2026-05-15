"""GPT Image (OpenAI gpt-image-1). 동기 엔진 (image edit / generate → image).

reference image + prompt → image. 키 OPENAI_API_KEY 가 없으면 mocking 모드.
"""

from __future__ import annotations

import base64
import io
import os

from .base import BaseGenAIAdapter, PollResult, SubmitResult


_FAKE_PNG_PLACEHOLDER = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\rIDATx\x9cc\xfc\xff\xff?\x03\x00\x06\xff\x02\xfe\xa3\xa6T\x9d\x00\x00\x00\x00IEND\xaeB`\x82"
)


class GPTImageAdapter(BaseGenAIAdapter):
    engine = "gpt_image"
    output_media = "image"
    is_synchronous = True
    output_ext = ".png"

    def __init__(self) -> None:
        self.api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        self.model = os.getenv("GPT_IMAGE_MODEL", "gpt-image-1")
        self.size = os.getenv("GPT_IMAGE_SIZE", "1024x1024")
        self.is_mock = not self.api_key

    def submit(self, image_bytes, image_filename, prompt, options=None):
        if self.is_mock:
            return SubmitResult(
                provider_job_id=f"gpt-mock-{image_filename}",
                immediate_result=_FAKE_PNG_PLACEHOLDER,
                immediate_ext=".png",
                cost_units=0.0,
                is_synchronous=True,
            )
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError("openai SDK 미설치. requirements.txt 확인.") from exc

        client = OpenAI(api_key=self.api_key)
        # gpt-image-1 의 image edit 모드 — reference image 와 prompt 동시 수신
        # image= 는 file-like 또는 bytes-like accepted.
        opts = options or {}
        size = opts.get("size") or self.size
        result = client.images.edit(
            model=self.model,
            image=("input.png", io.BytesIO(image_bytes), "image/png"),
            prompt=prompt,
            size=size,
            n=1,
        )
        if not result.data:
            raise RuntimeError("gpt-image 응답에 data 없음")
        b64 = result.data[0].b64_json
        if not b64:
            raise RuntimeError("gpt-image 응답에 b64_json 없음")
        return SubmitResult(
            provider_job_id=f"gpt-{result.created if hasattr(result, 'created') else 'sync'}",
            immediate_result=base64.b64decode(b64),
            immediate_ext=".png",
            cost_units=None,
            is_synchronous=True,
        )

    def poll(self, provider_job_id):
        return PollResult(status="done")

    def download_result(self, result_url):
        return _FAKE_PNG_PLACEHOLDER
