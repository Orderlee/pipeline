"""Nanobanana = Google Gemini 2.5 Flash Image. 동기 엔진 (image → image).

Vertex AI 에서 호출. 인증은 GEMINI_GOOGLE_APPLICATION_CREDENTIALS (또는
GOOGLE_APPLICATION_CREDENTIALS) 가 가리키는 service account JSON.

키 미설정 시 mocking 모드 — submit() 이 더미 PNG 1픽셀 반환.
"""

from __future__ import annotations

import os

from .base import (
    _FAKE_PNG_PLACEHOLDER,
    BaseGenAIAdapter,
    PollResult,
    SubmitResult,
    _has_vertex_creds,
    _image_mime,
)


class NanobananaAdapter(BaseGenAIAdapter):
    engine = "nanobanana"
    output_media = "image"
    is_synchronous = True
    output_ext = ".png"

    def __init__(self) -> None:
        self.project = os.getenv("GEMINI_PROJECT", "your-gcp-project")
        self.location = os.getenv("GEMINI_LOCATION", "us-central1")
        self.model_id = os.getenv("NANOBANANA_MODEL", "gemini-2.5-flash-image")
        self.is_mock = not _has_vertex_creds()

    def submit(self, image_bytes, image_filename, prompt, options=None):
        if self.is_mock:
            return SubmitResult(
                provider_job_id=f"nano-mock-{image_filename}",
                immediate_result=_FAKE_PNG_PLACEHOLDER,
                immediate_ext=".png",
                cost_units=0.0,
                is_synchronous=True,
            )
        # 실제 호출: Vertex AI generative SDK 사용
        try:
            from google import genai as google_genai
            from google.genai import types
        except ImportError as exc:
            raise RuntimeError(
                "google-genai SDK 미설치. requirements.txt 의 google-genai 확인."
            ) from exc

        client = google_genai.Client(vertexai=True, project=self.project, location=self.location)
        result = client.models.generate_content(
            model=self.model_id,
            contents=[
                types.Part.from_bytes(data=image_bytes, mime_type=_image_mime(image_filename)),
                prompt,
            ],
        )
        # 응답에서 이미지 part 추출
        for cand in result.candidates or []:
            for part in (cand.content.parts or []):
                inline = getattr(part, "inline_data", None)
                if inline and inline.data:
                    return SubmitResult(
                        provider_job_id=f"nano-{getattr(result, 'response_id', 'sync')}",
                        immediate_result=inline.data,
                        immediate_ext=".png",
                        cost_units=None,
                        is_synchronous=True,
                    )

        # 이미지 없는 응답 — safety block / text-only fallback. 명확히 실패 사유 노출.
        finish_reasons: list[str] = []
        text_parts: list[str] = []
        for cand in result.candidates or []:
            fr = getattr(cand, "finish_reason", None)
            if fr:
                finish_reasons.append(str(fr))
            for part in (cand.content.parts or []):
                txt = getattr(part, "text", None)
                if txt:
                    text_parts.append(txt[:200])
        block_reason = getattr(getattr(result, "prompt_feedback", None), "block_reason", None)
        raise RuntimeError(
            "nanobanana 응답에 이미지 inline_data 없음 "
            f"(block_reason={block_reason!r}, finish_reasons={finish_reasons}, "
            f"text_excerpt={text_parts[:1]})"
        )

    def poll(self, provider_job_id):
        # 동기 엔진은 submit 시점에 결과까지 받으므로 polling 호출되지 않는다.
        return PollResult(status="done")

    def download_result(self, result_url):
        # 동기 엔진은 submit 시 immediate_result 로 bytes 를 받으므로 사용되지 않음.
        return _FAKE_PNG_PLACEHOLDER
