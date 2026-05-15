"""Veo (Vertex AI) image2video 어댑터.

Google Veo 3.x via `google-genai` SDK in Vertex mode. 비동기 (Long-Running
Operation) — submit 시 operation 시작, poll 시 `client.operations.get(op)` 로
상태 확인, done 일 때 video URI 반환.

Output 은 Vertex Veo 의 기본 동작상 GCS URI 가 반환되며, `VEO_OUTPUT_GCS_URI`
env 가 설정되어 있으면 그 prefix 로 저장. inline bytes 도 일부 모델에서 가능
하나, 안정성을 위해 GCS URI 출력 + storage 클라이언트로 download.

자격증명: `GEMINI_GOOGLE_APPLICATION_CREDENTIALS` (Nanobanana 와 공유). 키 미설정
시 mocking 모드.
"""

from __future__ import annotations

import os
import time
from typing import Any

from .base import BaseGenAIAdapter, PollResult, SubmitResult


_FAKE_MP4_PLACEHOLDER = (
    b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"
    b"\x00\x00\x00\x08free"
    b"\x00\x00\x00\x08mdat"
)


def _has_vertex_creds() -> bool:
    for v in ("GEMINI_GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS"):
        path = (os.getenv(v) or "").strip()
        if path and os.path.exists(path):
            return True
    return False


class VeoAdapter(BaseGenAIAdapter):
    engine = "veo"
    output_media = "video"
    is_synchronous = False
    output_ext = ".mp4"

    DEFAULT_MODELS: tuple[str, ...] = (
        "veo-3.1-generate-001",        # GA, 표준
        "veo-3.1-fast-generate-001",   # GA, 빠른 버전 (저비용)
        "veo-3.0-generate-001",        # 직전 GA (지원되면)
    )

    # operation 객체 in-memory 보관 — 컨테이너 재시작 시 잃음. provider_job_id 가 LRO
    # operation.name 이라 SDK 의 별도 lookup 이 없는 경우 polling 불가. follow-up 으로
    # operation.name 만으로 재구성 가능한지 검증 필요.
    _operations: dict[str, dict[str, Any]] = {}

    DEFAULT_DURATIONS: tuple[int, ...] = (4, 6, 8)   # Veo 3.1 가용

    def __init__(self) -> None:
        self.creds_path = (os.getenv("GEMINI_GOOGLE_APPLICATION_CREDENTIALS")
                            or os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
        self.project = os.getenv("GEMINI_PROJECT", "your-gcp-project")
        self.location = os.getenv("GEMINI_LOCATION", "us-central1")
        self.model_name = os.getenv("VEO_MODEL_NAME", "veo-3.1-generate-001").strip()
        self.aspect_ratio = os.getenv("VEO_ASPECT_RATIO", "16:9").strip()
        self.duration = int(os.getenv("VEO_DURATION_SECONDS", "8"))
        self.output_gcs_uri = os.getenv("VEO_OUTPUT_GCS_URI", "").strip()
        models_csv = os.getenv("VEO_AVAILABLE_MODELS", "").strip()
        self.available_models: tuple[str, ...] = (
            tuple(m.strip() for m in models_csv.split(",") if m.strip())
            if models_csv else self.DEFAULT_MODELS
        )
        durations_csv = os.getenv("VEO_AVAILABLE_DURATIONS", "").strip()
        self.available_durations: tuple[int, ...] = (
            tuple(int(d) for d in durations_csv.split(",") if d.strip().isdigit())
            if durations_csv else self.DEFAULT_DURATIONS
        )
        self.poll_sleep = int(os.getenv("VEO_MOCK_DONE_AFTER_SEC", "3"))
        self.is_mock = not _has_vertex_creds()

    # --------------------------------------------------------------
    def submit(self, image_bytes, image_filename, prompt, options=None):
        opts = options or {}
        # mode == 'txt2video' 또는 image_bytes 비어있으면 text-only 호출 (image= 인자 생략)
        mode = (opts.get("mode") or "").strip().lower()
        is_text_only = (mode == "txt2video") or not image_bytes

        if self.is_mock:
            import uuid
            rid = f"veo-mock-{uuid.uuid4().hex[:12]}"
            self._operations[rid] = {"submitted_at": time.time(), "is_mock": True}
            return SubmitResult(provider_job_id=rid, is_synchronous=False)

        try:
            from google import genai as ggenai
            from google.genai.types import GenerateVideosConfig, Image
        except ImportError as exc:
            raise RuntimeError("google-genai SDK 미설치") from exc

        client = ggenai.Client(vertexai=True, project=self.project, location=self.location)
        model = opts.get("model_name", self.model_name)

        cfg_kwargs: dict[str, Any] = {
            "aspect_ratio": opts.get("aspect_ratio", self.aspect_ratio),
        }
        dur_raw = opts.get("duration") or opts.get("duration_seconds")
        try:
            cfg_kwargs["duration_seconds"] = int(dur_raw) if dur_raw is not None else self.duration
        except (TypeError, ValueError):
            cfg_kwargs["duration_seconds"] = self.duration
        if self.output_gcs_uri:
            cfg_kwargs["output_gcs_uri"] = self.output_gcs_uri

        gen_kwargs: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "config": GenerateVideosConfig(**cfg_kwargs),
        }
        if not is_text_only:
            gen_kwargs["image"] = Image(
                image_bytes=image_bytes,
                mime_type=_image_mime(image_filename),
            )
        operation = client.models.generate_videos(**gen_kwargs)
        op_name = getattr(operation, "name", None) or f"veo-op-{int(time.time())}"
        self._operations[op_name] = {
            "operation": operation,
            "client": client,
            "submitted_at": time.time(),
        }
        return SubmitResult(provider_job_id=op_name, is_synchronous=False)

    def poll(self, provider_job_id):
        if provider_job_id.startswith("veo-mock"):
            rec = self._operations.get(provider_job_id) or {}
            ts = rec.get("submitted_at", 0)
            if time.time() - ts < self.poll_sleep:
                return PollResult(status="running")
            return PollResult(
                status="done",
                result_url=f"mock://veo/{provider_job_id}.mp4",
                cost_units=0.0,
            )

        rec = self._operations.get(provider_job_id)
        if rec is not None:
            op = rec["operation"]
            client = rec["client"]
            try:
                op = client.operations.get(op)
                rec["operation"] = op
            except Exception as exc:
                return PollResult(status="failed", error_message=f"veo poll exception: {exc}")
            return self._parse_sdk_operation(op)

        # in-memory tracker miss — 컨테이너 재시작 / 다른 인스턴스에서 생성된 경우.
        # operation.name 만으로 Vertex AI REST fetchPredictOperation 호출.
        return self._poll_via_rest(provider_job_id)

    def _parse_sdk_operation(self, op) -> "PollResult":
        if not getattr(op, "done", False):
            return PollResult(status="running")
        err = getattr(op, "error", None)
        if err:
            return PollResult(status="failed", error_message=str(err))
        try:
            result = op.result
            videos = getattr(result, "generated_videos", None) or []
            if not videos:
                # 가장 흔한 케이스: Vertex safety filter (rai_media_filtered_count > 0).
                # 실제 SDK 필드명은 rai_media_filtered_count / rai_media_filtered_reasons.
                rai_count = getattr(result, "rai_media_filtered_count", None)
                rai_reasons = getattr(result, "rai_media_filtered_reasons", None) or []
                if rai_count or rai_reasons:
                    msg = (
                        f"Vertex AI safety filter 차단 (count={rai_count}). "
                        f"reasons={rai_reasons!r}. "
                        "프롬프트/이미지를 완화해서 재시도하세요."
                    )
                else:
                    msg = (
                        "veo SDK 응답에 generated_videos 없음 (RAI 정보도 없음). "
                        f"result attrs={[a for a in dir(result) if not a.startswith('_')][:15]}"
                    )
                return PollResult(status="failed", error_message=msg)
            v = videos[0].video
            # Veo 응답은 두 가지 모드:
            #   (1) output_gcs_uri 설정 시 v.uri = 'gs://...'
            #   (2) inline 응답 시 v.video_bytes = bytes + v.mime_type = 'video/mp4'
            # 이전 버전은 uri 만 검사해 inline 케이스를 빈 result_url 로 반환 →
            # finalize 가 download_result("") = _FAKE_MP4_PLACEHOLDER 40byte 를 NAS
            # 에 기록하는 버그가 있었다. video_bytes fallback 추가.
            uri = getattr(v, "uri", None) or ""
            if uri:
                return PollResult(status="done", result_url=uri, cost_units=None)
            vbytes = getattr(v, "video_bytes", None)
            if vbytes:
                import base64
                mime = getattr(v, "mime_type", None) or "video/mp4"
                b64 = base64.b64encode(vbytes).decode("ascii")
                return PollResult(
                    status="done",
                    result_url=f"data:{mime};base64,{b64}",
                    cost_units=None,
                )
            return PollResult(
                status="failed",
                error_message=(
                    "veo SDK Video 객체에 uri/video_bytes 둘 다 없음. "
                    f"attrs={[a for a in dir(v) if not a.startswith('_')][:15]}"
                ),
            )
        except Exception as exc:
            return PollResult(status="failed", error_message=f"veo result parse: {exc}")

    def _poll_via_rest(self, operation_name: str) -> "PollResult":
        # operation_name format:
        #   projects/<proj>/locations/<loc>/publishers/google/models/<model>/operations/<op_id>
        parts = operation_name.split("/")
        if len(parts) < 10 or parts[0] != "projects":
            return PollResult(
                status="failed",
                error_message=f"veo unrecognized operation name: {operation_name[:120]}",
            )
        proj = parts[1]
        loc = parts[3]
        model = parts[7]
        endpoint = (
            f"https://{loc}-aiplatform.googleapis.com/v1/"
            f"projects/{proj}/locations/{loc}/publishers/google/models/{model}:fetchPredictOperation"
        )

        try:
            import google.auth
            import google.auth.transport.requests as gar
            import requests
        except ImportError as exc:
            return PollResult(status="failed", error_message=f"veo REST deps 미설치: {exc}")

        try:
            creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            creds.refresh(gar.Request())
        except Exception as exc:
            return PollResult(status="failed", error_message=f"veo auth: {exc}")

        try:
            r = requests.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {creds.token}",
                    "Content-Type": "application/json",
                },
                json={"operationName": operation_name},
                timeout=30,
            )
        except Exception as exc:
            return PollResult(status="failed", error_message=f"veo REST exception: {exc}")

        if not r.ok:
            return PollResult(
                status="failed",
                error_message=f"veo REST HTTP {r.status_code}: {r.text[:400]}",
            )
        try:
            data = r.json()
        except Exception as exc:
            return PollResult(status="failed", error_message=f"veo REST json parse: {exc}")

        if not data.get("done"):
            return PollResult(status="running")
        if data.get("error"):
            return PollResult(status="failed", error_message=str(data["error"])[:400])
        resp = data.get("response") or {}
        # Vertex Veo 응답 필드 후보: 'videos' / 'generatedSamples' / 'generated_videos' / 'predictions'
        videos = (resp.get("videos") or resp.get("generated_videos")
                  or resp.get("generatedSamples") or resp.get("predictions") or [])
        if not videos:
            # safety filter / RAI block 등 — 응답 전체 dump 로 사용자가 사유 확인.
            import json as _json
            blob = _json.dumps(resp, ensure_ascii=False)[:800]
            return PollResult(
                status="failed",
                error_message=(
                    f"veo REST 응답에 videos 필드 없음. response: {blob}"
                ),
            )
        v0 = videos[0]
        # 영상 메타 필드 후보: 'video', 'videoUri', 'gcsUri', 'bytesBase64Encoded'
        if isinstance(v0, dict):
            uri = (v0.get("gcsUri") or v0.get("videoUri")
                   or (v0.get("video") or {}).get("uri")
                   or v0.get("uri") or "")
            b64 = v0.get("bytesBase64Encoded") or (v0.get("video") or {}).get("bytesBase64Encoded")
            if uri:
                return PollResult(status="done", result_url=uri, cost_units=None)
            if b64:
                # inline 응답 — download_result 에서 처리하도록 data URI 로 전달
                return PollResult(status="done", result_url=f"data:video/mp4;base64,{b64}",
                                  cost_units=None)
        return PollResult(
            status="failed",
            error_message=f"veo REST video item parse fail: keys={list(v0.keys())[:10] if isinstance(v0, dict) else type(v0)}",
        )

    def download_result(self, result_url):
        if not result_url or result_url.startswith("mock://"):
            return _FAKE_MP4_PLACEHOLDER
        if result_url.startswith("gs://"):
            return _download_gcs(result_url, self.project)
        if result_url.startswith("data:"):
            # inline base64 video — REST fetchPredictOperation 의 bytesBase64Encoded fallback
            import base64
            try:
                _, b64 = result_url.split(",", 1)
                return base64.b64decode(b64)
            except Exception as exc:
                raise RuntimeError(f"veo inline base64 디코드 실패: {exc}")
        # HTTPS URL fallback
        import requests
        r = requests.get(result_url, timeout=300, stream=True)
        r.raise_for_status()
        return r.content


def _download_gcs(gcs_uri: str, project: str) -> bytes:
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-storage 미설치 — Veo 결과 GCS 다운로드 불가. "
            "requirements.txt 의 google-cloud-storage 확인."
        ) from exc
    from urllib.parse import urlparse
    p = urlparse(gcs_uri)
    bucket = p.netloc
    blob_path = p.path.lstrip("/")
    client = storage.Client(project=project)
    return client.bucket(bucket).blob(blob_path).download_as_bytes()


def _image_mime(filename: str) -> str:
    name = (filename or "").lower()
    if name.endswith(".png"):
        return "image/png"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    if name.endswith(".webp"):
        return "image/webp"
    return "image/png"
