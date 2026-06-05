"""Reusable Gemini/Vertex AI helpers for the data pipeline."""

from __future__ import annotations

import json
import logging
import mimetypes
import os
import re
import stat
import time
from pathlib import Path
from typing import Any

from .env_utils import float_env, int_env as _int_env_impl
from .gemini_prompts import IMAGE_PROMPT, VIDEO_PROMPT

TEMP_GEMINI_CREDENTIALS_PATH = Path("/tmp/gemini-service-account.json")
REQUIRED_GEMINI_SERVICE_ACCOUNT_FIELDS = (
    "type",
    "project_id",
    "private_key",
    "client_email",
    "token_uri",
)

logger = logging.getLogger(__name__)


def _collect_text_from_parts(parts: Any) -> list[str]:
    collected: list[str] = []
    for part in parts or []:
        text_value = None
        if isinstance(part, dict):
            text_value = part.get("text")
        else:
            try:
                text_value = getattr(part, "text", None)
            except Exception:
                text_value = None
        rendered = str(text_value or "").strip()
        if rendered:
            collected.append(rendered)
    return collected


def _extract_response_text(response: Any) -> str:
    try:
        text_value = getattr(response, "text", None)
    except Exception:
        text_value = None

    rendered = str(text_value or "").strip()
    if rendered:
        return rendered

    collected: list[str] = []
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = None
        if isinstance(candidate, dict):
            content = candidate.get("content")
        else:
            try:
                content = getattr(candidate, "content", None)
            except Exception:
                content = None

        if isinstance(content, dict):
            parts = content.get("parts")
        else:
            try:
                parts = getattr(content, "parts", None)
            except Exception:
                parts = None
        collected.extend(_collect_text_from_parts(parts))

    joined = "\n".join(part for part in collected if part).strip()
    if joined:
        return joined

    raise RuntimeError("Gemini response did not contain readable text parts")


def _validate_credentials_file(path_value: str | Path, source_name: str) -> Path:
    candidate = Path(path_value).expanduser()
    if not candidate.exists():
        raise FileNotFoundError(f"Gemini credentials invalid: {source_name} points to a missing file: {candidate}")
    if not candidate.is_file():
        raise FileNotFoundError(f"Gemini credentials invalid: {source_name} is not a file: {candidate}")
    return candidate


def _write_service_account_json(raw_json: str) -> Path:
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("Gemini credentials invalid: GEMINI_SERVICE_ACCOUNT_JSON is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("Gemini credentials invalid: GEMINI_SERVICE_ACCOUNT_JSON must decode to an object")

    missing = [
        field_name
        for field_name in REQUIRED_GEMINI_SERVICE_ACCOUNT_FIELDS
        if not str(payload.get(field_name) or "").strip()
    ]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Gemini credentials invalid: missing or empty required field(s): {joined}")

    if str(payload.get("type")).strip() != "service_account":
        raise ValueError("Gemini credentials invalid: type must be 'service_account'")

    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    temp_path = TEMP_GEMINI_CREDENTIALS_PATH
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    existing = None
    if temp_path.exists():
        try:
            existing = temp_path.read_text(encoding="utf-8")
        except OSError:
            existing = None

    if existing != rendered:
        temp_path.write_text(rendered, encoding="utf-8")

    os.chmod(temp_path, stat.S_IRUSR | stat.S_IWUSR)
    return temp_path


def _default_credentials_path() -> tuple[Path | None, list[str]]:
    tried: list[str] = []
    for env_name in (
        "GEMINI_GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        raw_value = (os.getenv(env_name) or "").strip()
        if not raw_value:
            tried.append(f"{env_name}: not set")
            continue
        try:
            resolved = _validate_credentials_file(raw_value, env_name)
        except FileNotFoundError as exc:
            # env는 설정됐지만 실제 파일이 없음 — 다음 fallback으로 진행
            logger.warning(
                "Gemini credentials: %s set but file invalid, trying next source (%s)",
                env_name,
                exc,
            )
            tried.append(f"{env_name}={raw_value}: {exc}")
            continue
        return resolved, tried

    raw_json = (os.getenv("GEMINI_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw_json:
        try:
            return _write_service_account_json(raw_json), tried
        except ValueError as exc:
            logger.warning(
                "Gemini credentials: GEMINI_SERVICE_ACCOUNT_JSON invalid, trying bundled (%s)",
                exc,
            )
            tried.append(f"GEMINI_SERVICE_ACCOUNT_JSON: {exc}")
    else:
        tried.append("GEMINI_SERVICE_ACCOUNT_JSON: not set")

    bundled = Path(__file__).resolve().parents[2] / "gemini" / "assets" / "your-gcp-project-credentials.json"
    if bundled.exists():
        return bundled, tried
    tried.append(f"bundled file: not found at {bundled}")
    return None, tried


def resolve_gemini_credentials_path(credentials_path: str | None = None) -> str:
    if credentials_path and str(credentials_path).strip():
        try:
            return str(_validate_credentials_file(credentials_path, "credentials_path"))
        except FileNotFoundError as exc:
            # 명시 인자는 엄격 유지(보안/과금 footgun 방지) — 다만 메시지를 풍부하게
            raise FileNotFoundError(
                f"{exc}. Explicit credentials_path arguments do not fall back to "
                "environment-based sources; fix the path or drop the argument to use "
                "GEMINI_GOOGLE_APPLICATION_CREDENTIALS / GEMINI_SERVICE_ACCOUNT_JSON."
            ) from exc

    credentials, tried = _default_credentials_path()
    if credentials is None:
        reasons = "\n  - ".join(tried) if tried else "(no sources configured)"
        raise FileNotFoundError(
            "Gemini credentials not found. All sources exhausted:\n  - "
            f"{reasons}\nConfigure one of: GEMINI_GOOGLE_APPLICATION_CREDENTIALS, "
            "GOOGLE_APPLICATION_CREDENTIALS, GEMINI_SERVICE_ACCOUNT_JSON."
        )
    return str(credentials)


def _load_vertex_ai() -> tuple[Any, Any, Any]:
    try:
        import vertexai
        from vertexai.preview.generative_models import GenerativeModel, Part
    except ImportError as exc:  # pragma: no cover - dependency presence varies by env
        raise RuntimeError(
            "google-cloud-aiplatform is required for Gemini integration. "
            "Install it from requirements before using vlm_pipeline.lib.gemini."
        ) from exc
    return vertexai, GenerativeModel, Part


def _load_generation_config_cls() -> Any:
    from vertexai.preview.generative_models import GenerationConfig

    return GenerationConfig


_int_env = _int_env_impl


def is_vertex_rate_limit_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "429" in message or "resource exhausted" in message


_is_vertex_rate_limit_error = is_vertex_rate_limit_error


def is_vertex_server_error(exc: BaseException) -> bool:
    """Phase 4-A (#10) — Vertex AI 일시적 서버측 오류 판별.

    503 ServiceUnavailable / 504 DeadlineExceeded 가 대표 케이스. SDK 버전에 따라
    ``google.api_core.exceptions`` 타입 또는 ``vertexai.generative_models`` 자체 예외로
    surfac 되므로 isinstance 단독으론 부족. 메시지 매칭 + (가능 시) isinstance OR
    조합으로 이중 안전망.
    """
    # 메시지 매칭 — vertexai SDK 가 grpc error 를 str() 변환 시 status code 를 포함한다.
    # gRPC 가 ``DEADLINE_EXCEEDED`` (underscore) 와 ``Deadline Exceeded`` (공백) 둘 다
    # 쓸 수 있으므로 두 표기 모두 매칭. ``UNAVAILABLE`` status code 표기 + gateway
    # timeout 도 포함 (codex review 반영, 2026-05-28).
    message = str(exc).lower()
    if (
        "503" in message
        or "504" in message
        or "500" in message
        or "service unavailable" in message
        or "unavailable" in message  # gRPC StatusCode.UNAVAILABLE str repr
        or "deadline exceeded" in message
        or "deadline_exceeded" in message  # gRPC underscore 표기
        or "internal error" in message  # google.api_core.InternalServerError
        or "gateway timeout" in message
    ):
        return True
    # isinstance 보강 (SDK 가 있는 환경 한정 — try/except import guard).
    try:
        from google.api_core import exceptions as _gax_exc  # type: ignore
    except Exception:  # noqa: BLE001
        return False
    return isinstance(
        exc,
        (
            _gax_exc.ServiceUnavailable,
            _gax_exc.DeadlineExceeded,
            _gax_exc.InternalServerError,
        ),
    )


_is_vertex_server_error = is_vertex_server_error


class GeminiAnalyzer:
    """Thin wrapper around Vertex AI Gemini for image/video analysis."""

    def __init__(
        self,
        *,
        model_name: str = "gemini-2.5-flash",
        project: str | None = None,
        location: str | None = None,
        credentials_path: str | None = None,
    ) -> None:
        vertexai, generative_model_cls, part_cls = _load_vertex_ai()

        project_value = (project or os.getenv("GEMINI_PROJECT") or "your-gcp-project").strip()
        location_value = (location or os.getenv("GEMINI_LOCATION") or "us-central1").strip()
        credentials_value = resolve_gemini_credentials_path(credentials_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_value

        vertexai.init(project=project_value, location=location_value)
        self.model = generative_model_cls(model_name=model_name)
        self.model_name = model_name
        self.project = project_value
        self.location = location_value
        self.credentials_path = credentials_value
        self._part_cls = part_cls
        self._rate_limit_max_retries = _int_env("GEMINI_RATE_LIMIT_MAX_RETRIES", 2, 0)
        self._rate_limit_base_delay_sec = float_env("GEMINI_RATE_LIMIT_BASE_DELAY_SEC", 2.0, 0.0)
        self._rate_limit_backoff = float_env("GEMINI_RATE_LIMIT_BACKOFF", 2.0, 1.0)
        self._rate_limit_max_delay_sec = float_env("GEMINI_RATE_LIMIT_MAX_DELAY_SEC", 15.0, 0.0)
        # Phase 4-A (#10) — 503/deadline 별도 backoff. 일반적으로 server error 는 rate-limit
        # 보다 회복 시간이 길어 base delay 가 더 크다 (10s default, max 60s).
        self._server_error_max_retries = _int_env("GEMINI_SERVER_ERROR_MAX_RETRIES", 3, 0)
        self._server_error_base_delay_sec = float_env("GEMINI_SERVER_ERROR_BASE_DELAY_SEC", 10.0, 0.0)
        self._server_error_backoff = float_env("GEMINI_SERVER_ERROR_BACKOFF", 2.0, 1.0)
        self._server_error_max_delay_sec = float_env("GEMINI_SERVER_ERROR_MAX_DELAY_SEC", 60.0, 0.0)
        # in-process usage metering — Vertex 청구 분석 + 회귀 탐지용.
        # response.usage_metadata 가 노출하는 prompt/candidates/total token 누적.
        self._call_count = 0
        self._total_prompt_tokens = 0
        self._total_response_tokens = 0
        self._total_tokens = 0

    def _generate_content_with_retry(
        self,
        parts: list[Any],
        *,
        content_type: str,
        source_name: str,
        generation_config: Any | None = None,
    ) -> Any:
        """Vertex Gemini generate_content + transient retry funnel.

        Phase 4-A (#10): 두 클래스의 transient 오류를 *별도* 예산으로 처리.
          - 429 / resource exhausted: ``_rate_limit_*`` budget
          - 503 / 504 / deadline exceeded / internal error: ``_server_error_*`` budget

        한 오류 클래스의 예산을 소진했다 해서 다른 클래스 예산이 소진되지 않도록
        클래스별 attempt counter 를 분리. 둘 다 OFF (max_retries=0) 면 1회 시도 후
        예외 전파 — 호환 보존.
        """
        rate_limit_attempts_left = self._rate_limit_max_retries
        server_error_attempts_left = self._server_error_max_retries
        rate_limit_attempt_index = 0
        server_error_attempt_index = 0
        # 무한루프 방지를 위한 상한 (각 클래스 + 1회 성공 시도).
        total_cap = self._rate_limit_max_retries + self._server_error_max_retries + 1

        for _attempt_no in range(1, total_cap + 1):
            try:
                if generation_config is not None:
                    response = self.model.generate_content(parts, generation_config=generation_config)
                else:
                    response = self.model.generate_content(parts)
            except Exception as exc:
                if _is_vertex_rate_limit_error(exc):
                    if rate_limit_attempts_left <= 0:
                        raise
                    rate_limit_attempts_left -= 1
                    rate_limit_attempt_index += 1
                    delay_sec = min(
                        self._rate_limit_max_delay_sec,
                        self._rate_limit_base_delay_sec * (self._rate_limit_backoff ** (rate_limit_attempt_index - 1)),
                    )
                    logger.warning(
                        "Gemini Vertex 429 retry: type=%s source=%s attempt=%d/%d delay=%.2fs err=%s",
                        content_type,
                        source_name,
                        rate_limit_attempt_index,
                        self._rate_limit_max_retries,
                        delay_sec,
                        exc,
                    )
                    if delay_sec > 0:
                        time.sleep(delay_sec)
                    continue
                if _is_vertex_server_error(exc):
                    if server_error_attempts_left <= 0:
                        raise
                    server_error_attempts_left -= 1
                    server_error_attempt_index += 1
                    delay_sec = min(
                        self._server_error_max_delay_sec,
                        self._server_error_base_delay_sec
                        * (self._server_error_backoff ** (server_error_attempt_index - 1)),
                    )
                    logger.warning(
                        "Gemini Vertex 503/deadline retry: type=%s source=%s attempt=%d/%d delay=%.2fs err=%s",
                        content_type,
                        source_name,
                        server_error_attempt_index,
                        self._server_error_max_retries,
                        delay_sec,
                        exc,
                    )
                    if delay_sec > 0:
                        time.sleep(delay_sec)
                    continue
                # 분류 외 예외 — 그대로 전파.
                raise

            # 성공 경로 — usage 누적 후 반환. usage_metadata 없거나 0 이면 silent.
            self._accumulate_usage(response)
            return response

        # 모든 retry 슬롯 소진된 비정상 경로 — 방어적 message.
        raise RuntimeError("gemini_retry_unexpected_state")

    def _accumulate_usage(self, response: Any) -> None:
        """response.usage_metadata 의 prompt/response/total token 을 인스턴스 counter 에 누적.

        Phase 4-A (#10): in-process counter. metric exporter (Prometheus 등) 도입 전 단계로
        디버깅 시 ``analyzer._call_count`` / ``analyzer._total_*_tokens`` 를 직접 inspect.
        usage_metadata 가 없거나 필드 일부가 None 이어도 silent — 모듈 hard fail 방지.
        """
        self._call_count += 1
        usage = getattr(response, "usage_metadata", None)
        if usage is None:
            return
        prompt = getattr(usage, "prompt_token_count", None) or 0
        candidates = getattr(usage, "candidates_token_count", None) or 0
        total = getattr(usage, "total_token_count", None) or 0
        try:
            self._total_prompt_tokens += int(prompt)
            self._total_response_tokens += int(candidates)
            self._total_tokens += int(total)
        except (TypeError, ValueError):
            # SDK 가 비정상 값 반환 시 counter 만 skip — 호출 자체엔 영향 0.
            pass

    def usage_summary(self) -> dict[str, int]:
        """현재까지의 누적 usage 스냅샷 (in-process). 모니터링/디버깅용 helper."""
        return {
            "call_count": self._call_count,
            "total_prompt_tokens": self._total_prompt_tokens,
            "total_response_tokens": self._total_response_tokens,
            "total_tokens": self._total_tokens,
        }

    def analyze_image(self, image_path: str, prompt: str | None = None) -> str:
        path = Path(image_path)
        image_bytes = path.read_bytes()
        image_part = self._part_cls.from_data(data=image_bytes, mime_type="image/jpeg")
        response = self._generate_content_with_retry(
            [image_part, prompt or IMAGE_PROMPT],
            content_type="image",
            source_name=path.name,
        )
        return _extract_response_text(response)

    def analyze_video(
        self,
        video_path: str,
        prompt: str | None = None,
        mime_type: str | None = None,
        *,
        response_mime_type: str | None = None,
        response_schema: Any | None = None,
    ) -> str:
        path = Path(video_path)
        video_bytes = path.read_bytes()
        resolved_mime_type = mime_type
        if resolved_mime_type is None:
            guessed_mime, _ = mimetypes.guess_type(str(path))
            resolved_mime_type = guessed_mime or "video/mp4"
        video_part = self._part_cls.from_data(data=video_bytes, mime_type=resolved_mime_type)

        generation_config = None
        if response_mime_type is not None or response_schema is not None:
            config_kwargs: dict[str, Any] = {}
            if response_mime_type is not None:
                config_kwargs["response_mime_type"] = response_mime_type
            if response_schema is not None:
                config_kwargs["response_schema"] = response_schema
            generation_config = _load_generation_config_cls()(**config_kwargs)

        response = self._generate_content_with_retry(
            [video_part, prompt or VIDEO_PROMPT],
            content_type="video",
            source_name=path.name,
            generation_config=generation_config,
        )
        return _extract_response_text(response)

    def change_model(self, model_name: str) -> None:
        _, generative_model_cls, _ = _load_vertex_ai()
        self.model = generative_model_cls(model_name=model_name)
        self.model_name = model_name


def extract_clean_json_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    cleaned = re.sub(r"^(?i:json)\s*\n", "", cleaned)
    start_candidates = [pos for pos in (cleaned.find("["), cleaned.find("{")) if pos != -1]
    if start_candidates:
        cleaned = cleaned[min(start_candidates) :]

    end = max(cleaned.rfind("]"), cleaned.rfind("}"))
    if end != -1:
        cleaned = cleaned[: end + 1]
    return cleaned.strip()


def _repair_clean_json_strings(text: str) -> str:
    """Best-effort repair of common Gemini JSON string glitches.

    Inside string values, replaces raw LF/CR/TAB with `\\n`/`\\r`/`\\t`
    and escapes an internal `"` whose next non-whitespace char is not a
    JSON structural delimiter (`,`, `:`, `]`, `}`, EOF).
    """
    out: list[str] = []
    in_string = False
    escape = False
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if escape:
            out.append(ch)
            escape = False
            i += 1
            continue
        if ch == "\\":
            out.append(ch)
            escape = True
            i += 1
            continue
        if in_string:
            if ch == '"':
                j = i + 1
                while j < n and text[j] in " \t\r\n":
                    j += 1
                next_ch = text[j] if j < n else ""
                if next_ch in (",", ":", "]", "}", ""):
                    in_string = False
                    out.append(ch)
                else:
                    out.append('\\"')
                i += 1
                continue
            if ch == "\n":
                out.append("\\n")
                i += 1
                continue
            if ch == "\r":
                out.append("\\r")
                i += 1
                continue
            if ch == "\t":
                out.append("\\t")
                i += 1
                continue
            out.append(ch)
            i += 1
        else:
            if ch == '"':
                in_string = True
            out.append(ch)
            i += 1
    return "".join(out)


def load_clean_json(text: str) -> Any:
    cleaned = extract_clean_json_text(text)
    decoder = json.JSONDecoder()
    try:
        return decoder.decode(cleaned)
    except json.JSONDecodeError as exc:
        if exc.msg == "Extra data":
            payload, _ = decoder.raw_decode(cleaned)
            return payload
        repaired = _repair_clean_json_strings(cleaned)
        if repaired != cleaned:
            try:
                payload = decoder.decode(repaired)
                logger.warning("load_clean_json: repaired malformed JSON (orig_err=%s)", exc)
                return payload
            except json.JSONDecodeError:
                try:
                    payload, _ = decoder.raw_decode(repaired)
                    logger.warning(
                        "load_clean_json: repaired malformed JSON via raw_decode (orig_err=%s)",
                        exc,
                    )
                    return payload
                except json.JSONDecodeError:
                    pass
        raise


from .gemini_script_utils import (  # noqa: F401, E402  # backward-compat re-exports
    DEFAULT_VIDEO_EXTENSIONS,
    json_save_path_same_dir_as_video,
    list_video_files,
    process_video_folder,
    save_response_as_json,
)
