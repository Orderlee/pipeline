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

from .gemini_prompts import IMAGE_PROMPT, VIDEO_PROMPT

DEFAULT_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}
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
        raise FileNotFoundError(
            f"Gemini credentials invalid: {source_name} points to a missing file: {candidate}"
        )
    if not candidate.is_file():
        raise FileNotFoundError(
            f"Gemini credentials invalid: {source_name} is not a file: {candidate}"
        )
    return candidate


def _path_from_env(var_name: str) -> Path | None:
    raw_value = (os.getenv(var_name) or "").strip()
    if not raw_value:
        return None
    return _validate_credentials_file(raw_value, var_name)


def _write_service_account_json(raw_json: str) -> Path:
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Gemini credentials invalid: GEMINI_SERVICE_ACCOUNT_JSON is not valid JSON"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            "Gemini credentials invalid: GEMINI_SERVICE_ACCOUNT_JSON must decode to an object"
        )

    missing = [
        field_name
        for field_name in REQUIRED_GEMINI_SERVICE_ACCOUNT_FIELDS
        if not str(payload.get(field_name) or "").strip()
    ]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            f"Gemini credentials invalid: missing or empty required field(s): {joined}"
        )

    if str(payload.get("type")).strip() != "service_account":
        raise ValueError(
            "Gemini credentials invalid: type must be 'service_account'"
        )

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


def _default_credentials_path() -> Path | None:
    for env_name in (
        "GEMINI_GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        resolved = _path_from_env(env_name)
        if resolved is not None:
            return resolved

    raw_json = (os.getenv("GEMINI_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw_json:
        return _write_service_account_json(raw_json)

    bundled = (
        Path(__file__).resolve().parents[2]
        / "gemini"
        / "assets"
        / "gmail-361002-cbcf95afec4a.json"
    )
    if bundled.exists():
        return bundled
    return None


def resolve_gemini_credentials_path(credentials_path: str | None = None) -> str:
    if credentials_path and str(credentials_path).strip():
        return str(_validate_credentials_file(credentials_path, "credentials_path"))

    credentials = _default_credentials_path()
    if credentials is None:
        raise FileNotFoundError(
            "Gemini credentials not found. Set GEMINI_GOOGLE_APPLICATION_CREDENTIALS, "
            "GOOGLE_APPLICATION_CREDENTIALS, or GEMINI_SERVICE_ACCOUNT_JSON."
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


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    raw_value = os.getenv(name)
    try:
        parsed = int(raw_value) if raw_value is not None else int(default)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, parsed)


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw_value = os.getenv(name)
    try:
        parsed = float(raw_value) if raw_value is not None else float(default)
    except (TypeError, ValueError):
        parsed = float(default)
    return max(minimum, parsed)


def _is_vertex_rate_limit_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "429" in message or "resource exhausted" in message


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

        project_value = (project or os.getenv("GEMINI_PROJECT") or "gmail-361002").strip()
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
        self._rate_limit_base_delay_sec = _float_env("GEMINI_RATE_LIMIT_BASE_DELAY_SEC", 2.0, 0.0)
        self._rate_limit_backoff = _float_env("GEMINI_RATE_LIMIT_BACKOFF", 2.0, 1.0)
        self._rate_limit_max_delay_sec = _float_env("GEMINI_RATE_LIMIT_MAX_DELAY_SEC", 15.0, 0.0)

    def _generate_content_with_retry(
        self,
        parts: list[Any],
        *,
        content_type: str,
        source_name: str,
    ) -> Any:
        attempts = self._rate_limit_max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                return self.model.generate_content(parts)
            except Exception as exc:
                if not _is_vertex_rate_limit_error(exc) or attempt >= attempts:
                    raise

                delay_sec = min(
                    self._rate_limit_max_delay_sec,
                    self._rate_limit_base_delay_sec * (self._rate_limit_backoff ** (attempt - 1)),
                )
                logger.warning(
                    "Gemini Vertex 429 retry: type=%s source=%s attempt=%d/%d delay=%.2fs err=%s",
                    content_type,
                    source_name,
                    attempt,
                    attempts,
                    delay_sec,
                    exc,
                )
                if delay_sec > 0:
                    time.sleep(delay_sec)
        raise RuntimeError("gemini_retry_unexpected_state")

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
    ) -> str:
        path = Path(video_path)
        video_bytes = path.read_bytes()
        resolved_mime_type = mime_type
        if resolved_mime_type is None:
            guessed_mime, _ = mimetypes.guess_type(str(path))
            resolved_mime_type = guessed_mime or "video/mp4"
        video_part = self._part_cls.from_data(data=video_bytes, mime_type=resolved_mime_type)
        response = self._generate_content_with_retry(
            [video_part, prompt or VIDEO_PROMPT],
            content_type="video",
            source_name=path.name,
        )
        return _extract_response_text(response)

    def change_model(self, model_name: str) -> None:
        _, generative_model_cls, _ = _load_vertex_ai()
        self.model = generative_model_cls(model_name=model_name)
        self.model_name = model_name


def json_save_path_same_dir_as_video(video_path: str) -> str:
    abs_video = os.path.abspath(video_path)
    video_dir = os.path.dirname(abs_video)
    base_name_no_ext = os.path.splitext(os.path.basename(abs_video))[0]
    return os.path.join(video_dir, base_name_no_ext + ".json")


def extract_clean_json_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    cleaned = re.sub(r"^(?i:json)\s*\n", "", cleaned)
    start_candidates = [pos for pos in (cleaned.find("["), cleaned.find("{")) if pos != -1]
    if start_candidates:
        cleaned = cleaned[min(start_candidates):]

    end = max(cleaned.rfind("]"), cleaned.rfind("}"))
    if end != -1:
        cleaned = cleaned[: end + 1]
    return cleaned.strip()


def save_response_as_json(response_text: str, video_path: str) -> str:
    json_path = json_save_path_same_dir_as_video(video_path)
    cleaned = extract_clean_json_text(response_text)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        Path(json_path).write_text(cleaned, encoding="utf-8")
        return json_path

    Path(json_path).write_text(
        json.dumps(parsed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return json_path


def list_video_files(root_dir: str, recursive: bool = False) -> list[str]:
    root_path = Path(root_dir).resolve()
    if recursive:
        iterable = root_path.rglob("*")
    else:
        iterable = root_path.iterdir()

    collected: list[str] = []
    for path in iterable:
        if not path.is_file():
            continue
        if path.suffix.lower() not in DEFAULT_VIDEO_EXTENSIONS:
            continue
        collected.append(str(path))
    return sorted(collected)


def process_video_folder(
    folder: str,
    *,
    analyzer: GeminiAnalyzer,
    recursive: bool = False,
    prompt: str | None = None,
    mime_type: str | None = None,
) -> dict[str, Any]:
    videos = list_video_files(folder, recursive=recursive)
    summary: dict[str, Any] = {
        "folder": str(Path(folder).resolve()),
        "recursive": bool(recursive),
        "total": len(videos),
        "success": 0,
        "failed": 0,
        "outputs": [],
        "errors": [],
    }
    for video_path in videos:
        try:
            response_text = analyzer.analyze_video(video_path, prompt=prompt, mime_type=mime_type)
            json_path = save_response_as_json(response_text, video_path)
            summary["success"] += 1
            summary["outputs"].append({"video_path": video_path, "json_path": json_path})
        except Exception as exc:  # noqa: BLE001
            summary["failed"] += 1
            summary["errors"].append({"video_path": video_path, "error": str(exc)})
    return summary
