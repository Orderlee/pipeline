"""Gemini/Vertex AI credential resolution helpers (no vertexai dependency)."""

from __future__ import annotations

import json
import logging
import os
import stat
from pathlib import Path

logger = logging.getLogger(__name__)

TEMP_GEMINI_CREDENTIALS_PATH = Path("/tmp/gemini-service-account.json")
REQUIRED_GEMINI_SERVICE_ACCOUNT_FIELDS = (
    "type",
    "project_id",
    "private_key",
    "client_email",
    "token_uri",
)


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
