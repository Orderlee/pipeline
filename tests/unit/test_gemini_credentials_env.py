from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from vlm_pipeline.lib import gemini as gemini_lib


def _clear_gemini_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "GEMINI_GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GEMINI_SERVICE_ACCOUNT_JSON",
    ):
        monkeypatch.delenv(name, raising=False)


def _service_account_json(private_key: str = "test-private-key") -> str:
    payload = {
        "type": "service_account",
        "project_id": "gmail-361002",
        "private_key_id": "",
        "private_key": private_key,
        "client_email": "gemini-api@gmail-361002.iam.gserviceaccount.com",
        "client_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gemini-api%40gmail-361002.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    }
    return json.dumps(payload)


def test_resolve_credentials_path_from_json_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_gemini_env(monkeypatch)
    temp_credentials = tmp_path / "gemini-service-account.json"
    monkeypatch.setattr(gemini_lib, "TEMP_GEMINI_CREDENTIALS_PATH", temp_credentials)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", _service_account_json())

    resolved = gemini_lib.resolve_gemini_credentials_path()

    assert resolved == str(temp_credentials)
    assert temp_credentials.exists()
    assert json.loads(temp_credentials.read_text(encoding="utf-8"))["project_id"] == "gmail-361002"
    assert stat.S_IMODE(temp_credentials.stat().st_mode) == 0o600


def test_invalid_json_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_gemini_env(monkeypatch)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", "{not-json}")

    with pytest.raises(ValueError, match="not valid JSON"):
        gemini_lib.resolve_gemini_credentials_path()


def test_blank_private_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_gemini_env(monkeypatch)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", _service_account_json(private_key=""))

    with pytest.raises(ValueError, match="missing or empty required field"):
        gemini_lib.resolve_gemini_credentials_path()


def test_path_env_takes_precedence_over_json_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_gemini_env(monkeypatch)
    credentials_file = tmp_path / "existing-creds.json"
    credentials_file.write_text('{"type":"service_account"}', encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(credentials_file))
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", "{not-json}")

    resolved = gemini_lib.resolve_gemini_credentials_path()

    assert resolved == str(credentials_file)
