from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from vlm_pipeline.lib import gemini as gemini_lib
from vlm_pipeline.lib import gemini_credentials


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
        "project_id": "your-gcp-project",
        "private_key_id": "",
        "private_key": private_key,
        "client_email": "gemini-api@your-gcp-project.iam.gserviceaccount.com",
        "client_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gemini-api%40your-gcp-project.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    }
    return json.dumps(payload)


def test_resolve_credentials_path_from_json_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_gemini_env(monkeypatch)
    temp_credentials = tmp_path / "gemini-service-account.json"
    monkeypatch.setattr(gemini_credentials, "TEMP_GEMINI_CREDENTIALS_PATH", temp_credentials)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", _service_account_json())

    resolved = gemini_lib.resolve_gemini_credentials_path()

    assert resolved == str(temp_credentials)
    assert temp_credentials.exists()
    assert json.loads(temp_credentials.read_text(encoding="utf-8"))["project_id"] == "your-gcp-project"
    assert stat.S_IMODE(temp_credentials.stat().st_mode) == 0o600


def test_invalid_json_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # 깨진 JSON은 silently 성공하지 않아야 하고, 최종 에러에 원인이 포함돼야 함
    _clear_gemini_env(monkeypatch)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", "{not-json}")

    with pytest.raises(FileNotFoundError, match="not valid JSON"):
        gemini_lib.resolve_gemini_credentials_path()


def test_blank_private_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_gemini_env(monkeypatch)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", _service_account_json(private_key=""))

    with pytest.raises(FileNotFoundError, match="missing or empty required field"):
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


def test_missing_path_env_falls_back_to_json_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # env는 설정되어 있지만 실제 파일이 없을 때 JSON env fallback이 동작해야 함
    _clear_gemini_env(monkeypatch)
    missing_path = tmp_path / "does-not-exist.json"
    temp_credentials = tmp_path / "gemini-service-account.json"
    monkeypatch.setattr(gemini_credentials, "TEMP_GEMINI_CREDENTIALS_PATH", temp_credentials)
    monkeypatch.setenv("GEMINI_GOOGLE_APPLICATION_CREDENTIALS", str(missing_path))
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", _service_account_json())

    resolved = gemini_lib.resolve_gemini_credentials_path()

    assert resolved == str(temp_credentials)
    assert temp_credentials.exists()


def test_missing_path_env_falls_back_to_secondary_path_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # 첫 env가 깨졌어도 두 번째 path env가 살아있으면 성공해야 함
    _clear_gemini_env(monkeypatch)
    missing_path = tmp_path / "does-not-exist.json"
    existing_path = tmp_path / "existing-creds.json"
    existing_path.write_text('{"type":"service_account"}', encoding="utf-8")
    monkeypatch.setenv("GEMINI_GOOGLE_APPLICATION_CREDENTIALS", str(missing_path))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(existing_path))

    resolved = gemini_lib.resolve_gemini_credentials_path()

    assert resolved == str(existing_path)


def test_explicit_credentials_path_still_strict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # 명시적으로 전달된 경로는 fallback하지 않고 즉시 실패해야 함(보안/과금 footgun 방지)
    # 단, 에러 메시지는 fallback 옵션을 안내해야 함
    _clear_gemini_env(monkeypatch)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", _service_account_json())
    missing_path = tmp_path / "does-not-exist.json"

    with pytest.raises(FileNotFoundError) as excinfo:
        gemini_lib.resolve_gemini_credentials_path(str(missing_path))
    message = str(excinfo.value)
    assert "credentials_path" in message
    assert "do not fall back" in message
    assert "GEMINI_SERVICE_ACCOUNT_JSON" in message


def test_aggregated_error_lists_all_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # 모든 source 실패 시 최종 에러가 각 source의 상태를 포함해야 함
    _clear_gemini_env(monkeypatch)
    missing_path = tmp_path / "missing-one.json"
    monkeypatch.setenv("GEMINI_GOOGLE_APPLICATION_CREDENTIALS", str(missing_path))

    with pytest.raises(FileNotFoundError) as excinfo:
        gemini_lib.resolve_gemini_credentials_path()
    message = str(excinfo.value)
    assert "All sources exhausted" in message
    assert "GEMINI_GOOGLE_APPLICATION_CREDENTIALS" in message
    assert "GOOGLE_APPLICATION_CREDENTIALS: not set" in message
    assert "GEMINI_SERVICE_ACCOUNT_JSON: not set" in message
    assert "bundled file" in message


def test_broken_json_env_falls_through_when_bundled_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # JSON이 깨져도 번들까지 모두 없을 때만 최종 실패, 에러에 JSON 원인도 포함
    _clear_gemini_env(monkeypatch)
    monkeypatch.setenv("GEMINI_SERVICE_ACCOUNT_JSON", "{not-json}")

    with pytest.raises(FileNotFoundError) as excinfo:
        gemini_lib.resolve_gemini_credentials_path()
    message = str(excinfo.value)
    assert "not valid JSON" in message
    assert "bundled file" in message
