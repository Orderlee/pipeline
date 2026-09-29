"""Dispatch webhook server 단위 테스트."""

from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _reset_env(tmp_path, monkeypatch):
    """각 테스트마다 깨끗한 pending 디렉토리와 환경변수를 설정."""
    monkeypatch.setenv("INCOMING_DIR", str(tmp_path / "incoming"))
    monkeypatch.setenv("DISPATCH_WEBHOOK_SECRET", "")
    monkeypatch.setenv("DISPATCH_WEBHOOK_PORT", "8090")

    import vlm_pipeline.defs.dispatch.webhook_server as ws

    ws.INCOMING_DIR = str(tmp_path / "incoming")
    ws.DISPATCH_PENDING_DIR = Path(tmp_path / "incoming" / ".dispatch" / "pending")
    ws.DISPATCH_WEBHOOK_SECRET = ""


@pytest.fixture
def client():
    from vlm_pipeline.defs.dispatch.webhook_server import app

    return TestClient(app)


@pytest.fixture
def pending_dir(tmp_path):
    return tmp_path / "incoming" / ".dispatch" / "pending"


class TestReceiveDispatch:
    def test_valid_request(self, client, pending_dir) -> None:
        payload = {
            "request_id": "test-req-001",
            "folder_name": "test_folder",
            "outputs": ["bbox"],
        }
        resp = client.post("/webhook/dispatch", json=payload)

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "accepted"
        assert body["request_id"] == "test-req-001"

        saved = json.loads((pending_dir / "test-req-001.json").read_text())
        assert saved["folder_name"] == "test_folder"
        assert saved["transfer_tool"] == "webhook"
        assert "received_at" in saved

    def test_auto_generates_request_id(self, client, pending_dir) -> None:
        payload = {"folder_name": "auto_id_folder"}
        resp = client.post("/webhook/dispatch", json=payload)

        assert resp.status_code == 202
        body = resp.json()
        assert body["request_id"].startswith("webhook_")

        files = list(pending_dir.glob("*.json"))
        assert len(files) == 1

    def test_missing_folder_name(self, client) -> None:
        resp = client.post("/webhook/dispatch", json={"request_id": "no-folder"})
        assert resp.status_code == 400
        assert "folder_name" in resp.json()["error"]

    def test_invalid_json(self, client) -> None:
        resp = client.post(
            "/webhook/dispatch",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_json"

    def test_payload_not_object(self, client) -> None:
        resp = client.post("/webhook/dispatch", json=["array", "not", "object"])
        assert resp.status_code == 400
        assert resp.json()["error"] == "payload_must_be_object"

    def test_duplicate_request_id_gets_suffix(self, client, pending_dir) -> None:
        payload = {"request_id": "dup-001", "folder_name": "folder_a"}
        resp1 = client.post("/webhook/dispatch", json=payload)
        assert resp1.status_code == 202

        resp2 = client.post("/webhook/dispatch", json=payload)
        assert resp2.status_code == 202

        files = sorted(f.name for f in pending_dir.glob("*.json"))
        assert "dup-001.json" in files
        assert "dup-001__2.json" in files


class TestHmacSignature:
    def test_valid_signature(self, client, monkeypatch) -> None:
        import vlm_pipeline.defs.dispatch.webhook_server as ws

        secret = "test-secret-key"
        ws.DISPATCH_WEBHOOK_SECRET = secret

        payload = json.dumps({"folder_name": "hmac_test"}).encode()
        sig = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

        resp = client.post(
            "/webhook/dispatch",
            content=payload,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": sig,
            },
        )
        assert resp.status_code == 202

    def test_invalid_signature(self, client, monkeypatch) -> None:
        import vlm_pipeline.defs.dispatch.webhook_server as ws

        ws.DISPATCH_WEBHOOK_SECRET = "real-secret"

        resp = client.post(
            "/webhook/dispatch",
            content=json.dumps({"folder_name": "bad_sig"}).encode(),
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": "sha256=invalid",
            },
        )
        assert resp.status_code == 403
        assert resp.json()["error"] == "invalid_signature"

    def test_no_secret_skips_verification(self, client) -> None:
        import vlm_pipeline.defs.dispatch.webhook_server as ws

        ws.DISPATCH_WEBHOOK_SECRET = ""

        resp = client.post(
            "/webhook/dispatch",
            json={"folder_name": "no_secret"},
        )
        assert resp.status_code == 202


class TestHealth:
    def test_health_endpoint(self, client) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "pending_dir" in body
