"""ls_webhook.py 보안 강화 단위 테스트 (SEC-WEBHOOK-AUTH codex follow-up)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_module_globals(tmp_path, monkeypatch):
    """ls_webhook 모듈 레벨 전역을 매 테스트마다 초기화."""
    monkeypatch.setenv("LS_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "slack-secret")
    monkeypatch.setenv("LS_STATE_FILE", str(tmp_path / "state.json"))
    import gemini.ls_webhook as lw

    lw.LS_WEBHOOK_SECRET = "test-secret"
    lw.SLACK_SIGNING_SECRET = "slack-secret"
    lw.STATE_FILE = tmp_path / "state.json"
    monkeypatch.setattr("gemini.ls_webhook_state.STATE_FILE", tmp_path / "state.json")


@pytest.fixture
def client():
    gemini_dir = str(Path(__file__).parents[2] / "src" / "gemini")
    if gemini_dir not in sys.path:
        sys.path.insert(0, gemini_dir)
    import gemini.ls_webhook as lw

    return TestClient(lw.app)


# ---------------------------------------------------------------------------
# Item 2: cmd_register idempotency
# ---------------------------------------------------------------------------


class TestCmdRegisterIdempotency:
    def _make_wh(self, wh_id: int, url: str, token: str | None) -> dict:
        return {
            "id": wh_id,
            "url": url,
            "headers": {"X-Webhook-Token": token} if token else {},
        }

    def test_same_url_same_secret_skips(self, monkeypatch):
        import gemini.ls_webhook as lw

        lw.LS_WEBHOOK_SECRET = "s3cr3t"
        lw.WEBHOOK_HOST = "myhost"
        lw.WEBHOOK_PORT = 9000

        existing = [self._make_wh(7, "http://myhost:9000/webhook", "s3cr3t")]
        mock_get = MagicMock()
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = existing

        mock_post = MagicMock()

        with patch("gemini.ls_webhook.requests.get", mock_get), patch(
            "gemini.ls_webhook.requests.post", mock_post
        ), patch("gemini.ls_webhook.requests.delete", MagicMock()):
            lw.cmd_register(project_id=42)

        mock_post.assert_not_called()

    def test_same_url_headerless_deletes_and_reposts(self, monkeypatch):
        import gemini.ls_webhook as lw

        lw.LS_WEBHOOK_SECRET = "s3cr3t"
        lw.WEBHOOK_HOST = "myhost"
        lw.WEBHOOK_PORT = 9000

        existing = [self._make_wh(8, "http://myhost:9000/webhook", None)]
        mock_get = MagicMock()
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = existing

        mock_delete = MagicMock()
        mock_delete.return_value.raise_for_status = MagicMock()

        mock_post_resp = MagicMock()
        mock_post_resp.raise_for_status = MagicMock()
        mock_post_resp.json.return_value = {"id": 99}
        mock_post = MagicMock(return_value=mock_post_resp)

        with patch("gemini.ls_webhook.requests.get", mock_get), patch(
            "gemini.ls_webhook.requests.post", mock_post
        ), patch("gemini.ls_webhook.requests.delete", mock_delete):
            lw.cmd_register(project_id=42)

        mock_delete.assert_called_once()
        mock_post.assert_called_once()
        posted_body = mock_post.call_args.kwargs.get("json") or mock_post.call_args[1].get("json")
        assert posted_body["headers"]["X-Webhook-Token"] == "s3cr3t"

    def test_no_existing_webhooks_posts(self, monkeypatch):
        import gemini.ls_webhook as lw

        lw.LS_WEBHOOK_SECRET = "s3cr3t"
        lw.WEBHOOK_HOST = "myhost"
        lw.WEBHOOK_PORT = 9000

        mock_get = MagicMock()
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = []

        mock_post_resp = MagicMock()
        mock_post_resp.raise_for_status = MagicMock()
        mock_post_resp.json.return_value = {"id": 10}
        mock_post = MagicMock(return_value=mock_post_resp)

        with patch("gemini.ls_webhook.requests.get", mock_get), patch(
            "gemini.ls_webhook.requests.post", mock_post
        ):
            lw.cmd_register(project_id=5)

        mock_post.assert_called_once()

    def test_missing_secret_raises_systemexit(self, monkeypatch):
        import gemini.ls_webhook as lw

        lw.LS_WEBHOOK_SECRET = ""
        with pytest.raises(SystemExit):
            lw.cmd_register(project_id=1)


# ---------------------------------------------------------------------------
# Item 4: query-param ?token= fallback 제거
# ---------------------------------------------------------------------------


class TestWebhookQueryParamRemoved:
    def test_query_param_token_returns_403(self, client):
        """?token= 으로 보내면 header 없으므로 403."""
        resp = client.post(
            "/webhook",
            params={"token": "test-secret"},
            content=json.dumps({"action": "ANNOTATION_CREATED", "project": {"id": 1, "title": "t"}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 403

    def test_header_token_returns_200(self, client):
        """X-Webhook-Token 헤더 사용 시 처리됨 (미완료 task → pending)."""
        with patch("gemini.ls_webhook.count_incomplete_tasks", return_value=1):
            resp = client.post(
                "/webhook",
                content=json.dumps(
                    {"action": "ANNOTATION_CREATED", "project": {"id": 1, "title": "t"}}
                ),
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-Token": "test-secret",
                },
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"


# ---------------------------------------------------------------------------
# Item 5: hmac.compare_digest TypeError → 403
# ---------------------------------------------------------------------------


class TestCompareDigestTypeError:
    def test_webhook_compare_digest_typeerror_returns_403(self, client):
        """compare_digest が TypeError を上げると 403 を返す (bytes-like 入力想定)."""
        with patch("gemini.ls_webhook.hmac.compare_digest", side_effect=TypeError("mismatch")):
            resp = client.post(
                "/webhook",
                content=json.dumps({"action": "ANNOTATION_CREATED", "project": {"id": 1, "title": "t"}}),
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-Token": "any-token",
                },
            )
        assert resp.status_code == 403

    def test_slack_compare_digest_typeerror_returns_403(self, client):
        """Slack sig compare_digest TypeError → 403."""
        import time

        ts = str(int(time.time()))
        body = "command=%2Fsync-list&text="

        import hashlib
        import hmac as _hmac
        import gemini.ls_webhook as lw

        sig = "v0=" + _hmac.new(lw.SLACK_SIGNING_SECRET.encode(), f"v0:{ts}:{body}".encode(), hashlib.sha256).hexdigest()

        with patch("gemini.ls_webhook.hmac.compare_digest", side_effect=TypeError("mismatch")):
            resp = client.post(
                "/slack/commands",
                content=body.encode(),
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-Slack-Request-Timestamp": ts,
                    "X-Slack-Signature": sig,
                },
            )
        assert resp.status_code == 403
