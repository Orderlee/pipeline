"""embedding 서비스 정비 게이트: maintenance 활성 시 503 + lazy-reload 거부."""

from __future__ import annotations

import importlib
import pathlib
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")

_SVC_DIR = str(pathlib.Path("docker/embedding").resolve())


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    monkeypatch.setenv("EMBEDDING_BACKEND", "open_clip")
    import backends.open_clip_be as be

    state = {"loaded": False, "load_calls": 0}

    def _load(self):
        state["load_calls"] += 1
        state["loaded"] = True

    monkeypatch.setattr(be.OpenClipBackend, "load", _load)
    monkeypatch.setattr(be.OpenClipBackend, "unload", lambda self: state.__setitem__("loaded", False))
    monkeypatch.setattr(be.OpenClipBackend, "is_loaded", lambda self: state["loaded"])
    monkeypatch.setattr(be.OpenClipBackend, "embed_image", lambda self, b: [0.1] * 1024)
    monkeypatch.setattr(be.OpenClipBackend, "embed_text", lambda self, t: [0.2] * 1024)

    sys.modules.pop("app", None)
    app_mod = importlib.import_module("app")
    with TestClient(app_mod.app) as c:
        c.__dict__["_test_state"] = state  # expose for assertions (avoid TestClient._state clash)
        yield c


def test_embed_503_under_maintenance(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "run-1"}).status_code == 200
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 503
    assert "maintenance" in r.json()["detail"]


def test_warmup_503_under_maintenance(client):
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    assert client.post("/warmup").status_code == 503


def test_lazy_reload_refused_under_maintenance(client):
    # unload, then enter maintenance, then /embed must NOT reload (load_calls frozen)
    client.post("/unload")
    calls_before = client.__dict__["_test_state"]["load_calls"]
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 503
    assert client.__dict__["_test_state"]["load_calls"] == calls_before  # no lazy reload happened


def test_exit_restores_normal_operation(client):
    client.post("/maintenance/enter", data={"owner_run_id": "run-1"})
    assert client.post("/embed_text", data={"text": "x"}).status_code == 503
    assert client.post("/maintenance/exit").status_code == 200
    r = client.post("/embed_text", data={"text": "x"})
    assert r.status_code == 200
    assert len(r.json()["vector"]) == 1024


def test_status_reflects_flag(client):
    assert client.get("/maintenance/status").json()["active"] is False
    client.post("/maintenance/enter", data={"owner_run_id": "run-7", "note": "ft"})
    s = client.get("/maintenance/status").json()
    assert s["active"] is True and s["owner_run_id"] == "run-7"


def test_normal_operation_unaffected_when_clear(client):
    r = client.post("/embed", files={"file": ("i.jpg", b"x", "application/octet-stream")})
    assert r.status_code == 200 and len(r.json()["vector"]) == 1024
