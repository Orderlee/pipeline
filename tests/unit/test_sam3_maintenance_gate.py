"""SAM3 서비스 정비 게이트: maintenance 활성 시 503 + lazy-reload 거부."""
from __future__ import annotations

import importlib
import pathlib
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")

_SVC_DIR = str(pathlib.Path("docker/sam3").resolve())

_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01"
    b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    if _SVC_DIR not in sys.path:
        sys.path.insert(0, _SVC_DIR)
    sys.modules.pop("app", None)
    app_mod = importlib.import_module("app")

    state = {"load_calls": 0}

    def _fake_load():
        state["load_calls"] += 1
        app_mod._processor = object()
        app_mod._model = object()
        app_mod._model_loaded_at = 1.0
        app_mod._load_error = None

    monkeypatch.setattr(app_mod, "_load_model", _fake_load)
    monkeypatch.setattr(app_mod, "_run_segmentation", lambda *a, **k: ([], {}))
    monkeypatch.setattr(app_mod, "_reset_gpu_peak_memory", lambda: None)
    monkeypatch.setattr(app_mod, "_gpu_peak_memory_gb", lambda: None)

    with TestClient(app_mod.app) as c:
        c.__dict__["_test_mod"] = app_mod
        c.__dict__["_test_state"] = state
        yield c


def _segment(client):
    return client.post(
        "/segment",
        files={"file": ("i.png", _PNG, "image/png")},
        data={"prompts_json": '["fire"]'},
    )


def test_segment_503_under_maintenance(client):
    assert client.post("/maintenance/enter", data={"owner_run_id": "r1"}).status_code == 200
    r = _segment(client)
    assert r.status_code == 503
    assert r.json()["detail"] == "gpu_under_maintenance"


def test_warmup_503_under_maintenance(client):
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert client.post("/warmup").status_code == 503


def test_lazy_reload_refused_under_maintenance(client):
    client.post("/unload")
    calls_before = client.__dict__["_test_state"]["load_calls"]
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert _segment(client).status_code == 503
    assert client.__dict__["_test_state"]["load_calls"] == calls_before


def test_exit_restores_normal_operation(client):
    client.post("/maintenance/enter", data={"owner_run_id": "r1"})
    assert _segment(client).status_code == 503
    assert client.post("/maintenance/exit").status_code == 200
    assert client.post("/warmup").status_code == 200
    assert _segment(client).status_code == 200


def test_normal_operation_unaffected_when_clear(client):
    assert _segment(client).status_code == 200
